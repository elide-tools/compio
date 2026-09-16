use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use flume::{Receiver, Sender};
use polling::{Event, Events, PollMode, Poller};

mod op;
pub use op::*;

use crate::{
    AsyncifyPool, Entry,
    panic::catch_unwind_io,
    sys::{driver::AwakeFlag, extra::PollExtra, prelude::*},
};

/// Registration mode used for every source.
///
/// Level triggering keeps a source reporting readiness for as long as an
/// operation is queued for it, so a source is registered once and only changed
/// when the set of queued interests changes. Edge triggering would require
/// every opcode to attempt its syscall before waiting, which is not part of the
/// [`OpCode`] contract.
const MODE: PollMode = PollMode::Level;

/// Interests currently queued for, or registered on, one source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Mask {
    readable: bool,
    writable: bool,
}

impl Mask {
    fn is_empty(self) -> bool {
        !self.readable && !self.writable
    }

    fn get(self, interest: Interest) -> bool {
        match interest {
            Interest::Readable => self.readable,
            Interest::Writable => self.writable,
        }
    }

    fn set(&mut self, interest: Interest, value: bool) {
        match interest {
            Interest::Readable => self.readable = value,
            Interest::Writable => self.writable = value,
        }
    }

    fn event(self, fd: RawFd) -> Event {
        let mut event = Event::none(fd as usize);
        event.readable = self.readable;
        event.writable = self.writable;
        event
    }
}

#[derive(Debug, Default)]
struct FdQueue {
    read_queue: VecDeque<ErasedKey>,
    write_queue: VecDeque<ErasedKey>,
    /// Mask currently registered with the poller, or `None` when the source is
    /// not registered.
    armed: Option<Mask>,
    /// Whether this fd is already listed in [`Driver::dirty`].
    listed: bool,
    /// Interests the source is believed not to be ready for, because an operation saw `EAGAIN` or
    /// because the last readiness for that interest was consumed.
    ///
    /// Level triggering makes a wrong belief self-correcting: a source that is in fact ready
    /// reports itself again as soon as it is registered, so the worst case is one delayed wakeup,
    /// never a lost one.
    stale: Mask,
}

impl FdQueue {
    fn is_empty(&self) -> bool {
        self.read_queue.is_empty() && self.write_queue.is_empty()
    }

    pub fn push_back_interest(&mut self, key: ErasedKey, interest: Interest) {
        match interest {
            Interest::Readable => self.read_queue.push_back(key),
            Interest::Writable => self.write_queue.push_back(key),
        }
    }

    pub fn push_front_interest(&mut self, key: ErasedKey, interest: Interest) {
        match interest {
            Interest::Readable => self.read_queue.push_front(key),
            Interest::Writable => self.write_queue.push_front(key),
        }
    }

    pub fn remove(&mut self, key: &ErasedKey) {
        self.read_queue.retain(|k| k != key);
        self.write_queue.retain(|k| k != key);
    }

    /// Interests the queued operations currently need.
    fn desired(&self) -> Mask {
        Mask {
            readable: !self.read_queue.is_empty(),
            writable: !self.write_queue.is_empty(),
        }
    }

    pub fn pop_interest(&mut self, event: &Event) -> Option<(ErasedKey, Interest)> {
        if event.readable
            && let Some(key) = self.read_queue.pop_front()
        {
            return Some((key, Interest::Readable));
        }
        if event.writable
            && let Some(key) = self.write_queue.pop_front()
        {
            return Some((key, Interest::Writable));
        }
        None
    }
}

/// Low-level driver of polling.
pub(crate) struct Driver {
    events: Events,
    notify: Arc<Notify>,
    registry: HashMap<RawFd, FdQueue>,
    /// Sources whose desired mask may differ from the registered one.
    dirty: Vec<RawFd>,
    pool: AsyncifyPool,
    completed_tx: Sender<Entry>,
    completed_rx: Receiver<Entry>,
}

impl Driver {
    pub fn new(builder: &ProactorBuilder) -> io::Result<Self> {
        instrument!(compio_log::Level::TRACE, "new", ?builder);
        trace!("new poll driver");

        let events = if let Some(cap) = NonZeroUsize::new(builder.capacity as _) {
            Events::with_capacity(cap)
        } else {
            Events::new()
        };
        let poll = Poller::new()?;
        if !poll.supports_level() {
            // Event ports (illumos, Solaris) are one-shot only; the registration
            // model below has no one-shot path.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the poll driver needs level-triggered readiness",
            ));
        }
        let notify = Arc::new(Notify::new(poll));
        let (completed_tx, completed_rx) = flume::unbounded();

        Ok(Self {
            events,
            notify,
            registry: HashMap::new(),
            dirty: Vec::new(),
            pool: builder.create_or_get_thread_pool(),
            completed_tx,
            completed_rx,
        })
    }

    pub fn driver_type(&self) -> DriverType {
        DriverType::Poll
    }

    pub(in crate::sys) fn default_extra(&self) -> PollExtra {
        PollExtra::new()
    }

    fn poller(&self) -> &Poller {
        &self.notify.poll
    }

    fn with_events<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Self, &mut Events) -> R,
    {
        let mut events = std::mem::take(&mut self.events);
        let res = f(self, &mut events);
        // Clear the notification state to avoid empty loops.
        self.notify.set_awake();
        self.events = events;
        res
    }

    fn try_get_queue(&mut self, fd: RawFd) -> Option<&mut FdQueue> {
        self.registry.get_mut(&fd)
    }

    /// Record that `fd`'s desired mask may have changed. The poller is only
    /// touched by [`Driver::flush`], right before waiting.
    fn mark_dirty(registry: &mut HashMap<RawFd, FdQueue>, dirty: &mut Vec<RawFd>, fd: RawFd) {
        if let Some(queue) = registry.get_mut(&fd)
            && !queue.listed
        {
            queue.listed = true;
            dirty.push(fd);
        }
    }

    /// Register or re-register `fd`. Falls back to a modification when the
    /// source is still known to the poller, which happens when a previous
    /// deletion could not be delivered (a closed fd drops out of the kernel's
    /// interest list on its own).
    fn register(&self, fd: RawFd, mask: Mask) -> io::Result<()> {
        let event = mask.event(fd);
        // SAFETY: the source is deleted before the driver is dropped.
        match unsafe { self.poller().add_with_mode(fd, event, MODE) } {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                // SAFETY: the caller keeps the descriptor open for the queued operations.
                let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
                self.poller().modify_with_mode(borrowed, event, MODE)
            }
            res => res,
        }
    }

    /// Apply every pending registration change. Sources whose desired mask is
    /// unchanged since the last flush cost no syscall, so an operation that is
    /// re-submitted before the driver waits again never re-registers.
    ///
    /// Returns whether any queued operation was completed with an error, which
    /// the caller must treat as a reason not to sleep.
    fn flush_registrations(&mut self) -> bool {
        let mut failed = false;
        while let Some(fd) = self.dirty.pop() {
            let Some(queue) = self.registry.get_mut(&fd) else {
                continue;
            };
            queue.listed = false;
            let desired = queue.desired();
            let armed = queue.armed;
            if desired.is_empty() {
                if armed.is_some() {
                    self.forget(fd);
                }
                self.registry.remove(&fd);
                continue;
            }
            if armed == Some(desired) {
                continue;
            }
            let res = if armed.is_some() {
                // SAFETY: the queued operations hold the descriptor open.
                let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
                self.poller()
                    .modify_with_mode(borrowed, desired.event(fd), MODE)
            } else {
                self.register(fd, desired)
            };
            match res {
                Ok(()) => {
                    if let Some(queue) = self.registry.get_mut(&fd) {
                        queue.armed = Some(desired);
                    }
                }
                Err(e) => {
                    self.fail_queue(fd, &e);
                    failed = true;
                }
            }
        }
        failed
    }

    /// Drop an armed registration whose owner may already have closed the
    /// descriptor. Closing removed it from the kernel's interest list, so the
    /// removal error carries no information; the number is never borrowed.
    fn forget(&self, fd: RawFd) {
        // SAFETY: the source was added to this poller, and `attach` forgets a
        // reused number before its new owner is registered.
        unsafe { self.poller().delete_raw(fd) }.ok();
    }

    /// Complete every operation queued on `fd` with `error` and forget the
    /// source. Used when a registration change cannot be applied.
    fn fail_queue(&mut self, fd: RawFd, error: &io::Error) {
        let Some(mut queue) = self.registry.remove(&fd) else {
            return;
        };
        if queue.armed.is_some() {
            self.forget(fd);
        }
        // One operation may sit in both queues, and in other descriptors'
        // queues (`Splice` waits on two); complete it once and drop it
        // everywhere first, as `cancel` does, so no later readiness or
        // cancellation touches a completed key.
        let mut keys: Vec<ErasedKey> = Vec::new();
        for key in queue
            .read_queue
            .drain(..)
            .chain(queue.write_queue.drain(..))
        {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        for key in keys {
            let op_type = key.borrow().carrier.op_type();
            if let Some(OpType::Fd(fds)) = op_type {
                for other in fds {
                    if other != fd {
                        self.remove_one(&key, other);
                    }
                }
            }
            let res = Err(match error.raw_os_error() {
                Some(code) => io::Error::from_raw_os_error(code),
                None => io::Error::from(error.kind()),
            });
            Entry::new(key, res).notify();
        }
    }

    /// Submit a new operation to the end of the queue.
    ///
    ///  # Safety
    /// The input fd should be valid.
    unsafe fn submit(&mut self, key: ErasedKey, arg: WaitArg) {
        let Self {
            registry, dirty, ..
        } = self;
        let queue = registry.entry(arg.fd).or_default();
        queue.push_back_interest(key, arg.interest);
        Self::mark_dirty(registry, dirty, arg.fd);
    }

    /// Submit a new operation to the front of the queue.
    ///
    /// # Safety
    /// The input fd should be valid.
    unsafe fn submit_front(&mut self, key: ErasedKey, arg: WaitArg) {
        let Self {
            registry, dirty, ..
        } = self;
        let queue = registry.entry(arg.fd).or_default();
        queue.push_front_interest(key, arg.interest);
        Self::mark_dirty(registry, dirty, arg.fd);
    }

    /// Remove one interest from the queue.
    fn remove_one(&mut self, key: &ErasedKey, fd: RawFd) {
        let Self {
            registry, dirty, ..
        } = self;
        let Some(queue) = registry.get_mut(&fd) else {
            return;
        };
        queue.remove(key);
        Self::mark_dirty(registry, dirty, fd);
    }

    /// Remove one interest from the queue, and emit a cancelled entry.
    fn cancel_one(&mut self, key: ErasedKey, fd: RawFd) -> Entry {
        self.remove_one(&key, fd);
        Entry::new_cancelled(key)
    }

    /// Forget any registration state left over from a previous owner of this
    /// descriptor number.
    ///
    /// # Errors
    /// Never fails; the signature matches the other drivers.
    pub fn attach(&mut self, fd: RawFd) -> io::Result<()> {
        if let Some(queue) = self.registry.remove(&fd) {
            // A descriptor number can only be reattached after the previous
            // descriptor was closed. Pending operations cannot outlive that
            // close, and the kernel drops a closed fd from its interest list,
            // but a user-space table (the `poll(2)` backend) does not, so the
            // stale registration is removed by number rather than left behind.
            debug_assert!(queue.is_empty(), "attaching an fd with queued operations");
            if queue.armed.is_some() {
                self.forget(fd);
            }
            self.dirty.retain(|listed| *listed != fd);
        }
        Ok(())
    }

    pub fn cancel(&mut self, key: ErasedKey) {
        let op_type = key.borrow().carrier.op_type();
        match op_type {
            None => {}
            Some(OpType::Fd(fds)) => {
                let mut pushed = false;
                for fd in fds {
                    let entry = self.cancel_one(key.clone(), fd);
                    if !pushed {
                        _ = self.completed_tx.send(entry);
                        pushed = true;
                    }
                }
            }
            #[cfg(aio)]
            Some(OpType::Aio(aiocbp)) => {
                let aiocb = unsafe { aiocbp.as_ref() };
                let fd = aiocb.aio_fildes;
                syscall!(libc::aio_cancel(fd, aiocbp.as_ptr())).ok();
            }
        }
    }

    pub fn push(&mut self, key: ErasedKey) -> Poll<io::Result<usize>> {
        instrument!(compio_log::Level::TRACE, "push", ?key);
        // Skip the attempt when the source is known not to be ready for this operation's interest:
        // it would only spend a syscall to be told so.
        let hint = { key.borrow().carrier.interest_hint() };
        if let Some(arg) = hint
            && self
                .registry
                .get(&arg.fd)
                .is_some_and(|queue| queue.stale.get(arg.interest))
        {
            key.borrow()
                .extra_mut()
                .as_poll_mut()
                .set_args(Multi::from_buf([arg]));
            // SAFETY: fd is from the OpCode.
            unsafe { self.submit(key, arg) };
            return Poll::Pending;
        }
        match { key.borrow().carrier.pre_submit()? } {
            Decision::Wait(args) => {
                key.borrow()
                    .extra_mut()
                    .as_poll_mut()
                    .set_args(args.clone());
                for arg in args.iter().copied() {
                    // SAFETY: fd is from the OpCode.
                    unsafe { self.submit(key.clone(), arg) };
                    if let Some(queue) = self.registry.get_mut(&arg.fd) {
                        queue.stale.set(arg.interest, true);
                    }
                    trace!("register {:?}", arg);
                }
                Poll::Pending
            }
            Decision::Completed(res) => Poll::Ready(Ok(res)),
            Decision::Blocking => {
                self.push_blocking(key);
                Poll::Pending
            }
            #[cfg(aio)]
            Decision::Aio(AioArg { mut aiocbp, submit }) => {
                let aiocb = unsafe { aiocbp.as_mut() };
                let user_data = key.as_raw();
                #[cfg(freebsd)]
                {
                    // sigev_notify_kqueue
                    aiocb.aio_sigevent.sigev_signo = self.as_raw_fd();
                    aiocb.aio_sigevent.sigev_notify = libc::SIGEV_KEVENT;
                    aiocb.aio_sigevent.sigev_value.sival_ptr = user_data as _;
                }
                #[cfg(solarish)]
                let mut notify = libc::port_notify {
                    portnfy_port: self.as_raw_fd(),
                    portnfy_user: user_data as _,
                };
                #[cfg(solarish)]
                {
                    aiocb.aio_sigevent.sigev_notify = libc::SIGEV_PORT;
                    aiocb.aio_sigevent.sigev_value.sival_ptr = &mut notify as *mut _ as _;
                }
                match syscall!(submit(aiocbp.as_ptr())) {
                    Ok(_) => {
                        // Key is successfully submitted, leak it on this side.
                        key.into_raw();
                        Poll::Pending
                    }
                    // FreeBSD:
                    //   * EOPNOTSUPP: It's on a filesystem without AIO support. Just fallback to
                    //     blocking IO.
                    //   * EAGAIN: The process-wide queue is full. No safe way to remove the (maybe)
                    //     dead entries.
                    // Solarish:
                    //   * EAGAIN: Allocation failed.
                    Err(e)
                        if matches!(
                            e.raw_os_error(),
                            Some(libc::EOPNOTSUPP) | Some(libc::EAGAIN)
                        ) =>
                    {
                        self.push_blocking(key);
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
        }
    }

    fn push_blocking(&mut self, key: ErasedKey) {
        let waker = self.waker();
        let completed = self.completed_tx.clone();
        // SAFETY: we're submitting into the driver, so it's safe to freeze here.
        let mut key = unsafe { key.freeze() };

        let mut closure = move || {
            let operate = || match key.as_mut().carrier.operate() {
                Poll::Pending => unreachable!("this operation is not non-blocking"),
                Poll::Ready(res) => res,
            };
            let res = catch_unwind_io(AssertUnwindSafe(operate));
            let _ = completed.send(Entry::new(key.into_inner(), res));
            waker.wake();
        };

        while let Err(e) = self.pool.dispatch(closure) {
            closure = e.0;
            std::thread::yield_now();
        }
    }

    /// Prepare for an external wait on the driver's fd: apply pending
    /// registrations, then reset the notifier. Returns whether the caller
    /// should poll right away instead of waiting, because a notification was
    /// pending or a registration failure already completed queued operations.
    pub fn flush(&mut self) -> bool {
        let failed = self.flush_registrations();
        self.notify.reset() || failed
    }

    fn poll_completed(&mut self) -> bool {
        let mut ret = false;
        while let Ok(entry) = self.completed_rx.try_recv() {
            entry.notify();
            ret = true;
        }
        ret
    }

    #[allow(clippy::blocks_in_conditions)]
    fn poll_one(&mut self, event: Event, fd: RawFd) {
        let Some(queue) = self.try_get_queue(fd) else {
            // The source was dropped earlier in this batch; level triggering
            // will report it again if an operation is queued later.
            return;
        };

        // The report is current readiness, so it supersedes anything believed about the source.
        if event.readable {
            queue.stale.readable = false;
        }
        if event.writable {
            queue.stale.writable = false;
        }

        let Some((key, interest)) = queue.pop_interest(&event) else {
            return;
        };
        Self::mark_dirty(&mut self.registry, &mut self.dirty, fd);

        let ready = {
            let mut op = key.borrow();
            op.extra_mut().as_poll_mut().handle_event(fd)
        };
        if !ready {
            return;
        }

        let mut op = key.borrow();
        match { op.carrier.operate() } {
            // Submit all fd's back to the front of the queue
            Poll::Pending => {
                let extra = op.extra_mut().as_poll_mut();
                extra.reset();
                let args = extra.track.iter().map(|t| t.arg).collect::<Vec<_>>();
                drop(op);
                for arg in args {
                    // SAFETY: fd is from the OpCode.
                    unsafe { self.submit_front(key.clone(), arg) };
                }
            }
            Poll::Ready(res) => {
                drop(op);
                // The operation consumed this readiness. Assume it is spent; level triggering
                // reports the source again on the next wait if it is not.
                if let Some(queue) = self.registry.get_mut(&fd) {
                    queue.stale.set(interest, true);
                }
                Entry::new(key, res).notify()
            }
        }
    }

    pub fn poll(&mut self, mut timeout: Option<Duration>) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "poll", ?timeout);
        let timeout_is_some = timeout.is_some();
        // Apply registration changes accumulated since the last wait. A failed
        // registration completes its operations here, so it must not sleep.
        let failed = self.flush_registrations();
        let has_completed = !self.completed_rx.is_empty();
        let need_wait = !self.notify.reset();
        if !need_wait || has_completed || failed {
            timeout = Some(Duration::ZERO);
        }
        // We need to poll the poller first to make sure it handles the internal notify
        // event (if any).
        self.events.clear();
        self.notify.poll.wait(&mut self.events, timeout)?;
        self.notify.set_awake();
        if self.events.is_empty() {
            if self.poll_completed() {
                return Ok(());
            }
            if timeout_is_some {
                return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
            }
        } else if has_completed {
            self.poll_completed();
        }
        self.with_events(|this, events| {
            for event in events.iter() {
                trace!("receive {} for {:?}", event.key, event);
                let fd = event.key as RawFd;
                this.poll_one(event, fd);
            }

            Ok(())
        })
    }

    pub fn waker(&self) -> Waker {
        Waker::from(self.notify.clone())
    }

    pub fn pop_multishot(&mut self, _: &ErasedKey) -> Option<BufResult<usize, crate::sys::Extra>> {
        None
    }
}

impl AsRawFd for Driver {
    fn as_raw_fd(&self) -> RawFd {
        self.poller().as_raw_fd()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        for (fd, queue) in &self.registry {
            if queue.armed.is_some() {
                self.forget(*fd);
            }
        }
    }
}

impl Entry {
    pub(crate) fn new_cancelled(key: ErasedKey) -> Self {
        Entry::new(key, Err(io::Error::from_raw_os_error(libc::ECANCELED)))
    }
}

/// A notify handle to the inner driver.
pub(crate) struct Notify {
    poll: Poller,
    awake: AwakeFlag,
}

impl Notify {
    fn new(poll: Poller) -> Self {
        Self {
            poll,
            awake: AwakeFlag::new(),
        }
    }

    fn set_awake(&self) {
        self.awake.set();
    }

    fn reset(&self) -> bool {
        self.awake.reset()
    }
}

impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.awake.wake() {
            self.poll.notify().ok();
        }
    }
}
