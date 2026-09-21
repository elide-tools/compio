use std::{
    collections::{HashSet, VecDeque},
    marker::PhantomData,
    mem::ManuallyDrop,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use crate::sys::{extra::IourExtra, prelude::*};

mod_use![op, notify];

cfg_select! {
    feature = "io-uring-cqe32" => {
        use io_uring::cqueue::Entry32 as CEntry;
    }
    _ => {
        use io_uring::cqueue::Entry as CEntry;
    }
}

cfg_select! {
    feature = "io-uring-sqe128" => {
        use io_uring::squeue::Entry128 as SEntry;
    }
    _ => {
        use io_uring::squeue::Entry as SEntry;
    }
}

use flume::{Receiver, Sender};
use io_uring::{
    EnterFlags, IoUring,
    cqueue::more,
    opcode::{AsyncCancel, PollAdd},
    types::{Fd, SubmitArgs, Timespec},
};

use crate::{
    AsyncifyPool, DriverType, Entry, ProactorBuilder,
    key::{BorrowedKey, ErasedKey},
    panic::catch_unwind_io,
};

bitflags::bitflags! {
    /// Mutable driver state tracked as a small bit set.
    #[derive(Clone, Copy)]
    struct DriverFlags: u8 {
        /// The multishot notifier `PollAdd` is no longer armed and must be
        /// re-pushed before the next wait.
        const NEED_PUSH_NOTIFIER = 1 << 0;
        /// Set `IORING_ENTER_NO_IOWAIT` on blocking waits so an idle ring is not
        /// charged as iowait. Enabled only when SQPOLL is unused and the kernel
        /// reports `IORING_FEAT_NO_IOWAIT` (since 6.15).
        ///
        /// See io_uring_enter(2):
        /// <https://man7.org/linux/man-pages/man2/io_uring_enter.2.html>
        const NO_IOWAIT = 1 << 1;
    }
}

/// Low-level driver of io-uring.
pub(crate) struct Driver {
    // Ring close is not proof that asynchronous kernel teardown has finished.
    inner: ManuallyDrop<IoUring<SEntry, CEntry>>,
    notifier: Notifier,
    pool: AsyncifyPool,
    completed_tx: Sender<Entry>,
    completed_rx: Receiver<Entry>,
    flags: DriverFlags,
    /// Keys leaked via `into_raw()` into io_uring user_data; terminal CQEs
    /// release them.
    in_flight: HashSet<usize>,
    pending_cancel: HashSet<usize>,
    owner_completed: VecDeque<crate::OwnerCompletion>,
    owner_capacity: usize,
    _p: PhantomData<ErasedKey>,
}

impl Driver {
    const CANCEL: u64 = u64::MAX;
    const NOTIFY: u64 = u64::MAX - 1;

    pub fn new(builder: &ProactorBuilder) -> io::Result<Self> {
        instrument!(compio_log::Level::TRACE, "new", ?builder);
        trace!("new iour driver");
        // if op_flags is empty, this loop will not run
        for code in builder.op_flags.get_codes() {
            if !is_op_supported(code) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("io-uring does not support opcode {code:?}({code})"),
                ));
            }
        }
        let notifier = Notifier::new()?;
        let mut io_uring_builder = IoUring::builder();
        if let Some(sqpoll_idle) = builder.sqpoll_idle {
            io_uring_builder.setup_sqpoll(sqpoll_idle.as_millis() as _);
            if let Some(cpu) = builder.sqpoll_cpu {
                io_uring_builder.setup_sqpoll_cpu(cpu);
            }
        }
        if builder.single_issuer {
            io_uring_builder.setup_single_issuer();
            if builder.defer_taskrun {
                io_uring_builder.setup_defer_taskrun();
            }
        }
        if builder.coop_taskrun {
            io_uring_builder.setup_coop_taskrun();
        }
        if builder.taskrun_flag {
            io_uring_builder.setup_taskrun_flag();
        }
        if let Some(cqsize) = builder.cqsize {
            io_uring_builder.setup_cqsize(cqsize);
        }
        io_uring_builder.dontfork();

        let inner = io_uring_builder.build(builder.capacity)?;

        let submitter = inner.submitter();

        if let Some(fd) = builder.eventfd {
            submitter.register_eventfd(fd)?;
        }

        let (completed_tx, completed_rx) = flume::unbounded();

        // NO_IOWAIT needs kernel 6.15+ (the IORING_FEAT_NO_IOWAIT feature bit)
        // and is meaningless under SQPOLL: the polling thread, not an `enter`
        // wait, drives submission, so there is no CQE wait to mark.
        let mut flags = DriverFlags::NEED_PUSH_NOTIFIER;
        flags.set(
            DriverFlags::NO_IOWAIT,
            builder.sqpoll_idle.is_none() && inner.params().is_feature_no_iowait(),
        );

        Ok(Self {
            inner: ManuallyDrop::new(inner),
            notifier,
            completed_tx,
            completed_rx,
            pool: builder.create_or_get_thread_pool(),
            flags,
            in_flight: HashSet::new(),
            pending_cancel: HashSet::new(),
            owner_completed: VecDeque::new(),
            owner_capacity: 0,
            _p: PhantomData,
        })
    }

    pub fn driver_type(&self) -> DriverType {
        DriverType::IoUring
    }

    #[allow(dead_code)]
    pub fn as_iour(&self) -> Option<&Self> {
        Some(self)
    }

    #[allow(dead_code)]
    pub fn as_iour_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }

    pub fn owner_init(&mut self, capacity: usize) -> io::Result<()> {
        if !self.inner.params().is_feature_nodrop() || self.inner.params().is_setup_sqpoll() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owner lane requires IORING_FEAT_NODROP without SQPOLL",
            ));
        }
        if capacity == 0 || self.owner_capacity != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid or already initialized owner capacity",
            ));
        }
        self.owner_completed = VecDeque::with_capacity(capacity);
        self.owner_capacity = capacity;
        Ok(())
    }

    pub unsafe fn owner_push(
        &mut self,
        entry: io_uring::squeue::Entry,
        token: u64,
    ) -> io::Result<()> {
        if self.owner_capacity == 0 || token >= (u64::MAX >> 1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owner lane uninitialized or token reserved",
            ));
        }
        self.push_pending_cancels();
        #[allow(clippy::useless_conversion)]
        let entry: SEntry = entry.user_data((token << 1) | 1).into();
        let mut sq = self.inner.submission();
        unsafe { sq.push(&entry) }.map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        sq.sync();
        Ok(())
    }

    pub fn owner_drain(&mut self, out: &mut Vec<crate::OwnerCompletion>, limit: usize) -> usize {
        let n = limit.min(self.owner_completed.len());
        out.extend(self.owner_completed.drain(..n));
        n
    }

    pub fn owner_progress(&mut self) -> io::Result<()> {
        self.poll_entries();
        self.push_pending_cancels();
        // GETEVENTS is required even without submissions to flush NODROP
        // overflow and drive deferred task work. Never wait for an
        // event here.
        let n = self.inner.submission().len() as u32;
        let result = unsafe {
            self.inner
                .submitter()
                .enter::<libc::sigset_t>(n, 0, EnterFlags::GETEVENTS.bits(), None)
        };
        self.poll_entries();
        self.check_owner_completions()?;
        match result {
            Ok(_) => Ok(()),
            // CQ pressure and interrupted progress are retryable. The owner
            // drains its bounded lane before the next progress attempt.
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::EBUSY | libc::EAGAIN | libc::EINTR)
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn check_owner_completions(&mut self) -> io::Result<()> {
        if self.owner_capacity != 0 && self.inner.completion().overflow() != 0 {
            return Err(io::Error::other(
                "io-uring lost CQEs; retain all in-flight owner resources",
            ));
        }
        Ok(())
    }

    pub fn owner_register_files_sparse(&self, n: u32) -> io::Result<()> {
        self.inner.submitter().register_files_sparse(n)
    }

    pub fn owner_update_files(&self, offset: u32, fds: &[RawFd]) -> io::Result<usize> {
        self.inner.submitter().register_files_update(offset, fds)
    }

    pub unsafe fn owner_register_buf_ring(
        &self,
        addr: u64,
        entries: u16,
        group: u16,
    ) -> io::Result<()> {
        unsafe {
            self.inner
                .submitter()
                .register_buf_ring_with_flags(addr, entries, group, 0)
        }
    }

    pub fn owner_unregister_buf_ring(&self, group: u16) -> io::Result<()> {
        self.inner.submitter().unregister_buf_ring(group)
    }

    pub fn register_files(&self, fds: &[RawFd]) -> io::Result<()> {
        self.inner.submitter().register_files(fds)?;
        Ok(())
    }

    pub fn unregister_files(&self) -> io::Result<()> {
        self.inner.submitter().unregister_files()?;
        Ok(())
    }

    pub fn register_personality(&self) -> io::Result<u16> {
        self.inner.submitter().register_personality()
    }

    pub fn unregister_personality(&self, personality: u16) -> io::Result<()> {
        self.inner.submitter().unregister_personality(personality)
    }

    pub(in crate::sys) fn inner(&mut self) -> &mut IoUring<SEntry, CEntry> {
        &mut self.inner
    }

    // Auto means that it choose to wait or not automatically.
    fn submit_auto(&mut self, timeout: Option<Duration>, need_wait: bool) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "submit_auto", ?timeout);
        self.push_pending_cancels();

        // when taskrun is true, there are completed cqes wait to handle, no
        // need to block the submit
        let want_sqe =
            if !need_wait || !self.pending_cancel.is_empty() || self.inner.submission().taskrun() {
                0
            } else {
                1
            };

        // Only a wait that can actually sleep is charged as iowait; a zero
        // timeout (the drain calls from `push_raw`/`flush`) returns
        // immediately, so it keeps the plain combined path.
        //
        // On the sleeping path, opt out of iowait accounting (see
        // `DriverFlags::NO_IOWAIT`) by carrying NO_IOWAIT on the same
        // submit-and-wait `enter`.
        let can_block = want_sqe > 0 && timeout != Some(Duration::ZERO);
        let no_iowait = self.flags.contains(DriverFlags::NO_IOWAIT) && can_block;
        let res = if self.owner_capacity != 0 || no_iowait {
            self.submit_and_wait_explicit(want_sqe, timeout, no_iowait)
        } else {
            self.submit_and_wait(want_sqe, timeout)
        };
        trace!("submit result: {res:?}");
        match res {
            Ok(_) => {
                if want_sqe > 0 && self.inner.completion().is_empty() {
                    Err(io::ErrorKind::TimedOut.into())
                } else {
                    Ok(())
                }
            }
            Err(e) => match e.raw_os_error() {
                Some(libc::ETIME) => Err(io::ErrorKind::TimedOut.into()),
                Some(libc::EBUSY) | Some(libc::EAGAIN) => Err(io::ErrorKind::Interrupted.into()),
                _ => Err(e),
            },
        }
    }

    /// The combined submit+wait. Used for zero-timeout drains and when
    /// `NO_IOWAIT` is unavailable.
    fn submit_and_wait(&self, want_sqe: usize, timeout: Option<Duration>) -> io::Result<usize> {
        if let Some(duration) = timeout {
            let timespec = timespec(duration);
            let args = SubmitArgs::new().timespec(&timespec);
            self.inner.submitter().submit_with_args(want_sqe, &args)
        } else {
            self.inner.submit_and_wait(want_sqe)
        }
    }

    /// Drive GETEVENTS on every owner turn, including zero-timeout/empty-SQ
    /// turns, without a second enter syscall. Sleeping waits can opt out of
    /// iowait.
    fn submit_and_wait_explicit(
        &mut self,
        want_sqe: usize,
        timeout: Option<Duration>,
        no_iowait: bool,
    ) -> io::Result<usize> {
        // Publish the SQ tail and read how many staged SQEs to submit this
        // call.
        let to_submit = self.inner.submission().len() as u32;
        let submitter = self.inner.submitter();
        let mut flags = EnterFlags::GETEVENTS;
        flags.set(EnterFlags::NO_IOWAIT, no_iowait);
        if let Some(duration) = timeout {
            let timespec = timespec(duration);
            let args = SubmitArgs::new().timespec(&timespec);
            flags.insert(EnterFlags::EXT_ARG);
            // SAFETY: `args` outlives the call; the SQ is synced and holds
            // `to_submit` valid SQEs.
            unsafe { submitter.enter(to_submit, want_sqe as u32, flags.bits(), Some(&args)) }
        } else {
            // SAFETY: the SQ is synced and holds `to_submit` valid SQEs; no arg
            // payload is referenced.
            unsafe {
                submitter.enter::<libc::sigset_t>(to_submit, want_sqe as u32, flags.bits(), None)
            }
        }
    }

    fn poll_blocking(&mut self) -> bool {
        let mut has_entry = false;
        while let Ok(entry) = self.completed_rx.try_recv() {
            entry.notify();
            has_entry = true;
        }
        has_entry
    }

    fn poll_entries(&mut self) -> bool {
        let mut cqueue = self.inner.completion();
        let has_entry = !cqueue.is_empty();
        while self.owner_capacity == 0 || self.owner_completed.len() < self.owner_capacity {
            let Some(entry) = cqueue.next() else { break };
            match entry.user_data() {
                Self::CANCEL => {}
                Self::NOTIFY => {
                    let flags = entry.flags();
                    if !more(flags) {
                        self.flags.insert(DriverFlags::NEED_PUSH_NOTIFIER);
                    }
                    if let Err(e) = self.notifier.clear() {
                        error!("failed to clear notifier: {e:?}");
                    }
                }
                token if token & 1 != 0 => {
                    self.owner_completed.push_back(crate::OwnerCompletion {
                        token: token >> 1,
                        result: entry.result(),
                        flags: entry.flags(),
                    });
                }
                key => {
                    let flags = entry.flags();
                    if more(flags) {
                        let key = unsafe { BorrowedKey::from_raw(key as _) };
                        let mut key = key.borrow();
                        let mut extra: crate::sys::Extra = IourExtra::new().into();
                        extra.set_flags(entry.flags());
                        unsafe {
                            key.carrier
                                .push_multishot(create_result(entry.result()), extra);
                        }
                        key.wake_by_ref();
                    } else {
                        self.in_flight.remove(&(key as usize));
                        self.pending_cancel.remove(&(key as usize));
                        create_entry(entry).notify()
                    }
                }
            }
        }
        has_entry
    }

    pub(in crate::sys) fn default_extra(&self) -> IourExtra {
        IourExtra::new()
    }

    pub fn attach(&mut self, _fd: RawFd) -> io::Result<()> {
        Ok(())
    }

    pub fn cancel(&mut self, key: ErasedKey) {
        instrument!(compio_log::Level::TRACE, "cancel", ?key);
        trace!("cancel RawOp");
        let raw = key.as_raw();
        if self.in_flight.contains(&raw) {
            // Deduplicated and bounded by the generic operations already alive.
            self.pending_cancel.insert(raw);
            self.push_pending_cancels();
        }
    }

    fn push_pending_cancels(&mut self) {
        while let Some(&raw) = self.pending_cancel.iter().next() {
            #[allow(clippy::useless_conversion)]
            let entry = AsyncCancel::new(raw as u64)
                .build()
                .user_data(Self::CANCEL)
                .into();
            if unsafe { self.inner.submission().push(&entry) }.is_err() {
                break;
            }
            self.pending_cancel.remove(&raw);
        }
    }

    fn push_raw_with_key(&mut self, entry: SEntry, key: ErasedKey) -> io::Result<()> {
        let user_data = key.as_raw();
        assert_eq!(user_data & 1, 0, "operation keys must be aligned");
        let entry = entry.user_data(user_data as _);
        self.push_raw(entry)?; // if push failed, do not leak the key. Drop it upon return.
        self.in_flight.insert(user_data);
        key.into_raw();
        Ok(())
    }

    fn push_raw(&mut self, entry: SEntry) -> io::Result<()> {
        loop {
            self.push_pending_cancels();
            let mut squeue = self.inner.submission();
            match unsafe { squeue.push(&entry) } {
                Ok(()) => {
                    squeue.sync();
                    break Ok(());
                }
                Err(_) => {
                    drop(squeue);
                    match self.submit_auto(Some(Duration::ZERO), true) {
                        Ok(()) => {}
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
                            ) => {}
                        Err(e) => return Err(e),
                    }
                    // If the CQEs are consumed here, we should make the driver
                    // aware of it. We should not mask
                    // `awake` here, otherwise the driver may wait for the next
                    // event indefinitely.
                    //
                    // Anyway it is not a hot path, so we can afford an extra
                    // `write` syscall here.
                    self.poll_entries();
                    if self.owner_capacity != 0
                        && self.owner_completed.len() == self.owner_capacity
                        && self.inner.submission().is_full()
                    {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                }
            }
        }
    }

    pub fn push(&mut self, key: ErasedKey) -> Poll<io::Result<usize>> {
        instrument!(compio_log::Level::TRACE, "push", ?key);
        let mut op_entry = key.borrow().create_entry::<false>();
        let mut has_fallbacked = false;
        loop {
            trace!(fallback = has_fallbacked, "push entry");
            match op_entry {
                OpEntry::Submission(entry) => {
                    if is_op_supported(entry.get_opcode() as _) {
                        #[allow(clippy::useless_conversion)]
                        self.push_raw_with_key(entry.into(), key)?;
                    } else if !has_fallbacked {
                        op_entry = key.borrow().create_entry::<true>();
                        has_fallbacked = true;
                        continue;
                    } else {
                        self.push_blocking(key);
                    }
                }
                #[cfg(feature = "io-uring-sqe128")]
                OpEntry::Submission128(entry) => {
                    if is_op_supported(entry.get_opcode() as _) {
                        self.push_raw_with_key(entry, key)?;
                    } else if !has_fallbacked {
                        op_entry = key.borrow().create_entry::<true>();
                        has_fallbacked = true;
                        continue;
                    } else {
                        self.push_blocking(key);
                    }
                }
                OpEntry::Blocking => self.push_blocking(key),
            }
            break;
        }
        Poll::Pending
    }

    fn push_blocking(&mut self, key: ErasedKey) {
        let waker = self.waker();
        let completed = self.completed_tx.clone();
        // SAFETY: we're submitting into the driver, so it's safe to freeze
        // here.
        let mut key = unsafe { key.freeze() };
        let mut closure = move || {
            let res = catch_unwind_io(AssertUnwindSafe(|| key.as_mut().carrier.call_blocking()));
            let _ = completed.send(Entry::new(key.into_inner(), res));
            waker.wake();
        };
        while let Err(e) = self.pool.dispatch(closure) {
            closure = e.0;
            std::thread::yield_now();
        }
    }

    pub fn flush(&mut self) -> bool {
        let succeed = self.submit_auto(Some(Duration::ZERO), false).is_ok();
        // If submission failed, return true to let the driver wake up
        // immediately.
        !succeed | self.notifier.reset()
    }

    pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        instrument!(compio_log::Level::TRACE, "poll", ?timeout);

        self.check_owner_completions()?;
        let blocking = self.poll_blocking();
        if blocking && self.owner_capacity == 0 {
            return Ok(());
        }

        trace!("start polling");

        let need_wait = !blocking && !self.notifier.reset() && self.owner_completed.is_empty();

        if self.flags.contains(DriverFlags::NEED_PUSH_NOTIFIER) {
            #[allow(clippy::useless_conversion)]
            self.push_raw(
                PollAdd::new(Fd(self.notifier.as_raw_fd()), libc::POLLIN as _)
                    .multi(true)
                    .build()
                    .user_data(Self::NOTIFY)
                    .into(),
            )?;
            self.flags.remove(DriverFlags::NEED_PUSH_NOTIFIER);
        }

        self.submit_auto(timeout, need_wait)?;

        self.notifier.set_awake();
        self.poll_entries();
        self.notifier.set_awake();

        self.check_owner_completions()
    }

    pub fn waker(&self) -> Waker {
        self.notifier.waker()
    }

    /// Prove generic operations are terminal before releasing their pool.
    pub(crate) fn quiesce(&mut self) -> bool {
        // Dispatch MORE through the carrier so accepted descriptors and
        // selected buffers keep their normal ownership. Only terminal
        // CQEs consume keys.
        self.poll_entries();
        // Submit staged operations before cancelling: sync cancellation does
        // not cover SQEs the kernel has not consumed. A zero timeout bounds
        // shutdown; only observed terminal CQEs authorize releasing storage.
        let _ = self.inner.submit();
        let _ = self
            .inner
            .submitter()
            .register_sync_cancel(Some(Timespec::new()), io_uring::types::CancelBuilder::any());
        self.poll_entries();
        self.in_flight.is_empty() && self.inner.completion().overflow() == 0
    }

    pub fn pop_multishot(
        &mut self,
        key: &ErasedKey,
    ) -> Option<BufResult<usize, crate::sys::Extra>> {
        key.borrow().carrier.pop_multishot()
    }
}

impl AsRawFd for Driver {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.quiesce();
        unsafe { ManuallyDrop::drop(&mut self.inner) };
        // Kernel ring teardown can run asynchronously. Keys without an observed
        // terminal CQE deliberately remain leaked, including on cancellation
        // failure or owner-lane backpressure. Never infer safety from close().
        // Raw owners must drain their terminal CQEs before freeing resources.
    }
}

fn create_entry(cq_entry: CEntry) -> Entry {
    let result = cq_entry.result();
    let result = create_result(result);
    let key = unsafe { ErasedKey::from_raw(cq_entry.user_data() as _) };
    let mut entry = Entry::new(key, result);
    entry.set_flags(cq_entry.flags());

    entry
}

fn create_result(result: i32) -> io::Result<usize> {
    if result < 0 {
        // ENOBUFS indicates the io_uring buffer pool has no available buffer.
        if -result == libc::ENOBUFS {
            Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "buffer ring has no available buffer",
            ))
        } else {
            Err(io::Error::from_raw_os_error(-result))
        }
    } else {
        Ok(result as _)
    }
}

fn timespec(duration: std::time::Duration) -> Timespec {
    Timespec::new()
        .sec(duration.as_secs())
        .nsec(duration.subsec_nanos())
}
