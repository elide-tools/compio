#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::{
    collections::HashSet,
    os::fd::AsRawFd,
    time::{Duration, Instant},
};

use compio_driver::{
    Proactor, PushEntry,
    op::ReadAt,
    uring::{cqueue, opcode, types::Fd},
};

#[test]
fn bounded_lane_preserves_completions_and_generic_keys() {
    let mut p = Proactor::new().unwrap();
    p.owner_init(2).unwrap();
    let file = std::fs::File::open("Cargo.toml").unwrap();
    let generic = p.push(ReadAt::new(file, 0, vec![0u8; 8]));
    for token in 0..32 {
        unsafe { p.owner_push(opcode::Nop::new().build(), token) }.unwrap();
    }
    assert!(unsafe { p.owner_push(opcode::Nop::new().build(), u64::MAX >> 1) }.is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut seen = HashSet::new();
    let mut batch = Vec::with_capacity(2);
    while seen.len() < 32 {
        assert!(Instant::now() < deadline);
        p.owner_progress().unwrap();
        batch.clear();
        assert!(p.owner_drain(&mut batch, 100) <= 2);
        for c in &batch {
            assert_eq!(c.result, 0);
            assert!(seen.insert(c.token));
        }
    }
    if let PushEntry::Pending(key) = generic {
        assert!(matches!(p.pop(key), PushEntry::Ready(_)));
    }
}

#[test]
fn cancellation_requires_multishot_target_terminal() {
    let mut p = Proactor::new().unwrap();
    p.owner_init(4).unwrap();
    let (reader, _writer) = std::os::unix::net::UnixStream::pair().unwrap();
    unsafe {
        p.owner_push(
            opcode::PollAdd::new(Fd(reader.as_raw_fd()), libc::POLLIN as _)
                .multi(true)
                .build(),
            4,
        )
        .unwrap();
    }
    p.owner_progress().unwrap();
    unsafe {
        p.owner_push(opcode::AsyncCancel::new((4 << 1) | 1).build(), 5)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut target = false;
    let mut cancel = false;
    let mut batch = Vec::with_capacity(4);
    while !target || !cancel {
        assert!(Instant::now() < deadline);
        p.owner_progress().unwrap();
        batch.clear();
        p.owner_drain(&mut batch, 4);
        for c in &batch {
            match c.token {
                4 => {
                    assert!(!cqueue::more(c.flags));
                    assert_eq!(c.result, -libc::ECANCELED);
                    target = true;
                }
                5 => {
                    assert_eq!(c.result, 0);
                    cancel = true;
                }
                _ => panic!("unexpected token"),
            }
        }
    }
}

#[test]
fn cq_overflow_and_sq_pressure_are_lossless() {
    let mut p = Proactor::builder().capacity(8).cqsize(8).build().unwrap();
    p.owner_init(1).unwrap();
    let mut submitted = 0;
    // Deliberately fill the owned lane, CQ and SQ before draining anything.
    for _ in 0..16 {
        while submitted < 128 {
            match unsafe { p.owner_push(opcode::Nop::new().build(), submitted) } {
                Ok(()) => submitted += 1,
                Err(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
                    break;
                }
            }
        }
        p.owner_progress().unwrap();
    }
    assert!(submitted > 8);
    let mut seen = HashSet::new();
    let mut batch = Vec::with_capacity(1);
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.len() < submitted as usize {
        assert!(Instant::now() < deadline);
        p.owner_progress().unwrap();
        batch.clear();
        assert!(p.owner_drain(&mut batch, 128) <= 1);
        for c in &batch {
            assert_eq!(c.result, 0);
            assert!(seen.insert(c.token));
        }
    }
}

#[test]
fn pool_is_retained_when_owner_pressure_hides_generic_terminal() {
    use std::{
        mem::MaybeUninit,
        ptr::NonNull,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use compio_driver::{BoxAllocator, BufferAllocator, SharedFd, op::RecvMulti};
    static FREED: AtomicUsize = AtomicUsize::new(0);
    struct Counted;
    impl BufferAllocator for Counted {
        fn allocate(len: u32) -> NonNull<MaybeUninit<u8>> {
            BoxAllocator::allocate(len)
        }

        unsafe fn deallocate(ptr: NonNull<MaybeUninit<u8>>, len: u32) {
            FREED.fetch_add(1, Ordering::SeqCst);
            unsafe { BoxAllocator::deallocate(ptr, len) };
        }
    }
    let mut p = Proactor::builder()
        .buffer_pool_allocator::<Counted>()
        .buffer_pool_buffer_len(64)
        .build()
        .unwrap();
    p.owner_init(1).unwrap();
    let pool = p.buffer_pool().unwrap();
    let (reader, _writer) = std::os::unix::net::UnixStream::pair().unwrap();
    unsafe { p.owner_push(opcode::Nop::new().build(), 1) }.unwrap();
    let key = p.push(
        RecvMulti::new(
            SharedFd::new(reader),
            &pool,
            64,
            rustix::net::RecvFlags::empty(),
        )
        .unwrap(),
    );
    p.owner_progress().unwrap();
    drop(p);
    drop(key);
    assert_eq!(FREED.load(Ordering::SeqCst), 0);
}

#[test]
fn generic_cancel_survives_full_owner_sq() {
    use compio_driver::{SharedFd, op::Recv};
    let mut p = Proactor::builder().capacity(8).build().unwrap();
    p.owner_init(16).unwrap();
    let _ = p.poll(Some(Duration::ZERO));
    let (reader, _writer) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = SharedFd::new(reader);
    let raw_fd = reader.as_raw_fd();
    let PushEntry::Pending(mut key) = p.push(Recv::new(
        reader.clone(),
        vec![0; 8],
        rustix::net::RecvFlags::empty(),
    )) else {
        panic!("receive must pend")
    };
    let mut submitted = 0;
    loop {
        match unsafe {
            p.owner_push(
                opcode::PollAdd::new(Fd(raw_fd), libc::POLLIN as _).build(),
                submitted,
            )
        } {
            Ok(()) => submitted += 1,
            Err(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
                break;
            }
        }
    }
    assert_eq!(submitted, 7);
    assert!(p.cancel(key.clone()).is_none());
    // Pending cancellation must prevent sleeping behind an SQ of idle
    // operations.
    p.poll(Some(Duration::from_millis(20))).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut batch = Vec::with_capacity(16);
    loop {
        assert!(Instant::now() < deadline);
        p.owner_progress().unwrap();
        batch.clear();
        p.owner_drain(&mut batch, 16);
        match p.pop(key) {
            PushEntry::Pending(k) => key = k,
            PushEntry::Ready(result) => {
                assert_eq!(result.0.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
                break;
            }
        }
    }
    for token in 0..submitted {
        unsafe {
            p.owner_push(
                opcode::AsyncCancel::new((token << 1) | 1).build(),
                100 + token,
            )
        }
        .unwrap();
    }
    let mut terminals = 0;
    while terminals < submitted {
        assert!(Instant::now() < deadline);
        p.owner_progress().unwrap();
        batch.clear();
        p.owner_drain(&mut batch, 16);
        for c in &batch {
            if c.token < submitted {
                assert_eq!(c.result, -libc::ECANCELED);
                terminals += 1;
            }
        }
    }
}

#[test]
fn regular_poll_drives_owner_deferred_work_without_submissions() {
    use std::io::Write;
    let mut p = Proactor::builder()
        .single_issuer(true)
        .defer_taskrun(true)
        .taskrun_flag(true)
        .build()
        .unwrap();
    p.owner_init(4).unwrap();
    let (reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
    unsafe {
        p.owner_push(
            opcode::PollAdd::new(Fd(reader.as_raw_fd()), libc::POLLIN as _).build(),
            9,
        )
    }
    .unwrap();
    let _ = p.poll(Some(Duration::ZERO));
    writer.write_all(b"ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut batch = Vec::with_capacity(4);
    loop {
        assert!(Instant::now() < deadline);
        match p.poll(Some(Duration::ZERO)) {
            Ok(()) => {}
            Err(e) => assert!(matches!(
                e.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted
            )),
        }
        if p.owner_drain(&mut batch, 4) != 0 {
            break;
        }
    }
    assert_eq!(batch[0].token, 9);
    assert_ne!(batch[0].result & libc::POLLIN as i32, 0);
}
