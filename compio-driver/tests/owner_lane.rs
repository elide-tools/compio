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
