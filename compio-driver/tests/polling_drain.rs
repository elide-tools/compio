#![cfg(all(unix, feature = "polling"))]

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    time::Duration,
};

use compio_buf::{BufResult, IntoInner};
use compio_driver::{
    AsRawFd, Proactor, PushEntry, SharedFd,
    op::{Recv, RecvFlags},
};

#[test]
fn bounded_receive_drains_ready_bytes_without_another_poll() {
    let mut driver = Proactor::builder().build().unwrap();
    if !driver.driver_type().is_polling() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = listener.accept().unwrap().0;
    socket.set_nonblocking(true).unwrap();
    let socket = SharedFd::new(socket);
    driver.attach(socket.as_raw_fd()).unwrap();
    let first = Recv::new(socket.clone(), vec![0u8; 2], RecvFlags::empty());
    let PushEntry::Pending(key) = driver.push(first) else {
        panic!("empty receive must wait")
    };
    peer.write_all(b"hello").unwrap();
    driver.poll(Some(Duration::from_secs(5))).unwrap();
    let PushEntry::Ready(BufResult(result, first)) = driver.pop(key) else {
        panic!("read must complete")
    };
    assert_eq!(result.unwrap(), 2);
    assert_eq!(first.into_inner(), b"he");
    for expected in [b"ll".as_slice(), b"o".as_slice()] {
        let next = Recv::new(socket.clone(), vec![0u8; 2], RecvFlags::empty());
        let PushEntry::Ready(BufResult(result, next)) = driver.push(next) else {
            panic!("ready bytes must not require another kernel poll")
        };
        assert_eq!(result.unwrap(), expected.len());
        assert_eq!(&next.into_inner()[..expected.len()], expected);
    }
    // The short inline read waits for fresh readiness. Even bytes arriving
    // before the next submission must not be lost when that conservative
    // hint skips an attempt.
    peer.write_all(b"ok").unwrap();
    let next = Recv::new(socket.clone(), vec![0u8; 2], RecvFlags::empty());
    let PushEntry::Pending(key) = driver.push(next) else {
        panic!("exhausted receive must wait")
    };
    driver.poll(Some(Duration::from_secs(5))).unwrap();
    let PushEntry::Ready(BufResult(result, next)) = driver.pop(key) else {
        panic!("new readiness was lost")
    };
    assert_eq!(result.unwrap(), 2);
    assert_eq!(next.into_inner(), b"ok");
}

#[test]
fn short_polled_receive_waits_for_fresh_readiness() {
    let mut driver = Proactor::builder().build().unwrap();
    if !driver.driver_type().is_polling() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = listener.accept().unwrap().0;
    socket.set_nonblocking(true).unwrap();
    let socket = SharedFd::new(socket);
    driver.attach(socket.as_raw_fd()).unwrap();
    let first = Recv::new(socket.clone(), vec![0u8; 16], RecvFlags::empty());
    let PushEntry::Pending(key) = driver.push(first) else {
        panic!("empty receive must wait")
    };
    peer.write_all(b"hello").unwrap();
    driver.poll(Some(Duration::from_secs(5))).unwrap();
    let PushEntry::Ready(BufResult(result, first)) = driver.pop(key) else {
        panic!("read must complete")
    };
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&first.into_inner()[..5], b"hello");
    peer.write_all(b"again").unwrap();
    let next = Recv::new(socket.clone(), vec![0u8; 16], RecvFlags::empty());
    let PushEntry::Pending(key) = driver.push(next) else {
        panic!("short read should wait for fresh readiness")
    };
    driver.poll(Some(Duration::from_secs(5))).unwrap();
    let PushEntry::Ready(BufResult(result, next)) = driver.pop(key) else {
        panic!("new readiness was lost")
    };
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&next.into_inner()[..5], b"again");
}
