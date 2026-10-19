use super::net::Listener;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpStream};

async fn round_trip(addr: SocketAddr) {
    let listener = match Listener::bind(addr) {
        Ok(listener) => listener,
        Err(e) if addr.is_ipv6() && e.kind() == ErrorKind::AddrNotAvailable => return,
        Err(e) => panic!("bind {addr}: {e}"),
    };
    let bound = listener.local_addr().unwrap();
    assert_eq!(bound.ip(), addr.ip());
    assert_ne!(bound.port(), 0);
    let client = TcpStream::connect(bound).unwrap();
    let accepted = listener.accept().await.unwrap();
    assert_eq!(accepted.peer, client.local_addr().unwrap());
}

#[tokio::test]
async fn test_addresses_cross_the_c_unchanged() {
    round_trip("127.0.0.1:0".parse().unwrap()).await;
    round_trip("[::1]:0".parse().unwrap()).await;
}
