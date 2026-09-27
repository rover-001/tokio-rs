use tokio_rs::{AsyncTcpListener, AsyncTcpStream, Executor};
use std::net::SocketAddr;
use std::sync::Arc;
use mio::Token;

fn main() {
    let mut executor = Executor::new();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let reactor = executor.reactor();
    let mut listener = AsyncTcpListener::bind(addr, Arc::clone(&reactor), Token(0)).unwrap();
    let server_addr = listener.local_addr().unwrap();

    println!("Echo server listening on {}", server_addr);

    let reactor2 = Arc::clone(&reactor);
    executor.spawn(async move {
        loop {
            let (tcp_stream, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            println!("Client connected from {}", peer_addr);

            let mut stream = AsyncTcpStream::new(tcp_stream, Arc::clone(&reactor2), Token(1)).unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n == 0 => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                let mut offset = 0;
                while offset < n {
                    match stream.write(&buf[offset..]).await {
                        Ok(0) => break,
                        Ok(w) => offset += w,
                        Err(_) => break,
                    }
                }
            }
            println!("Client {} disconnected", peer_addr);
        }
    });

    executor.spawn(async move {
        let mut stream = AsyncTcpStream::connect(server_addr, Arc::clone(&reactor), Token(2)).unwrap();
        stream.write(b"hello").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        println!("Client received: {}", String::from_utf8_lossy(&buf[..n]));
    });

    executor.run();
}
