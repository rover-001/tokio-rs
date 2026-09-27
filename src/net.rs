use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Token};
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub struct AsyncTcpStream {
    stream: TcpStream,
    token: Token,
    reactor: Arc<Mutex<crate::reactor::Reactor>>,
}

impl AsyncTcpStream {
    pub fn new(
        mut stream: TcpStream,
        reactor: Arc<Mutex<crate::reactor::Reactor>>,
        token: Token,
    ) -> io::Result<Self> {
        reactor
            .lock()
            .unwrap()
            .register(&mut stream, token)?;

        Ok(Self {
            stream,
            token,
            reactor,
        })
    }

    pub fn try_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }

    pub fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture { stream: self, buf }
    }

    pub fn write<'a>(&'a mut self, buf: &'a [u8]) -> WriteFuture<'a> {
        WriteFuture { stream: self, buf }
    }

    pub fn connect(
        addr: SocketAddr,
        reactor: Arc<Mutex<crate::reactor::Reactor>>,
        token: Token,
    ) -> io::Result<Self> {
        let std_stream = std::net::TcpStream::connect(addr)?;
        std_stream.set_nonblocking(true)?;
        let stream = TcpStream::from_std(std_stream);
        Self::new(stream, reactor, token)
    }
}

pub struct ReadFuture<'a> {
    stream: &'a mut AsyncTcpStream,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.stream.try_read(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),

            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                this.stream
                    .reactor
                    .lock()
                    .unwrap()
                    .register_waker(this.stream.token, waker);

                Poll::Pending
            }

            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct WriteFuture<'a> {
    stream: &'a mut AsyncTcpStream,
    buf: &'a [u8],
}

impl<'a> Future for WriteFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.stream.try_write(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),

            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                {
                    let reactor = this.stream.reactor.lock().unwrap();
                    reactor.register_waker(this.stream.token, waker);
                    drop(reactor);
                }
                Poll::Pending
            }

            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct AsyncTcpListener {
    listener: TcpListener,
    token: Token,
    reactor: Arc<Mutex<crate::reactor::Reactor>>,
}

impl AsyncTcpListener {
    pub fn new(
        mut listener: TcpListener,
        reactor: Arc<Mutex<crate::reactor::Reactor>>,
        token: Token,
    ) -> io::Result<Self> {
        reactor
            .lock()
            .unwrap()
            .registry()
            .register(&mut listener, token, Interest::READABLE)?;

        Ok(Self {
            listener,
            token,
            reactor,
        })
    }

    pub fn bind(
        addr: SocketAddr,
        reactor: Arc<Mutex<crate::reactor::Reactor>>,
        token: Token,
    ) -> io::Result<Self> {
        let std_listener = std::net::TcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;
        let mut listener = TcpListener::from_std(std_listener);
        reactor
            .lock()
            .unwrap()
            .registry()
            .register(&mut listener, token, Interest::READABLE)?;
        Ok(Self {
            listener,
            token,
            reactor,
        })
    }

    pub fn accept<'a>(&'a mut self) -> AcceptFuture<'a> {
        AcceptFuture {
            listener: &mut self.listener,
            token: self.token,
            reactor: Arc::clone(&self.reactor),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

pub struct AcceptFuture<'a> {
    listener: &'a mut TcpListener,
    token: Token,
    reactor: Arc<Mutex<crate::reactor::Reactor>>,
}

impl<'a> Future for AcceptFuture<'a> {
    type Output = io::Result<(mio::net::TcpStream, std::net::SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.listener.accept() {
            Ok(connection) => Poll::Ready(Ok(connection)),

            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                self.reactor
                    .lock()
                    .unwrap()
                    .register_waker(self.token, waker);
                Poll::Pending
            }

            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
