use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

pub struct Reactor {
    poll: Poll,
    wakers: Arc<Mutex<HashMap<Token, Waker>>>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poll: Poll::new()?,
            wakers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn registry(&self) -> &mio::Registry {
        self.poll.registry()
    }

    pub fn register(&self, stream: &mut TcpStream, token: Token) -> io::Result<()> {
        self.poll
            .registry()
            .register(stream, token, Interest::READABLE)
    }

    pub fn register_writable(&self, stream: &mut TcpStream, token: Token) -> io::Result<()> {
        self.poll
            .registry()
            .register(stream, token, Interest::WRITABLE)
    }

    pub fn register_readable(&self, stream: &mut TcpStream, token: Token) -> io::Result<()> {
        self.poll
            .registry()
            .register(stream, token, Interest::READABLE)
    }

    pub fn register_waker(&self, token: Token, waker: Waker) {
        self.wakers.lock().unwrap().insert(token, waker);
    }

    pub fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let mut events = Events::with_capacity(128);

        self.poll.poll(&mut events, timeout)?;

        let wakers = self.wakers.lock().unwrap();

        for event in events.iter() {
            if let Some(waker) = wakers.get(&event.token()) {
                waker.wake_by_ref();
            }
        }

        Ok(())
    }

    pub fn bind(&mut self, address: &str, token: Token) -> io::Result<TcpListener> {
        let mut listener = TcpListener::bind(address.parse().unwrap())?;

        self.poll
            .registry()
            .register(&mut listener, token, Interest::READABLE)?;

        Ok(listener)
    }

    pub fn accept(&mut self, listener: &mut TcpListener) -> io::Result<TcpStream> {
        let (stream, _) = listener.accept()?;
        Ok(stream)
    }
}
