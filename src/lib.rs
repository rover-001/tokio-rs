pub mod executor;
pub mod future;
pub mod net;
pub mod reactor;

pub use executor::Executor;
pub use future::count::CountFuture;
pub use future::timer::TimerFuture;
pub use net::{AsyncTcpListener, AsyncTcpStream};

#[cfg(test)]
mod tests {
    use crate::future::timer::TimerFuture;
    use crate::future::count::CountFuture;
    use crate::net::{AsyncTcpListener, AsyncTcpStream};
    use crate::executor::Executor;
    use mio::Token;
use std::sync::Arc;

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    #[test]
    fn count_future_completes_after_four_polls() {
        let mut future = CountFuture::new();

        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        let mut future = Pin::new(&mut future);

        assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(4));
    }

    #[test]
    fn executor_runs_future_to_completion() {
        let mut executor = Executor::new();

        executor.spawn(async {
            let result = CountFuture::new().await;
            assert_eq!(result, 4);
        });

        executor.run();
    }

    #[test]
    fn executor_runs_count_future() {
        let mut executor = Executor::new();

        executor.spawn(async {
            let result = CountFuture::new().await;
            assert_eq!(result, 4);
        });

        executor.run();
    }

    #[test]
    fn timer_future_completes() {
        let mut timer = TimerFuture::new(std::time::Duration::from_millis(10));

        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);

        let mut timer = std::pin::Pin::new(&mut timer);

        assert_eq!(timer.as_mut().poll(&mut context), std::task::Poll::Pending);

        std::thread::sleep(std::time::Duration::from_millis(20));

        assert_eq!(
            timer.as_mut().poll(&mut context),
            std::task::Poll::Ready(())
        );
    }

    #[test]
    fn executor_runs_timer_future() {
        let mut executor = Executor::new();

        executor.spawn(async {
            TimerFuture::new(std::time::Duration::from_millis(100)).await;
            println!("Timer completed!");
        });

        executor.run();
    }

    #[test]
    fn executor_finishes_after_task_completes() {
        let mut executor = Executor::new();

        executor.spawn(async {
            CountFuture::new().await;
        });

        executor.run();
    }

    #[test]
    fn executor_runs_tcp_echo() {
        let mut executor = Executor::new();
        let reactor = executor.reactor();
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut listener = AsyncTcpListener::bind(addr, Arc::clone(&reactor), Token(0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let reactor2 = Arc::clone(&reactor);

        executor.spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let mut stream = AsyncTcpStream::new(tcp_stream, Arc::clone(&reactor2), Token(2)).unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write(&buf[..n]).await.unwrap();
        });

        executor.spawn(async move {
            let mut stream = AsyncTcpStream::connect(addr, Arc::clone(&reactor), Token(1)).unwrap();
            stream.write(b"hello").await.unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"hello");
        });

        executor.run();
    }
}
