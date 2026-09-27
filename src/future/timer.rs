use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

pub struct TimerFuture {
    duration: Duration,
    start_time: Instant,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        let waker = Arc::new(Mutex::new(None::<Waker>));
        let thread_waker = Arc::clone(&waker);

        thread::spawn(move || {
            thread::sleep(duration);

            if let Some(waker) = thread_waker.lock().unwrap().take() {
                waker.wake();
            }
        });

        Self {
            duration,
            start_time: Instant::now(),
            waker,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.start_time.elapsed() >= self.duration {
            Poll::Ready(())
        } else {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
