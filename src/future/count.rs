use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct CountFuture {
    count: u32,
}

impl CountFuture {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountFuture {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for CountFuture {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.count += 1;

        if self.count < 4 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(self.count)
        }
    }
}
