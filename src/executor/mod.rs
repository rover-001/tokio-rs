use crate::reactor::Reactor;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

pub struct Executor {
    tasks: Arc<Mutex<VecDeque<Arc<crate::executor::task::Task>>>>,
    active_tasks: Arc<Mutex<usize>>,
    reactor: Arc<Mutex<Reactor>>,
}

pub use task::Task;

mod task;

impl Executor {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(VecDeque::new())),
            active_tasks: Arc::new(Mutex::new(0)),
            reactor: Arc::new(Mutex::new(Reactor::new().unwrap())),
        }
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(task::Task::new(future, Arc::clone(&self.tasks)));

        *self.active_tasks.lock().unwrap() += 1;

        self.tasks.lock().unwrap().push_back(task);
    }

    pub fn reactor(&self) -> Arc<Mutex<Reactor>> {
        Arc::clone(&self.reactor)
    }

    pub fn run(&mut self) {
        loop {
            let task = {
                let mut tasks = self.tasks.lock().unwrap();
                tasks.pop_front()
            };

            let Some(task) = task else {
                if *self.active_tasks.lock().unwrap() == 0 {
                    break;
                }

                self.reactor.lock().unwrap().wait(Some(Duration::from_millis(10))).unwrap();
                continue;
            };

            let waker = task::Task::waker(Arc::clone(&task));
            let mut context = Context::from_waker(&waker);

            match task.poll(&mut context) {
                Poll::Ready(()) => {
                    *self.active_tasks.lock().unwrap() -= 1;
                }
                Poll::Pending => {}
            }
        }
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}
