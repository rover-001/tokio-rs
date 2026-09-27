use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    queue: Arc<Mutex<std::collections::VecDeque<Arc<Task>>>>,
}

impl Task {
    pub fn new<F>(future: F, queue: Arc<Mutex<std::collections::VecDeque<Arc<Task>>>>) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            future: Mutex::new(Box::pin(future)),
            queue,
        }
    }

    pub fn poll(&self, context: &mut Context<'_>) -> Poll<()> {
        self.future.lock().unwrap().as_mut().poll(context)
    }

    pub(crate) fn waker(task: Arc<Task>) -> Waker {
        unsafe { Waker::from_raw(raw_waker(task)) }
    }
}

fn raw_waker(task: Arc<Task>) -> RawWaker {
    RawWaker::new(Arc::into_raw(task) as *const (), &VTABLE)
}

unsafe fn clone(data: *const ()) -> RawWaker {
    let task = unsafe { Arc::from_raw(data as *const Task) };
    let cloned = Arc::clone(&task);
    std::mem::forget(task);
    raw_waker(cloned)
}

unsafe fn wake(data: *const ()) {
    let task = unsafe { Arc::from_raw(data as *const Task) };
    let queue = Arc::clone(&task.queue);
    queue.lock().unwrap().push_back(Arc::clone(&task));
    std::mem::forget(task);
}

unsafe fn wake_by_ref(data: *const ()) {
    let task = unsafe { Arc::from_raw(data as *const Task) };
    let queue = Arc::clone(&task.queue);
    queue.lock().unwrap().push_back(Arc::clone(&task));
    std::mem::forget(task);
}

unsafe fn drop_waker(data: *const ()) {
    drop(unsafe { Arc::from_raw(data as *const Task) });
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);
