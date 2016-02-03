use rwlock2::{Mutex, MutexGuard, Condvar, TryLockError};
use std::ops::{Deref, DerefMut};

pub struct Signal<T> {
    state: Mutex<T>,
    cond: Condvar
}

pub struct SignalGuard<'signal, T: 'signal> {
    lock: MutexGuard<'signal, T>,
    cond: &'signal Condvar
}

impl<T> Signal<T> {
    pub fn new(val: T) -> Signal<T> {
        Signal {
            state: Mutex::new(val),
            cond: Condvar::new()
        }
    }

    pub fn into_inner(self) -> T { self.state.into_inner().unwrap() }
    pub fn get_mut(&mut self) -> &mut T { self.state.get_mut().unwrap() }

    pub fn lock(&self) -> SignalGuard<T> {
        SignalGuard {
            // TODO: Support handling poison errors.
            lock: self.state.lock().unwrap(),
            cond: &self.cond
        }
    }

    pub fn try_lock(&self) -> Option<SignalGuard<T>> {
        let lock = match self.state.try_lock() {
            Ok(lock) => lock,
            Err(TryLockError::WouldBlock) => return None,
            e => e.unwrap()
        };

        Some(SignalGuard {
            lock: lock,
            cond: &self.cond
        })
    }

    pub fn notify(&self) { self.cond().notify_one() }
    pub fn notify_all(&self) { self.cond().notify_all() }
    pub fn cond(&self) -> &Condvar { &self.cond }
}

impl<'signal, T> SignalGuard<'signal, T> {
    pub fn wait(self) -> Self {
        let lock = self.cond.wait(self.lock).unwrap();

        SignalGuard {
            lock: lock,
            cond: self.cond
        }
    }

    pub fn map<U, F>(self, cb: F) -> SignalGuard<'signal, U>
    where F: FnOnce(&'signal mut T) -> &'signal mut U {
        SignalGuard {
            lock: MutexGuard::map(self.lock, cb),
            cond: self.cond
        }
    }

    pub fn filter_map<U, E, F>(self, cb: F) -> Result<SignalGuard<'signal, U>, (Self, E)>
    where F: FnOnce(&'signal mut T) -> Result<&'signal mut U, E> {
        match MutexGuard::filter_map(self.lock, cb) {
            Ok(lock) => Ok(SignalGuard { lock: lock, cond: self.cond }),
            Err((lock, error)) => Err((SignalGuard { lock: lock, cond: self.cond }, error))
        }
    }

    pub fn notify(&self) { self.cond().notify_one() }
    pub fn notify_all(&self) { self.cond().notify_all() }
    pub fn cond(&self) -> &Condvar { &self.cond }
}

impl<'signal, T> Deref for SignalGuard<'signal, T> {
    type Target = T;

    fn deref(&self) -> &T { &*self.lock }
}

impl<'signal, T> DerefMut for SignalGuard<'signal, T> {
    fn deref_mut(&mut self) -> &mut T { &mut *self.lock }
}

