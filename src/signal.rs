use std::sync::{Mutex, MutexGuard, Condvar, LockResult, TryLockError};
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
}

impl<'signal, T> SignalGuard<'signal, T> {
    pub fn wait(self) -> Self {
        let lock = self.cond.wait(self.lock).unwrap();

        SignalGuard {
            lock: lock,
            cond: self.cond
        }
    }
}

impl<'signal, T> Deref for SignalGuard<'signal, T> {
    type Target = T;

    fn deref(&self) -> &T { &*self.lock }
}

impl<'signal, T> DerefMut for SignalGuard<'signal, T> {
    fn deref_mut(&mut self) -> &mut T { &mut *self.lock }
}

