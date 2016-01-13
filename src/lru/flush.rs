use threadpool::ThreadPool;

use fs::Fs;
use sparse::BLOCK_SIZE;
use {ChunkDescriptor, Version, Cache};

/// Instructions sent from the main threads to the flushing threads.
///
/// FlushMessages are asynchronous and are safe to be processed in any order,
/// giving maximum flexibility to the flushing threads.
#[derive(Debug, Clone)]
pub enum FlushMessage {
    /// Quit instructs the receiving end of the channel to hang up.
    ///
    /// Whenever a channel handle receives a Quit message, it must
    /// immediately rebroadcast that message. This allows a single sender
    /// to broadcast a Quit to all receiving handles over time.
    Quit,

    /// A request to flush the given Chunk at the exact specified Version to
    /// the backend Storage.
    ///
    /// If the Chunk has advanced to a version other than the given one,
    /// this operation is cancelled and should be ignored.
    Flush(ChunkDescriptor, Option<Version>)
}

#[derive(Clone)]
pub struct FlushPool {
    pool: ThreadPool,
    size: usize,
    fs: Fs
}

impl FlushPool {
    /// Initialize a new FlushPool which processes the flush queue
    /// of the given `Fs` instance over `size` threads.
    ///
    /// It is safe and correct (though odd) to run multiple FlushPools
    /// on the same Fs instance.
    pub fn new(size: usize, fs: Fs) -> FlushPool {
        FlushPool {
            pool: ThreadPool::new(size),
            size: size,
            fs: fs
        }
    }

    /// Run the FlushPool.
    pub fn run(&self) {
        for _ in 0..self.size {
            let this = self.clone();
            self.pool.execute(move || this.task());
        }
    }

    /// Terminates a running FlushPool.
    pub fn terminate(&self) {
        self.fs.local().flush.push(FlushMessage::Quit)
    }

    // Run a worker thread (should be called *on* a worker thread).
    //
    // Includes code for restarting the task under panics.
    fn task(self) {
        // Sentinel will restart the task if the thread panics.
        let mut sentinel = Sentinel(Some(self.clone()), None);

        let local = self.fs.local();
        let storage = self.fs.storage();
        let channel = &local.flush;

        loop {
            match channel.pop() {
                FlushMessage::Quit => {
                    // Rebroadcast to other threads.
                    channel.push(FlushMessage::Quit);
                    break;
                },

                FlushMessage::Flush(chunk, version) => {
                    // Schedule this task to be restarted under panics.
                    sentinel.1 = Some(FlushMessage::Flush(chunk.clone(), version.clone()));

                    let passed_version = version.as_ref().map(|v| v.load());
                    let current_version = local.version(&chunk);

                    // The versions are the same at the beginning of our read.
                    if current_version == passed_version {
                        // Do a speculative read.
                        let mut buffer: &mut [u8] = &mut [0; BLOCK_SIZE];
                        local.read(&chunk, version.clone(), buffer).unwrap();

                        // After the read, ensure the version number is still
                        // correct. If not, cancel.
                        let current_version = local.version(&chunk);
                        if current_version == passed_version {
                            storage.create(&chunk, version, buffer)
                                .unwrap()
                        }
                    } // Else the operation has been cancelled.
                }
            }
        }

        // We exited the task cleanly, so cancel the sentinel.
        sentinel.cancel();
    }
}

struct Sentinel(Option<FlushPool>, Option<FlushMessage>);

impl Sentinel {
    fn cancel(&mut self) { self.0.take(); }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        self.0.take().map(|pool| {
            // Reschedule an interrupted flush.
            self.1.take().map(|message| pool.fs.local().flush.push(message));

            let worker = pool.clone();
            pool.pool.execute(move || worker.task());
        });
    }
}

