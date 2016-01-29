use scoped_pool::Scope;

use fs::Fs;
use sparse::BLOCK_SIZE;
use {VolumeId, BlockIndex, Version, ContentId, Cache};

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
    Flush(VolumeId, BlockIndex, Version)
}

#[derive(Copy, Clone)]
pub struct FlushPool<'fs, 'id: 'fs> {
    fs: &'fs Fs<'id>
}

impl<'fs, 'id> FlushPool<'fs, 'id> {
    /// Initialize a new FlushPool which processes the flush queue
    /// of the given `Fs` instance on the given thread pool.
    ///
    /// It is safe and correct (though odd) to run multiple FlushPools
    /// on the same Fs instance.
    pub fn new(fs: &'fs Fs<'id>) -> Self {
        FlushPool {
            fs: fs
        }
    }

    /// Run the FlushPool.
    pub fn run(self, threads: usize, scope: &Scope<'fs>) {
        for _ in 0..threads {
            scope.recurse(move |next| self.task(next));
        }
    }

    /// Terminates a running FlushPool.
    pub fn terminate(&self) {
        self.fs.local().flush.push(FlushMessage::Quit)
    }

    // Run a worker thread (should be called *on* a worker thread).
    //
    // Includes code for restarting the task under panics.
    fn task(self, scope: &Scope<'fs>) {
        // Sentinel will restart the task if the thread panics.
        let mut sentinel = Sentinel::new(self, scope);

        let local = self.fs.local();
        let storage = self.fs.storage();
        let channel = &local.flush;

        loop {
            // Create a closure so we can catch errors.
            let res: ::Result<bool> = (|| {
                match channel.pop() {
                    FlushMessage::Quit => {
                        // Rebroadcast to other threads.
                        channel.push(FlushMessage::Quit);
                        return Ok(false)
                    },

                    FlushMessage::Flush(volume, block, version) => {
                        // Schedule this task to be restarted under panics.
                        sentinel.activate(FlushMessage::Flush(volume.clone(),
                                                              block,
                                                              version.clone()));

                        // let passed_version = version.load();
                        // let current_version = local.version(&volume, block);

                        // // Assert the versions are the same at the beginning of our read.
                        // if current_version != Some(passed_version) { return Ok(true) }

                        // // Do a speculative read.
                        // let mut buffer: &mut [u8] = &mut [0; BLOCK_SIZE];
                        // try!(local.read(&volume, block, Some(version.clone()), buffer));

                        // // After the read, ensure the version number is still
                        // // correct. If not, cancel.
                        // let current_version = local.version(&chunk);
                        // if current_version != Some(passed_version) { return Ok(true) }

                        // let content_id = unimplemented!();

                        // // Upload the object to storage.
                        // try!(storage.create(content_id, buffer));

                        // // Complete the flush.
                        // try!(local.complete_flush(&volume, block, version));

                        Ok(true)
                    }
                }
            })();

            match res {
                // We exited the task cleanly, so cancel the sentinel.
                Ok(continue_) => {
                    sentinel.cancel();

                    if continue_ { continue } else { break }
                },

                // Log the error, then the sentinel will be dropped and the task rescheduled.
                Err(e) => error!("Encountered error during flushing: {:?}", e),
            }
        }
    }
}

struct Sentinel<'fs: 'ctx, 'id: 'fs, 'ctx> {
    pool: FlushPool<'fs, 'id>,
    message: Option<FlushMessage>,
    scope: &'ctx Scope<'fs>
}

impl<'fs, 'id, 'ctx> Sentinel<'fs, 'id, 'ctx> {
    fn new(pool: FlushPool<'fs, 'id>, scope: &'ctx Scope<'fs>) -> Self {
        Sentinel {
            pool: pool,
            message: None,
            scope: scope
        }
    }

    fn activate(&mut self, message: FlushMessage) {
        self.message = Some(message);
    }

    fn cancel(&mut self) {
        self.message.take();
    }
}

impl<'fs, 'id, 'ctx> Drop for Sentinel<'fs, 'id, 'ctx> {
    fn drop(&mut self) {
        self.message.take().map(|message| {
            // Reschedule an interrupted flush.
            self.pool.fs.local().flush.push(message);

            // Restart the worker.
            let pool = self.pool.clone();
            self.scope.recurse(move |next| pool.task(next));
        });
    }
}

