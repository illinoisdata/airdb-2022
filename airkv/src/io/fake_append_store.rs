use std::fs::OpenOptions;
use std::io::Write;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

use url::Url;

pub struct Message {
    path: Url,
    data: Vec<u8>,
}

impl Message {
    pub fn new(path: Url, data: Vec<u8>) -> Self {
        Self { path, data }
    }
}

#[derive(Default)]
pub struct FakeAppendStore {
    sender: Option<Sender<Message>>,
}

impl FakeAppendStore {
    /*
     In multi thread context, init method is expected to be executed only once globally
    */
    pub fn init(&mut self) {
        // create message channel
        let (tx, rx) = mpsc::channel();
        self.sender = Some(tx);
        thread::spawn(move || {
            for msg in rx {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(msg.path.path())
                    .unwrap();

                f.write_all(&(msg.data)).unwrap_or_else(|_| {
                    panic!(
                        "Problem flushing the append data to path[{}]",
                        msg.path.path()
                    )
                });
            }
        });
    }

    pub fn get_sender(&self) -> Option<Sender<Message>> {
        self.sender.as_ref().cloned()
    }
}
