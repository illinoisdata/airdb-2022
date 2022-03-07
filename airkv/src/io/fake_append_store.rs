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

pub struct FakeAppendStore {
    sender: Sender<Message>,
}

impl Default for FakeAppendStore {
    fn default() -> Self {
        // create message channel
        let (tx, rx): (Sender<Message>, mpsc::Receiver<Message>) = mpsc::channel();

        // create consumer thread to flush appending data
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
        Self { sender: tx }
    }
}

impl FakeAppendStore {
    pub fn get_sender(&self) -> Sender<Message> {
        self.sender.clone()
    }
}
