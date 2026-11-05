//! STO-7: retain exact CommandComplete and ReadyForQuery semantics that the
//! client's row-count-only simple-query API otherwise hides. No auth/data logging.
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::Notify,
};

#[derive(Clone, Default)]
pub(super) struct Reply {
    pub ready: u64,
    pub commands: u64,
    pub command: String,
    pub status: u8,
    pub dead: bool,
}
#[derive(Default)]
pub(super) struct Protocol {
    pub reply: Mutex<Reply>,
    pub changed: Notify,
}
impl Protocol {
    pub fn snapshot(&self) -> Reply {
        self.reply.lock().expect("protocol lock").clone()
    }
    pub fn dead(&self) {
        self.reply.lock().expect("protocol lock").dead = true;
        self.changed.notify_one();
    }
    pub async fn complete(&self, after: u64) -> Reply {
        loop {
            let snapshot = self.snapshot();
            if snapshot.ready > after || snapshot.dead {
                return snapshot;
            }
            self.changed.notified().await;
        }
    }
}

pub(super) struct Transport {
    socket: TcpStream,
    frames: Frames,
}
struct Frames {
    protocol: Arc<Protocol>,
    header: Vec<u8>,
    remaining: usize,
    capture: Vec<u8>,
    tag: u8,
}
impl Transport {
    pub fn new(socket: TcpStream, protocol: Arc<Protocol>) -> Self {
        Self {
            socket,
            frames: Frames::new(protocol),
        }
    }
}
impl Frames {
    fn new(protocol: Arc<Protocol>) -> Self {
        Self {
            protocol,
            header: Vec::with_capacity(5),
            remaining: 0,
            capture: Vec::with_capacity(128),
            tag: 0,
        }
    }
    fn observe(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            if self.remaining == 0 {
                let n = (5 - self.header.len()).min(bytes.len());
                self.header.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if self.header.len() < 5 {
                    break;
                }
                self.tag = self.header[0];
                let len = u32::from_be_bytes(self.header[1..5].try_into().expect("header"));
                if len < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid server frame",
                    ));
                }
                self.remaining = (len - 4) as usize;
                self.header.clear();
                self.capture.clear();
                if self.remaining == 0 {
                    continue;
                }
            }
            let n = self.remaining.min(bytes.len());
            if matches!(self.tag, b'C' | b'Z') {
                if self.capture.len() + n > 128 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "oversized command/status frame",
                    ));
                }
                self.capture.extend_from_slice(&bytes[..n]);
            }
            bytes = &bytes[n..];
            self.remaining -= n;
            if self.remaining == 0 && matches!(self.tag, b'C' | b'Z') {
                let mut reply = self.protocol.reply.lock().expect("protocol lock");
                if self.tag == b'C' {
                    reply.commands += 1;
                    reply.command = String::from_utf8(
                        self.capture
                            .strip_suffix(&[0])
                            .unwrap_or(&self.capture)
                            .to_vec(),
                    )
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid command tag")
                    })?;
                } else {
                    if self.capture.len() != 1 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid ready status",
                        ));
                    }
                    reply.ready += 1;
                    reply.status = self.capture[0];
                    self.protocol.changed.notify_one();
                }
            }
        }
        Ok(())
    }
}
impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // A zero-capacity read is a no-op, not evidence of transport death.
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.socket).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() == before {
                    self.frames.protocol.dead();
                }
                Poll::Ready(self.frames.observe(&buf.filled()[before..]))
            }
            result => result,
        }
    }
}
impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut result = vec![tag];
        result.extend(((body.len() + 4) as u32).to_be_bytes());
        result.extend(body);
        result
    }
    #[test]
    fn sto7_command_and_ready_survive_every_fragment_boundary() {
        for command in [b"COMMIT\0".as_slice(), b"ROLLBACK\0"] {
            let mut bytes = frame(b'D', b"arbitrary row bytes, not a command");
            bytes.extend(frame(b'C', command));
            bytes.extend(frame(b'Z', b"I"));
            for split in 0..=bytes.len() {
                let protocol = Arc::new(Protocol::default());
                let mut frames = Frames::new(protocol.clone());
                frames.observe(&bytes[..split]).unwrap();
                frames.observe(&bytes[split..]).unwrap();
                let reply = protocol.snapshot();
                assert_eq!(reply.command.as_bytes(), &command[..command.len() - 1]);
                assert_eq!((reply.commands, reply.ready, reply.status), (1, 1, b'I'));
            }
            let protocol = Arc::new(Protocol::default());
            let mut frames = Frames::new(protocol.clone());
            for byte in bytes {
                frames.observe(&[byte]).unwrap();
            }
            assert_eq!(protocol.snapshot().ready, 1);
        }
    }
    #[test]
    fn sto7_partial_reply_and_large_other_frames_cannot_fabricate_completion() {
        let protocol = Arc::new(Protocol::default());
        let mut frames = Frames::new(protocol.clone());
        frames.observe(&frame(b'C', b"COMMIT\0")).unwrap();
        assert_eq!(protocol.snapshot().ready, 0);
        let mut huge = vec![b'D'];
        huge.extend(u32::MAX.to_be_bytes());
        frames.observe(&huge).unwrap();
        frames.observe(&frame(b'Z', b"I")).unwrap(); // still row payload
        assert_eq!(protocol.snapshot().ready, 0);
        assert!(frames.capture.is_empty());
        let mut frames = Frames::new(protocol);
        assert!(frames.observe(&frame(b'C', &[b'x'; 129])).is_err());
    }
}
