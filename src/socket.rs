use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use maidsafe_utilities::serialisation::{deserialise_from, serialise_into};
use mio::{Evented, Poll, PollOpt, Ready, Token};
use mio::tcp::TcpStream;
use rustc_serialize::{Decodable, Encodable};
use std::collections::VecDeque;
use std::io::{self, Cursor, ErrorKind, Read, Write};
use std::mem;
use std::net::SocketAddr;

pub struct Socket {
    inner: Option<SockInner>,
}

impl Socket {
    pub fn _connect(addr: &SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self::wrap(stream))
    }

    pub fn wrap(stream: TcpStream) -> Self {
        Socket {
            inner: Some(SockInner {
                stream: stream,
                read_buffer: Vec::new(),
                read_len: 0,
                write_queue: VecDeque::new(),
                current_write: None,
            }),
        }
    }

    pub fn _peer_addr(&self) -> io::Result<SocketAddr> {
        let inner = unwrap!(self.inner.as_ref());
        Ok(inner.stream.peer_addr()?)
    }

    // Read message from the socket. Call this from inside the `ready` handler.
    //
    // Returns:
    //   - Ok(Some(data)): data has been successfuly read from the socket
    //   - Ok(None):       there is not enough data in the socket. Call `read`
    //                     again in the next invocation of the `ready` handler.
    //   - Err(error):     there was an error reading from the socket.
    pub fn read<T: Decodable>(&mut self, poll: &Poll, token: Token) -> io::Result<Option<T>> {
        let inner = unwrap!(self.inner.as_mut());
        inner.read(poll, token)
    }

    // Write a message to the socket.
    //
    // Returns:
    //   - Ok(true):   the message has been successfuly written.
    //   - Ok(false):  the message has been queued, but not yet fully written.
    //                 Write event is already scheduled for next time.
    //   - Err(error): there was an error while writing to the socket.
    pub fn write<T: Encodable>(&mut self,
                               poll: &Poll,
                               token: Token,
                               msg: Option<T>)
                               -> io::Result<bool> {
        let inner = unwrap!(self.inner.as_mut());
        inner.write(poll, token, msg)
    }
}

impl Default for Socket {
    fn default() -> Self {
        Socket { inner: None }
    }
}

impl Evented for Socket {
    fn register(&self,
                poll: &Poll,
                token: Token,
                interest: Ready,
                opts: PollOpt)
                -> io::Result<()> {
        let inner = unwrap!(self.inner.as_ref());
        inner.register(poll, token, interest, opts)
    }

    fn reregister(&self,
                  poll: &Poll,
                  token: Token,
                  interest: Ready,
                  opts: PollOpt)
                  -> io::Result<()> {
        let inner = unwrap!(self.inner.as_ref());
        inner.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        let inner = unwrap!(self.inner.as_ref());
        inner.deregister(poll)
    }
}

struct SockInner {
    stream: TcpStream,
    read_buffer: Vec<u8>,
    read_len: usize,
    write_queue: VecDeque<Vec<u8>>,
    current_write: Option<Vec<u8>>,
}

impl SockInner {
    // Read message from the socket. Call this from inside the `ready` handler.
    //
    // Returns:
    //   - Ok(Some(data)): data has been successfuly read from the socket
    //   - Ok(None):       there is not enough data in the socket. Call `read`
    //                     again in the next invocation of the `ready` handler.
    //   - Err(error):     there was an error reading from the socket.
    fn read<T: Decodable>(&mut self, _poll: &Poll, _token: Token) -> io::Result<Option<T>> {
        if let Some(message) = self.read_from_buffer()? {
            return Ok(Some(message));
        }

        // the mio reading window is max at 64k (64 * 1024)
        let mut buffer = [0; 65536];

        match self.stream.read(&mut buffer) {
            Ok(bytes_read) => {
                // println!("Total bytes rxd: {}", bytes_read);
                self.read_buffer.extend_from_slice(&buffer[0..bytes_read]);
                self.read_from_buffer()
            }

            Err(error) => {
                if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::Interrupted {
                    println!("================== Error in read {:?}", error.kind());
                    // This re-registration is not required for Linux - it automatically re-fires
                    // read readiness when socket is no longer wouldblock. On windows you get this
                    // WouldBlock wayyy more often and once you get it sometimes re-fires when
                    // actually ready and sometimes doesn't. On putting this it fires most of the
                    // time but there are times it does not fire readiness any more so it's not a
                    // solution but sort of reduces the problem to some extent.
                    // unwrap!(poll.reregister(self,
                    //                         token,
                    //                         Ready::readable() | Ready::error() | Ready::hup(),
                    //                         PollOpt::edge()));
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn read_from_buffer<T: Decodable>(&mut self) -> io::Result<Option<T>> {
        let u32_size = mem::size_of::<u32>();

        if self.read_len == 0 {
            if self.read_buffer.len() < u32_size {
                println!("Payload size not obtained yet");
                return Ok(None);
            }

            self.read_len = Cursor::new(&self.read_buffer).read_u32::<LittleEndian>()? as usize;

            self.read_buffer = self.read_buffer[u32_size..].to_owned();
        }

        if self.read_len > self.read_buffer.len() {
            println!("Buffer not yet as big as indicated by Payload size");
            return Ok(None);
        }

        let result = unwrap!(deserialise_from(&mut Cursor::new(&self.read_buffer)));

        self.read_buffer = self.read_buffer[self.read_len..].to_owned();
        self.read_len = 0;

        Ok(Some(result))
    }

    // Write a message to the socket.
    //
    // Returns:
    //   - Ok(true):   the message has been successfuly written.
    //   - Ok(false):  the message has been queued, but not yet fully written.
    //                 Write event is already scheduled for next time.
    //   - Err(error): there was an error while writing to the socket.
    fn write<T: Encodable>(&mut self,
                           poll: &Poll,
                           token: Token,
                           msg: Option<T>)
                           -> io::Result<bool> {
        if let Some(msg) = msg {
            let mut data = Cursor::new(Vec::with_capacity(mem::size_of::<u32>()));

            let _ = data.write_u32::<LittleEndian>(0);

            unwrap!(serialise_into(&msg, &mut data));

            let len = data.position() - mem::size_of::<u32>() as u64;
            data.set_position(0);
            data.write_u32::<LittleEndian>(len as u32)?;

            self.write_queue.push_back(data.into_inner());
        }

        if self.current_write.is_none() {
            let data = unwrap!(self.write_queue.pop_front());
            self.current_write = Some(data);
        }

        if let Some(data) = self.current_write.take() {
            match self.stream.write(&data) {
                Ok(bytes_txd) => {
                    if bytes_txd < data.len() {
                        self.current_write = Some(data[bytes_txd..].to_owned());
                    }
                }
                Err(error) => {
                    if error.kind() == ErrorKind::WouldBlock ||
                       error.kind() == ErrorKind::Interrupted {
                        self.current_write = Some(data);
                    } else {
                        return Err(From::from(error));
                    }
                }
            }
        }

        let done = self.current_write.is_none() && self.write_queue.is_empty();

        let event_set = if done {
            Ready::error() | Ready::hup() | Ready::readable()
        } else {
            Ready::error() | Ready::hup() | Ready::readable() | Ready::writable()
        };

        poll.reregister(self, token, event_set, PollOpt::edge())?;

        Ok(done)
    }
}

impl Evented for SockInner {
    fn register(&self,
                poll: &Poll,
                token: Token,
                interest: Ready,
                opts: PollOpt)
                -> io::Result<()> {
        self.stream.register(poll, token, interest, opts)
    }

    fn reregister(&self,
                  poll: &Poll,
                  token: Token,
                  interest: Ready,
                  opts: PollOpt)
                  -> io::Result<()> {
        self.stream.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.stream.deregister(poll)
    }
}
