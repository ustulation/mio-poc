extern crate byteorder;
extern crate mio;
extern crate maidsafe_utilities;
extern crate rustc_serialize;
#[macro_use]
extern crate unwrap;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use mio::{Ready, PollOpt, Poll, Token, Events};
use mio::channel::{self, Sender};
use mio::tcp::TcpListener;
use std::time::Duration;
use std::str::FromStr;
use socket::Socket;
use maidsafe_utilities::thread::{named as named_thread, Joiner};
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::mem;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use rustc_serialize::Decodable;
use maidsafe_utilities::serialisation::{serialise, deserialise};

mod socket;

const MSG_TOKEN: usize = 0;
const LISTENER_TOKEN: usize = MSG_TOKEN + 1;
const SESSION_TOKEN: usize = LISTENER_TOKEN + 1;

struct CoreMsg(Option<Box<FnMut(&Poll) + 'static + Send>>);
impl CoreMsg {
    fn _new<F: FnOnce(&Poll) + 'static + Send>(f: F) -> Self {
        let mut f = Some(f);
        CoreMsg(Some(Box::new(move |p: &Poll| {
            let f = f.take().unwrap();
            f(p)
        })))
    }

    fn build_terminator() -> Self {
        CoreMsg(None)
    }
}

struct El {
    tx: Sender<CoreMsg>,
    listener_started_rx: Receiver<()>,
    _joiner: Joiner,
}
impl Drop for El {
    fn drop(&mut self) {
        let _ = self.tx.send(CoreMsg::build_terminator());
    }
}

struct Server {
    listener: TcpListener,
    session: Option<Session>,
}

struct Session(Socket);
impl Session {
    fn ready(&mut self, poll: &Poll, kind: Ready) {
        if kind.is_error() || kind.is_hup() {
            return;
        } else if kind.is_readable() {
            let data = match self.0.read::<Vec<u8>>(poll, Token(SESSION_TOKEN)) {
                Ok(Some(msg)) => msg,
                Err(e) => panic!("Error in read {:?}", e),
                Ok(None) => return,
            };
            println!("Async socket Rxd from peer: {:?}", data);
            let _ = self.0.write(poll, Token(SESSION_TOKEN), Some(vec![2u8, 3, 4, 5, 6]));
        } else if kind.is_writable() {
            let _ = self.0.write(poll, Token(SESSION_TOKEN), Option::None::<Vec<u8>>);
        } else {
            println!("Session socket: {:?}", kind);
        }
    }
}

fn spawn_el() -> El {
    let (tx, rx) = channel::channel::<CoreMsg>();
    let (listener_started_tx, listener_started_rx) = mpsc::channel();

    let joiner = named_thread("Event Loop", move || {
        let poll = unwrap!(Poll::new());
        let mut events = Events::with_capacity(1024);

        unwrap!(poll.register(&rx,
                              Token(MSG_TOKEN),
                              Ready::readable() | Ready::error() | Ready::hup(),
                              PollOpt::edge()));

        let listener = unwrap!(TcpListener::bind(&unwrap!(SocketAddr::from_str("0.0.0.0:5000"))));
        unwrap!(poll.register(&listener,
                              Token(LISTENER_TOKEN),
                              Ready::readable() | Ready::error() | Ready::hup(),
                              PollOpt::edge()));

        let mut server = Server {
            listener: listener,
            session: None,
        };

        unwrap!(listener_started_tx.send(()));

        'event_loop: loop {
            unwrap!(poll.poll(&mut events, None), "Failed to Poll");

            for event in events.iter() {
                println!("{:?}", event);
                match event.token() {
                    Token(t) if t == LISTENER_TOKEN => {
                        loop {
                            let (peer, _) = match server.listener.accept() {
                                Ok((peer, peer_addr)) => (Socket::wrap(peer), peer_addr),
                                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock ||
                                              e.kind() == io::ErrorKind::Interrupted => {
                                    break;
                                }
                                Err(e) => panic!("Error in listening: {:?}", e),
                            };

                            unwrap!(poll.register(&peer,
                                                  Token(SESSION_TOKEN),
                                                  Ready::readable() | Ready::error() |
                                                  Ready::hup(),
                                                  PollOpt::edge()));
                            server.session = Some(Session(peer));
                            // Uncomment to simulate a WouldBlock
                            // unwrap!(server.session.as_mut()).ready(&poll, event.kind());
                        }
                    }
                    Token(t) if t == SESSION_TOKEN => {
                        unwrap!(server.session.as_mut()).ready(&poll, event.kind());
                    }
                    Token(t) if t == MSG_TOKEN => {
                        let msg = unwrap!(rx.try_recv());
                        if let Some(mut f) = msg.0 {
                            f(&poll);
                        } else {
                            break 'event_loop;
                        }
                    }
                    x => panic!("Unexpected token: {:?}", x),
                }
            }
        }

        println!("Exiting Event loop");
    });

    El {
        tx: tx,
        listener_started_rx: listener_started_rx,
        _joiner: joiner,
    }
}

fn sync_write(stream: &mut TcpStream, message: Vec<u8>) {
    let mut size_vec = Vec::<u8>::with_capacity(mem::size_of::<u32>());
    unwrap!(size_vec.write_u32::<LittleEndian>(message.len() as u32));

    unwrap!(stream.write_all(&size_vec));
    thread::sleep(Duration::from_secs(1));
    unwrap!(stream.write_all(&message));
}

#[allow(unsafe_code)]
fn sync_read<T: Decodable>(stream: &mut TcpStream) -> T {
    let mut payload_size_buffer = [0; 4];
    unwrap!(stream.read_exact(&mut payload_size_buffer));

    let payload_size = unwrap!(Cursor::new(&payload_size_buffer[..])
        .read_u32::<LittleEndian>()) as usize;

    let mut payload = Vec::with_capacity(payload_size);
    unsafe {
        payload.set_len(payload_size);
    }
    unwrap!(stream.read_exact(&mut payload));

    unwrap!(deserialise(&payload), "Could not deserialise.")
}

fn main() {
    let el = spawn_el();
    unwrap!(el.listener_started_rx.recv());
    let mut us = unwrap!(TcpStream::connect("127.0.0.1:5000"));
    thread::sleep(Duration::from_secs(1));
    sync_write(&mut us, unwrap!(serialise(&vec![1u8, 2, 3, 4, 60, 97])));
    let _blocking_rx = sync_read::<Vec<u8>>(&mut us);
    sync_write(&mut us,
               unwrap!(serialise(&vec![11u8, 21, 33, 41, 160, 99, 233])));
    println!("Written - If windows has gone into wouldblock then this will hang...");
    let _blocking_rx = sync_read::<Vec<u8>>(&mut us);
}
