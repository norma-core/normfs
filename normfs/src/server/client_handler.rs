use log::{debug, error, info};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use super::command_processor::{CommandProcessor, ResponseSender};
use super::frame::{self, FRAME_HEADER_SIZE, FRAME_MAX_PAYLOAD};
use super::net::{os_error, Conn};
use super::request;
use super::response;
use crate::proto::ServerResponse;
use crate::NormFS;

/// Frames of one class on their way to the socket before a sender waits, as
/// the channels of ten did; the C queue holds sixteen.
const QUEUED: usize = 10;

const REPLY: c_int = 0;
const READ: c_int = 1;
const WANT_READ: c_int = 1;
const WANT_WRITE: c_int = 2;

const EV_NEED_BUFFER: c_int = 1;
const EV_REQUEST: c_int = 2;
const EV_READ_CLOSED: c_int = 3;
const EV_WRITE_CLOSED: c_int = 4;
const EV_SENT: c_int = 5;

const NET_OK: c_int = 0;
const NET_EOF: c_int = 2;
const NET_ERR_IO: c_int = 3;
const NET_ERR_TOO_LARGE: c_int = 4;
const CONN_TIMEOUT: c_int = 100;

#[repr(C)]
#[derive(Clone, Copy)]
struct CEvent {
    conn: u64,
    value: u64,
    buf: *mut u8,
    kind: c_int,
    status: c_int,
    os_error: c_int,
    in_payload: c_int,
}

const _: () = assert!(std::mem::size_of::<CEvent>() == 40);

#[repr(C)]
struct CEvents {
    ev: *mut CEvent,
    n: usize,
    cap: usize,
}

const _: () = assert!(std::mem::size_of::<CEvents>() == 24);

unsafe extern "C" {
    fn normfs_conn_rx_size() -> usize;
    fn normfs_conn_tx_size() -> usize;
    fn normfs_conn_events_room() -> usize;
    fn normfs_conn_pending_max() -> usize;
    fn normfs_conn_rx_init(r: *mut u8, fd: c_int, token: u64, now: i64);
    fn normfs_conn_read(r: *mut u8, out: *mut CEvents, now: i64) -> c_int;
    fn normfs_conn_give_buffer(r: *mut u8, buf: *mut u8, now: i64);
    fn normfs_conn_ack(r: *mut u8, now: i64);
    fn normfs_conn_tick(r: *mut u8, out: *mut CEvents, now: i64);
    fn normfs_conn_stop(r: *mut u8, out: *mut CEvents);
    fn normfs_conn_rx_interest(r: *const u8) -> c_int;
    fn normfs_conn_deadline(r: *const u8) -> i64;
    fn normfs_conn_tx_init(t: *mut u8, fd: c_int, token: u64);
    fn normfs_conn_submit(
        t: *mut u8,
        out: *mut CEvents,
        cls: c_int,
        buf: *const u8,
        len: usize,
        cookie: u64,
    ) -> c_int;
    fn normfs_conn_write(t: *mut u8, out: *mut CEvents) -> c_int;
    fn normfs_conn_tx_interest(t: *const u8) -> c_int;
    fn normfs_conn_tx_drained(t: *const u8) -> c_int;
}

enum Command {
    Submit {
        class: c_int,
        frame: Vec<u8>,
        permit: OwnedSemaphorePermit,
    },
    Ack(u32),
    Release,
}

/// TCP-specific implementation of ResponseSender
pub struct TcpResponseSender {
    client_addr: SocketAddr,
    commands: mpsc::UnboundedSender<Command>,
    replies: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    closed: Arc<AtomicBool>,
}

impl TcpResponseSender {
    fn fail(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.commands.send(Command::Release);
    }
}

impl ResponseSender for TcpResponseSender {
    fn send_response(
        &self,
        response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return false;
            }
            // Read responses keep one queue so a read's end never overtakes its
            // entries; pongs and write acks go ahead of them.
            let (slots, class) = if response.read.is_some() {
                (&self.reads, READ)
            } else {
                (&self.replies, REPLY)
            };
            let Ok(permit) = slots.clone().acquire_owned().await else {
                return false;
            };
            let mut frame =
                match response::encode_response(&response, FRAME_HEADER_SIZE, FRAME_MAX_PAYLOAD) {
                    Ok(frame) => frame,
                    Err(e) => {
                        error!(
                            "Cannot encode response (client_addr: {}, error: {})",
                            self.client_addr, e
                        );
                        self.fail();
                        return false;
                    }
                };
            let size = frame.len() - FRAME_HEADER_SIZE;
            match frame::encode_header(size as u64) {
                Ok(header) => frame[..FRAME_HEADER_SIZE].copy_from_slice(&header),
                Err(e) => {
                    error!(
                        "Cannot frame response (client_addr: {}, error: {})",
                        self.client_addr, e
                    );
                    self.fail();
                    return false;
                }
            }
            self.commands
                .send(Command::Submit {
                    class,
                    frame,
                    permit,
                })
                .is_ok()
                && !self.closed.load(Ordering::Acquire)
        })
    }

    fn client_id(&self) -> String {
        self.client_addr.to_string()
    }
}

impl Drop for TcpResponseSender {
    /// The last request task is done with the connection.
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Release);
    }
}

pub struct ClientHandler {
    normfs: Arc<NormFS>,
    addr: SocketAddr,
}

/// A connection's two sides in C, in memory Rust owns.
struct Sides {
    rx: Vec<u64>,
    tx: Vec<u64>,
    events: Vec<CEvent>,
}

// SAFETY: the event pointers point into payload buffers the driver owns, and the C keeps
// no per-thread state, so the driver may move between tokio workers.
unsafe impl Send for Sides {}

impl Sides {
    fn rx(&mut self) -> *mut u8 {
        self.rx.as_mut_ptr().cast()
    }

    fn tx(&mut self) -> *mut u8 {
        self.tx.as_mut_ptr().cast()
    }

    fn out(&mut self) -> CEvents {
        CEvents {
            ev: self.events.as_mut_ptr(),
            n: 0,
            cap: self.events.len(),
        }
    }
}

fn now_ms(start: Instant) -> i64 {
    start.elapsed().as_millis() as i64
}

impl ClientHandler {
    pub fn new(normfs: Arc<NormFS>, addr: SocketAddr) -> Self {
        ClientHandler { normfs, addr }
    }

    pub async fn handle_connection(self, conn: Conn) {
        debug!("Starting handler (client_addr: {})", self.addr);
        let addr = self.addr;
        let (commands, receiver) = mpsc::unbounded_channel();
        let (requests, incoming) = mpsc::unbounded_channel();
        let closed = Arc::new(AtomicBool::new(false));
        let sender = Arc::new(TcpResponseSender {
            client_addr: addr,
            commands: commands.clone(),
            replies: Arc::new(Semaphore::new(QUEUED)),
            reads: Arc::new(Semaphore::new(QUEUED)),
            closed: closed.clone(),
        });
        // SAFETY: a plain constant query.
        let pending = unsafe { normfs_conn_pending_max() };
        let process = tokio::spawn(Self::process_loop(
            self.normfs.clone(),
            incoming,
            sender,
            commands,
            pending,
            addr,
        ));
        Self::drive(conn, receiver, requests, closed, addr).await;
        let _ = process.await;
        info!("Connection closed (client_addr: {})", addr);
    }

    async fn process_loop(
        normfs: Arc<NormFS>,
        mut incoming: mpsc::UnboundedReceiver<Vec<u8>>,
        sender: Arc<TcpResponseSender>,
        commands: mpsc::UnboundedSender<Command>,
        pending: usize,
        addr: SocketAddr,
    ) {
        debug!("Process loop started (client_addr: {})", addr);
        let command_processor = CommandProcessor::new(normfs);
        let mut taken = 0u32;
        while let Some(payload) = incoming.recv().await {
            // No reply can reach the client, which may retry a write it thinks was lost.
            if sender.closed.load(Ordering::Acquire) {
                break;
            }
            // The C pauses at pending requests not acknowledged; acknowledging in
            // batches spares the driver a wake per request.
            taken += 1;
            if incoming.len() + taken as usize >= pending {
                let _ = commands.send(Command::Ack(taken));
                taken = 0;
            }
            match request::decode_request(&payload) {
                Ok(request) => {
                    command_processor
                        .handle_request(request, sender.clone())
                        .await
                }
                Err(e) => {
                    error!(
                        "Failed to decode client request (client_addr: {}, error: {}, payload_size: {})",
                        addr,
                        e,
                        payload.len()
                    );
                    // A follow still holding the sender would keep the connection open.
                    sender.fail();
                    break;
                }
            }
        }
        debug!("Process loop ended (client_addr: {})", addr);
    }

    /// Runs the connection's C state on tokio's readiness until both sides are
    /// done and the request tasks have let go of it.
    async fn drive(
        conn: Conn,
        mut commands: mpsc::UnboundedReceiver<Command>,
        requests: mpsc::UnboundedSender<Vec<u8>>,
        closed: Arc<AtomicBool>,
        addr: SocketAddr,
    ) {
        let words = |bytes: usize| bytes.div_ceil(8);
        // SAFETY: plain size queries.
        let (rx_size, tx_size, room) = unsafe {
            (
                normfs_conn_rx_size(),
                normfs_conn_tx_size(),
                normfs_conn_events_room(),
            )
        };
        let blank = CEvent {
            conn: 0,
            value: 0,
            buf: std::ptr::null_mut(),
            kind: 0,
            status: 0,
            os_error: 0,
            in_payload: 0,
        };
        let mut sides = Sides {
            rx: vec![0u64; words(rx_size)],
            tx: vec![0u64; words(tx_size)],
            events: vec![blank; room],
        };
        let fd = conn.fd().as_raw_fd();
        let start = Instant::now();
        // SAFETY: rx and tx are zeroed, 8-aligned and as large as the C asked for.
        unsafe {
            normfs_conn_rx_init(sides.rx(), fd, 0, now_ms(start));
            normfs_conn_tx_init(sides.tx(), fd, 0);
        }
        let mut state = Driver {
            sides,
            requests: Some(requests),
            closed,
            addr,
            buffer: None,
            frames: HashMap::new(),
            next_cookie: 0,
            released: false,
            write_closed: false,
            start,
        };
        let sleep = tokio::time::sleep(Duration::from_secs(3600));
        tokio::pin!(sleep);
        let mut armed = i64::MIN;
        // A closed channel is ready forever and would keep the loop spinning.
        let mut commands_open = true;
        loop {
            // Nothing read can be answered any more.
            if state.write_closed {
                state.release();
            }
            let (want, deadline) = state.wants();
            if state.released && want & WANT_READ == 0 && state.drained() {
                break;
            }
            if deadline >= 0 && deadline != armed {
                armed = deadline;
                let at = start + Duration::from_millis(deadline as u64);
                sleep.as_mut().reset(tokio::time::Instant::from_std(at));
            }
            tokio::select! {
                command = commands.recv(), if commands_open => match command {
                    Some(command) => {
                        tokio::task::consume_budget().await;
                        state.apply(command);
                    }
                    None => {
                        commands_open = false;
                        state.release();
                    }
                },
                guard = conn.fd().readable(), if want & WANT_READ != 0 => match guard {
                    Ok(mut guard) => {
                        tokio::task::consume_budget().await;
                        if state.read() {
                            guard.clear_ready();
                        }
                        // Replies keep pace with a peer that never stops sending.
                        state.write();
                    }
                    Err(e) => {
                        error!("Error reading message size (client_addr: {}, error: {})", addr, e);
                        state.release();
                    }
                },
                guard = conn.fd().writable(), if want & WANT_WRITE != 0 => match guard {
                    Ok(mut guard) => {
                        tokio::task::consume_budget().await;
                        if state.write() {
                            guard.clear_ready();
                        }
                    }
                    Err(e) => {
                        error!("Error writing response (client_addr: {}, error: {})", addr, e);
                        state.release();
                    }
                },
                _ = &mut sleep, if deadline >= 0 => state.tick(),
            }
        }
        // The peer may still be reading; end the stream as dropping tokio's write half did.
        if !state.write_closed {
            conn.shutdown_write();
        }
    }
}

struct Driver {
    sides: Sides,
    requests: Option<mpsc::UnboundedSender<Vec<u8>>>,
    closed: Arc<AtomicBool>,
    addr: SocketAddr,
    buffer: Option<Vec<u8>>,
    frames: HashMap<u64, (Vec<u8>, OwnedSemaphorePermit)>,
    next_cookie: u64,
    released: bool,
    write_closed: bool,
    start: Instant,
}

impl Driver {
    fn now(&self) -> i64 {
        now_ms(self.start)
    }

    fn wants(&mut self) -> (c_int, i64) {
        let (rx, tx) = (self.sides.rx(), self.sides.tx());
        // SAFETY: rx and tx are the initialised sides.
        unsafe {
            (
                normfs_conn_rx_interest(rx) | normfs_conn_tx_interest(tx),
                normfs_conn_deadline(rx),
            )
        }
    }

    fn drained(&mut self) -> bool {
        let tx = self.sides.tx();
        // SAFETY: tx is the initialised write side.
        unsafe { normfs_conn_tx_drained(tx) != 0 }
    }

    /// Reads what the socket has; true once it would block.
    fn read(&mut self) -> bool {
        loop {
            let now = self.now();
            let rx = self.sides.rx();
            let mut out = self.sides.out();
            // SAFETY: rx is the initialised read side and out has room for a whole read.
            let blocked = unsafe { normfs_conn_read(rx, &mut out, now) } != 0;
            if !self.handle(out.n) {
                return blocked;
            }
        }
    }

    /// Writes what the socket takes; true once it would block.
    fn write(&mut self) -> bool {
        let tx = self.sides.tx();
        let mut out = self.sides.out();
        // SAFETY: tx is the initialised write side and out has room for every queued frame.
        let blocked = unsafe { normfs_conn_write(tx, &mut out) } != 0;
        self.handle(out.n);
        blocked
    }

    fn tick(&mut self) {
        let now = self.now();
        let rx = self.sides.rx();
        let mut out = self.sides.out();
        // SAFETY: as in read.
        unsafe { normfs_conn_tick(rx, &mut out, now) };
        self.handle(out.n);
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let rx = self.sides.rx();
        let mut out = self.sides.out();
        // SAFETY: as in read.
        unsafe { normfs_conn_stop(rx, &mut out) };
        self.handle(out.n);
    }

    fn apply(&mut self, command: Command) {
        match command {
            Command::Submit {
                class,
                frame,
                permit,
            } => {
                let cookie = self.next_cookie;
                self.next_cookie += 1;
                let (ptr, len) = (frame.as_ptr(), frame.len());
                self.frames.insert(cookie, (frame, permit));
                let tx = self.sides.tx();
                let mut out = self.sides.out();
                // SAFETY: the frame stays in self.frames, unmoved, until its SENT event;
                // the class semaphore keeps the queue below its sixteen.
                if unsafe { normfs_conn_submit(tx, &mut out, class, ptr, len, cookie) } != 0 {
                    error!("Response queue full (client_addr: {})", self.addr);
                    self.frames.remove(&cookie);
                    self.closed.store(true, Ordering::Release);
                    self.release();
                }
                self.handle(out.n);
                self.write();
            }
            Command::Ack(count) => {
                let now = self.now();
                let rx = self.sides.rx();
                for _ in 0..count {
                    // SAFETY: rx is the initialised read side; count never exceeds the
                    // requests it handed over.
                    unsafe { normfs_conn_ack(rx, now) };
                }
            }
            Command::Release => self.release(),
        }
    }

    /// Handles a call's events; true when a payload buffer was handed over, so
    /// reading can go on into it.
    fn handle(&mut self, n: usize) -> bool {
        // Before the frames given back free their slots, so no sender is told one went out.
        if self.sides.events[..n]
            .iter()
            .any(|e| e.kind == EV_WRITE_CLOSED)
        {
            self.closed.store(true, Ordering::Release);
        }
        let mut gave = false;
        for k in 0..n {
            let e = self.sides.events[k];
            match e.kind {
                EV_NEED_BUFFER => {
                    // A fresh zeroed buffer can take new pages without writing them;
                    // resizing writes every new byte before the read does.
                    let mut payload = vec![0u8; e.value as usize];
                    if let Some(old) = self.buffer.take() {
                        payload[..old.len()].copy_from_slice(&old);
                    }
                    let now = self.now();
                    let rx = self.sides.rx();
                    // SAFETY: payload holds the bytes asked for, those read so far at its
                    // start, and stays in self.buffer unmoved until the next NEED_BUFFER,
                    // REQUEST or READ_CLOSED event.
                    unsafe { normfs_conn_give_buffer(rx, payload.as_mut_ptr(), now) };
                    self.buffer = Some(payload);
                    gave = true;
                }
                EV_REQUEST => {
                    let payload = self.buffer.take();
                    debug_assert!(payload.as_ref().is_some_and(|p| p.as_ptr() == e.buf));
                    if let (Some(payload), Some(requests)) = (payload, &self.requests) {
                        let _ = requests.send(payload);
                    }
                }
                EV_READ_CLOSED => {
                    self.buffer = None;
                    self.requests = None;
                    log_read_closed(self.addr, e.status, e.os_error, e.value, e.in_payload != 0);
                }
                EV_WRITE_CLOSED => {
                    error!(
                        "Error writing response (client_addr: {}, error: {})",
                        self.addr,
                        os_error(e.os_error)
                    );
                    self.closed.store(true, Ordering::Release);
                    self.write_closed = true;
                    self.requests = None;
                }
                EV_SENT => {
                    self.frames.remove(&e.value);
                }
                kind => error!(
                    "Unknown connection event (client_addr: {}, kind: {})",
                    self.addr, kind
                ),
            }
        }
        gave
    }
}

fn log_read_closed(addr: SocketAddr, status: c_int, os_err: c_int, size: u64, in_payload: bool) {
    match (status, in_payload) {
        (NET_OK, _) => {}
        (NET_EOF, false) => debug!("Client disconnected (EOF) (client_addr: {})", addr),
        (NET_EOF, true) => debug!(
            "Client disconnected (EOF in payload) (client_addr: {})",
            addr
        ),
        (NET_ERR_TOO_LARGE, _) => error!(
            "Message size exceeds maximum (client_addr: {}, message_size: {}, max_size: {})",
            addr, size, FRAME_MAX_PAYLOAD
        ),
        (NET_ERR_IO, false) => error!(
            "Error reading message size (client_addr: {}, error: {})",
            addr,
            os_error(os_err)
        ),
        (NET_ERR_IO, true) => error!(
            "Error reading message payload (client_addr: {}, error: {})",
            addr,
            os_error(os_err)
        ),
        (CONN_TIMEOUT, false) => info!("Client idle timeout (client_addr: {})", addr),
        (CONN_TIMEOUT, true) => info!(
            "Client idle timeout waiting for payload (client_addr: {})",
            addr
        ),
        (other, _) => error!(
            "Read side closed with unknown status (client_addr: {}, status: {})",
            addr, other
        ),
    }
}
