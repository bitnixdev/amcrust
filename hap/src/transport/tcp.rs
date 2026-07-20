use aead::{generic_array::GenericArray, AeadInPlace, NewAead};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BytesMut};
use chacha20poly1305::{ChaCha20Poly1305, Nonce, Tag};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    io::Error,
    Stream,
};
use log::{debug, error};
use std::{
    cmp::min,
    collections::VecDeque,
    future::Future,
    io::{self, ErrorKind},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use uuid::Uuid;

use crate::{pointer, Result};

#[derive(Debug, Default)]
struct SerializedOutputState {
    http_response: Vec<u8>,
    deferred_events: VecDeque<Vec<u8>>,
}

/// Serializes complete HTTP responses and asynchronous EVENT messages before
/// they enter the encrypted transport. Hyper may issue several `poll_write`
/// calls for one response, so events are deferred until `poll_flush` publishes
/// the complete response as a single queue item.
#[derive(Clone)]
pub struct SerializedOutput {
    sender: UnboundedSender<Vec<u8>>,
    outgoing_waker: Arc<Mutex<Option<Waker>>>,
    state: Arc<Mutex<SerializedOutputState>>,
    snapshot_delivery_handler: pointer::SnapshotDeliveryHandler,
}

impl SerializedOutput {
    fn new(
        sender: UnboundedSender<Vec<u8>>,
        outgoing_waker: Arc<Mutex<Option<Waker>>>,
        snapshot_delivery_handler: pointer::SnapshotDeliveryHandler,
    ) -> Self {
        Self {
            sender,
            outgoing_waker,
            state: Arc::new(Mutex::new(SerializedOutputState::default())),
            snapshot_delivery_handler,
        }
    }

    fn append_http(&self, bytes: &[u8]) {
        self.state
            .lock()
            .expect("accessing serialized output")
            .http_response
            .extend_from_slice(bytes);
    }

    fn flush_http(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("accessing serialized output");
        if !state.http_response.is_empty() {
            let response = std::mem::take(&mut state.http_response);
            let is_snapshot = snapshot_body_len(&response).is_some();
            if self.sender.unbounded_send(response).is_err() {
                if is_snapshot {
                    notify_snapshot_delivery(
                        &self.snapshot_delivery_handler,
                        pointer::SnapshotDelivery::Failed,
                    );
                }
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "controller disconnected",
                ));
            }
        }
        while let Some(event) = state.deferred_events.pop_front() {
            self.sender.unbounded_send(event).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "controller disconnected")
            })?;
        }
        drop(state);
        self.wake_writer();
        Ok(())
    }

    pub fn send_event(&self, event: Vec<u8>) -> io::Result<()> {
        let mut state = self.state.lock().expect("accessing serialized output");
        if state.http_response.is_empty() {
            self.sender.unbounded_send(event).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "controller disconnected")
            })?;
        } else {
            state.deferred_events.push_back(event);
        }
        drop(state);
        self.wake_writer();
        Ok(())
    }

    fn wake_writer(&self) {
        if let Some(waker) = self
            .outgoing_waker
            .lock()
            .expect("accessing outgoing_waker")
            .take()
        {
            waker.wake();
        }
    }
}

pub struct StreamWrapper {
    incoming_receiver: UnboundedReceiver<Vec<u8>>,
    serialized_output: SerializedOutput,
    incoming_waker: Arc<Mutex<Option<Waker>>>,
    outgoing_waker: Arc<Mutex<Option<Waker>>>,
    incoming_buf: BytesMut,
}

impl StreamWrapper {
    pub fn new(
        incoming_receiver: UnboundedReceiver<Vec<u8>>,
        serialized_output: SerializedOutput,
        incoming_waker: Arc<Mutex<Option<Waker>>>,
        outgoing_waker: Arc<Mutex<Option<Waker>>>,
    ) -> StreamWrapper {
        StreamWrapper {
            incoming_receiver,
            serialized_output,
            incoming_waker,
            outgoing_waker,
            incoming_buf: BytesMut::new(),
        }
    }

    fn poll_receiver(&mut self, cx: &mut Context) -> Poll<usize> {
        debug!("polling incoming TCP stream receiver");

        match Stream::poll_next(Pin::new(&mut self.incoming_receiver), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(incoming)) => {
                let r_len = incoming.len();
                self.incoming_buf.extend_from_slice(&incoming);

                debug!("received {} Bytes on incoming TCP stream receiver", &r_len);

                Poll::Ready(r_len)
            }
            Poll::Ready(None) => {
                debug!("received 0 Bytes on incoming TCP stream receiver");
                Poll::Ready(0)
            }
        }
    }
}

impl AsyncRead for StreamWrapper {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let stream_wrapper = Pin::into_inner(self);

        if !stream_wrapper.incoming_buf.is_empty() {
            let r_len = min(buf.remaining(), stream_wrapper.incoming_buf.len());
            buf.put_slice(&stream_wrapper.incoming_buf[..r_len]);
            stream_wrapper.incoming_buf.advance(r_len);
            return Poll::Ready(Ok(()));
        }

        match stream_wrapper.poll_receiver(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_r_len) => {
                let r_len = min(buf.remaining(), stream_wrapper.incoming_buf.len());
                buf.put_slice(&stream_wrapper.incoming_buf[..r_len]);
                stream_wrapper.incoming_buf.advance(r_len);

                if let Some(waker) = stream_wrapper
                    .outgoing_waker
                    .lock()
                    .expect("accessing outgoing_waker")
                    .take()
                {
                    waker.wake()
                }
                if let Some(waker) = stream_wrapper
                    .incoming_waker
                    .lock()
                    .expect("accessing incoming_waker")
                    .take()
                {
                    waker.wake()
                }

                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for StreamWrapper {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        let stream_wrapper = Pin::into_inner(self);

        debug!("writing {} Bytes to outgoing TCP stream sender", buf.len());

        stream_wrapper.serialized_output.append_http(buf);
        if let Some(waker) = stream_wrapper
            .incoming_waker
            .lock()
            .expect("accessing incoming_waker")
            .take()
        {
            waker.wake()
        }

        let w_len = buf.len();

        debug!("wrote {} Bytes to outgoing TCP stream sender", &w_len);

        Poll::Ready(Ok(w_len))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let stream_wrapper = Pin::into_inner(self);
        Poll::Ready(stream_wrapper.serialized_output.flush_http())
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let stream_wrapper = Pin::into_inner(self);
        Poll::Ready(stream_wrapper.serialized_output.flush_http())
    }
}

#[derive(Debug)]
pub struct Session {
    pub controller_id: Uuid,
    pub shared_secret: [u8; 32],
}

pub struct EncryptedStream {
    stream: TcpStream,
    incoming_sender: UnboundedSender<Vec<u8>>,
    outgoing_receiver: UnboundedReceiver<Vec<u8>>,
    outgoing_current: Option<Vec<u8>>,
    incoming_waker: Arc<Mutex<Option<Waker>>>,
    outgoing_waker: Arc<Mutex<Option<Waker>>>,
    session_receiver: oneshot::Receiver<Session>,
    pub controller_id: Arc<RwLock<Option<Uuid>>>,
    /// The session's pair-verify shared secret, shared with the HTTP layer so
    /// features like HomeKit Data Stream can derive session keys from it.
    pub session_shared_secret: Arc<RwLock<Option<[u8; 32]>>>,
    shared_secret: Option<[u8; 32]>,
    decrypt_count: u64,
    encrypt_count: u64,
    pending_encrypted_write: BytesMut,
    pending_plaintext_len: usize,
    encrypted_buf: BytesMut,
    decrypted_buf: BytesMut,
    snapshot_delivery_handler: pointer::SnapshotDeliveryHandler,
}

impl EncryptedStream {
    pub fn new(
        stream: TcpStream,
        snapshot_delivery_handler: pointer::SnapshotDeliveryHandler,
    ) -> (
        EncryptedStream,
        UnboundedReceiver<Vec<u8>>,
        SerializedOutput,
        oneshot::Sender<Session>,
        Arc<Mutex<Option<Waker>>>,
        Arc<Mutex<Option<Waker>>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        let (incoming_sender, incoming_receiver) = mpsc::unbounded();
        let (outgoing_sender, outgoing_receiver) = mpsc::unbounded();
        let incoming_waker = Arc::new(Mutex::new(None));
        let outgoing_waker = Arc::new(Mutex::new(None));
        let encrypted_buf = BytesMut::with_capacity(1042);
        let decrypted_buf = BytesMut::with_capacity(1024);
        let serialized_output = SerializedOutput::new(
            outgoing_sender,
            outgoing_waker.clone(),
            snapshot_delivery_handler.clone(),
        );

        (
            EncryptedStream {
                stream,
                incoming_sender,
                outgoing_receiver,
                outgoing_current: None,
                incoming_waker: incoming_waker.clone(),
                outgoing_waker: outgoing_waker.clone(),
                session_receiver: receiver,
                controller_id: Arc::new(RwLock::new(None)),
                session_shared_secret: Arc::new(RwLock::new(None)),
                shared_secret: None,
                decrypt_count: 0,
                encrypt_count: 0,
                pending_encrypted_write: BytesMut::new(),
                pending_plaintext_len: 0,
                encrypted_buf,
                decrypted_buf,
                snapshot_delivery_handler,
            },
            incoming_receiver,
            serialized_output,
            sender,
            incoming_waker,
            outgoing_waker,
        )
    }

    fn decrypt_buffered_record(&mut self) -> io::Result<bool> {
        if self.encrypted_buf.len() < 2 {
            return Ok(false);
        }
        let plaintext_len = LittleEndian::read_u16(&self.encrypted_buf[..2]) as usize;
        if plaintext_len > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encrypted HAP record exceeds 1024 bytes",
            ));
        }
        let record_len = 2 + plaintext_len + 16;
        if self.encrypted_buf.len() < record_len {
            return Ok(false);
        }
        let decrypted = decrypt_chunk(
            &self.shared_secret.expect("missing shared secret"),
            &self.encrypted_buf[..2],
            &self.encrypted_buf[2..2 + plaintext_len],
            &self.encrypted_buf[2 + plaintext_len..record_len],
            &mut self.decrypt_count,
        )
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decryption failed"))?;
        self.encrypted_buf.advance(record_len);
        self.decrypted_buf.extend_from_slice(&decrypted);
        Ok(true)
    }

    fn poll_outgoing(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let encrypted_stream = Pin::into_inner(self);
        loop {
            if let Some(data) = encrypted_stream.outgoing_current.take() {
                match AsyncWrite::poll_write(Pin::new(encrypted_stream), cx, &data) {
                    Poll::Pending => {
                        encrypted_stream.outgoing_current = Some(data);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        if snapshot_body_len(&data).is_some() {
                            encrypted_stream
                                .notify_snapshot_delivery(pointer::SnapshotDelivery::Failed);
                        }
                        match e.kind() {
                            io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::NotConnected => {
                                debug!("outgoing controller stream disconnected: {e}")
                            }
                            _ => error!("error writing to outgoing stream: {e}"),
                        }
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            ErrorKind::WriteZero,
                            "could not write outgoing HAP data",
                        )));
                    }
                    Poll::Ready(Ok(written)) if written < data.len() => {
                        encrypted_stream.outgoing_current = Some(data[written..].to_vec());
                        continue;
                    }
                    Poll::Ready(Ok(written)) => {
                        debug!("wrote {written} plaintext Bytes to outgoing TCP stream");
                        if let Some(bytes) = snapshot_body_len(&data) {
                            encrypted_stream.notify_snapshot_delivery(
                                pointer::SnapshotDelivery::Delivered { bytes },
                            );
                            log::info!(
                                "encrypted snapshot body flushed to controller TCP socket ({written} bytes)"
                            );
                        }
                        continue;
                    }
                }
            }

            match Stream::poll_next(Pin::new(&mut encrypted_stream.outgoing_receiver), cx) {
                Poll::Pending => {
                    *encrypted_stream
                        .outgoing_waker
                        .lock()
                        .expect("setting outgoing_waker") = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(Some(data)) => {
                    debug!("writing {} Bytes to outgoing TCP stream", data.len());
                    encrypted_stream.outgoing_current = Some(data);
                }
                Poll::Ready(None) => {
                    debug!("outgoing TCP stream ended");

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn notify_snapshot_delivery(&self, delivery: pointer::SnapshotDelivery) {
        notify_snapshot_delivery(&self.snapshot_delivery_handler, delivery);
    }

    fn poll_incoming(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let encrypted_stream = Pin::into_inner(self);

        let mut data_inner = [0; 1536];
        let mut data = ReadBuf::new(&mut data_inner);

        loop {
            match AsyncRead::poll_read(Pin::new(encrypted_stream), cx, &mut data) {
                Poll::Pending => {
                    *encrypted_stream
                        .incoming_waker
                        .lock()
                        .expect("setting incoming_waker") = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        *encrypted_stream
                            .incoming_waker
                            .lock()
                            .expect("setting incoming_waker") = Some(cx.waker().clone());
                        return Poll::Pending;
                    }
                    _ => {
                        return Poll::Ready(Err(e));
                    }
                },
                Poll::Ready(Ok(())) => {
                    let data_filled = data.filled();

                    if data_filled.len() == 0 {
                        return Poll::Ready(Ok(()));
                    }

                    encrypted_stream
                        .incoming_sender
                        .unbounded_send(data_filled.to_vec())
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "couldn't send incoming data")
                        })?;

                    data.clear();
                }
            }
        }
    }
}

impl Future for EncryptedStream {
    type Output = std::result::Result<(), io::Error>;

    #[allow(unused_must_use)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let encrypted_stream = Pin::into_inner(self);
        EncryptedStream::poll_outgoing(Pin::new(encrypted_stream), cx)?;
        EncryptedStream::poll_incoming(Pin::new(encrypted_stream), cx)
    }
}

impl AsyncRead for EncryptedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let encrypted_stream = Pin::into_inner(self);

        if encrypted_stream.shared_secret.is_none() {
            match encrypted_stream.session_receiver.try_recv() {
                Ok(Some(session)) => {
                    *encrypted_stream
                        .controller_id
                        .write()
                        .expect("setting controller_id") = Some(session.controller_id);
                    *encrypted_stream
                        .session_shared_secret
                        .write()
                        .expect("setting session shared secret") = Some(session.shared_secret);
                    encrypted_stream.shared_secret = Some(session.shared_secret);
                }
                _ => {
                    return AsyncRead::poll_read(Pin::new(&mut encrypted_stream.stream), cx, buf);
                }
            }
        }

        loop {
            if !encrypted_stream.decrypted_buf.is_empty() {
                let len = min(buf.remaining(), encrypted_stream.decrypted_buf.len());
                buf.put_slice(&encrypted_stream.decrypted_buf[..len]);
                encrypted_stream.decrypted_buf.advance(len);
                return Poll::Ready(Ok(()));
            }

            match encrypted_stream.decrypt_buffered_record() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return Poll::Ready(Err(error)),
            }

            let mut read_buf = [0u8; 4096];
            let mut read = ReadBuf::new(&mut read_buf);
            match AsyncRead::poll_read(Pin::new(&mut encrypted_stream.stream), cx, &mut read) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) if read.filled().is_empty() => {
                    if encrypted_stream.encrypted_buf.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "controller closed during an encrypted HAP record",
                    )));
                }
                Poll::Ready(Ok(())) => encrypted_stream
                    .encrypted_buf
                    .extend_from_slice(read.filled()),
            }
        }
    }
}

impl AsyncWrite for EncryptedStream {
    #[allow(unused_must_use)]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        let encrypted_stream = Pin::into_inner(self);

        if let Some(shared_secret) = encrypted_stream.shared_secret {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            if encrypted_stream.pending_encrypted_write.is_empty() {
                encrypted_stream.pending_plaintext_len = buf.len();
                for chunk in buf.chunks(1024) {
                    let (aad, ciphertext, auth_tag) = encrypt_chunk(
                        &shared_secret,
                        chunk,
                        &mut encrypted_stream.encrypt_count,
                    )
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "encryption failed"))?;
                    encrypted_stream
                        .pending_encrypted_write
                        .extend_from_slice(&aad);
                    encrypted_stream
                        .pending_encrypted_write
                        .extend_from_slice(&ciphertext);
                    encrypted_stream
                        .pending_encrypted_write
                        .extend_from_slice(&auth_tag);
                }
            }

            while !encrypted_stream.pending_encrypted_write.is_empty() {
                match AsyncWrite::poll_write(
                    Pin::new(&mut encrypted_stream.stream),
                    cx,
                    &encrypted_stream.pending_encrypted_write,
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            ErrorKind::WriteZero,
                            "could not write encrypted HAP data",
                        )));
                    }
                    Poll::Ready(Ok(written)) => {
                        encrypted_stream.pending_encrypted_write.advance(written);
                    }
                }
            }
            let written = encrypted_stream.pending_plaintext_len;
            encrypted_stream.pending_plaintext_len = 0;
            Poll::Ready(Ok(written))
        } else {
            AsyncWrite::poll_write(Pin::new(&mut encrypted_stream.stream), cx, buf)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<std::result::Result<(), Error>> {
        let encrypted_stream = Pin::into_inner(self);
        AsyncWrite::poll_flush(Pin::new(&mut encrypted_stream.stream), cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Poll<std::result::Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

fn decrypt_chunk(
    shared_secret: &[u8; 32],
    aad: &[u8],
    data: &[u8],
    auth_tag: &[u8],
    count: &mut u64,
) -> Result<Vec<u8>> {
    let read_key = compute_read_key(shared_secret)?;
    let aead = ChaCha20Poly1305::new(GenericArray::from_slice(&read_key));

    let mut nonce = vec![0; 4];
    let mut suffix = vec![0; 8];
    LittleEndian::write_u64(&mut suffix, *count);
    nonce.extend(suffix);
    *count += 1;

    let mut buffer = Vec::new();
    buffer.extend_from_slice(data);
    aead.decrypt_in_place_detached(
        Nonce::from_slice(&nonce),
        aad,
        &mut buffer,
        Tag::from_slice(&auth_tag),
    )?;

    Ok(buffer)
}

fn snapshot_body_len(response: &[u8]) -> Option<usize> {
    let body_offset = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?
        + 4;
    let body = &response[body_offset..];
    (body.starts_with(&[0xff, 0xd8]) && body.ends_with(&[0xff, 0xd9])).then_some(body.len())
}

fn notify_snapshot_delivery(
    handler: &pointer::SnapshotDeliveryHandler,
    delivery: pointer::SnapshotDelivery,
) {
    let callback = handler.read().ok().and_then(|handler| handler.clone());
    if let Some(callback) = callback {
        callback(delivery);
    }
}

fn encrypt_chunk(
    shared_secret: &[u8; 32],
    data: &[u8],
    count: &mut u64,
) -> Result<([u8; 2], Vec<u8>, [u8; 16])> {
    let write_key = compute_write_key(shared_secret)?;
    let aead = ChaCha20Poly1305::new(GenericArray::from_slice(&write_key));

    let mut nonce = vec![0; 4];
    let mut suffix = vec![0; 8];
    LittleEndian::write_u64(&mut suffix, *count);
    nonce.extend(suffix);
    *count += 1;

    let mut aad = [0; 2];
    LittleEndian::write_u16(&mut aad, data.len() as u16);

    let mut buffer = Vec::new();
    buffer.extend_from_slice(data);
    let auth_tag = aead.encrypt_in_place_detached(Nonce::from_slice(&nonce), &aad, &mut buffer)?;

    Ok((aad, buffer, auth_tag.into()))
}

fn compute_read_key(shared_secret: &[u8; 32]) -> Result<[u8; 32]> {
    compute_key(shared_secret, b"Control-Write-Encryption-Key")
}

fn compute_write_key(shared_secret: &[u8; 32]) -> Result<[u8; 32]> {
    compute_key(shared_secret, b"Control-Read-Encryption-Key")
}

fn compute_key(shared_secret: &[u8; 32], info: &[u8]) -> Result<[u8; 32]> {
    super::hkdf_extract_and_expand(b"Control-Salt", shared_secret, info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use hyper::{server::conn::Http, service::service_fn, Body, Response};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn delivery_handler() -> pointer::SnapshotDeliveryHandler {
        Arc::new(RwLock::new(None))
    }

    fn encrypt_controller_record(
        secret: &[u8; 32],
        plaintext: &[u8],
        counter: &mut u64,
    ) -> Vec<u8> {
        let key = compute_read_key(secret).unwrap();
        let aead = ChaCha20Poly1305::new(GenericArray::from_slice(&key));
        let mut nonce = [0u8; 12];
        LittleEndian::write_u64(&mut nonce[4..], *counter);
        *counter += 1;
        let mut aad = [0u8; 2];
        LittleEndian::write_u16(&mut aad, plaintext.len() as u16);
        let mut ciphertext = plaintext.to_vec();
        let tag = aead
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), &aad, &mut ciphertext)
            .unwrap();
        [aad.as_slice(), ciphertext.as_slice(), tag.as_slice()].concat()
    }

    fn decrypt_accessory_record(secret: &[u8; 32], wire: &[u8], counter: u64) -> Vec<u8> {
        let plaintext_len = LittleEndian::read_u16(&wire[..2]) as usize;
        assert_eq!(wire.len(), 2 + plaintext_len + 16);
        let key = compute_write_key(secret).unwrap();
        let aead = ChaCha20Poly1305::new(GenericArray::from_slice(&key));
        let mut nonce = [0u8; 12];
        LittleEndian::write_u64(&mut nonce[4..], counter);
        let mut plaintext = wire[2..2 + plaintext_len].to_vec();
        aead.decrypt_in_place_detached(
            Nonce::from_slice(&nonce),
            &wire[..2],
            &mut plaintext,
            Tag::from_slice(&wire[2 + plaintext_len..]),
        )
        .unwrap();
        plaintext
    }

    async fn connected_streams() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address);
        let server = listener.accept();
        let (client, server) = tokio::join!(client, server);
        let (server, _) = server.unwrap();
        (server, client.unwrap())
    }

    #[tokio::test]
    async fn encrypted_reader_accepts_fragmented_and_coalesced_records() {
        let (server, mut client) = connected_streams().await;
        let (mut encrypted, _, _, session_sender, _, _) =
            EncryptedStream::new(server, delivery_handler());
        let secret = [0x5a; 32];
        session_sender
            .send(Session {
                controller_id: Uuid::nil(),
                shared_secret: secret,
            })
            .unwrap();

        let first = b"first encrypted HAP request";
        let second = vec![0x42; 1024];
        let mut counter = 0;
        let mut wire = encrypt_controller_record(&secret, first, &mut counter);
        wire.extend_from_slice(&encrypt_controller_record(&secret, &second, &mut counter));

        client.write_all(&wire[..7]).await.unwrap();
        tokio::task::yield_now().await;
        client.write_all(&wire[7..]).await.unwrap();

        let mut plaintext = vec![0; first.len() + second.len()];
        encrypted.read_exact(&mut plaintext).await.unwrap();
        assert_eq!(&plaintext[..first.len()], first);
        assert_eq!(&plaintext[first.len()..], second);
    }

    #[tokio::test]
    async fn event_is_deferred_until_complete_http_response_is_flushed() {
        use crate::transport::http::{event_response, EventObject};

        let (_incoming_sender, incoming_receiver) = mpsc::unbounded();
        let (outgoing_sender, mut outgoing_receiver) = mpsc::unbounded();
        let incoming_waker = Arc::new(Mutex::new(None));
        let outgoing_waker = Arc::new(Mutex::new(None));
        let serialized =
            SerializedOutput::new(outgoing_sender, outgoing_waker.clone(), delivery_handler());
        let mut wrapper = StreamWrapper::new(
            incoming_receiver,
            serialized.clone(),
            incoming_waker,
            outgoing_waker,
        );

        wrapper
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")
            .await
            .unwrap();
        let event = event_response(vec![EventObject {
            iid: 2,
            aid: 1,
            value: serde_json::json!(true),
        }])
        .unwrap();
        serialized.send_event(event.clone()).unwrap();
        wrapper.write_all(b"JPEG").await.unwrap();
        wrapper.flush().await.unwrap();

        assert_eq!(
            outgoing_receiver.next().await.unwrap(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nJPEG"
        );
        assert_eq!(outgoing_receiver.next().await.unwrap(), event);
    }

    #[tokio::test]
    async fn encrypted_http_snapshot_round_trip_reports_delivery() {
        let (server, mut client) = connected_streams().await;
        let deliveries = Arc::new(AtomicU64::new(0));
        let delivery_bytes = Arc::new(AtomicU64::new(0));
        let deliveries_ = deliveries.clone();
        let delivery_bytes_ = delivery_bytes.clone();
        let handler: pointer::SnapshotDeliveryHandler =
            Arc::new(RwLock::new(Some(Arc::new(move |delivery| {
                if let pointer::SnapshotDelivery::Delivered { bytes } = delivery {
                    deliveries_.fetch_add(1, Ordering::Relaxed);
                    delivery_bytes_.fetch_add(bytes as u64, Ordering::Relaxed);
                }
            }))));
        let secret = [0x33; 32];
        let (encrypted, incoming, serialized, session_sender, incoming_waker, outgoing_waker) =
            EncryptedStream::new(server, handler);
        let wrapper = StreamWrapper::new(incoming, serialized, incoming_waker, outgoing_waker);
        session_sender
            .send(Session {
                controller_id: Uuid::nil(),
                shared_secret: secret,
            })
            .unwrap();

        let encrypted_task = tokio::spawn(encrypted);
        let http_task = tokio::spawn(async move {
            Http::new()
                .http1_only(true)
                .serve_connection(
                    wrapper,
                    service_fn(|_| async {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(200)
                                .header("Content-Type", "image/jpeg")
                                .header("Content-Length", "4")
                                .body(Body::from(vec![0xff, 0xd8, 0xff, 0xd9]))
                                .unwrap(),
                        )
                    }),
                )
                .await
                .unwrap();
        });

        let request = b"POST /resource HTTP/1.1\r\nHost: accessory\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let mut request_counter = 0;
        let wire_request = encrypt_controller_record(&secret, request, &mut request_counter);
        client.write_all(&wire_request).await.unwrap();

        let mut header = [0u8; 2];
        client.read_exact(&mut header).await.unwrap();
        let encrypted_len = LittleEndian::read_u16(&header) as usize + 16;
        let mut wire = Vec::from(header);
        wire.resize(2 + encrypted_len, 0);
        client.read_exact(&mut wire[2..]).await.unwrap();
        let response = decrypt_accessory_record(&secret, &wire, 0);
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(response.ends_with(&[0xff, 0xd8, 0xff, 0xd9]));
        tokio::task::yield_now().await;
        assert_eq!(deliveries.load(Ordering::Relaxed), 1);
        assert_eq!(delivery_bytes.load(Ordering::Relaxed), 4);

        drop(client);
        http_task.await.unwrap();
        encrypted_task.await.unwrap().unwrap();
    }
}
