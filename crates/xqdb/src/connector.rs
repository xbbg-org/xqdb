use crate::errors::XqdbError;
use crate::serde6::{compress, decompress, deserialize, serialize_into};
use crate::types::{MsgType, SymbolEncoding, K};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, StreamOwned};
use rustls_platform_verifier::BuilderVerifierExt;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) trait QStream: IoRead + IoWrite {}

impl<S: IoRead + IoWrite> QStream for S {}

/// Sized `Read` adapter over the boxed stream so `Read` combinators requiring `Self: Sized`
/// (e.g. `take`) apply to it.
struct StreamReader<'a>(&'a mut (dyn QStream + Send + Sync));

impl IoRead for StreamReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

#[derive(Debug)]
struct SharedTcpStream(Arc<TcpStream>);

impl IoRead for SharedTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.as_ref().read(buffer)
    }
}

impl IoWrite for SharedTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.as_ref().write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.as_ref().flush()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConnectorAbortHandle {
    active_stream: Arc<Mutex<Option<Arc<TcpStream>>>>,
}

impl ConnectorAbortHandle {
    pub fn abort(&self) -> Result<(), XqdbError> {
        let stream = self
            .active_stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match stream {
            Some(stream) => match stream.shutdown(Shutdown::Both) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
                Err(error) => Err(XqdbError::IOError(error)),
            },
            None => Ok(()),
        }
    }

    fn set_active_stream(&self, stream: Arc<TcpStream>) {
        *self
            .active_stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(stream);
    }
}

pub struct Connector {
    pub enable_tls: bool,
    pub is_local: bool,
    pub port: u16,
    pub version: u8,
    pub host: String,
    pub user: String,
    pub password: String,
    pub timeout: Duration,
    pub symbol_encoding: SymbolEncoding,
    stream: Option<Box<dyn QStream + Send + Sync>>,
    abort_handle: ConnectorAbortHandle,
}

const IPC_HEADER_LENGTH: usize = 8;
const MIN_SERIALIZED_VALUE_LENGTH: usize = 2;
/// Largest length the q IPC header can express: the byte at index 3 carries bits 32..40 and the
/// little-endian u32 at index 4..8 carries the low bits. This is the wire format's own limit, not
/// a policy ceiling, so nothing below it is refused.
const MAX_IPC_LENGTH_FIELD: usize = (1 << 40) - 1;
/// Upper bound on how far the q IPC decompressor can expand its input.
///
/// A group spends one control byte plus up to eight units; a back-reference unit spends two input
/// bytes and emits at most 257, so the worst case is 2056 output bytes per 17 input bytes (< 121x).
/// Output declared above this is unreachable from the payload already held in memory, so it is
/// rejected before the destination buffer is allocated and zeroed. Measured against this client's
/// own compressor on maximally compressible payloads: 120.935x at 10 MB, always below the bound.
const MAX_IPC_DECOMPRESSION_RATIO: u64 = 121;
/// First reservation for a message body, before the peer has delivered any of it, and the factor by
/// which it grows once full.
///
/// The floor is what a declared length alone can reserve, and the cost of gating is a chunked read
/// rather than the growth copies: measured against kola, a 64 KiB floor lost roughly a quarter of
/// the throughput on a 51 MiB table and cutting copy volume sevenfold did not recover it. A floor
/// above ordinary table payloads keeps those reads in a single pass, so only frames large enough to
/// be worth gating pay for a growth step.
const INITIAL_BODY_RESERVATION: usize = 32 * 1024 * 1024;
const BODY_RESERVATION_FACTOR: usize = 8;

/// Splits an IPC message length into the 40-bit form the q header carries, returning the high byte
/// written at index 3 and the little-endian u32 written at index 4..8.
pub(crate) fn ipc_length_header(total_length: usize) -> Result<(u8, u32), XqdbError> {
    if total_length > MAX_IPC_LENGTH_FIELD {
        return Err(XqdbError::Err(format!(
            "IPC message length {total_length} exceeds the {MAX_IPC_LENGTH_FIELD}-byte q header length field"
        )));
    }
    Ok(((total_length >> 32) as u8, total_length as u32))
}

fn checked_outgoing_message_length(
    body_length: usize,
    description: &str,
) -> Result<(usize, u8, u32), XqdbError> {
    let total_length = body_length
        .checked_add(IPC_HEADER_LENGTH)
        .ok_or_else(|| XqdbError::Err(format!("{description} length overflowed")))?;
    let (high_byte, low_length) = ipc_length_header(total_length)?;
    Ok((total_length, high_byte, low_length))
}

fn allocate_buffer(length: usize, description: &str) -> Result<Vec<u8>, XqdbError> {
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(length).map_err(|error| {
        XqdbError::Err(format!(
            "Unable to allocate {description} of {length} bytes: {error}"
        ))
    })?;
    Ok(buffer)
}

fn checked_body_length(total_length: u64, description: &str) -> Result<usize, XqdbError> {
    let total_length = usize::try_from(total_length).map_err(|_| {
        XqdbError::Err(format!(
            "{description} length cannot be represented on this platform"
        ))
    })?;
    let body_length = total_length.checked_sub(IPC_HEADER_LENGTH).ok_or_else(|| {
        XqdbError::Err(format!(
            "{description} length {total_length} is shorter than the {IPC_HEADER_LENGTH}-byte header"
        ))
    })?;
    if body_length < MIN_SERIALIZED_VALUE_LENGTH {
        return Err(XqdbError::Err(format!(
            "{description} body length {body_length} is too short to contain a serialized q value"
        )));
    }
    Ok(body_length)
}

/// Reads a declared message body, keeping the reservation within `BODY_RESERVATION_FACTOR` times
/// the bytes the peer has actually delivered.
///
/// A declared length alone must never turn into an allocation: on Windows, the platform this client
/// ships prebuilt binaries for, a large `HeapAlloc` is forwarded to `VirtualAlloc(MEM_COMMIT)` and
/// charged against the system commit limit immediately, so an 8-byte header claiming a terabyte
/// would starve every other allocation in the process rather than merely reserving address space.
/// The buffer therefore starts at `INITIAL_BODY_RESERVATION` and grows only once the previous
/// reservation is full. Frames at or below the floor are reserved exactly and read in one pass.
///
/// Each `take` limit is clamped to the bytes still owed, which preserves the frame boundary and
/// keeps `read_to_end` off the infallible `small_probe_read` growth path it takes when full.
fn read_message_body(
    stream: &mut (dyn QStream + Send + Sync),
    body_length: usize,
) -> Result<Vec<u8>, XqdbError> {
    let mut body = allocate_buffer(
        body_length.min(INITIAL_BODY_RESERVATION),
        "IPC message body",
    )?;
    while body.len() < body_length {
        if body.len() == body.capacity() {
            let target = body_length.min(body.capacity().saturating_mul(BODY_RESERVATION_FACTOR));
            body.try_reserve_exact(target - body.len())
                .map_err(|error| {
                    XqdbError::Err(format!(
                        "Unable to grow the IPC message body to {target} bytes: {error}"
                    ))
                })?;
        }
        let owed = body_length - body.len();
        let spare = (body.capacity() - body.len()).min(owed) as u64;
        match StreamReader(&mut *stream)
            .take(spare)
            .read_to_end(&mut body)
        {
            Ok(0) => {
                return Err(XqdbError::IOError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "IPC message body ended before its declared length",
                )))
            }
            Ok(_) => (),
            Err(error) => return Err(XqdbError::IOError(error)),
        }
    }
    Ok(body)
}

/// Validates a declared decompressed length against the payload that must produce it, so a corrupt
/// or hostile prefix cannot demand an allocation those compressed bytes could never fill.
fn checked_decompressed_body_length(
    declared_total_length: u64,
    compressed_payload_length: usize,
) -> Result<usize, XqdbError> {
    let body_length = checked_body_length(declared_total_length, "Decompressed IPC message")?;
    let maximum_body_length =
        (compressed_payload_length as u64).saturating_mul(MAX_IPC_DECOMPRESSION_RATIO);
    if body_length as u64 > maximum_body_length {
        return Err(XqdbError::DeserializationErr(format!(
            "Decompressed IPC message length {declared_total_length} is unreachable from {compressed_payload_length} compressed bytes, which expand to at most {maximum_body_length} bytes"
        )));
    }
    Ok(body_length)
}

fn allocate_zeroed_buffer(length: usize, description: &str) -> Result<Vec<u8>, XqdbError> {
    let mut buffer = allocate_buffer(length, description)?;
    buffer.resize(length, 0);
    Ok(buffer)
}

fn compressed_message_length(body: &[u8], mode: u8) -> Result<(u64, usize), XqdbError> {
    match mode {
        1 => {
            let prefix: [u8; 4] = body
                .get(..4)
                .ok_or_else(|| {
                    XqdbError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 1 requires a 4-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    XqdbError::Err("Invalid compression mode 1 length prefix".to_owned())
                })?;
            Ok((u64::from(u32::from_le_bytes(prefix)), 4))
        }
        2 => {
            let prefix: [u8; 8] = body
                .get(..8)
                .ok_or_else(|| {
                    XqdbError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 2 requires an 8-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    XqdbError::Err("Invalid compression mode 2 length prefix".to_owned())
                })?;
            Ok((u64::from_le_bytes(prefix), 8))
        }
        _ => Err(XqdbError::Err(format!(
            "Unsupported IPC compression mode {mode}"
        ))),
    }
}

fn connect_to_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
    timeout: Duration,
) -> io::Result<TcpStream> {
    let started = Instant::now();
    let mut last_error = None;
    for address in addresses {
        let result = if timeout.is_zero() {
            TcpStream::connect(address)
        } else {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            };
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            }
            TcpStream::connect_timeout(&address, remaining)
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "host resolved to no socket addresses",
        )
    }))
}

fn tls_client_config() -> Result<ClientConfig, XqdbError> {
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| XqdbError::Err(error.to_string()))?
            .with_platform_verifier()
            .map_err(|error| XqdbError::Err(error.to_string()))?;
    Ok(builder.with_no_client_auth())
}

fn tls_server_name(host: &str) -> Result<ServerName<'static>, XqdbError> {
    ServerName::try_from(host.to_owned())
        .map_err(|error| XqdbError::Err(format!("Invalid TLS server name: {error}")))
}

impl Connector {
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> Self {
        let host = if host.is_empty() { "127.0.0.1" } else { host };
        let is_local = host == "127.0.0.1" || host == "localhost";
        Connector {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            enable_tls,
            stream: None,
            abort_handle: ConnectorAbortHandle::default(),
            is_local,
            timeout: Duration::new(timeout, 0),
            version,
            symbol_encoding: SymbolEncoding::Strict,
        }
    }

    pub fn abort_handle(&self) -> ConnectorAbortHandle {
        self.abort_handle.clone()
    }

    fn auth(&self, q_stream: &mut impl QStream) -> Result<(), XqdbError> {
        let credential_length = self
            .user
            .len()
            .checked_add(self.password.len())
            .and_then(|length| length.checked_add(3))
            .ok_or_else(|| {
                XqdbError::Err("Authentication credential length overflowed".to_owned())
            })?;
        let mut credential = allocate_buffer(credential_length, "authentication credential")?;
        credential.extend_from_slice(self.user.as_bytes());
        credential.push(b':');
        credential.extend_from_slice(self.password.as_bytes());
        credential.push(self.version);
        credential.push(0);
        q_stream.write_all(&credential)?;
        let mut support_version = [0u8];
        match q_stream.read(&mut support_version) {
            Ok(read_length) => {
                if read_length == 1 {
                    if support_version[0] >= 1 {
                        Ok(())
                    } else {
                        Err(XqdbError::VersionErr())
                    }
                } else {
                    Err(XqdbError::AuthErr())
                }
            }
            Err(e) => Err(XqdbError::IOError(e)),
        }
    }

    pub fn send(&mut self, msg_type: MsgType, expr: &str, args: &[K]) -> Result<(), XqdbError> {
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let expr = expr.trim();
                if args.is_empty() {
                    let body_length = 6usize.checked_add(expr.len()).ok_or_else(|| {
                        XqdbError::Err("IPC request length overflowed".to_string())
                    })?;
                    let (total_length, high_byte, low_length) =
                        checked_outgoing_message_length(body_length, "IPC request")?;
                    let expression_length =
                        i32::try_from(expr.len()).map_err(|_| XqdbError::OverLengthErr())?;
                    let mut vec = allocate_buffer(total_length, "IPC request")?;
                    vec.write_all(&[1, msg_type as u8, 0, high_byte])?;
                    vec.write_all(&low_length.to_le_bytes())?;
                    vec.write_all(&[10, 0])?;
                    vec.write_all(&expression_length.to_le_bytes())?;
                    vec.write_all(expr.as_bytes())?;
                    match stream.write_all(&vec) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            self.shutdown()?;
                            Err(XqdbError::IOError(e))
                        }
                    }
                } else {
                    if args.len() > 8 {
                        return Err(XqdbError::TooManyArgumentErr());
                    }
                    let is_lambda = expr.starts_with('{') && expr.ends_with('}');
                    let body_prefix_length = 12usize
                        .checked_add(if is_lambda { 2 } else { 0 })
                        .and_then(|length| length.checked_add(expr.len()))
                        .ok_or_else(|| {
                            XqdbError::Err("IPC request length overflowed".to_string())
                        })?;
                    let mut body_length = body_prefix_length;
                    for k in args {
                        body_length = body_length.checked_add(k.j6_len()?).ok_or_else(|| {
                            XqdbError::Err("IPC request length overflowed".to_string())
                        })?;
                    }
                    let (total_length, high_byte, low_length) =
                        checked_outgoing_message_length(body_length, "IPC request")?;
                    let expression_length =
                        i32::try_from(expr.len()).map_err(|_| XqdbError::OverLengthErr())?;
                    let argument_count =
                        i32::try_from(args.len() + 1).map_err(|_| XqdbError::OverLengthErr())?;

                    let mut vec = allocate_buffer(total_length, "IPC request")?;
                    vec.extend_from_slice(&[1, msg_type as u8, 0, high_byte]);
                    vec.extend_from_slice(&low_length.to_le_bytes());
                    vec.extend_from_slice(&[0, 0]);
                    vec.extend_from_slice(&argument_count.to_le_bytes());
                    if is_lambda {
                        vec.extend_from_slice(&[100, 0]);
                    }
                    vec.extend_from_slice(&[10, 0]);
                    vec.extend_from_slice(&expression_length.to_le_bytes());
                    vec.extend_from_slice(expr.as_bytes());
                    for k in args {
                        serialize_into(k, &mut vec)?;
                    }
                    if vec.len() != total_length {
                        return Err(XqdbError::Err(
                            "Serialized argument length differs from its declared q length"
                                .to_string(),
                        ));
                    }
                    let payload = if self.is_local || total_length < 10_000_000 {
                        vec
                    } else {
                        compress(vec)?
                    };
                    match stream.write_all(&payload) {
                        Ok(_) => (),
                        Err(e) => {
                            self.shutdown()?;
                            return Err(XqdbError::IOError(e));
                        }
                    };
                    Ok(())
                }
            } else {
                Err(XqdbError::NotConnectedErr())
            }
        } else {
            Err(XqdbError::NotConnectedErr())
        }
    }

    pub fn receive(&mut self) -> Result<K, XqdbError> {
        let symbol_encoding = self.symbol_encoding;
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let mut header = [0u8; IPC_HEADER_LENGTH];
                match stream.read_exact(&mut header) {
                    Ok(_) => (),
                    Err(e) => {
                        self.shutdown()?;
                        return Err(XqdbError::IOError(e));
                    }
                };
                let encoding = header[0];
                if encoding == 0 {
                    self.shutdown()?;
                    return Err(XqdbError::NotSupportedBigEndianErr());
                }
                let compression_mode = header[2];
                if compression_mode > 2 {
                    self.shutdown()?;
                    return Err(XqdbError::Err(format!(
                        "Unsupported IPC compression mode {compression_mode}"
                    )));
                }
                let low_length = u64::from(u32::from_le_bytes([
                    header[4], header[5], header[6], header[7],
                ]));
                let high_length = match u64::from(header[3]).checked_shl(32) {
                    Some(length) => length,
                    None => {
                        self.shutdown()?;
                        return Err(XqdbError::Err(
                            "IPC message length extension overflowed".to_owned(),
                        ));
                    }
                };
                let total_length = match high_length.checked_add(low_length) {
                    Some(length) => length,
                    None => {
                        self.shutdown()?;
                        return Err(XqdbError::Err("IPC message length overflowed".to_owned()));
                    }
                };
                let body_length = match checked_body_length(total_length, "IPC message") {
                    Ok(length) => length,
                    Err(error) => {
                        self.shutdown()?;
                        return Err(error);
                    }
                };
                let vec = match read_message_body(stream.as_mut(), body_length) {
                    Ok(vec) => vec,
                    Err(error) => {
                        self.shutdown()?;
                        return Err(error);
                    }
                };
                if compression_mode == 1 || compression_mode == 2 {
                    let (decompressed_length, prefix_length) =
                        match compressed_message_length(&vec, compression_mode) {
                            Ok(length) => length,
                            Err(error) => {
                                self.shutdown()?;
                                return Err(error);
                            }
                        };
                    let decompressed_body_length = match checked_decompressed_body_length(
                        decompressed_length,
                        vec.len().saturating_sub(prefix_length),
                    ) {
                        Ok(length) => length,
                        Err(error) => {
                            self.shutdown()?;
                            return Err(error);
                        }
                    };
                    let mut decompressed = match allocate_zeroed_buffer(
                        decompressed_body_length,
                        "decompressed IPC body",
                    ) {
                        Ok(vec) => vec,
                        Err(error) => {
                            self.shutdown()?;
                            return Err(error);
                        }
                    };
                    if let Err(error) = decompress(&vec, &mut decompressed, prefix_length) {
                        // A frame that cannot be decompressed is protocol-malformed, so drop the
                        // connection instead of letting a peer repeat the allocation indefinitely.
                        self.shutdown()?;
                        return Err(error);
                    }
                    deserialize(&decompressed, &mut 0, symbol_encoding)
                } else {
                    deserialize(&vec, &mut 0, symbol_encoding)
                }
            } else {
                Err(XqdbError::NotConnectedErr())
            }
        } else {
            Err(XqdbError::NotConnectedErr())
        }
    }

    pub fn connect(&mut self) -> Result<(), XqdbError> {
        if self.stream.is_some() {
            return Ok(());
        }

        let tls = if self.enable_tls {
            Some((tls_server_name(&self.host)?, tls_client_config()?))
        } else {
            None
        };
        let mut addresses = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(XqdbError::IOError)?
            .peekable();
        if addresses.peek().is_none() {
            return Err(XqdbError::FailedToConnectErr(
                "host resolved to no socket addresses".to_owned(),
            ));
        }
        let tcp_stream =
            connect_to_addresses(addresses, self.timeout).map_err(XqdbError::IOError)?;
        tcp_stream.set_nodelay(true)?;
        if !self.timeout.is_zero() {
            tcp_stream
                .set_read_timeout(Some(self.timeout))
                .map_err(XqdbError::IOError)?;
            tcp_stream.set_write_timeout(Some(self.timeout))?;
        }

        let tcp_stream = Arc::new(tcp_stream);
        self.abort_handle.set_active_stream(Arc::clone(&tcp_stream));
        let shared_stream = SharedTcpStream(tcp_stream);
        let result = match tls {
            Some((server_name, config)) => {
                rustls::ClientConnection::new(Arc::new(config), server_name)
                    .map_err(|error| XqdbError::Err(error.to_string()))
                    .and_then(|connection| {
                        self.install_authenticated_stream(StreamOwned::new(
                            connection,
                            shared_stream,
                        ))
                    })
            }
            None => self.install_authenticated_stream(shared_stream),
        };
        if result.is_err() {
            let _ = self.abort_handle.abort();
        }
        result
    }

    pub fn shutdown(&mut self) -> Result<(), XqdbError> {
        if self.stream.take().is_none() {
            return Err(XqdbError::NotConnectedErr());
        }
        self.abort_handle.abort()
    }

    fn install_authenticated_stream<S>(&mut self, mut stream: S) -> Result<(), XqdbError>
    where
        S: QStream + Send + Sync + 'static,
    {
        self.auth(&mut stream)?;
        self.stream = Some(Box::new(stream));
        Ok(())
    }

    pub fn execute(&mut self, expr: &str, args: &[K]) -> Result<K, XqdbError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Sync, expr, args)?;
        self.receive()
    }

    pub fn execute_async(&mut self, expr: &str, args: &[K]) -> Result<(), XqdbError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Async, expr, args)
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        self.stream = None;
        let _ = self.abort_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde6::serialize;
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn connector_with_response(response: Vec<u8>) -> Connector {
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector.stream = Some(Box::new(Cursor::new(response)));
        connector
    }

    fn response_header(compression_mode: u8, total_length: u64) -> Vec<u8> {
        let mut response = vec![
            1,
            2,
            compression_mode,
            (total_length >> 32) as u8,
            0,
            0,
            0,
            0,
        ];
        response[4..8].copy_from_slice(&(total_length as u32).to_le_bytes());
        response
    }

    #[test]
    fn receive_rejects_short_header() {
        let error = connector_with_response(vec![1, 2, 0])
            .receive()
            .expect_err("short IPC header should fail");
        assert!(matches!(error, XqdbError::IOError(_)));
    }

    #[test]
    fn receive_rejects_length_shorter_than_header() {
        let mut connector = connector_with_response(response_header(0, 7));
        let error = connector
            .receive()
            .expect_err("invalid IPC length should fail");
        assert!(error.to_string().contains("shorter than the 8-byte header"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_unknown_compression_mode_and_disconnects() {
        let mut connector = connector_with_response(response_header(3, 10));
        let error = connector
            .receive()
            .expect_err("unknown compression mode should fail");
        assert!(error
            .to_string()
            .contains("Unsupported IPC compression mode 3"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_body_too_short_for_a_q_value() {
        for total_length in [8, 9] {
            let error = connector_with_response(response_header(0, total_length))
                .receive()
                .expect_err("empty or one-byte IPC body should fail");
            assert!(error
                .to_string()
                .contains("too short to contain a serialized q value"));
        }
    }

    #[test]
    fn receive_rejects_short_compression_prefixes() {
        for (compression_mode, prefix_length) in [(1, 4usize), (2, 8usize)] {
            let body = vec![0; prefix_length - 1];
            let error = compressed_message_length(&body, compression_mode)
                .expect_err("short compression prefix should fail");
            let message = error.to_string();
            assert!(
                message.contains("requires") && message.contains(&format!("{prefix_length}-byte")),
                "unexpected error: {message}"
            );
        }
    }

    #[test]
    fn receive_rejects_a_compressed_prefix_without_payload() {
        let mut response = response_header(1, 12);
        response.extend_from_slice(&10u32.to_le_bytes());
        assert!(matches!(
            connector_with_response(response).receive(),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn outgoing_frame_length_is_bounded_only_by_the_q_length_field() {
        let maximum_body = MAX_IPC_LENGTH_FIELD - IPC_HEADER_LENGTH;
        let (total_length, high_byte, low_length) =
            checked_outgoing_message_length(maximum_body, "test frame").expect("limit-sized frame");
        assert_eq!(total_length, MAX_IPC_LENGTH_FIELD);
        assert_eq!(high_byte, 0xff);
        assert_eq!(low_length, u32::MAX);
        // A frame above 4 GiB is emitted through the 40-bit form rather than refused.
        let (_, high_byte, low_length) =
            checked_outgoing_message_length(4 * 1024 * 1024 * 1024, "test frame")
                .expect("frame above u32::MAX");
        assert_eq!(high_byte, 1);
        assert_eq!(low_length, 8);

        let message = checked_outgoing_message_length(maximum_body + 1, "test frame")
            .expect_err("frame beyond the length field must fail")
            .to_string();
        assert!(
            message.contains("q header length field") || message.contains("length overflowed"),
            "unexpected error: {message}"
        );
        assert!(checked_outgoing_message_length(usize::MAX, "test frame").is_err());
    }

    #[test]
    fn allocation_helpers_reserve_fallibly_and_zero_on_request() {
        let buffer = allocate_buffer(32, "test buffer").expect("allocation should succeed");
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= 32);
        assert_eq!(
            allocate_zeroed_buffer(4, "test buffer").expect("allocation should succeed"),
            vec![0; 4]
        );
        allocate_buffer(usize::MAX, "oversized test buffer")
            .expect_err("capacity overflow should be reported");
    }

    #[test]
    fn receive_rejects_trailing_frame_bytes() {
        let mut body = serialize(&K::I32(42)).expect("test value should serialize");
        body.push(0);
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut response = response_header(0, total_length);
        response.extend_from_slice(&body);
        let error = connector_with_response(response)
            .receive()
            .expect_err("trailing frame bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn receive_rejects_decompressed_length_unreachable_from_its_payload() {
        // An 8-byte prefix plus 8 compressed bytes expands to at most 968 bytes.
        let mut response = response_header(2, 24);
        response.extend_from_slice(&(64 * 1024 * 1024u64).to_le_bytes());
        response.extend_from_slice(&[0u8; 8]);
        let message = connector_with_response(response)
            .receive()
            .expect_err("unreachable decompressed length should fail")
            .to_string();
        assert!(
            message.contains("Decompressed IPC message length 67108864")
                && message.contains("unreachable from 8 compressed bytes"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn receive_rejects_an_absurd_decompressed_length_without_an_absolute_ceiling() {
        // u64::MAX is addressable on a 64-bit target, so only the expansion bound rejects it.
        let mut response = response_header(2, 16);
        response.extend_from_slice(&u64::MAX.to_le_bytes());
        let message = connector_with_response(response)
            .receive()
            .expect_err("absurd decompressed length should fail")
            .to_string();
        assert!(
            message.contains("Decompressed IPC message length 18446744073709551615")
                && message.contains("unreachable from 0 compressed bytes"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn no_absolute_ceiling_rejects_a_large_frame() {
        // Both lengths from the report that the retired 512 MiB ceiling rejected: the wire frame
        // and, for a compressed response, the declared decompressed length.
        let total_length = 706_440_911u64;
        let expected_body_length =
            usize::try_from(total_length).expect("length fits usize") - IPC_HEADER_LENGTH;
        assert_eq!(
            checked_body_length(total_length, "IPC message").expect("large frame length"),
            expected_body_length
        );
        // 6 MB of compressed payload expands 118x here, inside the decompressor's 121x reach.
        assert_eq!(
            checked_decompressed_body_length(total_length, 6_000_000)
                .expect("large decompressed length"),
            expected_body_length
        );
        // Nothing below the q length field is refused, including lengths far above 4 GiB.
        let terabyte = 1024 * 1024 * 1024 * 1024u64;
        assert_eq!(
            checked_body_length(terabyte, "IPC message").expect("terabyte frame length"),
            usize::try_from(terabyte).expect("length fits usize") - IPC_HEADER_LENGTH
        );
    }

    #[test]
    fn a_declared_length_alone_does_not_reserve_the_body() {
        // 512 GiB declared, 64 bytes delivered. The gate means the reservation never leaves the
        // initial chunk, so this fails as a short read; reserving the declared length up front
        // would instead fail as an allocation error wherever the allocator refuses 512 GiB.
        let mut response = response_header(0, 512 * 1024 * 1024 * 1024);
        response.extend_from_slice(&[0u8; 64]);
        let error = connector_with_response(response)
            .receive()
            .expect_err("a stalled oversized frame must fail");
        assert!(
            matches!(&error, XqdbError::IOError(io_error)
                if io_error.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn receive_stops_at_the_declared_length_and_reports_a_short_body() {
        // A body that ends early fails as a short read instead of deserializing a partial frame.
        let mut response = response_header(0, 4096);
        response.extend_from_slice(&[0u8; 64]);
        let error = connector_with_response(response)
            .receive()
            .expect_err("a truncated frame must fail");
        assert!(
            matches!(&error, XqdbError::IOError(io_error)
                if io_error.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );

        let mut body = serialize(&K::CharVector(vec![b'z'; 32])).expect("value should serialize");
        let declared = u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("length fits u64");
        let mut response = response_header(0, declared);
        response.append(&mut body);
        // A trailing byte of a following frame must survive: the read stops at the declared length.
        response.push(0xaa);
        let mut connector = connector_with_response(response);
        assert_eq!(
            connector.receive().expect("small frame should deserialize"),
            K::CharVector(vec![b'z'; 32])
        );
    }

    #[test]
    fn receive_decompresses_a_genuine_compressed_frame() {
        let body =
            serialize(&K::CharVector(vec![b'a'; 4096])).expect("test value should serialize");
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut frame = response_header(0, total_length);
        frame.extend_from_slice(&body);
        let compressed = compress(frame).expect("test frame should compress");
        assert_eq!(compressed[2], 1, "the test frame must arrive compressed");

        let value = connector_with_response(compressed)
            .receive()
            .expect("compressed frame should decompress");
        assert_eq!(value, K::CharVector(vec![b'a'; 4096]));
    }

    #[test]
    fn receive_applies_the_connector_symbol_encoding() {
        let body = [245, b'c', b'a', b'f', 0xe9, 0];
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut frame = response_header(0, total_length);
        frame.extend_from_slice(&body);

        let mut strict = connector_with_response(frame.clone());
        assert_eq!(strict.symbol_encoding, SymbolEncoding::Strict);
        assert!(matches!(
            strict.receive(),
            Err(XqdbError::DeserializationErr(_))
        ));

        let mut lossy = connector_with_response(frame);
        lossy.symbol_encoding = SymbolEncoding::Lossy;
        assert_eq!(
            lossy.receive().expect("lossy symbol atom should decode"),
            K::Symbol("caf\u{FFFD}".to_string())
        );
    }

    #[test]
    fn tls_client_config_uses_platform_verification() {
        tls_client_config().expect("platform verifier configuration should build");
    }

    #[test]
    fn invalid_tls_server_name_returns_an_error_before_networking() {
        let mut connector = Connector::new("not a valid server name", 0, "", "", true, 0, 6);
        let error = connector
            .connect()
            .expect_err("invalid TLS server name should fail");
        assert!(matches!(
            error,
            XqdbError::Err(message) if message.contains("Invalid TLS server name")
        ));
    }

    #[test]
    fn connection_attempts_each_resolved_address() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let available = listener
            .local_addr()
            .expect("listener should have an address");
        let unavailable = SocketAddr::from(([127, 0, 0, 1], 0));

        let stream = connect_to_addresses([unavailable, available], Duration::from_secs(1))
            .expect("second address should connect");
        let (accepted, _) = listener
            .accept()
            .expect("listener should accept connection");
        drop((stream, accepted));
    }

    #[test]
    fn abort_handle_interrupts_active_io_and_is_idempotent() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept connection");
            let mut credential = [0; 3];
            stream
                .read_exact(&mut credential)
                .expect("server should read authentication");
            stream
                .write_all(&[6])
                .expect("server should acknowledge authentication");
            let mut byte = [0];
            let _ = stream.read(&mut byte);
        });

        let mut connector = Connector::new("127.0.0.1", address.port(), "", "", false, 2, 6);
        let abort_handle = connector.abort_handle();
        connector.connect().expect("connector should authenticate");
        let worker = thread::spawn(move || connector.receive());

        let started = std::time::Instant::now();
        abort_handle.abort().expect("first abort should succeed");
        abort_handle
            .abort()
            .expect("repeated abort should be idempotent");
        let error = worker
            .join()
            .expect("connector worker should not panic")
            .expect_err("aborted receive should fail");
        assert!(matches!(error, XqdbError::IOError(_)));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "abort should interrupt receive before the configured socket timeout"
        );
        server.join().expect("server should not panic");
    }
}
