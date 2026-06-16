//! stryke-zmq — ZeroMQ cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn zmq__*` is a JSON-string-in /
//! JSON-string-out wrapper around the `zmq` crate (which vendors libzmq).
//! stryke's FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols
//! at first `use Zmq`, registers each as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string.
//!
//! Persistent state — the whole point of a cdylib over a fork-per-call helper:
//!   * `CONTEXT` — one shared `zmq::Context` for the process. Cheap to share
//!     (`RawContext` is `Send + Sync`); all sockets are spawned from it.
//!   * `SOCKETS` — `HashMap<u64, Socket>` registry. ZMQ sockets are *stateful
//!     and long-lived*: a SUB must persist to keep receiving, REQ/REP is a
//!     send→recv state machine, PUB needs the connection held open. A
//!     fork-per-call model (create socket, do one op, drop) loses SUB
//!     messages between calls and re-pays connection setup every time —
//!     the exact anti-pattern the kafka v1 helper had. So `zmq__socket`
//!     returns an integer handle the caller keeps; later `send`/`recv`/
//!     `subscribe` reference it.
//!
//! `zmq::Socket` is `Send` but not `Sync`. The `Mutex` both serializes
//! access (ZMQ requires one-thread-at-a-time per socket) and provides the
//! full memory fence ZMQ mandates when a socket migrates between threads.
//!
//! Surface: full socket lifecycle, the complete libzmq socket-option
//! table (set + get), binary-safe send/recv (utf8/hex/base64 framing),
//! dynamic bind/connect/unbind/disconnect, single- and multi-socket poll,
//! socket-event monitoring, a backgrounded `proxy`/steerable proxy device,
//! CURVE keypair generation + z85 codec, the vendored libzmq version, and
//! capability probing via `has`.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};
use zmq::{Context, Socket, SocketType};

// ── shared context + socket registry ────────────────────────────────────────

static CONTEXT: OnceCell<Context> = OnceCell::new();

fn ctx() -> &'static Context {
    CONTEXT.get_or_init(Context::new)
}

static SOCKETS: OnceCell<Mutex<HashMap<u64, Socket>>> = OnceCell::new();

fn sockets() -> &'static Mutex<HashMap<u64, Socket>> {
    SOCKETS.get_or_init(|| Mutex::new(HashMap::new()))
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

fn next_handle() -> u64 {
    NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
}

/// Run a closure against the socket registered under `handle`, holding the
/// registry lock for the duration. The lock gives both ZMQ's required
/// single-threaded access and the cross-thread memory fence.
fn with_socket<F, T>(handle: u64, f: F) -> Result<T>
where
    F: FnOnce(&Socket) -> Result<T>,
{
    let map = sockets().lock();
    let sock = map
        .get(&handle)
        .ok_or_else(|| anyhow!("unknown socket handle: {handle}"))?;
    f(sock)
}

/// Remove a socket from the registry, returning it for ownership transfer
/// (used by `proxy`, which moves sockets into a background thread).
fn take_socket(handle: u64) -> Result<Socket> {
    sockets()
        .lock()
        .remove(&handle)
        .ok_or_else(|| anyhow!("unknown socket handle: {handle}"))
}

// ── arg helpers ─────────────────────────────────────────────────────────────

fn req_u64(opts: &Value, key: &str) -> Result<u64> {
    opts.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing {key}"))
}

fn req_str<'a>(opts: &'a Value, key: &str) -> Result<&'a str> {
    opts.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing {key}"))
}

/// Accept either a single string or an array of strings under `key`.
fn str_list(opts: &Value, key: &str) -> Vec<String> {
    match opts.get(key) {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

fn parse_socket_type(s: &str) -> Result<SocketType> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "req" => SocketType::REQ,
        "rep" => SocketType::REP,
        "pub" | "publish" => SocketType::PUB,
        "sub" | "subscribe" => SocketType::SUB,
        "push" => SocketType::PUSH,
        "pull" => SocketType::PULL,
        "dealer" => SocketType::DEALER,
        "router" => SocketType::ROUTER,
        "pair" => SocketType::PAIR,
        "xpub" => SocketType::XPUB,
        "xsub" => SocketType::XSUB,
        "stream" => SocketType::STREAM,
        other => return Err(anyhow!("unknown socket type: {other}")),
    })
}

/// Stringify a `SocketType` back to its lowercase name (for `get type`).
fn socket_type_name(ty: SocketType) -> &'static str {
    match ty {
        SocketType::REQ => "req",
        SocketType::REP => "rep",
        SocketType::PUB => "pub",
        SocketType::SUB => "sub",
        SocketType::PUSH => "push",
        SocketType::PULL => "pull",
        SocketType::DEALER => "dealer",
        SocketType::ROUTER => "router",
        SocketType::PAIR => "pair",
        SocketType::XPUB => "xpub",
        SocketType::XSUB => "xsub",
        SocketType::STREAM => "stream",
    }
}

// ── binary-safe payload framing ──────────────────────────────────────────────
//
// ZMQ frames are arbitrary bytes; stryke strings are UTF-8. The default
// "utf8" encoding round-trips text losslessly but mangles binary (lossy
// replacement on recv). "hex" and "base64" carry arbitrary bytes intact.
// Encoders are inlined to keep the cdylib dependency-free and vendorable.

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err(anyhow!("hex payload must have an even length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| anyhow!("invalid hex byte at offset {i}"))
        })
        .collect()
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(B64[(n >> 18 & 0x3f) as usize] as char);
        out.push(B64[(n >> 12 & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6 & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(anyhow!("invalid base64 character")),
        }
    }
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !s.len().is_multiple_of(4) {
        return Err(anyhow!("base64 payload length must be a multiple of 4"));
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let n = (val(chunk[0])? << 18)
            | (val(chunk[1])? << 12)
            | (if chunk[2] == b'=' { 0 } else { val(chunk[2])? } << 6)
            | (if chunk[3] == b'=' { 0 } else { val(chunk[3])? });
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

/// Decode an outbound payload string into raw bytes per `encoding`
/// (default "utf8"). Used by every send path.
fn payload_to_bytes(opts: &Value, data: &str) -> Result<Vec<u8>> {
    match opts
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("utf8")
    {
        "utf8" | "text" => Ok(data.as_bytes().to_vec()),
        "hex" => hex_decode(data),
        "base64" | "b64" => base64_decode(data),
        other => Err(anyhow!("unknown encoding: {other} (want utf8|hex|base64)")),
    }
}

/// Encode received raw bytes into a stryke string per `encoding`
/// (default "utf8", lossy). Used by every recv path.
fn bytes_to_payload(opts: &Value, bytes: &[u8]) -> Result<String> {
    Ok(
        match opts
            .get("encoding")
            .and_then(Value::as_str)
            .unwrap_or("utf8")
        {
            "utf8" | "text" => String::from_utf8_lossy(bytes).into_owned(),
            "hex" => hex_encode(bytes),
            "base64" | "b64" => base64_encode(bytes),
            other => return Err(anyhow!("unknown encoding: {other} (want utf8|hex|base64)")),
        },
    )
}

// ── socket option table ──────────────────────────────────────────────────────

/// Apply one settable socket option. The single source of truth for both
/// `socket` creation opts and the standalone `set` op, so the two never drift.
/// Covers the full libzmq settable surface the `zmq` 0.10 crate exposes.
fn set_one_opt(sock: &Socket, key: &str, val: &Value) -> Result<()> {
    let i32v = || -> Result<i32> {
        val.as_i64()
            .map(|v| v as i32)
            .ok_or_else(|| anyhow!("{key} expects an integer"))
    };
    let i64v = || -> Result<i64> {
        val.as_i64()
            .ok_or_else(|| anyhow!("{key} expects an integer"))
    };
    let u64v = || -> Result<u64> {
        val.as_u64()
            .ok_or_else(|| anyhow!("{key} expects an unsigned integer"))
    };
    let boolv = || -> Result<bool> {
        val.as_bool()
            .ok_or_else(|| anyhow!("{key} expects a boolean"))
    };
    let strv = || -> Result<&str> {
        val.as_str()
            .ok_or_else(|| anyhow!("{key} expects a string"))
    };
    match key {
        // i32 tuning
        "sndhwm" => sock.set_sndhwm(i32v()?)?,
        "rcvhwm" => sock.set_rcvhwm(i32v()?)?,
        "linger" => sock.set_linger(i32v()?)?,
        "sndtimeo" => sock.set_sndtimeo(i32v()?)?,
        "rcvtimeo" => sock.set_rcvtimeo(i32v()?)?,
        "sndbuf" => sock.set_sndbuf(i32v()?)?,
        "rcvbuf" => sock.set_rcvbuf(i32v()?)?,
        "rate" => sock.set_rate(i32v()?)?,
        "recovery_ivl" => sock.set_recovery_ivl(i32v()?)?,
        "reconnect_ivl" => sock.set_reconnect_ivl(i32v()?)?,
        "reconnect_ivl_max" => sock.set_reconnect_ivl_max(i32v()?)?,
        "backlog" => sock.set_backlog(i32v()?)?,
        "multicast_hops" => sock.set_multicast_hops(i32v()?)?,
        "tos" => sock.set_tos(i32v()?)?,
        "connect_timeout" => sock.set_connect_timeout(i32v()?)?,
        "handshake_ivl" => sock.set_handshake_ivl(i32v()?)?,
        "heartbeat_ivl" => sock.set_heartbeat_ivl(i32v()?)?,
        "heartbeat_ttl" => sock.set_heartbeat_ttl(i32v()?)?,
        "heartbeat_timeout" => sock.set_heartbeat_timeout(i32v()?)?,
        "tcp_keepalive" => sock.set_tcp_keepalive(i32v()?)?,
        "tcp_keepalive_cnt" => sock.set_tcp_keepalive_cnt(i32v()?)?,
        "tcp_keepalive_idle" => sock.set_tcp_keepalive_idle(i32v()?)?,
        "tcp_keepalive_intvl" => sock.set_tcp_keepalive_intvl(i32v()?)?,
        // wider ints
        "maxmsgsize" => sock.set_maxmsgsize(i64v()?)?,
        "affinity" => sock.set_affinity(u64v()?)?,
        // booleans
        "ipv6" => sock.set_ipv6(boolv()?)?,
        "immediate" => sock.set_immediate(boolv()?)?,
        "conflate" => sock.set_conflate(boolv()?)?,
        "probe_router" => sock.set_probe_router(boolv()?)?,
        "router_mandatory" => sock.set_router_mandatory(boolv()?)?,
        "router_handover" => sock.set_router_handover(boolv()?)?,
        "req_relaxed" => sock.set_req_relaxed(boolv()?)?,
        "req_correlate" => sock.set_req_correlate(boolv()?)?,
        "xpub_verbose" => sock.set_xpub_verbose(boolv()?)?,
        "plain_server" => sock.set_plain_server(boolv()?)?,
        "curve_server" => sock.set_curve_server(boolv()?)?,
        "gssapi_server" => sock.set_gssapi_server(boolv()?)?,
        "gssapi_plaintext" => sock.set_gssapi_plaintext(boolv()?)?,
        // byte-string identity / topic filters
        "identity" => sock.set_identity(strv()?.as_bytes())?,
        "subscribe" => sock.set_subscribe(strv()?.as_bytes())?,
        "unsubscribe" => sock.set_unsubscribe(strv()?.as_bytes())?,
        // CURVE keys — accept 40-char z85 or raw bytes; libzmq detects length
        "curve_publickey" => sock.set_curve_publickey(strv()?.as_bytes())?,
        "curve_secretkey" => sock.set_curve_secretkey(strv()?.as_bytes())?,
        "curve_serverkey" => sock.set_curve_serverkey(strv()?.as_bytes())?,
        // plain text auth + misc strings
        "plain_username" => sock.set_plain_username(Some(strv()?))?,
        "plain_password" => sock.set_plain_password(Some(strv()?))?,
        "socks_proxy" => sock.set_socks_proxy(Some(strv()?))?,
        "zap_domain" => sock.set_zap_domain(strv()?)?,
        "xpub_welcome_msg" => sock.set_xpub_welcome_msg(Some(strv()?))?,
        "gssapi_principal" => sock.set_gssapi_principal(strv()?)?,
        "gssapi_service_principal" => sock.set_gssapi_service_principal(strv()?)?,
        other => return Err(anyhow!("unknown or unsettable option: {other}")),
    }
    Ok(())
}

/// Read one socket option. Returns a JSON scalar matching the option's type;
/// byte-string options come back hex-encoded and CURVE keys as z85.
fn get_one_opt(sock: &Socket, key: &str) -> Result<Value> {
    /// Collapse the crate's `Result<Result<String, Vec<u8>>>` (valid-UTF-8 vs
    /// raw-bytes) into a JSON string, hex-encoding the non-UTF-8 case.
    fn str_or_hex(r: zmq::Result<std::result::Result<String, Vec<u8>>>) -> Result<Value> {
        Ok(match r? {
            Ok(s) => Value::String(s),
            Err(b) => Value::String(hex_encode(&b)),
        })
    }
    Ok(match key {
        "type" => json!(socket_type_name(sock.get_socket_type()?)),
        "rcvmore" => json!(sock.get_rcvmore()?),
        "sndhwm" => json!(sock.get_sndhwm()?),
        "rcvhwm" => json!(sock.get_rcvhwm()?),
        "linger" => json!(sock.get_linger()?),
        "sndtimeo" => json!(sock.get_sndtimeo()?),
        "rcvtimeo" => json!(sock.get_rcvtimeo()?),
        "sndbuf" => json!(sock.get_sndbuf()?),
        "rcvbuf" => json!(sock.get_rcvbuf()?),
        "rate" => json!(sock.get_rate()?),
        "recovery_ivl" => json!(sock.get_recovery_ivl()?),
        "reconnect_ivl" => json!(sock.get_reconnect_ivl()?),
        "reconnect_ivl_max" => json!(sock.get_reconnect_ivl_max()?),
        "backlog" => json!(sock.get_backlog()?),
        "multicast_hops" => json!(sock.get_multicast_hops()?),
        "tos" => json!(sock.get_tos()?),
        "connect_timeout" => json!(sock.get_connect_timeout()?),
        "handshake_ivl" => json!(sock.get_handshake_ivl()?),
        "heartbeat_ivl" => json!(sock.get_heartbeat_ivl()?),
        "heartbeat_ttl" => json!(sock.get_heartbeat_ttl()?),
        "heartbeat_timeout" => json!(sock.get_heartbeat_timeout()?),
        "tcp_keepalive" => json!(sock.get_tcp_keepalive()?),
        "maxmsgsize" => json!(sock.get_maxmsgsize()?),
        "affinity" => json!(sock.get_affinity()?),
        "fd" => json!(sock.get_fd()?),
        "mechanism" => json!(format!("{:?}", sock.get_mechanism()?)),
        "identity" => json!(hex_encode(&sock.get_identity()?)),
        "last_endpoint" => str_or_hex(sock.get_last_endpoint())?,
        "socks_proxy" => str_or_hex(sock.get_socks_proxy())?,
        "plain_username" => str_or_hex(sock.get_plain_username())?,
        "plain_password" => str_or_hex(sock.get_plain_password())?,
        "zap_domain" => str_or_hex(sock.get_zap_domain())?,
        "curve_publickey" => {
            json!(zmq::z85_encode(&sock.get_curve_publickey()?).map_err(|e| anyhow!("{e}"))?)
        }
        "curve_secretkey" => {
            json!(zmq::z85_encode(&sock.get_curve_secretkey()?).map_err(|e| anyhow!("{e}"))?)
        }
        "curve_serverkey" => {
            json!(zmq::z85_encode(&sock.get_curve_serverkey()?).map_err(|e| anyhow!("{e}"))?)
        }
        other => return Err(anyhow!("unknown or unreadable option: {other}")),
    })
}

/// Apply the creation-time opts: every settable socket option, plus the
/// `bind`/`connect`/`subscribe` connection keys.
fn apply_opts(sock: &Socket, opts: &Value) -> Result<()> {
    // Reserved keys are consumed elsewhere; everything else is a socket option.
    const RESERVED: &[&str] = &["type", "bind", "connect", "subscribe"];
    if let Some(map) = opts.as_object() {
        for (k, v) in map {
            if RESERVED.contains(&k.as_str()) {
                continue;
            }
            set_one_opt(sock, k, v)?;
        }
    }
    for ep in str_list(opts, "bind") {
        sock.bind(&ep)?;
    }
    for ep in str_list(opts, "connect") {
        sock.connect(&ep)?;
    }
    // SUB sockets must subscribe to receive anything; empty string = all.
    for topic in str_list(opts, "subscribe") {
        sock.set_subscribe(topic.as_bytes())?;
    }
    Ok(())
}

// ── ops ─────────────────────────────────────────────────────────────────────

fn op_socket(opts: Value) -> Result<Value> {
    let ty_name = req_str(&opts, "type")?.to_string();
    let ty = parse_socket_type(&ty_name)?;
    let sock = ctx().socket(ty)?;
    apply_opts(&sock, &opts)?;
    let handle = next_handle();
    sockets().lock().insert(handle, sock);
    Ok(json!({"handle": handle, "type": ty_name}))
}

fn op_send(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let bytes = payload_to_bytes(&opts, req_str(&opts, "data")?)?;
    let more = opts.get("more").and_then(Value::as_bool).unwrap_or(false);
    let flags = if more { zmq::SNDMORE } else { 0 };
    with_socket(handle, |s| {
        s.send(&bytes, flags)?;
        Ok(json!({"ok": true, "bytes": bytes.len()}))
    })
}

fn op_send_multipart(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let parts = opts
        .get("parts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing parts (expected array of strings)"))?;
    let bufs: Vec<Vec<u8>> = parts
        .iter()
        .map(|v| payload_to_bytes(&opts, v.as_str().unwrap_or("")))
        .collect::<Result<_>>()?;
    let n = bufs.len();
    with_socket(handle, |s| {
        s.send_multipart(&bufs, 0)?;
        Ok(json!({"ok": true, "parts": n}))
    })
}

/// `EAGAIN` is ZMQ's "would block / timed out" signal once RCVTIMEO is set.
/// We surface it as `{timeout: true}` rather than an error so callers can
/// poll-loop without exception handling.
fn is_eagain(e: &anyhow::Error) -> bool {
    e.downcast_ref::<zmq::Error>() == Some(&zmq::Error::EAGAIN)
}

/// Map an `EAGAIN` result to `{timeout:true}`; propagate any other error.
fn or_timeout(r: Result<Value>) -> Result<Value> {
    match r {
        Ok(v) => Ok(v),
        Err(e) if is_eagain(&e) => Ok(json!({"timeout": true})),
        Err(e) => Err(e),
    }
}

fn op_recv(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64);
    or_timeout(with_socket(handle, |s| {
        if let Some(t) = timeout_ms {
            s.set_rcvtimeo(t as i32)?;
        }
        let bytes = s.recv_bytes(0)?;
        Ok(json!({
            "data": bytes_to_payload(&opts, &bytes)?,
            "bytes": bytes.len(),
            "more": s.get_rcvmore()?,
        }))
    }))
}

fn op_recv_multipart(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64);
    or_timeout(with_socket(handle, |s| {
        if let Some(t) = timeout_ms {
            s.set_rcvtimeo(t as i32)?;
        }
        let frames = s.recv_multipart(0)?;
        let parts: Vec<Value> = frames
            .iter()
            .map(|f| Ok(Value::String(bytes_to_payload(&opts, f)?)))
            .collect::<Result<_>>()?;
        Ok(json!({"parts": parts}))
    }))
}

fn op_subscribe(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let topic = req_str(&opts, "topic")?.to_string();
    with_socket(handle, |s| {
        s.set_subscribe(topic.as_bytes())?;
        Ok(json!({"ok": true}))
    })
}

fn op_unsubscribe(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let topic = req_str(&opts, "topic")?.to_string();
    with_socket(handle, |s| {
        s.set_unsubscribe(topic.as_bytes())?;
        Ok(json!({"ok": true}))
    })
}

fn op_set(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let key = req_str(&opts, "opt")?.to_string();
    let val = opts
        .get("value")
        .ok_or_else(|| anyhow!("missing value"))?
        .clone();
    with_socket(handle, |s| {
        set_one_opt(s, &key, &val)?;
        Ok(json!({"ok": true}))
    })
}

fn op_get(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let key = req_str(&opts, "opt")?.to_string();
    with_socket(handle, |s| Ok(json!({ "value": get_one_opt(s, &key)? })))
}

fn op_poll(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64).unwrap_or(0);
    with_socket(handle, |s| {
        let in_events = s.poll(zmq::POLLIN, timeout_ms)?;
        let out_events = s.poll(zmq::POLLOUT, 0)?;
        Ok(json!({
            "readable": in_events != 0,
            "writable": out_events != 0,
        }))
    })
}

/// Poll several sockets in one libzmq `zmq_poll` call. `handles` is an array
/// of socket handles; returns a parallel array of `{handle, readable, writable}`.
fn op_poll_many(opts: Value) -> Result<Value> {
    let handles: Vec<u64> = opts
        .get("handles")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing handles (expected array of handles)"))?
        .iter()
        .map(|v| {
            v.as_u64()
                .ok_or_else(|| anyhow!("handle must be an integer"))
        })
        .collect::<Result<_>>()?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64).unwrap_or(0);
    let map = sockets().lock();
    let socks: Vec<&Socket> = handles
        .iter()
        .map(|h| {
            map.get(h)
                .ok_or_else(|| anyhow!("unknown socket handle: {h}"))
        })
        .collect::<Result<_>>()?;
    let mut items: Vec<zmq::PollItem> = socks
        .iter()
        .map(|s| s.as_poll_item(zmq::POLLIN | zmq::POLLOUT))
        .collect();
    zmq::poll(&mut items, timeout_ms)?;
    let states: Vec<Value> = handles
        .iter()
        .zip(items.iter())
        .map(|(h, it)| {
            json!({
                "handle": h,
                "readable": it.is_readable(),
                "writable": it.is_writable(),
                "error": it.is_error(),
            })
        })
        .collect();
    Ok(json!({ "states": states }))
}

fn op_close(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let removed = sockets().lock().remove(&handle).is_some();
    // Dropping the Socket closes it (libzmq zmq_close on Drop).
    Ok(json!({"ok": true, "closed": removed}))
}

// ── dynamic endpoint management ──────────────────────────────────────────────

fn op_bind(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let endpoint = req_str(&opts, "endpoint")?.to_string();
    with_socket(handle, |s| {
        s.bind(&endpoint)?;
        // After bind to a wildcard port (tcp://*:0) the concrete endpoint is
        // only knowable via LAST_ENDPOINT — surface it so callers can dial in.
        let bound = match s.get_last_endpoint()? {
            Ok(ep) => ep,
            Err(b) => hex_encode(&b),
        };
        Ok(json!({"ok": true, "endpoint": bound}))
    })
}

fn op_connect(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let endpoint = req_str(&opts, "endpoint")?.to_string();
    with_socket(handle, |s| {
        s.connect(&endpoint)?;
        Ok(json!({"ok": true}))
    })
}

fn op_unbind(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let endpoint = req_str(&opts, "endpoint")?.to_string();
    with_socket(handle, |s| {
        s.unbind(&endpoint)?;
        Ok(json!({"ok": true}))
    })
}

fn op_disconnect(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let endpoint = req_str(&opts, "endpoint")?.to_string();
    with_socket(handle, |s| {
        s.disconnect(&endpoint)?;
        Ok(json!({"ok": true}))
    })
}

// ── socket event monitoring ──────────────────────────────────────────────────

/// Map a monitor event-name to its libzmq `ZMQ_EVENT_*` flag.
fn event_flag(name: &str) -> Result<i32> {
    use zmq::SocketEvent::*;
    Ok(match name.to_ascii_lowercase().as_str() {
        "connected" => CONNECTED as i32,
        "connect_delayed" => CONNECT_DELAYED as i32,
        "connect_retried" => CONNECT_RETRIED as i32,
        "listening" => LISTENING as i32,
        "bind_failed" => BIND_FAILED as i32,
        "accepted" => ACCEPTED as i32,
        "accept_failed" => ACCEPT_FAILED as i32,
        "closed" => CLOSED as i32,
        "close_failed" => CLOSE_FAILED as i32,
        "disconnected" => DISCONNECTED as i32,
        "monitor_stopped" => MONITOR_STOPPED as i32,
        "handshake_failed_no_detail" => HANDSHAKE_FAILED_NO_DETAIL as i32,
        "handshake_succeeded" => HANDSHAKE_SUCCEEDED as i32,
        "handshake_failed_protocol" => HANDSHAKE_FAILED_PROTOCOL as i32,
        "handshake_failed_auth" => HANDSHAKE_FAILED_AUTH as i32,
        "all" => ALL as i32,
        other => return Err(anyhow!("unknown monitor event: {other}")),
    })
}

/// Lowercase name for a raw monitor event id (inverse of `event_flag`).
fn event_name(raw: u16) -> &'static str {
    use zmq::SocketEvent::*;
    match zmq::SocketEvent::from_raw(raw) {
        CONNECTED => "connected",
        CONNECT_DELAYED => "connect_delayed",
        CONNECT_RETRIED => "connect_retried",
        LISTENING => "listening",
        BIND_FAILED => "bind_failed",
        ACCEPTED => "accepted",
        ACCEPT_FAILED => "accept_failed",
        CLOSED => "closed",
        CLOSE_FAILED => "close_failed",
        DISCONNECTED => "disconnected",
        MONITOR_STOPPED => "monitor_stopped",
        HANDSHAKE_FAILED_NO_DETAIL => "handshake_failed_no_detail",
        HANDSHAKE_SUCCEEDED => "handshake_succeeded",
        HANDSHAKE_FAILED_PROTOCOL => "handshake_failed_protocol",
        HANDSHAKE_FAILED_AUTH => "handshake_failed_auth",
        ALL => "all",
    }
}

/// Attach an event monitor to a socket. libzmq publishes lifecycle events to
/// an inproc PAIR `endpoint`; the caller then creates a PAIR socket connected
/// there and drains them with `monitor_recv`. `events` is a name, an array of
/// names, or omitted (defaults to "all").
fn op_monitor(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let endpoint = req_str(&opts, "endpoint")?.to_string();
    let mask = match opts.get("events") {
        None | Some(Value::Null) => zmq::SocketEvent::ALL as i32,
        Some(Value::String(s)) => event_flag(s)?,
        Some(Value::Array(a)) => {
            let mut m = 0;
            for v in a {
                m |= event_flag(
                    v.as_str()
                        .ok_or_else(|| anyhow!("event must be a string"))?,
                )?;
            }
            m
        }
        Some(_) => return Err(anyhow!("events must be a string or array of strings")),
    };
    with_socket(handle, |s| {
        s.monitor(&endpoint, mask)?;
        Ok(json!({"ok": true, "endpoint": endpoint}))
    })
}

/// Receive and decode one monitor event from a PAIR socket connected to a
/// monitor endpoint. The wire format is two frames: a 6-byte
/// (u16 event, u32 value) header and the affected endpoint string.
fn op_monitor_recv(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64);
    or_timeout(with_socket(handle, |s| {
        if let Some(t) = timeout_ms {
            s.set_rcvtimeo(t as i32)?;
        }
        let frames = s.recv_multipart(0)?;
        let hdr = frames
            .first()
            .ok_or_else(|| anyhow!("monitor event missing header frame"))?;
        if hdr.len() < 6 {
            return Err(anyhow!("monitor header frame too short"));
        }
        let event = u16::from_le_bytes([hdr[0], hdr[1]]);
        let value = u32::from_le_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]);
        let endpoint = frames
            .get(1)
            .map(|f| String::from_utf8_lossy(f).into_owned())
            .unwrap_or_default();
        Ok(json!({
            "event": event_name(event),
            "event_id": event,
            "value": value,
            "endpoint": endpoint,
        }))
    }))
}

// ── proxy device ─────────────────────────────────────────────────────────────

/// Run a `zmq_proxy` device on a background thread. Consumes the `frontend`
/// and `backend` handles (removed from the registry and moved into the
/// thread). Optional `capture` mirrors all traffic to a third socket;
/// optional `control` makes it a steerable proxy (PAUSE/RESUME/TERMINATE).
/// The proxy blocks its thread until the context terminates or a steerable
/// TERMINATE arrives, so it must run off the calling thread.
fn op_proxy(opts: Value) -> Result<Value> {
    let mut fe = take_socket(req_u64(&opts, "frontend")?)?;
    let mut be = take_socket(req_u64(&opts, "backend")?)?;
    let mut capture = match opts.get("capture").and_then(Value::as_u64) {
        Some(h) => Some(take_socket(h)?),
        None => None,
    };
    let mut control = match opts.get("control").and_then(Value::as_u64) {
        Some(h) => Some(take_socket(h)?),
        None => None,
    };
    std::thread::spawn(move || {
        let _ = match (&mut capture, &mut control) {
            (None, None) => zmq::proxy(&fe, &be),
            (Some(cap), None) => zmq::proxy_with_capture(&mut fe, &mut be, cap),
            (None, Some(ctl)) => zmq::proxy_steerable(&mut fe, &mut be, ctl),
            (Some(cap), Some(ctl)) => zmq::proxy_steerable_with_capture(&mut fe, &mut be, cap, ctl),
        };
    });
    Ok(json!({"ok": true, "running": true}))
}

// ── security / codec / introspection ─────────────────────────────────────────

/// Generate a fresh CURVE keypair as a pair of 40-char z85 strings.
fn op_curve_keypair(_opts: Value) -> Result<Value> {
    let kp = zmq::CurveKeyPair::new().map_err(|e| anyhow!("{e}"))?;
    let public = zmq::z85_encode(&kp.public_key).map_err(|e| anyhow!("{e}"))?;
    let secret = zmq::z85_encode(&kp.secret_key).map_err(|e| anyhow!("{e}"))?;
    Ok(json!({"public": public, "secret": secret}))
}

// libzmq's CURVE public-key derivation (`zmq_curve_public`, libzmq ≥ 4.2). The
// high-level `zmq` 0.10 crate does not re-export it and `zmq-sys`'s `ffi` module
// is private, so declare the extern directly — the symbol is already present in
// the libzmq static archive that `zmq-sys` links into this cdylib.
extern "C" {
    fn zmq_curve_public(
        z85_public_key: *mut std::os::raw::c_char,
        z85_secret_key: *const std::os::raw::c_char,
    ) -> std::os::raw::c_int;
}

/// Derive the Z85 CURVE public key from a Z85 secret key — libzmq's
/// `zmq_curve_public`, the companion to `curve_keypair` for when only the secret
/// is stored. A CURVE secret is exactly 40 Z85 chars (32 raw Curve25519 bytes);
/// the public is a deterministic function of it. opts: `secret` (required).
/// Returns `{secret, public}`. Pure (no socket, no I/O).
fn op_curve_public(opts: Value) -> Result<Value> {
    let secret = req_str(&opts, "secret")?.to_string();
    if secret.len() != 40 || !secret.chars().all(|c| Z85_ALPHABET.contains(c)) {
        return Err(anyhow!(
            "secret must be a 40-character Z85 CURVE key (32 bytes)"
        ));
    }
    let c_secret =
        std::ffi::CString::new(secret.as_bytes()).map_err(|_| anyhow!("secret contains a NUL"))?;
    // libzmq writes 40 Z85 chars + a trailing NUL.
    let mut public = [0u8; 41];
    let rc = unsafe { zmq_curve_public(public.as_mut_ptr() as *mut _, c_secret.as_ptr()) };
    if rc != 0 {
        return Err(anyhow!(
            "zmq_curve_public failed (libzmq built without CURVE support?)"
        ));
    }
    let nul = public.iter().position(|&b| b == 0).unwrap_or(public.len());
    let public = std::str::from_utf8(&public[..nul])
        .map_err(|_| anyhow!("libzmq returned a non-UTF8 key"))?
        .to_string();
    Ok(json!({"secret": secret, "public": public}))
}

fn op_z85_encode(opts: Value) -> Result<Value> {
    let bytes = payload_to_bytes(&opts, req_str(&opts, "data")?)?;
    let z = zmq::z85_encode(&bytes).map_err(|e| anyhow!("{e}"))?;
    Ok(json!({"z85": z}))
}

fn op_z85_decode(opts: Value) -> Result<Value> {
    let bytes = zmq::z85_decode(req_str(&opts, "z85")?).map_err(|e| anyhow!("{e}"))?;
    // Echo the bytes back in whatever encoding the caller asked for (hex by
    // default would be safest, but keep utf8 default for symmetry with send).
    Ok(json!({"data": bytes_to_payload(&opts, &bytes)?, "bytes": bytes.len()}))
}

/// The 85-character Z85 alphabet, in encoder order (ZeroMQ RFC 32).
const Z85_ALPHABET: &str =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// Structurally validate a Z85 string per RFC 32 without decoding it (the
/// non-throwing predicate `z85_decode` lacks): the length must be a multiple of
/// 5 and every character must be in the Z85 alphabet. An empty string is valid
/// (it encodes zero bytes). This is a cheap structural check — it does not
/// verify that each 5-char group is below the 2^32 value ceiling, which only
/// `z85_decode` enforces. opts: `z85` (required). Returns `{z85, valid,
/// reason}`. Pure.
fn op_z85_valid(opts: Value) -> Result<Value> {
    let s = req_str(&opts, "z85")?;
    let reason: Option<&str> = if s.len() % 5 != 0 {
        Some("length must be a multiple of 5")
    } else if !s.chars().all(|c| Z85_ALPHABET.contains(c)) {
        Some("contains a character outside the Z85 alphabet")
    } else {
        None
    };
    Ok(json!({"z85": s, "valid": reason.is_none(), "reason": reason}))
}

/// Vendored libzmq version as `{major,minor,patch,version}`.
fn op_lib_version(_opts: Value) -> Result<Value> {
    let (major, minor, patch) = zmq::version();
    Ok(json!({
        "major": major,
        "minor": minor,
        "patch": patch,
        "version": format!("{major}.{minor}.{patch}"),
    }))
}

/// Probe an optional libzmq capability ("curve", "gssapi", "ipc", "pgm",
/// "tipc", "norm", "draft"). Returns `{capability, has}` — `has` is null if
/// the running libzmq is too old to answer.
fn op_has(opts: Value) -> Result<Value> {
    let cap = req_str(&opts, "capability")?;
    Ok(json!({"capability": cap, "has": zmq::has(cap)}))
}

/// One-shot REQ round-trip convenience: connect a fresh ephemeral REQ
/// socket, send, recv the reply, drop. For simple request/reply where
/// holding a handle is overkill. Real conversations should use a kept
/// `socket` handle.
fn op_request(opts: Value) -> Result<Value> {
    let endpoint = req_str(&opts, "endpoint")?;
    let bytes = payload_to_bytes(&opts, req_str(&opts, "data")?)?;
    let timeout_ms = opts
        .get("timeout_ms")
        .and_then(Value::as_i64)
        .unwrap_or(5000);
    let sock = ctx().socket(SocketType::REQ)?;
    sock.set_rcvtimeo(timeout_ms as i32)?;
    sock.set_sndtimeo(timeout_ms as i32)?;
    sock.set_linger(0)?;
    sock.connect(endpoint)?;
    sock.send(&bytes, 0)?;
    match sock.recv_bytes(0) {
        Ok(bytes) => Ok(json!({
            "reply": bytes_to_payload(&opts, &bytes)?,
            "bytes": bytes.len(),
        })),
        Err(zmq::Error::EAGAIN) => Ok(json!({"timeout": true})),
        Err(e) => Err(anyhow!(e)),
    }
}

// ── pure helpers (no socket) ─────────────────────────────────────────────────

/// Parse a ZeroMQ endpoint `transport://address` into its parts. For the
/// host:port transports (tcp/udp/ws/wss) the address is further split into
/// `host` and `port` (port may be the `*` wildcard). No socket is created.
fn op_parse_endpoint(opts: Value) -> Result<Value> {
    let ep = req_str(&opts, "endpoint")?;
    let (transport, address) = ep
        .split_once("://")
        .ok_or_else(|| anyhow!("not a ZMQ endpoint (missing `://`): {ep}"))?;
    if transport.is_empty() {
        return Err(anyhow!("empty transport in endpoint: {ep}"));
    }
    let known = matches!(
        transport,
        "tcp" | "ipc" | "inproc" | "pgm" | "epgm" | "vmci" | "ws" | "wss" | "udp"
    );
    let mut out = json!({
        "transport": transport,
        "address": address,
        "known_transport": known,
    });
    if matches!(transport, "tcp" | "udp" | "ws" | "wss") {
        // Port is after the LAST colon so bare IPv6 hosts survive; `*` is the
        // ZMQ bind wildcard for an ephemeral port.
        if let Some((host, port)) = address.rsplit_once(':') {
            out["host"] = json!(host);
            out["port"] = match port.parse::<u32>() {
                Ok(p) => json!(p),
                Err(_) => json!(port),
            };
        } else {
            out["host"] = json!(address);
        }
    }
    Ok(out)
}

/// Build a ZMQ endpoint string from parts — the inverse of `parse_endpoint`.
/// opts: `transport` (required), and either `address`, or `host` + optional
/// `port` for the IP transports (tcp/udp/ws/wss). Validates the transport name
/// and rejects an empty host. Pure.
fn op_build_endpoint(opts: Value) -> Result<Value> {
    let transport = req_str(&opts, "transport")?;
    if transport.is_empty() {
        return Err(anyhow!("empty transport"));
    }
    let known = matches!(
        transport,
        "tcp" | "ipc" | "inproc" | "pgm" | "epgm" | "vmci" | "ws" | "wss" | "udp"
    );
    // An explicit `address` wins; otherwise assemble it from host[:port].
    let address = if let Some(addr) = opts.get("address").and_then(Value::as_str) {
        addr.to_string()
    } else {
        let host = opts
            .get("host")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing address or host"))?;
        if host.is_empty() {
            return Err(anyhow!("empty host"));
        }
        // port may arrive as a number, the `*` bind wildcard, or be absent.
        match opts.get("port") {
            Some(Value::Number(n)) => format!("{host}:{n}"),
            Some(Value::String(s)) if !s.is_empty() => format!("{host}:{s}"),
            _ => host.to_string(),
        }
    };
    Ok(json!({
        "endpoint": format!("{transport}://{address}"),
        "known_transport": known,
    }))
}

/// A wildcard bind host that has no fixed address to connect to.
fn is_wildcard_host(h: &str) -> bool {
    matches!(h, "*" | "0.0.0.0" | "::" | "[::]")
}

/// Rewrite a ZMQ *bind* endpoint into a *connect*able one by replacing a
/// wildcard bind host (`*`, `0.0.0.0`, `::`, `[::]`) with a concrete host
/// (default `localhost`, override with `host`). Only the IP transports
/// (tcp/udp/ws/wss) carry a host to rewrite; other transports (ipc/inproc/…) and
/// already-concrete hosts pass through unchanged. The port — including a `*`
/// ephemeral-port wildcard, which is only known at runtime — is preserved. opts:
/// `endpoint` (required), optional `host`. Returns `{endpoint, bind_endpoint,
/// changed}`. Pure.
fn op_endpoint_bind_to_connect(opts: Value) -> Result<Value> {
    let ep = req_str(&opts, "endpoint")?;
    let connect_host = opts
        .get("host")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("localhost");
    let (transport, address) = ep
        .split_once("://")
        .ok_or_else(|| anyhow!("not a ZMQ endpoint (missing `://`): {ep}"))?;
    let is_ip = matches!(transport, "tcp" | "udp" | "ws" | "wss");
    let (new_address, changed) = if is_ip {
        // Host is before the LAST ':' so bare IPv6 literals survive, like parse_endpoint.
        match address.rsplit_once(':') {
            Some((host, port)) if is_wildcard_host(host) => {
                (format!("{connect_host}:{port}"), true)
            }
            // A host with no port at all.
            None if is_wildcard_host(address) => (connect_host.to_string(), true),
            _ => (address.to_string(), false),
        }
    } else {
        (address.to_string(), false)
    };
    Ok(json!({
        "endpoint": format!("{transport}://{new_address}"),
        "bind_endpoint": ep,
        "changed": changed,
    }))
}

/// Validate a ZMQ endpoint string by transport syntax — stricter than
/// `parse_endpoint`, which only splits and flags an unknown transport. Checks: a
/// non-empty `transport://address`; a known transport (tcp/ipc/inproc/pgm/epgm/
/// vmci/ws/wss/udp); for the IP transports (tcp/udp/ws/wss) a non-empty host and
/// a port that is `*` (bind wildcard) or 1–65535; for ipc a non-empty path; for
/// inproc a non-empty name; for pgm/epgm/vmci a non-empty address. A `ws`/`wss`
/// path (`/foo`) and a bracketed IPv6 host are tolerated. opts: `endpoint`.
/// Returns `{endpoint, valid, reason, transport}`. Pure.
fn op_valid_endpoint(opts: Value) -> Result<Value> {
    let ep = req_str(&opts, "endpoint")?;
    let (transport, address) = match ep.split_once("://") {
        Some(parts) => parts,
        None => {
            return Ok(json!({
                "endpoint": ep, "valid": false,
                "reason": "missing `://`", "transport": Value::Null,
            }));
        }
    };
    let known = matches!(
        transport,
        "tcp" | "ipc" | "inproc" | "pgm" | "epgm" | "vmci" | "ws" | "wss" | "udp"
    );
    let reason: Option<String> = if transport.is_empty() {
        Some("empty transport".into())
    } else if !known {
        Some(format!("unknown transport `{transport}`"))
    } else if matches!(transport, "tcp" | "udp" | "ws" | "wss") {
        // The authority is the part before any `/path` (ws/wss). Host is before
        // the LAST ':' so a bracketed IPv6 literal survives.
        let authority = address.split('/').next().unwrap_or(address);
        match authority.rsplit_once(':') {
            None => Some("missing `:port`".into()),
            Some((host, port)) => {
                let host = host.trim_start_matches('[').trim_end_matches(']');
                if host.is_empty() {
                    Some("empty host".into())
                } else if port == "*"
                    || matches!(port.parse::<u32>(), Ok(p) if (1..=65535).contains(&p))
                {
                    None
                } else {
                    Some(format!("invalid port `{port}` (1-65535 or `*`)"))
                }
            }
        }
    } else if address.is_empty() {
        Some(format!("empty {transport} address"))
    } else {
        None
    };
    Ok(json!({
        "endpoint": ep,
        "valid": reason.is_none(),
        "reason": reason,
        "transport": transport,
    }))
}

/// ZeroMQ SUB/XSUB matching: a subscription matches a topic when it is a
/// byte-prefix of it; an empty subscription matches everything. Mirrors
/// libzmq's prefix filter without a socket.
fn op_topic_match(opts: Value) -> Result<Value> {
    let sub = req_str(&opts, "subscription")?;
    let topic = req_str(&opts, "topic")?;
    let matched = topic.as_bytes().starts_with(sub.as_bytes());
    Ok(json!({"subscription": sub, "topic": topic, "match": matched}))
}

/// ZeroMQ XPUB routing: which of a set of `subscriptions` match a `topic`. A
/// subscription matches when it is a byte-prefix of the topic (an empty
/// subscription matches everything), exactly as `topic_match` does for one — this
/// is the set form a publisher uses to decide which subscribers receive a
/// message. opts: `topic` (required), `subscriptions` (array of strings).
/// Returns `{topic, match, matched}` where `match` is true if any subscription
/// matched and `matched` lists them in input order. Pure.
fn op_topic_match_any(opts: Value) -> Result<Value> {
    let topic = req_str(&opts, "topic")?;
    let subs = opts
        .get("subscriptions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing subscriptions (array of strings)"))?;
    let matched: Vec<Value> = subs
        .iter()
        .filter_map(Value::as_str)
        .filter(|s| topic.as_bytes().starts_with(s.as_bytes()))
        .map(|s| json!(s))
        .collect();
    Ok(json!({
        "topic": topic,
        "match": !matched.is_empty(),
        "matched": matched,
    }))
}

/// Validate a socket-type name, returning its canonical lowercase form (so
/// aliases like `publish` collapse to `pub`). Never errors — reports validity.
fn op_valid_socket_type(opts: Value) -> Result<Value> {
    let name = req_str(&opts, "type")?;
    match parse_socket_type(name) {
        Ok(ty) => Ok(json!({"valid": true, "canonical": socket_type_name(ty)})),
        Err(_) => Ok(json!({"valid": false, "canonical": Value::Null})),
    }
}

/// Single source of truth for the libzmq `zmq_socket` "Compatible peer sockets"
/// table: the canonical peer-type names a given type can be connected to.
/// Used by both `op_socket_peers` and `op_socket_types_compatible`.
fn compatible_peers(canonical: &str) -> Result<&'static [&'static str]> {
    Ok(match canonical {
        "req" => &["rep", "router"],
        "rep" => &["req", "dealer"],
        "dealer" => &["rep", "dealer", "router"],
        "router" => &["req", "dealer", "router"],
        "pub" => &["sub", "xsub"],
        "sub" => &["pub", "xpub"],
        "xpub" => &["sub", "xsub"],
        "xsub" => &["pub", "xpub"],
        "push" => &["pull"],
        "pull" => &["push"],
        "pair" => &["pair"],
        "stream" => &["stream"],
        other => return Err(anyhow!("no peer rule for socket type `{other}`")),
    })
}

/// Whether two ZeroMQ socket types can be connected as peers, per the libzmq
/// `zmq_socket` compatibility table (e.g. REQ↔REP, PUB↔SUB, PUSH↔PULL,
/// DEALER↔ROUTER, PAIR↔PAIR). Type names are case-insensitive and accept the
/// `publish`/`subscribe` aliases. opts: `a`, `b`. Returns
/// `{a, b, compatible, a_peers}` with canonical names. Pure.
fn op_socket_types_compatible(opts: Value) -> Result<Value> {
    let a = socket_type_name(parse_socket_type(req_str(&opts, "a")?)?);
    let b = socket_type_name(parse_socket_type(req_str(&opts, "b")?)?);
    let a_peers = compatible_peers(a)?;
    Ok(json!({
        "a": a,
        "b": b,
        "compatible": a_peers.contains(&b),
        "a_peers": a_peers,
    }))
}

/// The socket types a given type can validly connect to — ZeroMQ's documented
/// messaging-pattern compatibility (the `zmq_socket(3)` matrix). The input is
/// canonicalized (aliases accepted), and the peer list is canonical names.
/// Lets you validate a topology before wiring sockets. Pure.
fn op_socket_peers(opts: Value) -> Result<Value> {
    let canonical = socket_type_name(parse_socket_type(req_str(&opts, "type")?)?);
    Ok(json!({"type": canonical, "peers": compatible_peers(canonical)?}))
}

/// Classify a socket type by its ZeroMQ messaging `pattern` and report its
/// `can_send`/`can_recv` directionality, per `zmq_socket(3)`. PUB/PUSH are
/// send-only, SUB/PULL receive-only, XPUB/XSUB bidirectional; the request-reply,
/// exclusive-pair, and native-stream types send and receive. Lets you check
/// whether `zmq_send`/`zmq_recv` is even legal on a socket before wiring it.
/// The input is canonicalized (aliases accepted). Pure.
fn op_socket_caps(opts: Value) -> Result<Value> {
    let name = req_str(&opts, "type")?;
    let ty = parse_socket_type(name)?;
    let canonical = socket_type_name(ty);
    let (pattern, can_send, can_recv) = match canonical {
        "req" | "rep" | "dealer" | "router" => ("request-reply", true, true),
        "pub" => ("publish-subscribe", true, false),
        "sub" => ("publish-subscribe", false, true),
        "xpub" | "xsub" => ("publish-subscribe", true, true),
        "push" => ("pipeline", true, false),
        "pull" => ("pipeline", false, true),
        "pair" => ("exclusive-pair", true, true),
        "stream" => ("native", true, true),
        other => {
            return Err(anyhow::anyhow!(
                "no capability rule for socket type `{other}`"
            ))
        }
    };
    Ok(json!({
        "type": canonical,
        "pattern": pattern,
        "can_send": can_send,
        "can_recv": can_recv,
    }))
}

/// Every canonical socket-type name this package accepts.
fn op_socket_types(_opts: Value) -> Result<Value> {
    Ok(json!({"types": [
        "req", "rep", "pub", "sub", "push", "pull",
        "dealer", "router", "pair", "xpub", "xsub", "stream",
    ]}))
}

// ── ffi boundary ─────────────────────────────────────────────────────────────

/// JSON-string-in / JSON-string-out wrapper. Parses args (malformed →
/// `Value::Null`, never panics), runs the handler under `catch_unwind` so a
/// handler panic becomes a JSON error instead of crossing the FFI boundary
/// and aborting the host shell, and returns a heap `CString` the caller
/// frees with `stryke_free_cstring`.
fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-zmq handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

/// Declare a `#[no_mangle]` FFI export that forwards to an `op_*` handler.
macro_rules! export {
    ($name:ident => $handler:path) => {
        #[no_mangle]
        pub extern "C" fn $name(args: *const c_char) -> *const c_char {
            ffi_call(args, $handler)
        }
    };
}

#[no_mangle]
pub extern "C" fn zmq__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

export!(zmq__socket => op_socket);
export!(zmq__send => op_send);
export!(zmq__send_multipart => op_send_multipart);
export!(zmq__recv => op_recv);
export!(zmq__recv_multipart => op_recv_multipart);
export!(zmq__subscribe => op_subscribe);
export!(zmq__unsubscribe => op_unsubscribe);
export!(zmq__set => op_set);
export!(zmq__get => op_get);
export!(zmq__poll => op_poll);
export!(zmq__poll_many => op_poll_many);
export!(zmq__close => op_close);
export!(zmq__bind => op_bind);
export!(zmq__connect => op_connect);
export!(zmq__unbind => op_unbind);
export!(zmq__disconnect => op_disconnect);
export!(zmq__monitor => op_monitor);
export!(zmq__monitor_recv => op_monitor_recv);
export!(zmq__proxy => op_proxy);
export!(zmq__curve_keypair => op_curve_keypair);
export!(zmq__curve_public => op_curve_public);
export!(zmq__z85_encode => op_z85_encode);
export!(zmq__z85_decode => op_z85_decode);
export!(zmq__z85_valid => op_z85_valid);
export!(zmq__lib_version => op_lib_version);
export!(zmq__has => op_has);
export!(zmq__request => op_request);
export!(zmq__parse_endpoint => op_parse_endpoint);
export!(zmq__build_endpoint => op_build_endpoint);
export!(zmq__endpoint_bind_to_connect => op_endpoint_bind_to_connect);
export!(zmq__valid_endpoint => op_valid_endpoint);
export!(zmq__topic_match => op_topic_match);
export!(zmq__topic_match_any => op_topic_match_any);
export!(zmq__valid_socket_type => op_valid_socket_type);
export!(zmq__socket_types_compatible => op_socket_types_compatible);
export!(zmq__socket_peers => op_socket_peers);
export!(zmq__socket_caps => op_socket_caps);
export!(zmq__socket_types => op_socket_types);

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive an export the way stryke's FFI bridge does: JSON in, JSON out,
    /// then reclaim the returned `CString`. Returns the parsed response.
    fn call(f: extern "C" fn(*const c_char) -> *const c_char, arg: &str) -> Value {
        let cs = CString::new(arg).expect("arg must not contain NUL");
        let raw = f(cs.as_ptr());
        assert!(!raw.is_null(), "export returned null pointer");
        let out = unsafe { CStr::from_ptr(raw) }
            .to_str()
            .expect("output must be valid UTF-8")
            .to_string();
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        serde_json::from_str(&out).expect("output must be valid JSON")
    }

    fn err_of(v: &Value) -> &str {
        v.get("error")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("expected error field, got: {v}"))
    }

    /// `pkg_version` round-trips through the FFI allocator without touching
    /// libzmq. Pins the JSON-in/out + `CString::into_raw`→`stryke_free_cstring`
    /// contract; a regression to a non-`into_raw` pointer would be UB in free.
    #[test]
    fn pkg_version_round_trips() {
        let v = call(zmq__pkg_version, "{}");
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    }

    /// Full local socket lifecycle over inproc:// — a PAIR bind/connect pair,
    /// a send, a recv, then close. Exercises the registry insert→lookup→remove
    /// path and proves handles survive across separate FFI calls (the entire
    /// reason for a persistent cdylib over a fork-per-call helper).
    #[test]
    fn pair_send_recv_lifecycle_over_inproc() {
        let endpoint = "inproc://stryke-zmq-test-lifecycle";
        let server = call(
            zmq__socket,
            &format!(r#"{{"type":"pair","bind":"{endpoint}"}}"#),
        );
        let client = call(
            zmq__socket,
            &format!(r#"{{"type":"pair","connect":"{endpoint}"}}"#),
        );
        let sh = server["handle"].as_u64().expect("server handle");
        let ch = client["handle"].as_u64().expect("client handle");
        assert_ne!(sh, ch, "each socket gets a distinct handle");

        let sent = call(zmq__send, &format!(r#"{{"handle":{ch},"data":"ping"}}"#));
        assert_eq!(sent["ok"], true);
        assert_eq!(sent["bytes"], 4);

        let got = call(
            zmq__recv,
            &format!(r#"{{"handle":{sh},"timeout_ms":1000}}"#),
        );
        assert_eq!(got["data"], "ping", "recv must observe the sent payload");

        for h in [sh, ch] {
            let c = call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
            assert_eq!(c["closed"], true, "close must report the handle existed");
        }
    }

    /// recv with RCVTIMEO on a socket with nothing queued must surface
    /// `{timeout:true}`, NOT an error and NOT a hang. Pins the `EAGAIN`→
    /// timeout mapping (`is_eagain`); a regression that drops the mapping
    /// would turn every empty poll into a thrown error in caller code.
    #[test]
    fn recv_timeout_returns_flag_not_error() {
        let s = call(
            zmq__socket,
            r#"{"type":"pull","bind":"inproc://stryke-zmq-test-timeout"}"#,
        );
        let h = s["handle"].as_u64().unwrap();
        let start = std::time::Instant::now();
        let got = call(zmq__recv, &format!(r#"{{"handle":{h},"timeout_ms":100}}"#));
        let elapsed = start.elapsed();
        assert_eq!(got["timeout"], true, "empty recv must report timeout flag");
        assert!(got.get("error").is_none(), "timeout is not an error");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "RCVTIMEO must bound the wait; took {elapsed:?}"
        );
        call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
    }

    /// An operation against a handle that was never issued must fail with the
    /// documented "unknown socket handle" error — never panic, never silently
    /// succeed. Catches a regression where `with_socket` stops validating
    /// the lookup.
    #[test]
    fn op_on_unknown_handle_errors() {
        let v = call(zmq__send, r#"{"handle":999999,"data":"x"}"#);
        assert!(
            err_of(&v).starts_with("unknown socket handle"),
            "expected unknown-handle error; got: {}",
            err_of(&v)
        );
    }

    /// An unknown socket type must be rejected at validation before any libzmq
    /// socket is created — surfaces the documented error string.
    #[test]
    fn unknown_socket_type_rejected() {
        let v = call(zmq__socket, r#"{"type":"bogus"}"#);
        assert!(
            err_of(&v).starts_with("unknown socket type"),
            "expected unknown-type error; got: {}",
            err_of(&v)
        );
    }

    /// Missing required `type` on socket creation must surface "missing type",
    /// not a panic or a defaulted socket.
    #[test]
    fn socket_missing_type_errors() {
        let v = call(zmq__socket, r#"{}"#);
        assert_eq!(err_of(&v), "missing type");
    }

    /// Malformed JSON args must fall through to `Value::Null` and surface the
    /// validator's missing-key error — never panic across the FFI boundary.
    /// The `unwrap_or(Value::Null)` in `ffi_call` is load-bearing; a swap to
    /// `.unwrap()` would crash the host on any truncated/garbage input.
    #[test]
    fn malformed_json_does_not_panic() {
        let v = call(zmq__socket, "not-json-{[}");
        assert_eq!(
            err_of(&v),
            "missing type",
            "malformed input must coerce to Null and hit the validator"
        );
    }

    /// Empty-string args hit a different serde_json error path than garbage
    /// bytes; both must coerce to Null and surface as missing-key, not panic.
    #[test]
    fn empty_input_does_not_panic() {
        let v = call(zmq__send, "");
        assert_eq!(err_of(&v), "missing handle");
    }

    /// `send_multipart` with a non-array `parts` must surface the documented
    /// error, not silently treat it as an empty send.
    #[test]
    fn send_multipart_rejects_non_array_parts() {
        let v = call(zmq__send_multipart, r#"{"handle":1,"parts":"notanarray"}"#);
        assert!(
            err_of(&v).starts_with("missing parts"),
            "non-array parts must be rejected; got: {}",
            err_of(&v)
        );
    }

    /// `set` with an unknown option name must error rather than no-op
    /// silently — a silent no-op would let a typo'd option look applied.
    #[test]
    fn set_unknown_option_errors() {
        let s = call(
            zmq__socket,
            r#"{"type":"pub","bind":"inproc://stryke-zmq-test-set"}"#,
        );
        let h = s["handle"].as_u64().unwrap();
        let v = call(
            zmq__set,
            &format!(r#"{{"handle":{h},"opt":"nonsense","value":1}}"#),
        );
        assert!(
            err_of(&v).starts_with("unknown or unsettable option"),
            "unknown option must error; got: {}",
            err_of(&v)
        );
        call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
    }

    /// PUSH/PULL pipeline over inproc — proves the push/pull pattern, distinct
    /// from PAIR, routes a payload end to end through the handle registry.
    #[test]
    fn push_pull_pipeline() {
        let ep = "inproc://stryke-zmq-test-pushpull";
        let puller = call(zmq__socket, &format!(r#"{{"type":"pull","bind":"{ep}"}}"#));
        let pusher = call(
            zmq__socket,
            &format!(r#"{{"type":"push","connect":"{ep}"}}"#),
        );
        let ph = puller["handle"].as_u64().unwrap();
        let xh = pusher["handle"].as_u64().unwrap();
        call(zmq__send, &format!(r#"{{"handle":{xh},"data":"work"}}"#));
        let got = call(
            zmq__recv,
            &format!(r#"{{"handle":{ph},"timeout_ms":1000}}"#),
        );
        assert_eq!(got["data"], "work");
        call(zmq__close, &format!(r#"{{"handle":{ph}}}"#));
        call(zmq__close, &format!(r#"{{"handle":{xh}}}"#));
    }

    // ── new-surface coverage ─────────────────────────────────────────────────

    /// hex/base64 inline codecs must round-trip arbitrary bytes including a
    /// NUL and high bytes that `from_utf8_lossy` would corrupt. Pins the
    /// binary-safe framing the default utf8 path can't provide.
    #[test]
    fn binary_codecs_round_trip() {
        let raw = [0u8, 1, 2, 255, 254, 128, b'z'];
        assert_eq!(hex_decode(&hex_encode(&raw)).unwrap(), raw);
        assert_eq!(base64_decode(&base64_encode(&raw)).unwrap(), raw);
        // Known vectors against the public encodings.
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    /// A hex-framed send must arrive byte-identical when recv'd hex-framed —
    /// the end-to-end binary-safe path through real libzmq sockets. A payload
    /// with an embedded NUL would be truncated by any C-string path.
    #[test]
    fn hex_encoded_send_recv_is_binary_safe() {
        let ep = "inproc://stryke-zmq-test-binary";
        let srv = call(zmq__socket, &format!(r#"{{"type":"pair","bind":"{ep}"}}"#));
        let cli = call(
            zmq__socket,
            &format!(r#"{{"type":"pair","connect":"{ep}"}}"#),
        );
        let sh = srv["handle"].as_u64().unwrap();
        let ch = cli["handle"].as_u64().unwrap();
        // bytes [0,255,0,42] — has a NUL and a high byte.
        call(
            zmq__send,
            &format!(r#"{{"handle":{ch},"data":"00ff002a","encoding":"hex"}}"#),
        );
        let got = call(
            zmq__recv,
            &format!(r#"{{"handle":{sh},"timeout_ms":1000,"encoding":"hex"}}"#),
        );
        assert_eq!(
            got["data"], "00ff002a",
            "binary payload must survive intact"
        );
        assert_eq!(got["bytes"], 4);
        call(zmq__close, &format!(r#"{{"handle":{sh}}}"#));
        call(zmq__close, &format!(r#"{{"handle":{ch}}}"#));
    }

    /// `get` must read back an option that `set` (or socket creation) wrote.
    /// Round-trips an i32 (sndhwm) and the socket type to pin the get table.
    #[test]
    fn set_then_get_round_trips_option() {
        let s = call(
            zmq__socket,
            r#"{"type":"pub","sndhwm":4242,"bind":"inproc://stryke-zmq-test-getopt"}"#,
        );
        let h = s["handle"].as_u64().unwrap();
        let hwm = call(zmq__get, &format!(r#"{{"handle":{h},"opt":"sndhwm"}}"#));
        assert_eq!(hwm["value"], 4242, "creation-time option must be readable");
        let ty = call(zmq__get, &format!(r#"{{"handle":{h},"opt":"type"}}"#));
        assert_eq!(ty["value"], "pub");
        call(
            zmq__set,
            &format!(r#"{{"handle":{h},"opt":"linger","value":17}}"#),
        );
        let lg = call(zmq__get, &format!(r#"{{"handle":{h},"opt":"linger"}}"#));
        assert_eq!(lg["value"], 17, "set must be observable via get");
        call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
    }

    /// `bind` to a wildcard TCP port must return the concrete endpoint via
    /// LAST_ENDPOINT so a caller can hand the port to a peer. Pins the
    /// dynamic-bind discovery path.
    #[test]
    fn dynamic_bind_reports_concrete_endpoint() {
        let s = call(zmq__socket, r#"{"type":"rep"}"#);
        let h = s["handle"].as_u64().unwrap();
        let b = call(
            zmq__bind,
            &format!(r#"{{"handle":{h},"endpoint":"tcp://127.0.0.1:*"}}"#),
        );
        let ep = b["endpoint"].as_str().unwrap();
        assert!(
            ep.starts_with("tcp://127.0.0.1:") && !ep.ends_with(":*"),
            "wildcard bind must resolve to a concrete port; got {ep}"
        );
        call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
    }

    /// `poll_many` over two idle sockets must report neither readable, and
    /// must echo each handle back — pins the multi-socket poll wiring.
    #[test]
    fn poll_many_reports_per_handle_state() {
        let a = call(
            zmq__socket,
            r#"{"type":"pull","bind":"inproc://stryke-zmq-pm-a"}"#,
        );
        let b = call(
            zmq__socket,
            r#"{"type":"pull","bind":"inproc://stryke-zmq-pm-b"}"#,
        );
        let ah = a["handle"].as_u64().unwrap();
        let bh = b["handle"].as_u64().unwrap();
        let r = call(
            zmq__poll_many,
            &format!(r#"{{"handles":[{ah},{bh}],"timeout_ms":50}}"#),
        );
        let states = r["states"].as_array().unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0]["handle"], ah);
        assert_eq!(states[0]["readable"], false);
        assert_eq!(states[1]["handle"], bh);
        call(zmq__close, &format!(r#"{{"handle":{ah}}}"#));
        call(zmq__close, &format!(r#"{{"handle":{bh}}}"#));
    }

    /// `curve_keypair` must yield two distinct 40-char z85 keys, and `z85`
    /// encode/decode must round-trip. Pins the security + codec exports
    /// without needing a CURVE handshake.
    #[test]
    fn curve_keypair_and_z85_round_trip() {
        // CURVE keypair generation needs libsodium compiled into libzmq; the
        // vendored build may omit it. Gate the keypair assertions on the
        // runtime capability so the test reflects the actual build, while z85
        // (libzmq core, always present) is exercised unconditionally.
        if zmq::has("curve") == Some(true) {
            let kp = call(zmq__curve_keypair, "{}");
            let public = kp["public"].as_str().expect("curve public key");
            let secret = kp["secret"].as_str().expect("curve secret key");
            assert_eq!(public.len(), 40, "z85 public key is 40 chars");
            assert_eq!(secret.len(), 40, "z85 secret key is 40 chars");
            assert_ne!(public, secret);
            // curve_public must re-derive exactly the public the pair carried
            // (Curve25519 public is a deterministic function of the secret), and
            // be stable across repeated calls.
            let derived = call(zmq__curve_public, &format!(r#"{{"secret":"{secret}"}}"#));
            assert_eq!(
                derived["public"].as_str(),
                Some(public),
                "curve_public re-derives the keypair's public"
            );
            let again = call(zmq__curve_public, &format!(r#"{{"secret":"{secret}"}}"#));
            assert_eq!(again["public"], derived["public"], "curve_public is stable");
            // A malformed secret is rejected, not panicked.
            assert!(
                call(zmq__curve_public, r#"{"secret":"too-short"}"#)
                    .get("error")
                    .is_some(),
                "curve_public rejects a non-40-char secret"
            );
        } else {
            // Without CURVE the op must surface an error, never panic/null.
            let kp = call(zmq__curve_keypair, "{}");
            assert!(
                kp.get("error").is_some(),
                "no-CURVE build must report error"
            );
        }
        let enc = call(zmq__z85_encode, r#"{"data":"deadbeef","encoding":"hex"}"#);
        let z = enc["z85"].as_str().unwrap();
        let dec = call(
            zmq__z85_decode,
            &format!(r#"{{"z85":"{z}","encoding":"hex"}}"#),
        );
        assert_eq!(dec["data"], "deadbeef", "z85 must round-trip the bytes");
        // z85_valid agrees with the encoder: a freshly encoded string is valid.
        assert_eq!(
            call(zmq__z85_valid, &format!(r#"{{"z85":"{z}"}}"#))["valid"],
            json!(true)
        );
    }

    #[test]
    fn z85_valid_checks_length_and_alphabet() {
        // 5-char multiples of valid alphabet chars pass; the empty string passes.
        assert_eq!(
            call(zmq__z85_valid, r#"{"z85":"HelloWorld"}"#)["valid"],
            json!(true)
        );
        assert_eq!(call(zmq__z85_valid, r#"{"z85":""}"#)["valid"], json!(true));
        // Length not a multiple of 5 fails with a length reason.
        let short = call(zmq__z85_valid, r#"{"z85":"Hello1"}"#);
        assert_eq!(short["valid"], json!(false));
        assert!(short["reason"].as_str().unwrap().contains("multiple of 5"));
        // A character outside the Z85 alphabet (a backtick) fails.
        let bad = call(zmq__z85_valid, r#"{"z85":"Hell`"}"#);
        assert_eq!(bad["valid"], json!(false));
        assert!(bad["reason"].as_str().unwrap().contains("Z85 alphabet"));
        // Any genuinely encoded string passes the structural check (4 bytes →
        // z85 needs an input length divisible by 4).
        let enc = call(zmq__z85_encode, r#"{"data":"cafebabe","encoding":"hex"}"#);
        let z = enc["z85"].as_str().unwrap();
        assert_eq!(
            call(zmq__z85_valid, &format!(r#"{{"z85":"{z}"}}"#))["valid"],
            json!(true)
        );
        assert!(err_of(&call(zmq__z85_valid, "{}")).contains("z85"));
    }

    /// `lib_version` reports a libzmq ≥ 4 (the vendored zeromq-src is 4.x),
    /// and `has("curve")` answers a bool — pins the introspection exports.
    #[test]
    fn lib_version_and_has_report() {
        let v = call(zmq__lib_version, "{}");
        assert!(v["major"].as_i64().unwrap() >= 4, "vendored libzmq is 4.x+");
        let h = call(zmq__has, r#"{"capability":"curve"}"#);
        assert_eq!(h["capability"], "curve");
        assert!(h["has"].is_boolean() || h["has"].is_null());
    }

    /// A socket monitor must deliver a `listening` event when its target
    /// binds. Drives the monitor → PAIR-reader → `monitor_recv` decode path
    /// end to end. The monitor must be attached before the bind that triggers
    /// the event.
    #[test]
    fn monitor_observes_listening_event() {
        let s = call(zmq__socket, r#"{"type":"rep"}"#);
        let h = s["handle"].as_u64().unwrap();
        let mon_ep = "inproc://stryke-zmq-test-monitor";
        call(
            zmq__monitor,
            &format!(r#"{{"handle":{h},"endpoint":"{mon_ep}","events":"all"}}"#),
        );
        let reader = call(
            zmq__socket,
            &format!(r#"{{"type":"pair","connect":"{mon_ep}"}}"#),
        );
        let rh = reader["handle"].as_u64().unwrap();
        // Trigger an event by binding the monitored socket.
        call(
            zmq__bind,
            &format!(r#"{{"handle":{h},"endpoint":"tcp://127.0.0.1:*"}}"#),
        );
        // Drain until we see "listening" or time out.
        let mut saw_listening = false;
        for _ in 0..10 {
            let ev = call(
                zmq__monitor_recv,
                &format!(r#"{{"handle":{rh},"timeout_ms":500}}"#),
            );
            if ev.get("timeout").and_then(Value::as_bool) == Some(true) {
                break;
            }
            if ev["event"] == "listening" {
                saw_listening = true;
                break;
            }
        }
        assert!(saw_listening, "monitor must report the listening event");
        call(zmq__close, &format!(r#"{{"handle":{rh}}}"#));
        call(zmq__close, &format!(r#"{{"handle":{h}}}"#));
    }

    // ── pure helpers (no socket / no libzmq runtime) ─────────────────────────

    #[test]
    fn parse_endpoint_tcp_splits_host_and_port() {
        let v = call(
            zmq__parse_endpoint,
            r#"{"endpoint":"tcp://127.0.0.1:5555"}"#,
        );
        assert_eq!(v["transport"], json!("tcp"));
        assert_eq!(v["host"], json!("127.0.0.1"));
        assert_eq!(v["port"], json!(5555), "port parses to a number");
        assert_eq!(v["known_transport"], json!(true));
    }

    #[test]
    fn parse_endpoint_ipc_keeps_address_opaque() {
        let v = call(zmq__parse_endpoint, r#"{"endpoint":"ipc:///tmp/feeds/0"}"#);
        assert_eq!(v["transport"], json!("ipc"));
        assert_eq!(v["address"], json!("/tmp/feeds/0"));
        assert!(
            v.get("port").is_none(),
            "non-host:port transports carry no port"
        );
    }

    #[test]
    fn parse_endpoint_wildcard_bind_port() {
        let v = call(zmq__parse_endpoint, r#"{"endpoint":"tcp://*:*"}"#);
        assert_eq!(v["host"], json!("*"));
        // `*` is not numeric, so it round-trips as the literal wildcard.
        assert_eq!(v["port"], json!("*"));
    }

    #[test]
    fn parse_endpoint_rejects_missing_scheme() {
        let v = call(zmq__parse_endpoint, r#"{"endpoint":"127.0.0.1:5555"}"#);
        assert!(err_of(&v).contains("missing"), "no `://` must error");
    }

    #[test]
    fn build_endpoint_inverts_parse_endpoint() {
        // host + numeric port → tcp endpoint, round-trips through parse.
        let b = call(
            zmq__build_endpoint,
            r#"{"transport":"tcp","host":"127.0.0.1","port":5555}"#,
        );
        assert_eq!(b["endpoint"], json!("tcp://127.0.0.1:5555"));
        assert_eq!(b["known_transport"], json!(true));
        let back = call(
            zmq__parse_endpoint,
            r#"{"endpoint":"tcp://127.0.0.1:5555"}"#,
        );
        assert_eq!(back["host"], json!("127.0.0.1"));
        assert_eq!(back["port"], json!(5555));
        // `*` wildcard port stays literal; opaque address passes through.
        assert_eq!(
            call(
                zmq__build_endpoint,
                r#"{"transport":"tcp","host":"*","port":"*"}"#
            )["endpoint"],
            json!("tcp://*:*")
        );
        assert_eq!(
            call(
                zmq__build_endpoint,
                r#"{"transport":"ipc","address":"/tmp/feeds/0"}"#
            )["endpoint"],
            json!("ipc:///tmp/feeds/0")
        );
        // host without port omits the colon; empty host and missing address error.
        assert_eq!(
            call(
                zmq__build_endpoint,
                r#"{"transport":"inproc","host":"workers"}"#
            )["endpoint"],
            json!("inproc://workers")
        );
        assert!(err_of(&call(
            zmq__build_endpoint,
            r#"{"transport":"tcp","host":""}"#
        ))
        .contains("empty host"));
        assert!(err_of(&call(zmq__build_endpoint, r#"{"transport":"tcp"}"#))
            .contains("missing address or host"));
    }

    #[test]
    fn endpoint_bind_to_connect_rewrites_wildcard_hosts() {
        let conn = |ep: &str| {
            call(
                zmq__endpoint_bind_to_connect,
                &format!(r#"{{"endpoint":"{ep}"}}"#),
            )["endpoint"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Each wildcard host → localhost, port preserved.
        assert_eq!(conn("tcp://*:5555"), "tcp://localhost:5555");
        assert_eq!(conn("tcp://0.0.0.0:5555"), "tcp://localhost:5555");
        assert_eq!(conn("tcp://[::]:5555"), "tcp://localhost:5555");
        // A `*` ephemeral-port wildcard is preserved (runtime-only).
        assert_eq!(conn("tcp://*:*"), "tcp://localhost:*");
        // Concrete hosts (incl. an IPv6 literal and an interface name) are untouched.
        assert_eq!(conn("tcp://localhost:5555"), "tcp://localhost:5555");
        assert_eq!(conn("tcp://[::1]:5555"), "tcp://[::1]:5555");
        assert_eq!(conn("tcp://eth0:5555"), "tcp://eth0:5555");
        // Non-IP transports pass through; `changed` reflects no rewrite.
        let ipc = call(
            zmq__endpoint_bind_to_connect,
            r#"{"endpoint":"ipc:///tmp/s"}"#,
        );
        assert_eq!(ipc["endpoint"], json!("ipc:///tmp/s"));
        assert_eq!(ipc["changed"], json!(false));
        let tcp = call(
            zmq__endpoint_bind_to_connect,
            r#"{"endpoint":"tcp://*:5555"}"#,
        );
        assert_eq!(tcp["changed"], json!(true));
        assert_eq!(tcp["bind_endpoint"], json!("tcp://*:5555"));
        // A custom connect host is honored.
        assert_eq!(
            call(
                zmq__endpoint_bind_to_connect,
                r#"{"endpoint":"tcp://0.0.0.0:9000","host":"broker.internal"}"#
            )["endpoint"],
            json!("tcp://broker.internal:9000")
        );
        // Malformed endpoint errors.
        assert!(err_of(&call(
            zmq__endpoint_bind_to_connect,
            r#"{"endpoint":"no-scheme"}"#
        ))
        .contains("not a ZMQ endpoint"));
    }

    #[test]
    fn valid_endpoint_checks_transport_syntax() {
        let v = |ep: &str| {
            call(zmq__valid_endpoint, &format!(r#"{{"endpoint":"{ep}"}}"#))["valid"]
                .as_bool()
                .unwrap()
        };
        // Valid forms across transports.
        assert!(v("tcp://127.0.0.1:5555"));
        assert!(v("tcp://*:*"), "bind wildcard host+port is valid");
        assert!(v("tcp://[::1]:5555"), "bracketed IPv6 host");
        assert!(v("ws://host:80/path"), "ws path is tolerated");
        assert!(v("ipc:///tmp/feeds/0"));
        assert!(v("inproc://workers"));
        assert!(v("pgm://eth0;239.0.0.1:7000"));
        // Invalid: no scheme, unknown transport, missing port, bad port, empty addr.
        assert!(!v("no-scheme"));
        assert!(!v("zzz://x"));
        assert!(!v("tcp://127.0.0.1"), "tcp needs a port");
        assert!(!v("tcp://host:99999"), "port out of range");
        assert!(!v("tcp://:5555"), "empty host");
        assert!(!v("inproc://"), "empty inproc name");
        // The reason and transport fields are populated on failure.
        let bad = call(zmq__valid_endpoint, r#"{"endpoint":"tcp://host:abc"}"#);
        assert_eq!(bad["valid"], json!(false));
        assert!(bad["reason"].as_str().unwrap().contains("port"));
        assert_eq!(bad["transport"], json!("tcp"));
        // A no-scheme string reports the missing `://` with a null transport.
        let ns = call(zmq__valid_endpoint, r#"{"endpoint":"oops"}"#);
        assert_eq!(ns["transport"], Value::Null);
        assert!(ns["reason"].as_str().unwrap().contains("://"));
    }

    #[test]
    fn topic_match_uses_prefix_semantics() {
        let m = call(
            zmq__topic_match,
            r#"{"subscription":"weather.","topic":"weather.us.ny"}"#,
        );
        assert_eq!(m["match"], json!(true), "prefix matches");
        let n = call(
            zmq__topic_match,
            r#"{"subscription":"sports.","topic":"weather.us"}"#,
        );
        assert_eq!(n["match"], json!(false), "non-prefix does not match");
        let all = call(
            zmq__topic_match,
            r#"{"subscription":"","topic":"anything"}"#,
        );
        assert_eq!(all["match"], json!(true), "empty subscription matches all");
    }

    #[test]
    fn topic_match_any_returns_all_matching_subscriptions() {
        // Two of three subscriptions are prefixes of the topic.
        let v = call(
            zmq__topic_match_any,
            r#"{"topic":"weather.us.ny","subscriptions":["weather.","sports.","weather.us"]}"#,
        );
        assert_eq!(v["match"], json!(true));
        assert_eq!(
            v["matched"],
            json!(["weather.", "weather.us"]),
            "matches kept in input order"
        );
        // No subscription is a prefix → no match, empty list.
        let none = call(
            zmq__topic_match_any,
            r#"{"topic":"news.today","subscriptions":["weather.","sports."]}"#,
        );
        assert_eq!(none["match"], json!(false));
        assert_eq!(none["matched"], json!([]));
        // An empty subscription in the set matches everything.
        let all = call(
            zmq__topic_match_any,
            r#"{"topic":"anything","subscriptions":["x",""]}"#,
        );
        assert_eq!(all["match"], json!(true));
        assert_eq!(all["matched"], json!([""]));
        // Empty subscription set never matches.
        assert_eq!(
            call(zmq__topic_match_any, r#"{"topic":"t","subscriptions":[]}"#)["match"],
            json!(false)
        );
        // Missing subscriptions errors.
        assert!(err_of(&call(zmq__topic_match_any, r#"{"topic":"t"}"#))
            .to_lowercase()
            .contains("subscriptions"));
    }

    #[test]
    fn valid_socket_type_canonicalizes_and_rejects() {
        let v = call(zmq__valid_socket_type, r#"{"type":"PUBLISH"}"#);
        assert_eq!(v["valid"], json!(true));
        assert_eq!(v["canonical"], json!("pub"), "alias collapses to canonical");
        let bad = call(zmq__valid_socket_type, r#"{"type":"nope"}"#);
        assert_eq!(bad["valid"], json!(false));
        assert_eq!(bad["canonical"], Value::Null);
    }

    #[test]
    fn socket_types_lists_every_canonical_name() {
        let v = call(zmq__socket_types, "{}");
        let types = v["types"].as_array().unwrap();
        assert_eq!(types.len(), 12, "twelve canonical socket types");
        for name in [
            "req", "rep", "pub", "sub", "router", "dealer", "xpub", "stream",
        ] {
            assert!(types.iter().any(|t| t == name), "missing {name}");
            // Each listed name must itself validate.
            let chk = call(zmq__valid_socket_type, &format!(r#"{{"type":"{name}"}}"#));
            assert_eq!(chk["valid"], json!(true), "{name} must validate");
        }
    }

    #[test]
    fn socket_peers_follows_the_zmq_compatibility_matrix() {
        // REQ talks to REP and ROUTER.
        let req = call(zmq__socket_peers, r#"{"type":"req"}"#);
        assert_eq!(req["type"], json!("req"));
        assert_eq!(req["peers"], json!(["rep", "router"]));
        // Alias input is canonicalized (publish → pub), then X variants.
        assert_eq!(
            call(zmq__socket_peers, r#"{"type":"PUBLISH"}"#)["type"],
            json!("pub")
        );
        assert_eq!(
            call(zmq__socket_peers, r#"{"type":"pub"}"#)["peers"],
            json!(["sub", "xsub"])
        );
        assert_eq!(
            call(zmq__socket_peers, r#"{"type":"sub"}"#)["peers"],
            json!(["pub", "xpub"])
        );
        // Pipeline and exclusive-pair.
        assert_eq!(
            call(zmq__socket_peers, r#"{"type":"push"}"#)["peers"],
            json!(["pull"])
        );
        assert_eq!(
            call(zmq__socket_peers, r#"{"type":"pair"}"#)["peers"],
            json!(["pair"])
        );
        // Symmetry: every listed peer lists the original back.
        for ty in [
            "req", "rep", "pub", "sub", "push", "pull", "dealer", "router", "pair",
        ] {
            let peers = call(zmq__socket_peers, &format!(r#"{{"type":"{ty}"}}"#));
            for p in peers["peers"].as_array().unwrap() {
                let back = call(
                    zmq__socket_peers,
                    &format!(r#"{{"type":"{}"}}"#, p.as_str().unwrap()),
                );
                assert!(
                    back["peers"].as_array().unwrap().iter().any(|x| x == ty),
                    "{ty} ↔ {} must be mutual",
                    p.as_str().unwrap()
                );
            }
        }
        // Unknown socket type errors.
        assert!(err_of(&call(zmq__socket_peers, r#"{"type":"nope"}"#))
            .to_lowercase()
            .contains("socket"));
    }

    #[test]
    fn socket_types_compatible_matches_the_peer_table() {
        // Canonical compatible pairs.
        for (a, b) in [
            ("req", "rep"),
            ("req", "router"),
            ("pub", "sub"),
            ("push", "pull"),
            ("dealer", "router"),
            ("pair", "pair"),
        ] {
            let v = call(
                zmq__socket_types_compatible,
                &format!(r#"{{"a":"{a}","b":"{b}"}}"#),
            );
            assert_eq!(v["compatible"], json!(true), "{a} ↔ {b} must be compatible");
        }
        // Incompatible: REQ does not talk to PUB; PUSH does not talk to PUSH.
        assert_eq!(
            call(zmq__socket_types_compatible, r#"{"a":"req","b":"pub"}"#)["compatible"],
            json!(false)
        );
        assert_eq!(
            call(zmq__socket_types_compatible, r#"{"a":"push","b":"push"}"#)["compatible"],
            json!(false)
        );
        // Aliases canonicalize, and a_peers echoes the shared peer table.
        let v = call(zmq__socket_types_compatible, r#"{"a":"PUBLISH","b":"sub"}"#);
        assert_eq!(v["a"], json!("pub"));
        assert_eq!(v["compatible"], json!(true));
        assert_eq!(v["a_peers"], json!(["sub", "xsub"]));
        // Compatibility agrees with socket_peers for the same type.
        let peers = call(zmq__socket_peers, r#"{"type":"dealer"}"#);
        for p in peers["peers"].as_array().unwrap() {
            let chk = call(
                zmq__socket_types_compatible,
                &format!(r#"{{"a":"dealer","b":"{}"}}"#, p.as_str().unwrap()),
            );
            assert_eq!(chk["compatible"], json!(true));
        }
        // Unknown type errors.
        assert!(err_of(&call(
            zmq__socket_types_compatible,
            r#"{"a":"req","b":"nope"}"#
        ))
        .to_lowercase()
        .contains("socket"));
    }

    #[test]
    fn socket_caps_reports_pattern_and_directionality() {
        // PUB is send-only, SUB receive-only — the asymmetric pub/sub pair.
        let pubc = call(zmq__socket_caps, r#"{"type":"pub"}"#);
        assert_eq!(pubc["pattern"], json!("publish-subscribe"));
        assert_eq!(pubc["can_send"], json!(true));
        assert_eq!(pubc["can_recv"], json!(false));
        let subc = call(zmq__socket_caps, r#"{"type":"sub"}"#);
        assert_eq!(subc["can_send"], json!(false));
        assert_eq!(subc["can_recv"], json!(true));
        // PUSH/PULL pipeline directionality mirrors PUB/SUB.
        assert_eq!(
            call(zmq__socket_caps, r#"{"type":"push"}"#)["can_recv"],
            json!(false)
        );
        assert_eq!(
            call(zmq__socket_caps, r#"{"type":"pull"}"#)["can_send"],
            json!(false)
        );
        // X variants and request-reply / pair / stream are bidirectional.
        for ty in [
            "xpub", "xsub", "req", "rep", "dealer", "router", "pair", "stream",
        ] {
            let c = call(zmq__socket_caps, &format!(r#"{{"type":"{ty}"}}"#));
            assert_eq!(c["can_send"], json!(true), "{ty} can send");
            assert_eq!(c["can_recv"], json!(true), "{ty} can recv");
        }
        // Pattern names and alias canonicalization.
        assert_eq!(
            call(zmq__socket_caps, r#"{"type":"req"}"#)["pattern"],
            json!("request-reply")
        );
        assert_eq!(
            call(zmq__socket_caps, r#"{"type":"PUBLISH"}"#)["type"],
            json!("pub")
        );
        assert_eq!(
            call(zmq__socket_caps, r#"{"type":"stream"}"#)["pattern"],
            json!("native")
        );
        // Every advertised socket type has a capability rule.
        for ty in call(zmq__socket_types, "{}")["types"].as_array().unwrap() {
            let c = call(
                zmq__socket_caps,
                &format!(r#"{{"type":"{}"}}"#, ty.as_str().unwrap()),
            );
            assert!(c["pattern"].is_string(), "{} has a pattern", ty);
            assert!(c["can_send"] == json!(true) || c["can_recv"] == json!(true));
        }
        assert!(err_of(&call(zmq__socket_caps, r#"{"type":"nope"}"#))
            .to_lowercase()
            .contains("socket"));
    }
}
