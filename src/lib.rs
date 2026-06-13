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

/// Apply the optional socket tuning + connection keys shared by `socket`.
fn apply_opts(sock: &Socket, opts: &Value) -> Result<()> {
    if let Some(v) = opts.get("sndhwm").and_then(Value::as_i64) {
        sock.set_sndhwm(v as i32)?;
    }
    if let Some(v) = opts.get("rcvhwm").and_then(Value::as_i64) {
        sock.set_rcvhwm(v as i32)?;
    }
    if let Some(v) = opts.get("linger").and_then(Value::as_i64) {
        sock.set_linger(v as i32)?;
    }
    if let Some(v) = opts.get("sndtimeo").and_then(Value::as_i64) {
        sock.set_sndtimeo(v as i32)?;
    }
    if let Some(v) = opts.get("rcvtimeo").and_then(Value::as_i64) {
        sock.set_rcvtimeo(v as i32)?;
    }
    if let Some(v) = opts.get("identity").and_then(Value::as_str) {
        sock.set_identity(v.as_bytes())?;
    }
    if let Some(v) = opts.get("conflate").and_then(Value::as_bool) {
        sock.set_conflate(v)?;
    }
    // bind / connect accept a single endpoint or an array of them.
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
    let ty = parse_socket_type(req_str(&opts, "type")?)?;
    let sock = ctx().socket(ty)?;
    apply_opts(&sock, &opts)?;
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    sockets().lock().insert(handle, sock);
    Ok(json!({"handle": handle, "type": req_str(&opts, "type")?}))
}

fn op_send(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let data = req_str(&opts, "data")?.to_string();
    let more = opts.get("more").and_then(Value::as_bool).unwrap_or(false);
    let flags = if more { zmq::SNDMORE } else { 0 };
    with_socket(handle, |s| {
        s.send(data.as_bytes(), flags)?;
        Ok(json!({"ok": true, "bytes": data.len()}))
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
        .map(|v| v.as_str().unwrap_or("").as_bytes().to_vec())
        .collect();
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

fn op_recv(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64);
    let r = with_socket(handle, |s| {
        if let Some(t) = timeout_ms {
            s.set_rcvtimeo(t as i32)?;
        }
        let bytes = s.recv_bytes(0)?;
        Ok(json!({
            "data": String::from_utf8_lossy(&bytes),
            "bytes": bytes.len(),
        }))
    });
    match r {
        Ok(v) => Ok(v),
        Err(e) if is_eagain(&e) => Ok(json!({"timeout": true})),
        Err(e) => Err(e),
    }
}

fn op_recv_multipart(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let timeout_ms = opts.get("timeout_ms").and_then(Value::as_i64);
    let r = with_socket(handle, |s| {
        if let Some(t) = timeout_ms {
            s.set_rcvtimeo(t as i32)?;
        }
        let frames = s.recv_multipart(0)?;
        let parts: Vec<Value> = frames
            .iter()
            .map(|f| Value::String(String::from_utf8_lossy(f).into_owned()))
            .collect();
        Ok(json!({"parts": parts}))
    });
    match r {
        Ok(v) => Ok(v),
        Err(e) if is_eagain(&e) => Ok(json!({"timeout": true})),
        Err(e) => Err(e),
    }
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
    let as_i32 = || -> Result<i32> {
        val.as_i64()
            .map(|v| v as i32)
            .ok_or_else(|| anyhow!("{key} expects an integer value"))
    };
    with_socket(handle, |s| {
        match key.as_str() {
            "sndhwm" => s.set_sndhwm(as_i32()?)?,
            "rcvhwm" => s.set_rcvhwm(as_i32()?)?,
            "linger" => s.set_linger(as_i32()?)?,
            "sndtimeo" => s.set_sndtimeo(as_i32()?)?,
            "rcvtimeo" => s.set_rcvtimeo(as_i32()?)?,
            "conflate" => s.set_conflate(
                val.as_bool()
                    .ok_or_else(|| anyhow!("conflate expects a boolean value"))?,
            )?,
            "identity" => s.set_identity(
                val.as_str()
                    .ok_or_else(|| anyhow!("identity expects a string value"))?
                    .as_bytes(),
            )?,
            other => return Err(anyhow!("unknown or unsettable option: {other}")),
        }
        Ok(json!({"ok": true}))
    })
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

fn op_close(opts: Value) -> Result<Value> {
    let handle = req_u64(&opts, "handle")?;
    let removed = sockets().lock().remove(&handle).is_some();
    // Dropping the Socket closes it (libzmq zmq_close on Drop).
    Ok(json!({"ok": true, "closed": removed}))
}

/// One-shot REQ round-trip convenience: connect a fresh ephemeral REQ
/// socket, send, recv the reply, drop. For simple request/reply where
/// holding a handle is overkill. Real conversations should use a kept
/// `socket` handle.
fn op_request(opts: Value) -> Result<Value> {
    let endpoint = req_str(&opts, "endpoint")?;
    let data = req_str(&opts, "data")?;
    let timeout_ms = opts
        .get("timeout_ms")
        .and_then(Value::as_i64)
        .unwrap_or(5000);
    let sock = ctx().socket(SocketType::REQ)?;
    sock.set_rcvtimeo(timeout_ms as i32)?;
    sock.set_sndtimeo(timeout_ms as i32)?;
    sock.set_linger(0)?;
    sock.connect(endpoint)?;
    sock.send(data.as_bytes(), 0)?;
    match sock.recv_bytes(0) {
        Ok(bytes) => Ok(json!({
            "reply": String::from_utf8_lossy(&bytes),
            "bytes": bytes.len(),
        })),
        Err(zmq::Error::EAGAIN) => Ok(json!({"timeout": true})),
        Err(e) => Err(anyhow!(e)),
    }
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

#[no_mangle]
pub extern "C" fn zmq__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn zmq__socket(args: *const c_char) -> *const c_char {
    ffi_call(args, op_socket)
}

#[no_mangle]
pub extern "C" fn zmq__send(args: *const c_char) -> *const c_char {
    ffi_call(args, op_send)
}

#[no_mangle]
pub extern "C" fn zmq__send_multipart(args: *const c_char) -> *const c_char {
    ffi_call(args, op_send_multipart)
}

#[no_mangle]
pub extern "C" fn zmq__recv(args: *const c_char) -> *const c_char {
    ffi_call(args, op_recv)
}

#[no_mangle]
pub extern "C" fn zmq__recv_multipart(args: *const c_char) -> *const c_char {
    ffi_call(args, op_recv_multipart)
}

#[no_mangle]
pub extern "C" fn zmq__subscribe(args: *const c_char) -> *const c_char {
    ffi_call(args, op_subscribe)
}

#[no_mangle]
pub extern "C" fn zmq__unsubscribe(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unsubscribe)
}

#[no_mangle]
pub extern "C" fn zmq__set(args: *const c_char) -> *const c_char {
    ffi_call(args, op_set)
}

#[no_mangle]
pub extern "C" fn zmq__poll(args: *const c_char) -> *const c_char {
    ffi_call(args, op_poll)
}

#[no_mangle]
pub extern "C" fn zmq__close(args: *const c_char) -> *const c_char {
    ffi_call(args, op_close)
}

#[no_mangle]
pub extern "C" fn zmq__request(args: *const c_char) -> *const c_char {
    ffi_call(args, op_request)
}

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
}
