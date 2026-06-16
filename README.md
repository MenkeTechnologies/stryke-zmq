```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                    [ z m q ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-zmq/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-zmq/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[ZEROMQ CLIENT FOR STRYKE // REQ/REP + PUB/SUB + PUSH/PULL + DEALER/ROUTER]`

> *"Brokerless messaging, no daemon to babysit."*

ZeroMQ client for stryke — the brokerless messaging library. All four
canonical patterns (request/reply, publish/subscribe, push/pull pipeline,
dealer/router) plus PAIR, over TCP, IPC, or in-process transports. Opt-in
package tier, kept out of the stryke core binary so the daily-driver
install stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-kafka`](https://github.com/MenkeTechnologies/stryke-kafka) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] Socket handles](#0x03-socket-handles)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

ZeroMQ integration requires libzmq, the C library every ZMQ binding wraps.
The Rust binding (`zmq` → `zmq-sys` → `zeromq-src`) compiles libzmq and
libsodium from source and links them statically. The artifact is big
enough that it doesn't belong in stryke core. Opt in once, get all four
messaging patterns.

`stryke-zmq` ships a thin stryke library plus a Rust cdylib
(`libstryke_zmq.{dylib,so}`) dlopened in-process. **libzmq is vendored
into the cdylib** — there is no system `libzmq.so`/`.dylib` requirement on
the consumer machine.

## [0x01] Install

From a release (no rustc + cmake build on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-zmq
```

From a local checkout:

```sh
cd ~/projects/stryke-zmq
cargo build --release            # first build vendors libzmq via cmake (~1-2 min)
s pkg install -g .               # cdylib lands in ~/.stryke/store/zmq@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Zmq`. A shared
`zmq::Context` plus a socket-handle registry are held in `OnceCell`, so
sockets persist across calls — a SUB keeps receiving and a REQ/REP
conversation keeps its state between separate `Zmq::send`/`Zmq::recv`
calls. A build needs cmake + a C/C++ compiler; the first build is the slow
one, subsequent builds reuse the cached libzmq archive.

## [0x02] Quick start

```stryke
use Zmq

# REQ/REP over TCP
val $server = Zmq::socket("rep", bind => "tcp://*:5555")
val $client = Zmq::socket("req", connect => "tcp://localhost:5555")

Zmq::send($client, "ping")
val $request = Zmq::recv($server, timeout_ms => 1000)   # "ping"
Zmq::send($server, "pong")
val $reply = Zmq::recv($client, timeout_ms => 1000)     # "pong"

Zmq::close($client)
Zmq::close($server)
```

PUB/SUB with a topic filter:

```stryke
val $pub = Zmq::socket("pub", bind => "tcp://*:5556")
val $sub = Zmq::socket("sub", connect => "tcp://localhost:5556", subscribe => "weather")

Zmq::send($pub, "weather sunny")
val $msg = Zmq::recv($sub, timeout_ms => 500)
```

One-shot request without managing a handle:

```stryke
val $reply = Zmq::request("tcp://localhost:5555", "hello", timeout_ms => 2000)
```

## [0x03] Socket handles

ZeroMQ sockets are stateful and long-lived, so the API is handle-based:
`Zmq::socket` creates a socket, applies any bind/connect/options, and
returns an integer handle. Every later operation takes that handle.

This is deliberate. A SUB socket must stay open to keep receiving; a
fork-per-call model would drop messages between calls and re-pay
connection setup each time. The cdylib holds the sockets in a registry
behind a mutex (ZeroMQ requires one-thread-at-a-time access per socket;
the mutex provides exactly that plus the cross-thread memory fence libzmq
mandates).

Close sockets you no longer need with `Zmq::close($handle)`. Dropping the
process closes everything via libzmq's `zmq_close` on drop.

## [0x04] API reference

| Function | Returns | Notes |
|---|---|---|
| `Zmq::version()` | string | package (crate) version |
| `Zmq::lib_version()` | hashref | vendored libzmq `{ major, minor, patch, version }` |
| `Zmq::has($capability)` | bool \| undef | probe `curve`/`gssapi`/`ipc`/`pgm`/`tipc`/`norm`/`draft` |
| `Zmq::socket($type, %opts)` | handle (int) | type: req/rep/pub/sub/push/pull/dealer/router/pair/xpub/xsub/stream. opts: bind, connect, subscribe + any settable option (see `set`) |
| `Zmq::close($handle)` | hashref | closes + removes the socket |
| `Zmq::bind($handle, $endpoint)` | string | binds; returns the concrete endpoint (resolves `tcp://*:*`) |
| `Zmq::connect($handle, $endpoint)` | hashref | dynamic connect |
| `Zmq::unbind($handle, $endpoint)` | hashref | drop a bound endpoint |
| `Zmq::disconnect($handle, $endpoint)` | hashref | drop a connected endpoint |
| `Zmq::send($handle, $data, %opts)` | hashref | opts: `more => 1`; `encoding => utf8\|hex\|base64` |
| `Zmq::send_multipart($handle, $parts, %opts)` | hashref | `$parts` arrayref; opts: `encoding` |
| `Zmq::recv($handle, %opts)` | string \| undef | opts: timeout_ms, encoding. undef on timeout |
| `Zmq::recv_multipart($handle, %opts)` | list | opts: timeout_ms, encoding. empty list on timeout |
| `Zmq::subscribe($handle, $topic)` | hashref | SUB topic filter (`""` = all) |
| `Zmq::unsubscribe($handle, $topic)` | hashref | remove a subscription |
| `Zmq::set($handle, $opt, $value)` | hashref | full socket-option table: timeouts/buffers/hwm, tcp_keepalive\*, heartbeat\*, ipv6, immediate, conflate, router/req flags, CURVE keys, plain auth, identity… |
| `Zmq::get($handle, $opt)` | scalar | read back any option (type, last_endpoint, mechanism, fd, CURVE keys as z85, …) |
| `Zmq::poll($handle, %opts)` | hashref | `{ readable, writable }`; opts: timeout_ms |
| `Zmq::poll_many($handles, %opts)` | list | one `zmq_poll` over many handles → `{ handle, readable, writable, error }` |
| `Zmq::monitor($handle, $endpoint, %opts)` | hashref | publish lifecycle events to an inproc endpoint; opts: `events` |
| `Zmq::monitor_recv($handle, %opts)` | hashref \| undef | decode one event `{ event, value, endpoint }` from a monitor PAIR |
| `Zmq::proxy($frontend, $backend, %opts)` | hashref | backgrounded `zmq_proxy` device; opts: `capture`, `control` (steerable) |
| `Zmq::curve_keypair()` | hashref | `{ public, secret }` z85 keys (needs libsodium-enabled libzmq) |
| `Zmq::curve_public($secret)` | hashref | `{ secret, public }` — derive the public key from a z85 secret (`zmq_curve_public`) |
| `Zmq::z85_encode($data, %opts)` / `Zmq::z85_decode($z85, %opts)` | string | z85 codec; opts: `encoding` |
| `Zmq::z85_valid($z85)` | `{ z85, valid, reason }` | structural RFC-32 check: length ÷5 and Z85 alphabet (non-throwing predicate) |
| `Zmq::request($endpoint, $data, %opts)` | string \| undef | one-shot REQ round-trip; opts: timeout_ms (default 5000), encoding |
| `Zmq::parse_endpoint($endpoint)` | hashref | `{ transport, address, known_transport, host?, port? }` — no socket |
| `Zmq::build_endpoint(%opts)` | hashref | `{ endpoint, known_transport }` — inverse of parse_endpoint; opts: transport, address \| host + port |
| `Zmq::topic_match($subscription, $topic)` | 1 \| "" | ZMQ SUB prefix match (empty subscription matches all) |
| `Zmq::topic_match_any($topic, \@subscriptions)` | `{ topic, match, matched }` | XPUB set routing: which subscriptions prefix-match the topic (in input order) |
| `Zmq::valid_socket_type($type)` | hashref | `{ valid, canonical }` — aliases collapse (`publish` → `pub`) |
| `Zmq::socket_peers($type)` | list | socket types `$type` can validly connect to (ZMQ messaging-pattern compatibility) |
| `Zmq::socket_types_compatible($a, $b)` | 1 \| "" | whether types `$a` and `$b` can be connected as peers (REQ↔REP, PUB↔SUB, PUSH↔PULL, …) |
| `Zmq::socket_caps($type)` | `{ type, pattern, can_send, can_recv }` | messaging pattern + send/recv directionality (PUB send-only, SUB recv-only, …) |
| `Zmq::socket_types()` | list | every canonical socket-type name |

The last four are pure helpers — string/validation utilities that create no
socket. Endpoints follow ZeroMQ's transport syntax: `tcp://host:port`,
`ipc:///tmp/sock`, `inproc://name`, `pgm://`, `epgm://`.

Payloads default to UTF-8 framing. For arbitrary bytes (NULs, high bytes)
pass `encoding => "hex"` or `encoding => "base64"` on both the send and the
matching recv so the message round-trips intact.

## [0x05] FFI layer

Each `zmq__*` export in the cdylib is a JSON-string-in / JSON-string-out
function. stryke's FFI bridge resolves the symbols listed in
`stryke.toml`'s `[ffi]` table on first `use Zmq`, passes a JSON-encoded
args dict per call, and copies the returned JSON into a stryke string.
Allocations returned from the cdylib are freed via `stryke_free_cstring`.

A handler that errors returns `{"error": "..."}`, which the stryke wrapper
turns into a `die "Zmq::<op>: <reason>"`. A handler that panics is caught
at the boundary and returned as an error rather than crossing the FFI
boundary and aborting the host shell. A receive timeout returns
`{"timeout": true}`, which the wrapper maps to `undef`/empty-list.

## [0x06] Tests

```sh
cargo test                 # Rust unit + FFI-contract tests (run over inproc://)
s test t/                  # stryke assertion suite (needs the cdylib installed)
```

The Rust tests exercise the full socket lifecycle, push/pull and pair
round-trips, the timeout-to-flag mapping, binary-safe hex framing through
real sockets, `set`→`get` option round-trips, wildcard-bind endpoint
discovery, `poll_many` over multiple handles, the z85 codec, libzmq
introspection, an end-to-end socket-event monitor, and every
argument-validation path over `inproc://` transport — no external broker
required. The stryke suite re-checks the same patterns through the wrapper
plus a symbol-completeness pin over `lib/Zmq.stk`.

## [0x07] Dev workflow

```sh
make release      # cargo build --release
make debug        # cargo build
make test         # cargo test + s test t/
make install      # s pkg install -g .
make clean        # cargo clean
```

## [0x08] Layout

```
stryke-zmq/
  Cargo.toml             # cdylib crate (zmq -> vendored libzmq)
  src/lib.rs             # zmq__* exports + socket registry + tests
  stryke.toml            # package manifest + [ffi] table
  lib/Zmq.stk            # stryke wrapper (use Zmq)
  examples/              # req_rep, pub_sub, push_pull, multipart
  t/                     # stryke assertion suites
  tests/                 # docs/readme/polish lint gates
  docs/                  # GitHub Pages content
  Makefile
```

## [0x09] Roadmap

Shipped: binary-safe payloads (`encoding => hex|base64`), multi-socket
`poll_many`, the full socket-option table (`set`/`get`), dynamic
bind/connect/unbind/disconnect, socket-event monitoring, a backgrounded
`proxy` device, and the z85 codec + `curve_keypair` (CURVE key auth on
sockets via `curve_*` options; keypair generation needs a libsodium-enabled
libzmq — probe `Zmq::has("curve")`).

Open:

- Streaming `recv` callback loop for long-running SUB/PULL consumers.
- A higher-level steerable-proxy control helper (PAUSE/RESUME/TERMINATE).

## [0xFF] License

MIT. See [LICENSE](LICENSE).
