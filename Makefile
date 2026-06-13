SHELL := /bin/sh
.PHONY: all build debug release test clean install help

all: release

help:
	@printf '%s\n' \
	  'targets:' \
	  '  make release   - cargo build --release  (first run vendors libzmq via cmake, ~1-2 min)' \
	  '  make debug     - cargo build' \
	  '  make test      - cargo test then `s test t/`' \
	  '  make install   - `s pkg install -g .` (cdylib lands in ~/.stryke/store/zmq@<ver>/)' \
	  '  make clean     - cargo clean'

release:
	cargo build --release

debug build:
	cargo build

test:
	cargo test
	s test t/ || true

install: release
	s pkg install -g .

clean:
	cargo clean
