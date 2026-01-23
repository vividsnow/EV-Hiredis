# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EV-Hiredis is a Perl XS module providing asynchronous Redis client support using the hiredis C library and the EV event loop. It uses callback-based async patterns (not promises or exceptions).

## Build Commands

```bash
# Configure (compiles embedded hiredis dependency)
perl Build.PL

# Build XS module
./Build

# Run unit tests (requires Redis server)
./Build test

# Run all tests including author tests (POD, spelling)
./Build test --all

# Run single test
perl -Ilib -Iblib/arch t/command.t

# Install
./Build install
```

Alternative with Minilla: `minil build`, `minil test`, `minil install`

## Architecture

**Stack:** User Perl code → `lib/EV/Hiredis.pm` (AUTOLOAD wrapper) → `src/EV__Hiredis.xs` (XS layer) → hiredis C library → Redis

**Key files:**
- `lib/EV/Hiredis.pm` - Perl interface with AUTOLOAD for command dispatch
- `src/EV__Hiredis.xs` - Core XS implementation
- `src/libev_adapter.h` - Custom hiredis-libev integration
- `builder/MyBuilder.pm` - Custom build class that compiles `deps/hiredis` as static library

**Callback pattern:** All commands are async with `($result, $error)` callback signature:
```perl
$redis->get('key', sub {
    my ($result, $error) = @_;
    # $error is set on failure, $result contains value on success
});
```

**Persistent callbacks:** `subscribe`, `psubscribe`, `ssubscribe` (sharded pub/sub), `monitor` commands have persistent callbacks (called multiple times).

**Disconnect behavior:** On disconnect, all pending command callbacks (including persistent ones) are invoked with `(undef, $error)` before cleanup. This notifies user code that pending operations were cancelled. Handle in callbacks:
```perl
$redis->subscribe('channel', sub {
    my ($result, $error) = @_;
    if ($error && !defined $result) {
        # Disconnected - subscription no longer active
        return;
    }
    # Normal message handling...
});
```

**Connection callbacks:**
- `on_error($cb)` - Called on connection-level errors
- `on_connect($cb)` - Called when connection established
- `on_disconnect($cb)` - Called when disconnection occurs (normal or error)
- All callbacks support clearing by calling without arguments

**Automatic reconnect:**
- Enable via constructor: `reconnect => 1, reconnect_delay => 1000, max_reconnect_attempts => 5`
- Or via method: `$redis->reconnect(1, 1000, 5)` (enable, delay_ms, max_attempts)
- `reconnect_enabled()` - Returns true if reconnect is enabled
- Only triggers on unexpected disconnection (not explicit `disconnect()` calls)
- Max attempts of 0 means unlimited retries

**Utility methods:**
- `is_connected()` - Returns true if connected
- `connect_timeout($ms)` / `command_timeout($ms)` - Set/get timeouts (returns current value if no argument)

## Testing Requirements

- Tests require a running Redis server (Test::RedisServer spawns one)
- Memory leak tests use Devel::Refcount
- CI tests against Perl 5.12-5.36 and Redis 2.8-7.0

## Important Notes

- No UTF-8 support: strings are bytes, use `Encode::encode_utf8`/`decode_utf8` manually
- `command()` returns status code, actual data comes via callbacks
- Error handling via callbacks, not exceptions (callbacks use `G_EVAL` for exception safety)
- hiredis 1.1.1 is embedded as git submodule in `deps/hiredis/`
- Supports RESP3 reply types: DOUBLE, BOOL, MAP, SET, PUSH, BIGNUM, VERB (Redis 6+)
- AUTOLOAD installs methods on first call; `can()` returns true only after method is installed
