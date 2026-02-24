# NAME

EV::Hiredis - Asynchronous redis client using hiredis and EV

# SYNOPSIS

    use EV::Hiredis;

    my $redis = EV::Hiredis->new;
    $redis->connect('127.0.0.1');

    # or
    my $redis = EV::Hiredis->new( host => '127.0.0.1' );

    # command
    $redis->set('foo' => 'bar', sub {
        my ($res, $err) = @_;

        print $res; # OK

        $redis->get('foo', sub {
            my ($res, $err) = @_;

            print $res; # bar

            $redis->disconnect;
        });
    });

    # start main loop
    EV::run;

# DESCRIPTION

EV::Hiredis is an asynchronous client for Redis using hiredis and [EV](https://metacpan.org/pod/EV) as backend.

This module connects to [EV](https://metacpan.org/pod/EV) with C-level interface so that it runs faster.

# FEATURES

- Automatic reconnection with configurable delay and max attempts
- Flow control with local command queuing (`max_pending`, `waiting_timeout`)
- TLS/SSL support (optional, auto-detected at build time)
- RESP3 push messages and reply types (Redis 6.0+)
- Connection and command timeouts
- TCP keepalive, SO\_REUSEADDR, SOCK\_CLOEXEC socket options
- IPv4/IPv6 preference and source address binding

# ANYEVENT INTEGRATION

[AnyEvent](https://metacpan.org/pod/AnyEvent) has a support for EV as its one of backends, so [EV::Hiredis](https://metacpan.org/pod/EV%3A%3AHiredis) can be used in your AnyEvent applications seamlessly.

# NO UTF-8 SUPPORT

Unlike other redis modules, this module doesn't support utf-8 string.

This module handle all variables as bytes. You should encode your utf-8 string before passing commands like following:

    use Encode;

    # set $val
    $redis->set(foo => encode_utf8 $val, sub { ... });

    # get $val
    $redis->get(foo, sub {
        my $val = decode_utf8 $_[0];
    });

# METHODS

## new(%options);

Create new [EV::Hiredis](https://metacpan.org/pod/EV%3A%3AHiredis) instance.

Available `%options` are:

### Connection

- **host** => 'Str'
- **port** => 'Int'

    Hostname and port number of redis-server to connect. Mutually exclusive with `path`.

- **path** => 'Str'

    UNIX socket path to connect. Mutually exclusive with `host`.

- **connect\_timeout** => $num\_of\_milliseconds

    Connection timeout.

- **command\_timeout** => $num\_of\_milliseconds

    Command timeout.

- **loop** => 'EV::Loop'

    EV loop for running this instance. Default is `EV::default_loop`.

### Callbacks

- **on\_error** => $cb->($errstr)

    Error callback will be called when a connection level error occurs.
    If not provided (or `undef`), a default handler that calls `die` is installed.
    To have no error handler, call `$obj->on_error(undef)` after construction.

    This callback can be set by `$obj->on_error($cb)` method any time.

- **on\_connect** => $cb->()

    Connection callback will be called when connection successful and completed to redis server.

    This callback can be set by `$obj->on_connect($cb)` method any time.

- **on\_disconnect** => $cb->()

    Disconnect callback will be called when disconnection occurs (both normal and error cases).

    This callback can be set by `$obj->on_disconnect($cb)` method any time.

- **on\_push** => $cb->($reply)

    RESP3 push callback for server-initiated out-of-band messages (Redis 6.0+).
    Called with the decoded push message (an array reference).

### Reconnection

- **reconnect** => $bool

    Enable automatic reconnection on connection failure or unexpected disconnection. Default is disabled.

- **reconnect\_delay** => $num\_of\_milliseconds

    Delay between reconnection attempts. Default is 1000 (1 second).

- **max\_reconnect\_attempts** => $num

    Maximum number of reconnection attempts. 0 means unlimited. Default is 0.
    Negative values are treated as 0 (unlimited).

### Flow Control

- **max\_pending** => $num

    Maximum number of commands sent to Redis concurrently. When this limit is reached,
    additional commands are queued locally and sent as responses arrive.
    0 means unlimited (default).

- **waiting\_timeout** => $num\_of\_milliseconds

    Maximum time a command can wait in the local queue before being cancelled with
    "waiting timeout" error. 0 means unlimited (default).

- **resume\_waiting\_on\_reconnect** => $bool

    If true, waiting commands are preserved on disconnect and resumed after successful
    reconnection. Default is false (cancelled on disconnect).

### Socket Options

- **keepalive** => $seconds

    TCP keepalive interval. 0 means disabled (default).

- **cloexec** => $bool

    Set SOCK\_CLOEXEC on the socket. Enabled by default.

- **reuseaddr** => $bool

    Set SO\_REUSEADDR on the socket. Disabled by default.

- **prefer\_ipv4** => $bool

    Prefer IPv4 addresses when resolving hostnames.

- **prefer\_ipv6** => $bool

    Prefer IPv6 addresses when resolving hostnames.

- **source\_addr** => 'Str'

    Local address to bind the outbound connection to.

- **tcp\_user\_timeout** => $num\_of\_milliseconds

    TCP\_USER\_TIMEOUT socket option (Linux-specific).

- **priority** => $num

    Priority for the underlying libev IO watchers. Range: -2 to +2. Default is 0.

### TLS

Requires TLS support compiled in (auto-detected at build time via OpenSSL). Check with `EV::Hiredis->has_ssl`.

- **tls** => $bool

    Enable TLS encryption. Only valid with `host` connections, not `path`.

- **tls\_ca** => 'Str'

    Path to CA certificate file. Uses system default if omitted.

- **tls\_cert** => 'Str'

    Path to client certificate for mutual TLS.

- **tls\_key** => 'Str'

    Path to client private key for mutual TLS.

- **tls\_server\_name** => 'Str'

    Server name for SNI.

- **tls\_capath** => 'Str'

    Path to a directory containing CA certificate files (hashed filenames). Alternative to `tls_ca`.

- **tls\_verify** => $bool

    Enable or disable TLS peer verification. Default is true. Set to false for self-signed certificates.

All parameters are optional.

If parameters about connection (host&port or path) is not passed, you should call `connect` or `connect_unix` method by hand to connect to redis-server.

## connect($hostname, $port)

## connect\_unix($path)

Connect to a redis-server for `$hostname:$port` or `$path`.

on\_connect callback will be called if connection is successful, otherwise on\_error callback is called.

## command($commands..., $cb->($result, $error))

Do a redis command and return its result by callback.

    $redis->command('get', 'foo', sub {
        my ($result, $error) = @_;

        print $result; # value for key 'foo'
        print $error;  # redis error string, undef if no error
    });

If any error is occurred, `$error` presents the error message and `$result` is undef.
If no error, `$error` is undef and `$result` presents response from redis.

NOTE: Alternatively all commands can be called via AUTOLOAD interface.

    $redis->command('get', 'foo', sub { ... });

is equivalent to:

    $redis->get('foo', sub { ... });

**Note:** Calling `command()` while not connected will croak, unless automatic
reconnection is active (reconnect timer running). In that case, commands are
automatically queued and sent after successful reconnection. Queued commands
respect `waiting_timeout` if set.

## disconnect

Disconnect from redis-server. Safe to call when already disconnected (no-op).
Stops any pending reconnect timer. When called while already disconnected, also
clears any waiting commands (e.g., preserved by `resume_waiting_on_reconnect`).

## is\_connected

Returns true (1) if connected to redis-server, false (0) otherwise.

## has\_ssl

Class method. Returns true (1) if the module was built with TLS support, false (0) otherwise.

## reconnect($enable, $delay\_ms, $max\_attempts)

Configure automatic reconnection. `$delay_ms` defaults to 1000 (1 second).
`$max_attempts` defaults to 0 (unlimited).

    $redis->reconnect(1);                    # enable with defaults (1s delay, unlimited)
    $redis->reconnect(1, 0);                 # enable with immediate reconnect
    $redis->reconnect(1, 2000);              # enable with 2 second delay
    $redis->reconnect(1, 1000, 5);           # enable with 1s delay, max 5 attempts
    $redis->reconnect(0);                    # disable

## reconnect\_enabled

Returns true (1) if automatic reconnection is enabled, false (0) otherwise.

## skip\_waiting

Cancel only waiting (not yet sent) command callbacks. Each callback is invoked with `(undef, "skipped")`.

## skip\_pending

Cancel all pending and waiting command callbacks. Each callback is invoked with `(undef, "skipped")`.

## on\_error(\[$cb->($errstr)\])

Set error callback. With `undef` or without arguments, clears the handler.

## on\_connect(\[$cb->()\])

Set connect callback. With `undef` or without arguments, clears the handler.

## on\_disconnect(\[$cb->()\])

Set disconnect callback. With `undef` or without arguments, clears the handler.

## on\_push(\[$cb->($reply)\])

Set RESP3 push callback for server-initiated messages (Redis 6.0+).

## connect\_timeout(\[$ms\])

Get or set the connection timeout in milliseconds. Returns the current value, or undef if not set.

## command\_timeout(\[$ms\])

Get or set the command timeout in milliseconds. Returns the current value, or undef if not set.
When changed while connected, takes effect immediately.

## pending\_count

Returns the number of commands sent to Redis awaiting responses.
Persistent commands (subscribe, psubscribe, ssubscribe, monitor) are not
included in this count.
When called from inside a command callback, the count includes the
current command (it is decremented after the callback returns).

## waiting\_count

Returns the number of commands queued locally (not yet sent to Redis).

## max\_pending(\[$limit\])

Get or set the maximum number of concurrent commands sent to Redis.
Persistent commands (subscribe, psubscribe, ssubscribe, monitor) are not
subject to this limit.
0 means unlimited (default).

## waiting\_timeout(\[$ms\])

Get or set the maximum time in milliseconds a command can wait in the local queue.
0 means unlimited (default).

## resume\_waiting\_on\_reconnect(\[$bool\])

Get or set whether waiting commands are preserved on disconnect and resumed after reconnection.
Default is false.

## priority(\[$num\])

Get or set the libev watcher priority. Range: -2 to +2, default 0. Values outside this range are clamped.

## keepalive(\[$seconds\])

Get or set TCP keepalive interval. 0 means disabled (default).

## prefer\_ipv4(\[$bool\])

Get or set IPv4 preference for DNS resolution.

## prefer\_ipv6(\[$bool\])

Get or set IPv6 preference for DNS resolution.

## source\_addr(\[$addr\])

Get or set the local source address to bind to. Pass `undef` to clear.

## tcp\_user\_timeout(\[$ms\])

Get or set TCP\_USER\_TIMEOUT in milliseconds (Linux-specific). 0 means OS default.

## cloexec(\[$bool\])

Get or set SOCK\_CLOEXEC on the socket. Enabled by default.

## reuseaddr(\[$bool\])

Get or set SO\_REUSEADDR on the socket. Disabled by default.

# RECONNECTION EXAMPLE

    my $redis = EV::Hiredis->new(
        host                       => '127.0.0.1',
        reconnect                  => 1,
        reconnect_delay            => 2000,
        max_reconnect_attempts     => 10,
        resume_waiting_on_reconnect => 1,
        on_connect    => sub { warn "connected\n" },
        on_disconnect => sub { warn "disconnected\n" },
        on_error      => sub { warn "error: $_[0]\n" },
    );

# TLS EXAMPLE

    my $redis = EV::Hiredis->new(
        host            => 'redis.example.com',
        port            => 6380,
        tls             => 1,
        tls_ca          => '/path/to/ca.crt',
        tls_cert        => '/path/to/client.crt',
        tls_key         => '/path/to/client.key',
        tls_server_name => 'redis.example.com',
    );

# AUTHOR

Daisuke Murase <typester@cpan.org>

# COPYRIGHT AND LICENSE

Copyright (c) 2013 Daisuke Murase All rights reserved.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.
