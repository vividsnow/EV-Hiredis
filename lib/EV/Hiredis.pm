package EV::Hiredis;
use strict;
use warnings;

use EV;

BEGIN {
    use XSLoader;
    our $VERSION = '0.07';
    XSLoader::load __PACKAGE__, $VERSION;
}

sub new {
    my ($class, %args) = @_;

    my $loop = $args{loop} || EV::default_loop;
    my $self = $class->_new($loop);

    $self->on_error($args{on_error} || sub { die @_ });
    $self->on_connect($args{on_connect}) if $args{on_connect};
    $self->on_disconnect($args{on_disconnect}) if $args{on_disconnect};
    $self->connect_timeout($args{connect_timeout}) if $args{connect_timeout};
    $self->command_timeout($args{command_timeout}) if $args{command_timeout};
    $self->max_pending($args{max_pending}) if $args{max_pending};
    $self->waiting_timeout($args{waiting_timeout}) if $args{waiting_timeout};
    $self->resume_waiting_on_reconnect($args{resume_waiting_on_reconnect}) if $args{resume_waiting_on_reconnect};
    $self->priority($args{priority}) if defined $args{priority};

    # Configure reconnect if specified
    if ($args{reconnect}) {
        $self->reconnect(
            1,
            $args{reconnect_delay} || 1000,
            $args{max_reconnect_attempts} || 0
        );
    }

    if (exists $args{host}) {
        $self->connect($args{host}, defined $args{port} ? $args{port} : 6379);
    }
    elsif (exists $args{path}) {
        $self->connect_unix($args{path});
    }

    $self;
}

our $AUTOLOAD;

sub AUTOLOAD {
    (my $method = $AUTOLOAD) =~ s/.*:://;
    return if $method eq 'DESTROY';

    my $sub = sub {
        my $self = shift;
        $self->command($method, @_);
    };

    no strict 'refs';
    *$method = $sub;
    goto $sub;
}

sub can {
    my ($self, $method) = @_;

    # Check for installed methods (including those installed by AUTOLOAD)
    no strict 'refs';
    my $code = *{"EV::Hiredis::$method"}{CODE};
    return $code if $code;

    # Fall back to SUPER::can for inherited methods
    return $self->SUPER::can($method);
}

1;

=head1 NAME

EV::Hiredis - Asynchronous redis client using hiredis and EV

=head1 SYNOPSIS

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

=head1 DESCRIPTION

EV::Hiredis is a asynchronous client for Redis using hiredis and L<EV> as backend.

This module connected to L<EV> with C-Level interface so that it runs faster.

=head1 ANYEVENT INTEGRATION

L<AnyEvent> has a support for EV as its one of backends, so L<EV::Hiredis> can be used in your AnyEvent applications seamlessly.

=head1 NO UTF-8 SUPPORT

Unlike other redis modules, this module doesn't support utf-8 string.

This module handle all variables as bytes. You should encode your utf-8 string before passing commands like following:

    use Encode;
    
    # set $val
    $redis->set(foo => encode_utf8 $val, sub { ... });
    
    # get $val
    $redis->get(foo, sub {
        my $val = decode_utf8 $_[0];
    });

=head1 METHODS

=head2 new(%options);

Create new L<EV::Hiredis> instance.

Available C<%options> are:

=over

=item * host => 'Str'

=item * port => 'Int'

Hostname and port number of redis-server to connect.

=item * path => 'Str'

UNIX socket path to connect.

=item * on_error => $cb->($errstr)

Error callback will be called when a connection level error occurs.

This callback can be set by C<< $obj->on_error($cb) >> method any time.

=item * on_connect => $cb->()

Connection callback will be called when connection successful and completed to redis server.

This callback can be set by C<< $obj->on_connect($cb) >> method any time.

=item * on_disconnect => $cb->()

Disconnect callback will be called when disconnection occurs (both normal and error cases).

This callback can be set by C<< $obj->on_disconnect($cb) >> method any time.

=item * connect_timeout => $num_of_milliseconds

Connection timeout.

=item * command_timeout => $num_of_milliseconds

Command timeout.

=item * max_pending => $num

Maximum number of commands sent to Redis concurrently. When this limit is reached,
additional commands are queued locally and sent as responses arrive.
0 means unlimited (default). Use C<waiting_count> to check the local queue size.

=item * waiting_timeout => $num_of_milliseconds

Maximum time a command can wait in the local queue before being cancelled with
"waiting timeout" error. 0 means unlimited (default).

=item * resume_waiting_on_reconnect => $bool

Controls behavior of waiting queue on disconnect. If false (default), waiting
commands are cancelled with error on disconnect. If true, waiting commands are
preserved and resumed after successful reconnection.

=item * reconnect => $bool

Enable automatic reconnection on connection failure or unexpected disconnection.
Default is disabled (0).

=item * reconnect_delay => $num_of_milliseconds

Delay between reconnection attempts. Default is 1000 (1 second).

=item * max_reconnect_attempts => $num

Maximum number of reconnection attempts. 0 means unlimited. Default is 0.

=item * priority => $num

Priority for the underlying libev IO watchers. Higher priority watchers are
invoked before lower priority ones. Valid range is -2 (lowest) to +2 (highest),
with 0 being the default. See L<EV> documentation for details on priorities.

=item * loop => 'EV::loop',

EV loop for running this instance. Default is C<EV::default_loop>.

=back

All parameters are optional.

If parameters about connection (host&port or path) is not passed, you should call C<connect> or C<connect_unix> method by hand to connect to redis-server.

=head2 connect($hostname, $port)

=head2 connect_unix($path)

Connect to a redis-server for C<$hostname:$port> or C<$path>.

on_connect callback will be called if connection is successful, otherwise on_error callback is called.

=head2 command($commands..., $cb->($result, $error))

Do a redis command and return its result by callback.

    $redis->command('get', 'foo', sub {
        my ($result, $error) = @_;

        print $result; # value for key 'foo'
        print $error;  # redis error string, undef if no error
    });

If any error is occurred, C<$error> presents the error message and C<$result> is undef.
If no error, C<$error> is undef and C<$result> presents response from redis.

NOTE: Alternatively all commands can be called via AUTOLOAD interface.

    $redis->command('get', 'foo', sub { ... });

is equivalent to:

    $redis->get('foo', sub { ... });

=head2 disconnect

Disconnect from redis-server. This method is usable for exiting event loop.

=head2 is_connected

Returns true (1) if connected to redis-server, false (0) otherwise.

=head2 on_error($cb->($errstr))

Set new error callback to the instance.

=head2 on_connect($cb->())

Set new connect callback to the instance. Call without arguments to clear the callback.

=head2 on_disconnect($cb->())

Set disconnect callback to the instance. This callback will be called when
disconnection occurs (both normal and error cases). Call without arguments to clear the callback.

=head2 reconnect($enable, $delay_ms, $max_attempts)

Configure automatic reconnection.

    $redis->reconnect(1);                    # enable with defaults
    $redis->reconnect(1, 2000);              # enable with 2 second delay
    $redis->reconnect(1, 1000, 5);           # enable with 1s delay, max 5 attempts
    $redis->reconnect(0);                    # disable

When enabled, the client will automatically attempt to reconnect on connection
failure or unexpected disconnection. Intentional C<disconnect()> calls will
not trigger reconnection.

=head2 reconnect_enabled

Returns true (1) if automatic reconnection is enabled, false (0) otherwise.

=head2 pending_count

Returns the number of commands sent to Redis awaiting responses.

=head2 waiting_count

Returns the number of commands queued locally (not yet sent to Redis).
These are commands that exceeded the C<max_pending> limit.

=head2 max_pending($limit)

Get or set the maximum number of concurrent commands sent to Redis.
0 means unlimited (default). When the limit is reached, additional commands
are queued locally and sent as responses arrive.

=head2 waiting_timeout($ms)

Get or set the maximum time in milliseconds a command can wait in the local queue.
Commands exceeding this timeout are cancelled with "waiting timeout" error.
0 means unlimited (default).

=head2 resume_waiting_on_reconnect($bool)

Get or set whether waiting commands are preserved on disconnect and resumed
after reconnection. Default is false (waiting commands cancelled on disconnect).

=head2 priority($priority)

Get or set the priority for the underlying libev IO watchers. Higher priority
watchers are invoked before lower priority ones when multiple watchers are
pending. Valid range is -2 (lowest) to +2 (highest), with 0 being the default.
Can be changed at any time, including while connected.

    $redis->priority(1);     # higher priority
    $redis->priority(-1);    # lower priority
    my $prio = $redis->priority;  # get current priority

=head2 skip_waiting

Cancel only waiting (not yet sent) command callbacks. Each callback is invoked
with C<(undef, "skipped")>. In-flight commands continue normally.

=head2 skip_pending

Cancel all pending and waiting command callbacks. Each callback is invoked
with C<(undef, "skipped")>. Waiting commands are cancelled immediately;
pending commands are marked as skipped and cleaned up when Redis responds.

=head2 can($method)

Returns code reference if method is available, undef otherwise.
Methods installed via AUTOLOAD (Redis commands) will return true after first call.

=head1 AUTHOR

Daisuke Murase <typester@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2013 Daisuke Murase All rights reserved.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
