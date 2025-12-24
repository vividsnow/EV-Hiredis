use strict;
use warnings;
use Test::More;
use Test::RedisServer;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

plan tests => 9;

my %connect_info = $redis_server->connect_info;

use EV;
use EV::Hiredis;

# Test: reconnect configuration
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    is $r->reconnect_enabled, 0, 'reconnect disabled by default';

    $r->reconnect(1, 500, 3);
    is $r->reconnect_enabled, 1, 'reconnect enabled after call';

    $r->reconnect(0);
    is $r->reconnect_enabled, 0, 'reconnect disabled after call';

    $r->disconnect;
}

# Test: reconnect via constructor
{
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        reconnect => 1,
        reconnect_delay => 500,
        max_reconnect_attempts => 3,
    );

    is $r->reconnect_enabled, 1, 'reconnect enabled via constructor';
    $r->disconnect;
}

# Test: automatic reconnect on connection failure
{
    my $connect_count = 0;
    my $error_count = 0;

    my $r = EV::Hiredis->new(
        on_error => sub { $error_count++ },
        on_connect => sub { $connect_count++ },
        reconnect => 1,
        reconnect_delay => 100,
        max_reconnect_attempts => 2,
    );

    # Try connecting to invalid port - should fail and attempt reconnect
    $r->connect('127.0.0.1', 59999);

    # Wait for reconnect attempts
    my $timer = EV::timer 0.5, 0, sub {
        # Stop after timeout
    };
    EV::run;

    ok $error_count >= 1, 'error handler called on connection failure';
    is $r->is_connected, 0, 'not connected after failed reconnect attempts';
}

# Test: explicit disconnect does not trigger reconnect
{
    my $disconnected = 0;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        on_error => sub { },
        on_disconnect => sub { $disconnected = 1 },
    );

    # Wait for connection to be ready by doing a PING
    $r->ping(sub {
        my ($res, $err) = @_;
        ok $r->is_connected, 'initially connected';

        # Enable reconnect after connection
        $r->reconnect(1, 100, 1);

        $r->disconnect;
    });

    # Run event loop - will exit when all callbacks done
    my $timer = EV::timer 2, 0, sub { };
    EV::run;

    ok $disconnected, 'on_disconnect callback was called';
    is $r->is_connected, 0, 'disconnected after explicit disconnect (no reconnect)';
}

done_testing;
