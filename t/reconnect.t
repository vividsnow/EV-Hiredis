use strict;
use warnings;
use Test::More;
use Test::RedisServer;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

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
    $r->disconnect;
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

# Test: resume_waiting_on_reconnect getter/setter
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    is $r->resume_waiting_on_reconnect, 0, 'resume_waiting_on_reconnect defaults to 0';
    $r->resume_waiting_on_reconnect(1);
    is $r->resume_waiting_on_reconnect, 1, 'resume_waiting_on_reconnect set to 1';
    $r->resume_waiting_on_reconnect(0);
    is $r->resume_waiting_on_reconnect, 0, 'resume_waiting_on_reconnect set back to 0';

    $r->disconnect;
}

# Test: resume_waiting_on_reconnect via constructor
{
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        resume_waiting_on_reconnect => 1,
    );

    is $r->resume_waiting_on_reconnect, 1, 'resume_waiting_on_reconnect set via constructor';
    $r->disconnect;
}

# Test: waiting queue behavior on explicit disconnect (resume_waiting_on_reconnect=0)
{
    my @results;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        max_pending => 1,  # Commands queue up
    );

    # Queue up multiple commands
    $r->set('key1', 'val1', sub { push @results, ['set1', $_[1] ? 'error' : 'ok'] });
    $r->set('key2', 'val2', sub { push @results, ['set2', $_[1] ? 'error' : 'ok'] });
    $r->set('key3', 'val3', sub { push @results, ['set3', $_[1] ? 'error' : 'ok'] });

    # Disconnect immediately - pending and waiting should all error
    $r->disconnect;

    my $timer = EV::timer 0.5, 0, sub { };
    EV::run;

    is scalar(@results), 3, 'all callbacks were called on disconnect';
    # All should get errors since we disconnected
    my $errors = grep { $_->[1] eq 'error' } @results;
    ok $errors >= 1, 'at least pending command got error on disconnect';
}

# Test: waiting queue preserved with resume_waiting_on_reconnect=1 (explicit disconnect still errors)
{
    my @results;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        max_pending => 1,
        resume_waiting_on_reconnect => 1,  # Preserves waiting queue, but not for intentional disconnect
    );

    # Queue up multiple commands
    $r->set('key1', 'val1', sub { push @results, ['set1', $_[1] ? 'error' : 'ok'] });
    $r->set('key2', 'val2', sub { push @results, ['set2', $_[1] ? 'error' : 'ok'] });
    $r->set('key3', 'val3', sub { push @results, ['set3', $_[1] ? 'error' : 'ok'] });

    # Explicit disconnect should still error all callbacks (no reconnect for intentional disconnect)
    $r->disconnect;

    my $timer = EV::timer 0.5, 0, sub { };
    EV::run;

    # Note: With resume_waiting_on_reconnect=1, waiting queue is preserved across reconnect,
    # but explicit disconnect() sets intentional_disconnect which cancels everything.
    is scalar(@results), 3, 'all callbacks were called';
}

# Test: reconnect after unexpected disconnect (simulated via max_reconnect_attempts exhaustion)
{
    my $connect_count = 0;
    my $error_count = 0;
    my $disconnect_count = 0;

    my $r = EV::Hiredis->new(
        on_connect => sub { $connect_count++ },
        on_error => sub { $error_count++ },
        on_disconnect => sub { $disconnect_count++ },
        reconnect => 1,
        reconnect_delay => 50,
        max_reconnect_attempts => 2,
    );

    # Connect to invalid port - will fail and trigger reconnect attempts
    $r->connect('127.0.0.1', 59999);

    my $timer = EV::timer 0.5, 0, sub { };
    EV::run;

    is $connect_count, 0, 'never connected to invalid port';
    ok $error_count >= 1, 'error callback called for failed connection';
    # After max_reconnect_attempts, should give up
    is $r->is_connected, 0, 'not connected after exhausting reconnect attempts';
    $r->disconnect;
}

# Test: resume_waiting_on_reconnect getter/setter
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    is $r->resume_waiting_on_reconnect, 0, 'resume_waiting_on_reconnect default is 0';

    $r->resume_waiting_on_reconnect(1);
    is $r->resume_waiting_on_reconnect, 1, 'resume_waiting_on_reconnect can be set to 1';

    $r->resume_waiting_on_reconnect(0);
    is $r->resume_waiting_on_reconnect, 0, 'resume_waiting_on_reconnect can be set back to 0';

    # Via constructor
    my $r2 = EV::Hiredis->new(
        path => $connect_info{sock},
        resume_waiting_on_reconnect => 1,
    );
    is $r2->resume_waiting_on_reconnect, 1, 'resume_waiting_on_reconnect set via constructor';

    $r->disconnect;
    $r2->disconnect;
}

# Test: commands issued during disconnect callback
# Verifies that calling command() in on_disconnect triggers proper error
{
    my $error_in_callback = 0;
    my $r;
    $r = EV::Hiredis->new(
        path => $connect_info{sock},
        on_error => sub { },
        on_disconnect => sub {
            # Trying to issue command during disconnect should fail safely
            eval {
                $r->set('key', 'value', sub { });
            };
            $error_in_callback = 1 if $@;
        },
    );

    my $t; $t = EV::timer 0.1, 0, sub {
        undef $t;
        $r->disconnect;
    };

    EV::run;

    ok $error_in_callback, 'command during disconnect callback throws exception';
}

# Test: skip_waiting() during waiting queue callback (re-entrancy safety)
{
    my @results;
    my $skip_called = 0;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        max_pending => 1,  # Commands queue up in waiting
    );

    # Queue up multiple commands - first goes to pending, rest to waiting
    $r->set('key1', 'val1', sub { push @results, ['set1', $_[1] ? 'error' : 'ok'] });
    $r->set('key2', 'val2', sub {
        push @results, ['set2', $_[1] ? 'error' : 'ok'];
        # Calling skip_waiting during callback should be safe (no-op due to in_cleanup)
        $r->skip_waiting();
        $skip_called = 1;
    });
    $r->set('key3', 'val3', sub { push @results, ['set3', $_[1] ? 'error' : 'ok'] });

    # Disconnect triggers error callbacks for waiting commands
    $r->disconnect;

    my $timer = EV::timer 0.5, 0, sub { };
    EV::run;

    ok $skip_called, 'skip_waiting was called during waiting queue callback';
    is scalar(@results), 3, 'all callbacks were called despite skip_waiting re-entry';
}

# Test: reconnect configuration and on_connect callback count
# This verifies that on_connect is called on initial connect
{
    my $connect_count = 0;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        on_connect => sub { $connect_count++ },
        on_error => sub { },
    );

    # Do a simple command to verify connection works
    $r->ping(sub {
        my ($res, $err) = @_;
        $r->disconnect;
    });

    EV::run;

    is $connect_count, 1, 'on_connect called once on initial connection';
}

# Test: waiting queue is drained when connection becomes available
# (This tests the connect callback's waiting queue drain logic)
{
    my @results;
    my $r = EV::Hiredis->new(
        path => $connect_info{sock},
        max_pending => 1,  # Force commands to wait
    );

    # Queue commands - first goes pending, rest wait
    $r->set('drain_test_1', 'val1', sub { push @results, ['cmd1', $_[1] ? 'error' : 'ok'] });
    $r->set('drain_test_2', 'val2', sub { push @results, ['cmd2', $_[1] ? 'error' : 'ok'] });
    $r->set('drain_test_3', 'val3', sub { push @results, ['cmd3', $_[1] ? 'error' : 'ok'] });

    is $r->waiting_count, 2, 'two commands in waiting queue';

    my $timer; $timer = EV::timer 1, 0, sub {
        undef $timer;
        $r->disconnect;
    };

    EV::run;

    is scalar(@results), 3, 'all commands completed';
    is $results[0][1], 'ok', 'first command succeeded';
    is $results[1][1], 'ok', 'second command (from wait queue) succeeded';
    is $results[2][1], 'ok', 'third command (from wait queue) succeeded';
}

# Test: reconnect timer fires and attempts reconnection
# (Tests the reconnect scheduling path without requiring forced disconnect)
{
    my $error_count = 0;
    my $r = EV::Hiredis->new(
        on_error => sub { $error_count++ },
        reconnect => 1,
        reconnect_delay => 50,
        max_reconnect_attempts => 3,
    );

    # Connect to invalid port - will fail and schedule reconnect
    $r->connect('127.0.0.1', 59998);

    # Wait for reconnect attempts to exhaust
    my $timer; $timer = EV::timer 0.5, 0, sub { undef $timer };
    EV::run;

    # Should have multiple errors from reconnect attempts
    ok $error_count >= 2, "reconnect timer fired multiple times (got $error_count errors)";
    is $r->is_connected, 0, 'not connected after exhausting reconnect attempts';
    $r->disconnect;
}

# Test: reconnect_delay with zero/negative values defaults to 1000ms
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    # Zero delay should be clamped to 1000ms (internal default)
    $r->reconnect(1, 0, 3);
    is $r->reconnect_enabled, 1, 'reconnect enabled with zero delay';

    # Negative delay should also be clamped to 1000ms
    $r->reconnect(1, -100, 3);
    is $r->reconnect_enabled, 1, 'reconnect enabled with negative delay';

    # The actual delay value is internal and not exposed via API,
    # but the reconnect should still work without crashing
    $r->disconnect;
}

# Test: reconnect_delay overflow protection
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    my $died = 0;
    eval {
        $r->reconnect(1, 2000000001, 3);  # Exceeds MAX_TIMEOUT_MS
    };
    $died = 1 if $@;

    ok $died, 'reconnect_delay exceeding max throws exception';
    like $@, qr/reconnect_delay too large/, 'exception mentions reconnect_delay too large';

    # Valid large delay should work
    eval {
        $r->reconnect(1, 2000000000, 3);  # At MAX_TIMEOUT_MS limit
    };
    ok !$@, 'reconnect_delay at max limit accepted';
    is $r->reconnect_enabled, 1, 'reconnect enabled with max delay';

    $r->disconnect;
}

# Test: negative max_reconnect_attempts clamping
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    # Negative max_attempts should be clamped to 0 (unlimited retries)
    $r->reconnect(1, 100, -5);
    is $r->reconnect_enabled, 1, 'reconnect enabled with negative max_attempts';

    # With max_attempts=0 (unlimited), reconnect should keep trying
    # We just verify it doesn't crash and accepts the value
    $r->reconnect(1, 100, -999);
    is $r->reconnect_enabled, 1, 'reconnect enabled with very negative max_attempts';

    $r->disconnect;
}

# Test: constructor with negative/zero reconnect parameters
{
    # Zero reconnect_delay via constructor
    my $r1 = EV::Hiredis->new(
        path => $connect_info{sock},
        reconnect => 1,
        reconnect_delay => 0,
        max_reconnect_attempts => 3,
    );
    is $r1->reconnect_enabled, 1, 'reconnect enabled via constructor with zero delay';
    $r1->disconnect;

    # Negative max_reconnect_attempts via constructor
    my $r2 = EV::Hiredis->new(
        path => $connect_info{sock},
        reconnect => 1,
        reconnect_delay => 100,
        max_reconnect_attempts => -1,
    );
    is $r2->reconnect_enabled, 1, 'reconnect enabled via constructor with negative max_attempts';
    $r2->disconnect;
}

done_testing;
