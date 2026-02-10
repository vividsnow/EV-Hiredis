use strict;
use warnings;
use Test::More;
use Test::Deep;
use Test::RedisServer;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my %connect_info = $redis_server->connect_info;

use EV;
use EV::Hiredis;

# Get Redis version for conditional tests
my ($redis_version, $redis_minor) = (0, 0);
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    $r->info('server', sub {
        my ($info, $err) = @_;
        if ($info && $info =~ /redis_version:(\d+)\.(\d+)/) {
            $redis_version = $1;
            $redis_minor = $2;
        }
        $r->disconnect;
    });
    EV::run;
}
diag "Redis version: $redis_version.$redis_minor";

my $r = EV::Hiredis->new;
$r->connect_unix( $connect_info{sock} );

my $called = 0;
$r->command('get', 'foo', sub {
    my ($res, $err) = @_;

    $called++;
    ok !$res;
    ok !$err;

    $r->disconnect;
});
EV::run;
ok $called;

$called = 0;
$r->connect_unix( $connect_info{sock} );
$r->command('set', 'foo', 'bar', sub {
    my ($res, $err) = @_;

    $called++;
    is $res, 'OK';;
    ok !$err;

    $r->command('get', 'foo', sub {
        my ($res, $err) = @_;

        $called++;
        is $res, 'bar';
        ok !$err;

        $r->disconnect;
    });
});
EV::run;
is $called, 2;

$called = 0;
$r->connect_unix( $connect_info{sock} );
$r->command('set', '1', 'one', sub {
    $r->command('set', '2', 'two', sub {
        $r->command('keys', '*', sub {
            my ($res) = @_;

            $called++;
            cmp_deeply($res, bag('foo', '1', '2'));

            $r->disconnect;
        });
    });
});
EV::run;
is $called, 1;

$called = 0;
$r->connect_unix( $connect_info{sock} );
$r->command('set', 'foo', sub {
    my ($res, $err) = @_;

    $called++;

    ok !$res;
    ok $err;

    $r->disconnect;
});
EV::run;
is $called, 1;

# Test max_pending limit with waiting queue
{
    $r->connect_unix( $connect_info{sock} );
    is $r->max_pending, 0, 'max_pending defaults to 0 (unlimited)';
    is $r->waiting_count, 0, 'waiting_count is 0 initially';

    $r->max_pending(2);
    is $r->max_pending, 2, 'max_pending set to 2';

    my @results;
    $r->command('blpop', 'key1', 10, sub { push @results, ['cmd1', @_] });
    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 0, 'waiting_count is 0';

    $r->command('blpop', 'key2', 10, sub { push @results, ['cmd2', @_] });
    is $r->pending_count, 2, 'pending_count is 2';
    is $r->waiting_count, 0, 'waiting_count is 0';

    # Third command should be queued, not sent
    $r->command('blpop', 'key3', 10, sub { push @results, ['cmd3', @_] });
    is $r->pending_count, 2, 'pending_count still 2 (at limit)';
    is $r->waiting_count, 1, 'waiting_count is 1 (queued)';

    # Fourth command also queued
    $r->command('blpop', 'key4', 10, sub { push @results, ['cmd4', @_] });
    is $r->pending_count, 2, 'pending_count still 2';
    is $r->waiting_count, 2, 'waiting_count is 2';

    my $timer = EV::timer 0.1, 0, sub {
        $r->skip_pending;
        is $r->pending_count, 0, 'pending_count is 0 after skip';
        is $r->waiting_count, 0, 'waiting_count is 0 after skip';
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 4, 'all 4 callbacks called';
    # Waiting queue cleared first (FIFO: cmd3, cmd4), then pending (LIFO: cmd2, cmd1)
    my %seen = map { $_->[0] => $_->[2] } @results;
    is $seen{cmd1}, 'skipped', 'cmd1 was skipped';
    is $seen{cmd2}, 'skipped', 'cmd2 was skipped';
    is $seen{cmd3}, 'skipped', 'cmd3 was skipped';
    is $seen{cmd4}, 'skipped', 'cmd4 was skipped';

    $r->max_pending(0);
}

# Test waiting queue drain behavior
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;
    my $check_counts = sub {
        my ($exp_pending, $exp_waiting, $msg) = @_;
        is $r->pending_count, $exp_pending, "$msg: pending=$exp_pending";
        is $r->waiting_count, $exp_waiting, "$msg: waiting=$exp_waiting";
    };

    # Queue 4 SET commands - 2 should be pending, 2 waiting
    $r->command('set', 'drain_test_1', 'val1', sub { push @results, ['set1', @_] });
    $r->command('set', 'drain_test_2', 'val2', sub { push @results, ['set2', @_] });
    $check_counts->(2, 0, 'after 2 commands');

    $r->command('set', 'drain_test_3', 'val3', sub { push @results, ['set3', @_] });
    # Last command disconnects after verifying drain
    $r->command('set', 'drain_test_4', 'val4', sub {
        push @results, ['set4', @_];
        # pending_count is 1 (current) since it's decremented after callback
        is $r->pending_count, 1, 'pending_count is 1 in last callback (self)';
        is $r->waiting_count, 0, 'waiting_count is 0 in last callback';
        $r->disconnect;
    });
    $check_counts->(2, 2, 'after 4 commands');

    # Run event loop - responses should drain the waiting queue
    EV::run;

    # All commands should have completed
    is scalar(@results), 4, 'all 4 callbacks executed';

    # Verify all succeeded
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] returned OK";
    }

    # Verify the values were actually set
    $r->connect_unix( $connect_info{sock} );
    my $verified = 0;
    $r->command('get', 'drain_test_4', sub {
        my ($res, $err) = @_;
        is $res, 'val4', 'drain_test_4 has correct value';
        $verified = 1;
        $r->disconnect;
    });
    EV::run;
    ok $verified, 'verification callback executed';

    $r->max_pending(0);
}

# Test pending_count and skip_pending
{
    my @results;
    $r->connect_unix( $connect_info{sock} );

    is $r->pending_count, 0, 'pending_count is 0 initially';

    $r->command('blpop', 'nonexistent_key', 10, sub {
        push @results, \@_;
    });
    is $r->pending_count, 1, 'pending_count is 1 after first command';

    $r->command('blpop', 'nonexistent_key2', 10, sub {
        push @results, \@_;
    });
    is $r->pending_count, 2, 'pending_count is 2 after second command';

    my $timer = EV::timer 0.1, 0, sub {
        $r->skip_pending;
        is $r->pending_count, 0, 'pending_count is 0 after skip_pending';
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 2, 'both callbacks called';
    is $results[0][0], undef, 'first result is undef';
    is $results[0][1], 'skipped', 'first error is skipped';
    is $results[1][0], undef, 'second result is undef';
    is $results[1][1], 'skipped', 'second error is skipped';
}

# Test skip_waiting (only waiting queue, not pending)
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;

    # 2 commands sent to Redis (pending)
    $r->command('set', 'sw_test_1', 'val1', sub { push @results, ['cmd1', @_] });
    $r->command('set', 'sw_test_2', 'val2', sub { push @results, ['cmd2', @_] });
    # 2 commands queued locally (waiting)
    $r->command('set', 'sw_test_3', 'val3', sub { push @results, ['cmd3', @_] });
    $r->command('set', 'sw_test_4', 'val4', sub {
        push @results, ['cmd4', @_];
        $r->disconnect;
    });

    is $r->pending_count, 2, 'pending_count is 2';
    is $r->waiting_count, 2, 'waiting_count is 2';

    # Skip only waiting - pending should complete normally
    $r->skip_waiting;

    is $r->pending_count, 2, 'pending_count still 2 after skip_waiting';
    is $r->waiting_count, 0, 'waiting_count is 0 after skip_waiting';

    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';
    # Waiting commands skipped immediately
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd1}[1], 'OK', 'cmd1 completed normally';
    is $seen{cmd2}[1], 'OK', 'cmd2 completed normally';
    is $seen{cmd3}[2], 'skipped', 'cmd3 was skipped';
    is $seen{cmd4}[2], 'skipped', 'cmd4 was skipped';

    $r->max_pending(0);
}

# Test skip_pending from inside callback
{
    $r->connect_unix( $connect_info{sock} );

    my @results;
    my $skip_called = 0;

    # First command will call skip_pending
    $r->command('set', 'skip_test_1', 'val1', sub {
        push @results, ['cmd1', @_];
        $skip_called = 1;
        $r->skip_pending;
        $r->disconnect;
    });
    $r->command('set', 'skip_test_2', 'val2', sub {
        push @results, ['cmd2', @_];
    });
    $r->command('set', 'skip_test_3', 'val3', sub {
        push @results, ['cmd3', @_];
    });

    is $r->pending_count, 3, 'pending_count is 3 before run';
    EV::run;

    ok $skip_called, 'skip_pending was called from callback';
    is scalar(@results), 3, 'all 3 callbacks executed';
    is $results[0][0], 'cmd1', 'first result is cmd1';
    is $results[0][1], 'OK', 'cmd1 succeeded normally';
    is $results[1][0], 'cmd2', 'second result is cmd2';
    is $results[1][2], 'skipped', 'cmd2 was skipped';
    is $results[2][0], 'cmd3', 'third result is cmd3';
    is $results[2][2], 'skipped', 'cmd3 was skipped';
}

# Test waiting queue cleared on disconnect (default behavior)
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    is $r->resume_waiting_on_reconnect, 0, 'resume_waiting_on_reconnect defaults to 0';

    my @results;
    $r->command('set', 'dc_test_1', 'val1', sub {
        push @results, ['cmd1', @_];
        # Disconnect while cmd2 is waiting
        $r->disconnect;
    });
    $r->command('set', 'dc_test_2', 'val2', sub {
        push @results, ['cmd2', @_];
    });

    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 1, 'waiting_count is 1';

    EV::run;

    is scalar(@results), 2, 'both callbacks executed';
    is $results[0][0], 'cmd1', 'first is cmd1';
    is $results[0][1], 'OK', 'cmd1 succeeded';
    is $results[1][0], 'cmd2', 'second is cmd2';
    ok $results[1][2], 'cmd2 got error';

    $r->max_pending(0);
}

# Test waiting_timeout
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    $r->waiting_timeout(100);  # 100ms timeout

    my @results;

    # First command goes to pending
    $r->command('blpop', 'wt_test_key', 10, sub {
        push @results, ['cmd1', @_];
    });
    # Second and third go to waiting queue
    $r->command('set', 'wt_test_2', 'val2', sub {
        push @results, ['cmd2', @_];
    });
    $r->command('set', 'wt_test_3', 'val3', sub {
        push @results, ['cmd3', @_];
    });

    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 2, 'waiting_count is 2';

    # Wait for timeout to expire
    my $timer = EV::timer 0.2, 0, sub {
        # By now, waiting commands should have timed out
        is $r->waiting_count, 0, 'waiting_count is 0 after timeout';
        $r->skip_pending;
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 3, 'all 3 callbacks executed';
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd2}[2], 'waiting timeout', 'cmd2 got waiting timeout';
    is $seen{cmd3}[2], 'waiting timeout', 'cmd3 got waiting timeout';

    $r->max_pending(0);
    $r->waiting_timeout(0);
}

# Edge case: skip_pending/skip_waiting when queues are empty
{
    $r->connect_unix( $connect_info{sock} );

    my $done = 0;
    # Issue a command to ensure connection is established
    $r->command('ping', sub {
        my ($res, $err) = @_;

        # Note: pending_count is 1 inside callback (current command not yet decremented)
        is $r->pending_count, 1, 'pending_count is 1 inside callback (self)';
        is $r->waiting_count, 0, 'waiting_count is 0';

        # Should not crash or error when only current callback pending
        $r->skip_pending;
        $r->skip_waiting;

        # Current callback is not skipped (only others would be)
        is $r->pending_count, 1, 'pending_count still 1 (current cb not skipped)';
        is $r->waiting_count, 0, 'waiting_count still 0 after skip_waiting on empty';

        $done = 1;
        $r->disconnect;
    });
    EV::run;
    ok $done, 'skip on empty queues test completed';
}

# Edge case: changing max_pending while commands are queued
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    # First command goes to pending
    $r->command('set', 'mp_change_1', 'val1', sub { push @results, ['cmd1', @_] });
    # Second goes to waiting
    $r->command('set', 'mp_change_2', 'val2', sub { push @results, ['cmd2', @_] });
    # Third goes to waiting
    $r->command('set', 'mp_change_3', 'val3', sub { push @results, ['cmd3', @_] });

    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 2, 'waiting_count is 2';

    # Increase max_pending - should immediately send waiting commands
    $r->max_pending(10);

    # All waiting commands should now be pending
    is $r->pending_count, 3, 'pending_count is 3 after increasing max_pending';
    is $r->waiting_count, 0, 'waiting_count is 0 after increasing max_pending';

    # Add final command to disconnect
    $r->command('set', 'mp_change_4', 'val4', sub {
        push @results, ['cmd4', @_];
        $r->disconnect;
    });

    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] succeeded";
    }

    $r->max_pending(0);
}

# Edge case: decreasing max_pending while commands are pending
{
    $r->connect_unix( $connect_info{sock} );

    my @results;

    # Queue 3 commands (all pending since no limit)
    $r->command('set', 'mp_dec_1', 'val1', sub { push @results, ['cmd1', @_] });
    $r->command('set', 'mp_dec_2', 'val2', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'mp_dec_3', 'val3', sub { push @results, ['cmd3', @_] });

    is $r->pending_count, 3, 'pending_count is 3';

    # Set max_pending to 1 - should not affect already pending commands
    $r->max_pending(1);

    is $r->pending_count, 3, 'pending_count still 3 (already sent)';
    is $r->waiting_count, 0, 'waiting_count is 0';

    # New command should go to waiting since pending > max_pending
    $r->command('set', 'mp_dec_4', 'val4', sub {
        push @results, ['cmd4', @_];
        $r->disconnect;
    });

    is $r->waiting_count, 1, 'new command goes to waiting';

    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';

    $r->max_pending(0);
}

# Edge case: skip_waiting from inside callback
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    # First command - will skip waiting queue from callback
    $r->command('set', 'sw_cb_1', 'val1', sub {
        push @results, ['cmd1', @_];
        # Skip waiting commands from inside callback
        $r->skip_waiting;
    });

    # These go to waiting queue
    $r->command('set', 'sw_cb_2', 'val2', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'sw_cb_3', 'val3', sub {
        push @results, ['cmd3', @_];
        $r->disconnect;
    });

    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 2, 'waiting_count is 2';

    EV::run;

    is scalar(@results), 3, 'all 3 callbacks executed';
    is $results[0][0], 'cmd1', 'cmd1 executed first';
    is $results[0][1], 'OK', 'cmd1 succeeded';

    # cmd2 and cmd3 should be skipped
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd2}[2], 'skipped', 'cmd2 was skipped';
    is $seen{cmd3}[2], 'skipped', 'cmd3 was skipped';

    $r->max_pending(0);
}

# Edge case: multiple rapid skip calls
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;

    $r->command('blpop', 'rapid_key', 10, sub { push @results, ['cmd1', @_] });
    $r->command('blpop', 'rapid_key2', 10, sub { push @results, ['cmd2', @_] });
    $r->command('set', 'rapid_3', 'val', sub { push @results, ['cmd3', @_] });
    $r->command('set', 'rapid_4', 'val', sub { push @results, ['cmd4', @_] });

    is $r->pending_count, 2, 'pending_count is 2';
    is $r->waiting_count, 2, 'waiting_count is 2';

    # Multiple rapid skip calls
    $r->skip_waiting;
    $r->skip_waiting;  # Should be no-op
    $r->skip_pending;
    $r->skip_pending;  # Should be no-op

    is $r->pending_count, 0, 'pending_count is 0';
    is $r->waiting_count, 0, 'waiting_count is 0';

    $r->disconnect;
    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';
    for my $res (@results) {
        is $res->[2], 'skipped', "$res->[0] was skipped";
    }

    $r->max_pending(0);
}

# Edge case: very short waiting_timeout (immediate expiry)
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    $r->waiting_timeout(1);  # 1ms - nearly immediate

    my @results;

    # First command blocks
    $r->command('blpop', 'short_timeout_key', 10, sub {
        push @results, ['cmd1', @_];
    });

    # These should timeout almost immediately
    $r->command('set', 'short_2', 'val', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'short_3', 'val', sub { push @results, ['cmd3', @_] });

    is $r->waiting_count, 2, 'waiting_count is 2';

    my $timer = EV::timer 0.1, 0, sub {
        is $r->waiting_count, 0, 'waiting_count is 0 after short timeout';
        $r->skip_pending;
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 3, 'all 3 callbacks executed';
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd2}[2], 'waiting timeout', 'cmd2 got waiting timeout';
    is $seen{cmd3}[2], 'waiting timeout', 'cmd3 got waiting timeout';

    $r->max_pending(0);
    $r->waiting_timeout(0);
}

# Edge case: disable waiting_timeout while commands are waiting
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    $r->waiting_timeout(50);  # 50ms

    my @results;

    $r->command('blpop', 'disable_timeout_key', 10, sub {
        push @results, ['cmd1', @_];
    });
    $r->command('set', 'disable_2', 'val', sub { push @results, ['cmd2', @_] });

    is $r->waiting_count, 1, 'waiting_count is 1';

    # Disable timeout before it expires
    $r->waiting_timeout(0);

    # Wait longer than original timeout
    my $timer = EV::timer 0.1, 0, sub {
        # Command should still be waiting (timeout disabled)
        is $r->waiting_count, 1, 'waiting_count still 1 after disabling timeout';
        $r->skip_pending;
        $r->skip_waiting;
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 2, 'both callbacks executed';

    $r->max_pending(0);
}

# Edge case: enable waiting_timeout while commands are already waiting
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    # No timeout initially

    my @results;

    $r->command('blpop', 'enable_timeout_key', 10, sub {
        push @results, ['cmd1', @_];
    });
    $r->command('set', 'enable_2', 'val', sub { push @results, ['cmd2', @_] });

    is $r->waiting_count, 1, 'waiting_count is 1';

    # Enable timeout after commands are queued
    $r->waiting_timeout(50);  # 50ms

    my $timer = EV::timer 0.15, 0, sub {
        # Command should have timed out
        is $r->waiting_count, 0, 'waiting_count is 0 after enabling timeout';
        $r->skip_pending;
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 2, 'both callbacks executed';
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd2}[2], 'waiting timeout', 'cmd2 got waiting timeout after enabling';

    $r->max_pending(0);
    $r->waiting_timeout(0);
}

# Edge case: nested commands from within callback with max_pending
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    $r->command('set', 'nested_1', 'val1', sub {
        push @results, ['cmd1', @_];

        # Issue more commands from within callback
        $r->command('set', 'nested_2', 'val2', sub {
            push @results, ['cmd2', @_];

            $r->command('set', 'nested_3', 'val3', sub {
                push @results, ['cmd3', @_];
                $r->disconnect;
            });
        });
    });

    EV::run;

    is scalar(@results), 3, 'all 3 nested callbacks executed';
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] succeeded";
    }

    $r->max_pending(0);
}

# Edge case: disconnect with pending/waiting commands clears waiting
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    $r->command('set', 'dc_wait_1', 'val1', sub {
        push @results, ['cmd1', @_];
        # Disconnect while cmd2 is waiting
        $r->disconnect;
    });
    $r->command('set', 'dc_wait_2', 'val2', sub {
        push @results, ['cmd2', @_];
    });

    is $r->waiting_count, 1, 'cmd2 is waiting';

    EV::run;

    is scalar(@results), 2, 'both callbacks executed';
    is $results[0][0], 'cmd1', 'cmd1 first';
    is $results[0][1], 'OK', 'cmd1 succeeded';
    is $results[1][0], 'cmd2', 'cmd2 second';
    ok $results[1][2], 'cmd2 got error (was waiting during disconnect)';

    $r->max_pending(0);
}

# Edge case: reconnect after disconnect (separate event loop iterations)
{
    $r->connect_unix( $connect_info{sock} );

    my @results;

    $r->command('set', 'recon_1', 'val1', sub {
        push @results, ['cmd1', @_];
        $r->disconnect;
    });

    EV::run;

    is $results[0][1], 'OK', 'cmd1 succeeded before disconnect';

    # Now reconnect in a new event loop iteration
    $r->connect_unix( $connect_info{sock} );
    $r->command('set', 'recon_2', 'val2', sub {
        push @results, ['cmd2', @_];
        $r->disconnect;
    });

    EV::run;

    is scalar(@results), 2, 'both callbacks executed';
    is $results[1][0], 'cmd2', 'cmd2 executed after reconnect';
    is $results[1][1], 'OK', 'cmd2 succeeded';
}

# Edge case: max_pending = 1 (single command at a time)
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;
    my @order;

    for my $i (1..5) {
        $r->command('set', "single_$i", "val$i", sub {
            push @order, $i;
            push @results, ["cmd$i", @_];
            $r->disconnect if $i == 5;
        });
    }

    is $r->pending_count, 1, 'only 1 pending';
    is $r->waiting_count, 4, '4 waiting';

    EV::run;

    is scalar(@results), 5, 'all 5 callbacks executed';
    is_deeply \@order, [1, 2, 3, 4, 5], 'commands executed in order';
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] succeeded";
    }

    $r->max_pending(0);
}

# Edge case: command issued right after disconnect (before reconnect)
{
    $r->connect_unix( $connect_info{sock} );

    my @results;
    my $error_caught = 0;

    $r->on_error(sub {
        $error_caught = 1;
    });

    $r->command('set', 'after_dc_1', 'val1', sub {
        push @results, ['cmd1', @_];
        $r->disconnect;

        # Try to issue command after disconnect
        eval {
            $r->command('set', 'after_dc_2', 'val2', sub {
                push @results, ['cmd2', @_];
            });
        };
        # Should either work (queued) or error
    });

    EV::run;

    is $results[0][1], 'OK', 'cmd1 succeeded';

    # Reset error handler
    $r->on_error(sub { die @_ });
}

# Edge case: waiting_timeout with commands added during expiration callback
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);
    $r->waiting_timeout(50);  # 50ms

    my @results;
    my $added_during_timeout = 0;

    # Blocking command
    $r->command('blpop', 'expire_add_key', 10, sub {
        push @results, ['cmd1', @_];
    });

    # This will timeout, but we'll add another command during the timeout callback
    $r->command('set', 'expire_add_2', 'val', sub {
        push @results, ['cmd2', @_];
        # Add a new command during the timeout callback
        if (!$added_during_timeout) {
            $added_during_timeout = 1;
            $r->command('set', 'expire_add_3', 'val', sub {
                push @results, ['cmd3', @_];
            });
        }
    });

    my $timer = EV::timer 0.2, 0, sub {
        $r->skip_pending;
        $r->skip_waiting;
        $r->disconnect;
    };
    EV::run;

    is scalar(@results), 3, 'all callbacks executed';
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd2}[2], 'waiting timeout', 'cmd2 got waiting timeout';
    # cmd3 added during timeout should also be handled
    ok $seen{cmd3}, 'cmd3 callback was executed';

    $r->max_pending(0);
    $r->waiting_timeout(0);
}

# Edge case: skip_pending immediately after issuing commands (before any response)
{
    $r->connect_unix( $connect_info{sock} );

    my @results;

    $r->command('set', 'imm_skip_1', 'val1', sub { push @results, ['cmd1', @_] });
    $r->command('set', 'imm_skip_2', 'val2', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'imm_skip_3', 'val3', sub { push @results, ['cmd3', @_] });

    is $r->pending_count, 3, 'pending_count is 3';

    # Skip all pending immediately
    $r->skip_pending;

    is $r->pending_count, 0, 'pending_count is 0 after skip';

    $r->disconnect;
    EV::run;

    is scalar(@results), 3, 'all 3 callbacks executed';
    for my $res (@results) {
        is $res->[2], 'skipped', "$res->[0] was skipped";
    }
}

# Edge case: set max_pending to 0 (unlimited) with waiting commands
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    # First goes to pending
    $r->command('set', 'unlimit_1', 'val1', sub { push @results, ['cmd1', @_] });
    # Rest go to waiting
    $r->command('set', 'unlimit_2', 'val2', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'unlimit_3', 'val3', sub { push @results, ['cmd3', @_] });
    $r->command('set', 'unlimit_4', 'val4', sub {
        push @results, ['cmd4', @_];
        $r->disconnect;
    });

    is $r->pending_count, 1, 'pending_count is 1';
    is $r->waiting_count, 3, 'waiting_count is 3';

    # Remove limit - should send all waiting commands
    $r->max_pending(0);

    is $r->pending_count, 4, 'pending_count is 4 after removing limit';
    is $r->waiting_count, 0, 'waiting_count is 0 after removing limit';

    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] succeeded";
    }
}

# Edge case: interleaved pending and waiting operations
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;
    my @order;

    # Issue commands that interleave pending/waiting states
    for my $i (1..6) {
        $r->command('set', "interleave_$i", "val$i", sub {
            push @order, $i;
            push @results, ["cmd$i", @_];
            $r->disconnect if $i == 6;
        });
    }

    is $r->pending_count, 2, 'pending_count is 2';
    is $r->waiting_count, 4, 'waiting_count is 4';

    EV::run;

    is scalar(@results), 6, 'all 6 callbacks executed';
    is_deeply \@order, [1, 2, 3, 4, 5, 6], 'commands executed in FIFO order';
    for my $res (@results) {
        is $res->[1], 'OK', "$res->[0] succeeded";
    }

    $r->max_pending(0);
}

# Edge case: alternating skip_waiting and command during event loop
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(1);

    my @results;

    $r->command('set', 'alt_skip_1', 'val1', sub {
        push @results, ['cmd1', @_];

        # Skip waiting, then add more commands
        $r->skip_waiting;

        $r->command('set', 'alt_skip_4', 'val4', sub {
            push @results, ['cmd4', @_];
            $r->disconnect;
        });
    });

    # These will be skipped
    $r->command('set', 'alt_skip_2', 'val2', sub { push @results, ['cmd2', @_] });
    $r->command('set', 'alt_skip_3', 'val3', sub { push @results, ['cmd3', @_] });

    is $r->waiting_count, 2, 'waiting_count is 2';

    EV::run;

    is scalar(@results), 4, 'all 4 callbacks executed';
    my %seen = map { $_->[0] => $_ } @results;
    is $seen{cmd1}[1], 'OK', 'cmd1 succeeded';
    is $seen{cmd2}[2], 'skipped', 'cmd2 was skipped';
    is $seen{cmd3}[2], 'skipped', 'cmd3 was skipped';
    is $seen{cmd4}[1], 'OK', 'cmd4 succeeded (added after skip)';

    $r->max_pending(0);
}

# Edge case: persistent command with max_pending
# Note: Redis 6.0+ restricts commands in subscribe context to only
# (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT/RESET, so we use PING here
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;
    my $sub_count = 0;

    # Subscribe is persistent (callback called multiple times)
    $r->command('subscribe', 'edge_chan', sub {
        my ($r_msg, $e) = @_;
        $sub_count++;
        push @results, ['subscribe', $r_msg, $e];

        if ($r_msg && $r_msg->[0] eq 'subscribe') {
            # Waiting command should be blocked by persistent subscribe
            is $r->pending_count, 2, 'pending_count includes subscribe';
        }
    });

    # Use PING which is allowed in subscribe context (Redis 6.0+)
    $r->command('ping', sub {
        push @results, ['cmd1', @_];
    });

    # This goes to waiting (limit reached)
    $r->command('ping', sub {
        push @results, ['cmd2', @_];
    });

    is $r->pending_count, 2, 'pending_count is 2';
    is $r->waiting_count, 1, 'waiting_count is 1';

    my $timer = EV::timer 0.1, 0, sub {
        $r->skip_pending;
        $r->skip_waiting;
        $r->disconnect;
    };
    EV::run;

    ok $sub_count >= 1, 'subscribe callback called at least once';

    $r->max_pending(0);
}

# Test: priority validation and clamping
{
    $r->connect_unix( $connect_info{sock} );

    # Default priority is 0
    is $r->priority, 0, 'default priority is 0';

    # Valid priorities
    $r->priority(-2);
    is $r->priority, -2, 'priority set to -2 (minimum)';

    $r->priority(2);
    is $r->priority, 2, 'priority set to 2 (maximum)';

    $r->priority(0);
    is $r->priority, 0, 'priority set back to 0';

    $r->priority(-1);
    is $r->priority, -1, 'priority set to -1';

    $r->priority(1);
    is $r->priority, 1, 'priority set to 1';

    # Out-of-range values should be clamped
    $r->priority(100);
    is $r->priority, 2, 'priority 100 clamped to 2';

    $r->priority(-100);
    is $r->priority, -2, 'priority -100 clamped to -2';

    $r->priority(3);
    is $r->priority, 2, 'priority 3 clamped to 2';

    $r->priority(-3);
    is $r->priority, -2, 'priority -3 clamped to -2';

    # Verify commands still work with different priorities
    my $done = 0;
    $r->priority(2);
    $r->ping(sub {
        my ($res, $err) = @_;
        is $res, 'PONG', 'ping works with high priority';
        $done = 1;
        $r->disconnect;
    });
    EV::run;
    ok $done, 'high priority command completed';
}

# Test: priority setting via constructor
{
    my $r_prio = EV::Hiredis->new(
        path => $connect_info{sock},
        priority => 1,
    );
    is $r_prio->priority, 1, 'priority set via constructor';
    $r_prio->disconnect;
    EV::run;
}

# Test: priority clamping via constructor
{
    my $r_prio2 = EV::Hiredis->new(
        path => $connect_info{sock},
        priority => 99,
    );
    is $r_prio2->priority, 2, 'priority clamped via constructor';
    $r_prio2->disconnect;
    EV::run;
}

# Test: priority change with active command timeout timer
# This verifies that changing priority while a timeout timer is active
# preserves the timeout behavior (tests ev_timer_again fix)
{
    my $r_timeout = EV::Hiredis->new(
        path => $connect_info{sock},
        command_timeout => 200,  # 200ms timeout
        on_error => sub { },
    );

    my $callback_called = 0;
    my $got_timeout = 0;
    my $start_time = EV::now;
    my $elapsed;

    # Issue a blocking command that will timeout
    $r_timeout->blpop('priority_timeout_test_key', 10, sub {
        my ($res, $err) = @_;
        $callback_called = 1;
        $elapsed = EV::now - $start_time;
        $got_timeout = 1 if defined($err);
        $r_timeout->disconnect;
    });

    # Change priority while timeout timer is active
    # This exercises the ev_timer_again code path
    $r_timeout->priority(1);
    $r_timeout->priority(-1);
    $r_timeout->priority(2);

    # Fallback timer in case timeout doesn't work
    my $fallback = EV::timer 2, 0, sub {
        $r_timeout->disconnect unless $callback_called;
    };

    EV::run;

    ok $callback_called, 'callback was called after priority changes';
    ok $got_timeout, 'command timed out correctly after priority changes';
    # Timeout should still occur around 200ms, not be reset or lost
    # Allow some slack for timing variations
    ok $elapsed < 0.5, "timeout occurred within reasonable time (${elapsed}s < 0.5s)";
}

# Stress test: large waiting queue
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(5);

    my $num_commands = 100;
    my @results;
    my $completed = 0;

    for my $i (1..$num_commands) {
        $r->set("stress_test_$i", "value_$i", sub {
            my ($res, $err) = @_;
            push @results, { i => $i, res => $res, err => $err };
            $completed++;
            if ($completed == $num_commands) {
                $r->disconnect;
            }
        });
    }

    # With max_pending=5, most commands should be waiting
    ok $r->waiting_count > 0, "waiting_count > 0 with $num_commands commands";
    is $r->pending_count + $r->waiting_count, $num_commands, 'pending + waiting = total commands';

    EV::run;

    is scalar(@results), $num_commands, "all $num_commands callbacks executed";

    # Verify all succeeded
    my $all_ok = 1;
    for my $res (@results) {
        if ($res->{res} ne 'OK' || $res->{err}) {
            $all_ok = 0;
            last;
        }
    }
    ok $all_ok, 'all stress test commands succeeded';

    # Verify order was preserved
    my @order = map { $_->{i} } @results;
    my @expected = (1..$num_commands);
    is_deeply \@order, \@expected, 'commands executed in FIFO order';

    $r->max_pending(0);
}

# Stress test: rapid connect/disconnect cycles
{
    for my $cycle (1..5) {
        my $r_cycle = EV::Hiredis->new(
            path => $connect_info{sock},
            on_error => sub { },
        );

        my $done = 0;
        $r_cycle->ping(sub {
            my ($res, $err) = @_;
            is $res, 'PONG', "cycle $cycle: ping succeeded";
            $done = 1;
            $r_cycle->disconnect;
        });
        EV::run;
        ok $done, "cycle $cycle: completed";
    }
}
pass 'rapid connect/disconnect cycles completed';

# Stress test: alternating pending/waiting with skip
{
    $r->connect_unix( $connect_info{sock} );
    $r->max_pending(2);

    my @results;

    for my $round (1..3) {
        # Queue commands
        for my $i (1..5) {
            $r->set("alt_stress_${round}_$i", "val", sub {
                my ($res, $err) = @_;
                push @results, { name => "round${round}_$i", res => $res, err => $err };
            });
        }

        # Skip waiting after queueing
        $r->skip_waiting;
    }

    # Final command to disconnect
    $r->set("alt_stress_final", "val", sub {
        my ($res, $err) = @_;
        push @results, { name => "final", res => $res, err => $err };
        $r->disconnect;
    });

    EV::run;

    # Some callbacks should have completed (pending) and some skipped (waiting)
    my @skipped = grep { $_->{err} && $_->{err} eq 'skipped' } @results;
    my @completed = grep { $_->{res} && $_->{res} eq 'OK' } @results;

    ok scalar(@skipped) > 0, 'some commands were skipped';
    ok scalar(@completed) > 0, 'some commands completed successfully';
    is scalar(@results), scalar(@skipped) + scalar(@completed), 'all callbacks accounted for';

    $r->max_pending(0);
}

# Test: constructor with explicit zero values
{
    my $r_zero = EV::Hiredis->new(
        path => $connect_info{sock},
        max_pending => 0,        # explicit 0 (unlimited)
        waiting_timeout => 0,    # explicit 0 (unlimited)
        connect_timeout => 0,    # edge case
        priority => 0,           # explicit default
    );

    is $r_zero->max_pending, 0, 'max_pending explicitly set to 0';
    is $r_zero->priority, 0, 'priority explicitly set to 0';

    my $done = 0;
    $r_zero->ping(sub {
        $done = 1;
        $r_zero->disconnect;
    });
    EV::run;
    ok $done, 'connection with zero values works';
}

# Test: zero-length (empty) string arguments
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Test empty value
    $r->set('test:empty:value', '', sub {
        my ($res, $err) = @_;
        push @results, ['set_empty_value', $res, $err];

        $r->get('test:empty:value', sub {
            my ($res, $err) = @_;
            push @results, ['get_empty_value', $res, $err];

            # Test empty key (Redis allows this)
            $r->set('', 'empty_key_value', sub {
                my ($res, $err) = @_;
                push @results, ['set_empty_key', $res, $err];

                $r->get('', sub {
                    my ($res, $err) = @_;
                    push @results, ['get_empty_key', $res, $err];

                    # Test both empty
                    $r->set('', '', sub {
                        my ($res, $err) = @_;
                        push @results, ['set_both_empty', $res, $err];

                        $r->get('', sub {
                            my ($res, $err) = @_;
                            push @results, ['get_both_empty', $res, $err];
                            $r->disconnect;
                        });
                    });
                });
            });
        });
    });

    EV::run;

    is $results[0][1], 'OK', 'SET with empty value succeeds';
    is $results[1][1], '', 'GET returns empty string value';
    is $results[2][1], 'OK', 'SET with empty key succeeds';
    is $results[3][1], 'empty_key_value', 'GET with empty key returns correct value';
    is $results[4][1], 'OK', 'SET with empty key and empty value succeeds';
    is $results[5][1], '', 'GET with empty key returns empty value';
}

# Test: binary strings with embedded NUL bytes
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Create binary string with embedded NUL bytes
    my $binary_value = "hello\x00world\x00end";

    $r->set('test:binary:simple', $binary_value, sub {
        my ($res, $err) = @_;
        push @results, ['set_binary', $res, $err];

        $r->get('test:binary:simple', sub {
            my ($res, $err) = @_;
            push @results, ['get_binary', $res, $err];
            $r->disconnect;
        });
    });

    EV::run;

    is $results[0][1], 'OK', 'SET with binary value containing NUL succeeds';
    is $results[1][1], $binary_value, 'GET returns binary value with embedded NUL intact';
    is length($results[1][1]), length($binary_value), 'Binary value length preserved';
}

# Test: negative max_pending validation
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    my $died = 0;
    eval {
        $r->max_pending(-1);
    };
    $died = 1 if $@;

    ok $died, 'negative max_pending throws exception';
    like $@, qr/non-negative/, 'exception message mentions non-negative';

    $r->disconnect;
}

# Test: negative waiting_timeout validation
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    my $died = 0;
    eval {
        $r->waiting_timeout(-1);
    };
    $died = 1 if $@;

    ok $died, 'negative waiting_timeout throws exception';
    like $@, qr/non-negative/, 'exception message mentions non-negative';

    $r->disconnect;
}

# Test: negative connect_timeout validation
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    my $died = 0;
    eval {
        $r->connect_timeout(-1);
    };
    $died = 1 if $@;

    ok $died, 'negative connect_timeout throws exception';
    like $@, qr/non-negative/, 'exception message mentions non-negative';

    $r->disconnect;
}

# Test: negative command_timeout validation
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    my $died = 0;
    eval {
        $r->command_timeout(-1);
    };
    $died = 1 if $@;

    ok $died, 'negative command_timeout throws exception';
    like $@, qr/non-negative/, 'exception message mentions non-negative';

    $r->disconnect;
}

# Test: command() with insufficient arguments
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    # Only callback, no command name
    my $died = 0;
    eval {
        $r->command(sub { });
    };
    $died = 1 if $@;

    ok $died, 'command() with only callback throws exception';
    like $@, qr/Usage:/, 'exception mentions usage';

    $r->disconnect;
}

# Test: command() with non-CODE reference callback
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    # String instead of callback
    my $died = 0;
    eval {
        $r->command('GET', 'key', 'not_a_callback');
    };
    $died = 1 if $@;

    ok $died, 'command() with non-CODE callback throws exception';
    like $@, qr/CODE reference/, 'exception mentions CODE reference';

    # Array ref instead of callback
    $died = 0;
    eval {
        $r->command('GET', 'key', [1, 2, 3]);
    };
    $died = 1 if $@;

    ok $died, 'command() with arrayref callback throws exception';

    # Hash ref instead of callback
    $died = 0;
    eval {
        $r->command('GET', 'key', {a => 1});
    };
    $died = 1 if $@;

    ok $died, 'command() with hashref callback throws exception';

    $r->disconnect;
}

# Test: clearing callbacks (both no-arg and undef work)
{
    # Don't auto-connect - just test the setter behavior
    my $r = EV::Hiredis->new;

    # Set callbacks and verify clearing with undef works
    $r->on_error(sub { });
    $r->on_error(undef);
    ok !defined($r->on_error), 'on_error cleared by undef';

    $r->on_connect(sub { });
    $r->on_connect(undef);
    ok !defined($r->on_connect), 'on_connect cleared by undef';

    $r->on_disconnect(sub { });
    $r->on_disconnect(undef);
    ok !defined($r->on_disconnect), 'on_disconnect cleared by undef';

    # Verify clearing with no-arg also works
    $r->on_error(sub { });
    $r->on_error();
    ok !defined($r->on_error), 'on_error cleared by no-arg call';

    $r->on_connect(sub { });
    $r->on_connect();
    ok !defined($r->on_connect), 'on_connect cleared by no-arg call';

    $r->on_disconnect(sub { });
    $r->on_disconnect();
    ok !defined($r->on_disconnect), 'on_disconnect cleared by no-arg call';
}

# Test: replacing callbacks (memory management)
{
    # Don't auto-connect - just test the setter behavior
    my $r = EV::Hiredis->new;

    # Set and replace callbacks multiple times
    for (1..5) {
        $r->on_error(sub { });
        $r->on_connect(sub { });
        $r->on_disconnect(sub { });
    }

    # Clear them
    $r->on_error(undef);
    $r->on_connect(undef);
    $r->on_disconnect(undef);

    ok 1, 'repeatedly replacing callbacks does not crash';
}

# Test: timeout getter methods return current value
{
    my $r = EV::Hiredis->new(
        connect_timeout => 5000,
        command_timeout => 3000,
    );

    is $r->connect_timeout(), 5000, 'connect_timeout getter returns set value';
    is $r->command_timeout(), 3000, 'command_timeout getter returns set value';

    # Modify and verify getter reflects change
    $r->connect_timeout(7000);
    $r->command_timeout(4000);
    is $r->connect_timeout(), 7000, 'connect_timeout getter returns updated value';
    is $r->command_timeout(), 4000, 'command_timeout getter returns updated value';
}

# Test: timeout getters return undef when not set
{
    my $r = EV::Hiredis->new;

    ok !defined($r->connect_timeout()), 'connect_timeout returns undef when not set';
    ok !defined($r->command_timeout()), 'command_timeout returns undef when not set';
}

# Test: callback clearing via no-argument call
{
    my $r = EV::Hiredis->new;

    my $called = 0;
    $r->on_connect(sub { $called++ });

    # Clear the handler by calling without arguments
    $r->on_connect();

    # Verify handler was cleared by setting a new one and checking it works
    my $new_called = 0;
    $r->on_connect(sub { $new_called++ });

    # The new handler should work (old one was cleared)
    $r->connect_unix($connect_info{sock});
    my $t; $t = EV::timer 0.1, 0, sub { $r->disconnect; undef $t };
    EV::run;

    is $called, 0, 'old on_connect handler was cleared';
    is $new_called, 1, 'new on_connect handler works after clearing';
}

# Test: empty array reply (LRANGE on empty/nonexistent list)
{
    $r->connect_unix($connect_info{sock});

    my @results;

    # First ensure key doesn't exist
    $r->del('empty_list_test', sub {
        # Now LRANGE should return empty array
        $r->lrange('empty_list_test', 0, -1, sub {
            my ($res, $err) = @_;
            push @results, [$res, $err];
            $r->disconnect;
        });
    });

    EV::run;

    ok !$results[0][1], 'no error for LRANGE on empty list';
    ok ref($results[0][0]) eq 'ARRAY', 'LRANGE returns array';
    is scalar(@{$results[0][0]}), 0, 'LRANGE returns empty array for nonexistent list';
}

# Test: timeout overflow protection
{
    my $r = EV::Hiredis->new;

    # Valid large timeout should work (about 23 days)
    eval { $r->connect_timeout(2000000000) };
    ok !$@, 'large valid timeout accepted';
    is $r->connect_timeout, 2000000000, 'large timeout value preserved';

    # Timeout exceeding max should croak
    eval { $r->connect_timeout(2000000001) };
    like $@, qr/timeout too large/, 'timeout exceeding max rejected';

    # Same for command_timeout
    eval { $r->command_timeout(2000000001) };
    like $@, qr/timeout too large/, 'command_timeout exceeding max rejected';

    # Same for waiting_timeout
    eval { $r->waiting_timeout(2000000001) };
    like $@, qr/waiting_timeout too large/, 'waiting_timeout exceeding max rejected';

    # Valid waiting_timeout should work
    eval { $r->waiting_timeout(60000) };
    ok !$@, 'normal waiting_timeout accepted';
    is $r->waiting_timeout, 60000, 'waiting_timeout value preserved';
}

# Test: command() without connection throws exception
{
    my $r = EV::Hiredis->new;

    my $died = 0;
    eval {
        $r->command('GET', 'key', sub { });
    };
    $died = 1 if $@;

    ok $died, 'command() without connection throws exception';
    like $@, qr/connection required/, 'exception mentions connection required';
}

# Test: AUTOLOAD command without connection throws exception
{
    my $r = EV::Hiredis->new;

    my $died = 0;
    eval {
        $r->get('key', sub { });
    };
    $died = 1 if $@;

    ok $died, 'AUTOLOAD command without connection throws exception';
    like $@, qr/connection required/, 'AUTOLOAD exception mentions connection required';
}

# Test: Redis transactions (MULTI/EXEC)
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Clean up test keys
    $r->del('tx_key1', 'tx_key2', 'tx_counter', sub {
        # Start transaction
        $r->multi(sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'multi', res => $res, err => $err };

            # Queue commands
            $r->set('tx_key1', 'value1', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'set1', res => $res, err => $err };
            });

            $r->set('tx_key2', 'value2', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'set2', res => $res, err => $err };
            });

            $r->incr('tx_counter', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'incr', res => $res, err => $err };
            });

            $r->get('tx_key1', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'get', res => $res, err => $err };
            });

            # Execute transaction
            $r->exec(sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'exec', res => $res, err => $err };
                $r->disconnect;
            });
        });
    });

    EV::run;

    is scalar(@results), 6, 'all transaction callbacks called';
    is $results[0]{res}, 'OK', 'MULTI returns OK';
    is $results[1]{res}, 'QUEUED', 'SET returns QUEUED inside transaction';
    is $results[2]{res}, 'QUEUED', 'second SET returns QUEUED';
    is $results[3]{res}, 'QUEUED', 'INCR returns QUEUED';
    is $results[4]{res}, 'QUEUED', 'GET returns QUEUED';
    ok !$results[5]{err}, 'EXEC has no error';
    is ref($results[5]{res}), 'ARRAY', 'EXEC returns array';
    is scalar(@{$results[5]{res}}), 4, 'EXEC returns 4 results';
    is $results[5]{res}[0], 'OK', 'first SET result is OK';
    is $results[5]{res}[1], 'OK', 'second SET result is OK';
    is $results[5]{res}[2], 1, 'INCR result is 1';
    is $results[5]{res}[3], 'value1', 'GET result is value1';
}

# Test: DISCARD aborts transaction
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->set('discard_test', 'original', sub {
        $r->multi(sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'multi', res => $res };

            $r->set('discard_test', 'changed', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'set', res => $res };
            });

            $r->discard(sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'discard', res => $res };

                # Verify value unchanged
                $r->get('discard_test', sub {
                    my ($res, $err) = @_;
                    push @results, { cmd => 'get', res => $res };
                    $r->disconnect;
                });
            });
        });
    });

    EV::run;

    is scalar(@results), 4, 'all discard test callbacks called';
    is $results[0]{res}, 'OK', 'MULTI returns OK';
    is $results[1]{res}, 'QUEUED', 'SET returns QUEUED';
    is $results[2]{res}, 'OK', 'DISCARD returns OK';
    is $results[3]{res}, 'original', 'value unchanged after DISCARD';
}

# Test: WATCH for optimistic locking (Redis 2.2+)
SKIP: {
    skip 'WATCH requires Redis 2.2+', 8 if $redis_version < 2 || ($redis_version == 2 && $redis_minor < 2);

    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Setup: set initial value
    $r->set('watch_key', '100', sub {
        # Watch the key
        $r->watch('watch_key', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'watch', res => $res, err => $err };

            # Start transaction
            $r->multi(sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'multi', res => $res };

                $r->incr('watch_key', sub {
                    my ($res, $err) = @_;
                    push @results, { cmd => 'incr', res => $res };
                });

                $r->exec(sub {
                    my ($res, $err) = @_;
                    push @results, { cmd => 'exec', res => $res, err => $err };

                    # Verify result
                    $r->get('watch_key', sub {
                        my ($res, $err) = @_;
                        push @results, { cmd => 'get', res => $res };
                        $r->disconnect;
                    });
                });
            });
        });
    });

    EV::run;

    is scalar(@results), 5, 'all WATCH test callbacks called';
    is $results[0]{res}, 'OK', 'WATCH returns OK';
    is $results[1]{res}, 'OK', 'MULTI returns OK';
    is $results[2]{res}, 'QUEUED', 'INCR returns QUEUED';
    ok !$results[3]{err}, 'EXEC has no error';
    is ref($results[3]{res}), 'ARRAY', 'EXEC returns array (not aborted)';
    is $results[3]{res}[0], 101, 'INCR result is 101';
    is $results[4]{res}, '101', 'final value is 101';
}

# Test: EVAL Lua scripting (Redis 2.6+)
SKIP: {
    skip 'EVAL requires Redis 2.6+', 6 if $redis_version < 2 || ($redis_version == 2 && $redis_minor < 6);

    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Simple script: return arguments
    my $script1 = q{return {KEYS[1], ARGV[1], ARGV[2]}};

    $r->eval($script1, 1, 'mykey', 'arg1', 'arg2', sub {
        my ($res, $err) = @_;
        push @results, { cmd => 'eval1', res => $res, err => $err };

        # Script with computation
        my $script2 = q{return tonumber(ARGV[1]) + tonumber(ARGV[2])};
        $r->eval($script2, 0, '10', '25', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'eval2', res => $res, err => $err };
            $r->disconnect;
        });
    });

    EV::run;

    is scalar(@results), 2, 'both EVAL callbacks called';
    ok !$results[0]{err}, 'first EVAL has no error';
    is ref($results[0]{res}), 'ARRAY', 'EVAL returns array';
    is_deeply $results[0]{res}, ['mykey', 'arg1', 'arg2'], 'EVAL returns correct values';
    ok !$results[1]{err}, 'second EVAL has no error';
    is $results[1]{res}, 35, 'EVAL arithmetic works';
}

# Test: SCAN cursor iteration (Redis 2.8+)
SKIP: {
    skip 'SCAN requires Redis 2.8+', 4 if $redis_version < 2 || ($redis_version == 2 && $redis_minor < 8);

    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    # Setup a unique key for this test, then scan for it specifically
    my $unique_key = "scan_unique_$$";
    $r->set($unique_key, 'value', sub {
        # SCAN returns [cursor, [keys...]]
        $r->scan(0, 'MATCH', $unique_key, 'COUNT', 1000, sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'scan', res => $res, err => $err };
            $r->del($unique_key, sub { $r->disconnect; });
        });
    });

    EV::run;

    is scalar(@results), 1, 'SCAN callback called';
    ok !$results[0]{err}, 'SCAN has no error';
    is ref($results[0]{res}), 'ARRAY', 'SCAN returns array [cursor, keys]';
    is ref($results[0]{res}[1]), 'ARRAY', 'SCAN second element is array of keys';
}

# Test: HGETALL returns flat array
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->del('hash_test', sub {
        $r->hset('hash_test', 'field1', 'value1', sub {
            $r->hset('hash_test', 'field2', 'value2', sub {
                $r->hgetall('hash_test', sub {
                    my ($res, $err) = @_;
                    push @results, { cmd => 'hgetall', res => $res, err => $err };
                    $r->disconnect;
                });
            });
        });
    });

    EV::run;

    is scalar(@results), 1, 'HGETALL callback called';
    ok !$results[0]{err}, 'HGETALL has no error';
    is ref($results[0]{res}), 'ARRAY', 'HGETALL returns array';
    is scalar(@{$results[0]{res}}), 4, 'HGETALL returns 4 elements (2 field-value pairs)';
    my %hash = @{$results[0]{res}};
    is $hash{field1}, 'value1', 'HGETALL field1 correct';
    is $hash{field2}, 'value2', 'HGETALL field2 correct';
}

# Test: SETEX with expiry (Redis 2.0+)
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->setex('expiry_test', 10, 'temporary', sub {
        my ($res, $err) = @_;
        push @results, { cmd => 'setex', res => $res, err => $err };

        $r->ttl('expiry_test', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'ttl', res => $res, err => $err };
            $r->disconnect;
        });
    });

    EV::run;

    is scalar(@results), 2, 'SETEX/TTL callbacks called';
    is $results[0]{res}, 'OK', 'SETEX returns OK';
    ok $results[1]{res} > 0 && $results[1]{res} <= 10, 'TTL returns valid expiry';
}

# Test: MSET/MGET multiple keys
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->mset('mkey1', 'mval1', 'mkey2', 'mval2', 'mkey3', 'mval3', sub {
        my ($res, $err) = @_;
        push @results, { cmd => 'mset', res => $res, err => $err };

        $r->mget('mkey1', 'mkey2', 'mkey3', 'nonexistent', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'mget', res => $res, err => $err };
            $r->disconnect;
        });
    });

    EV::run;

    is scalar(@results), 2, 'MSET/MGET callbacks called';
    is $results[0]{res}, 'OK', 'MSET returns OK';
    is ref($results[1]{res}), 'ARRAY', 'MGET returns array';
    is_deeply $results[1]{res}, ['mval1', 'mval2', 'mval3', undef], 'MGET returns correct values (including nil)';
}

# Test: LPUSH/LRANGE list operations
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->del('list_test', sub {
        $r->lpush('list_test', 'c', 'b', 'a', sub {  # Results in: a, b, c
            my ($res, $err) = @_;
            push @results, { cmd => 'lpush', res => $res, err => $err };

            $r->lrange('list_test', 0, -1, sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'lrange', res => $res, err => $err };
                $r->disconnect;
            });
        });
    });

    EV::run;

    is scalar(@results), 2, 'LPUSH/LRANGE callbacks called';
    is $results[0]{res}, 3, 'LPUSH returns list length';
    is_deeply $results[1]{res}, ['a', 'b', 'c'], 'LRANGE returns list in order';
}

# Test: SADD/SMEMBERS set operations
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->del('set_test', sub {
        $r->sadd('set_test', 'a', 'b', 'c', 'a', sub {  # 'a' duplicate ignored
            my ($res, $err) = @_;
            push @results, { cmd => 'sadd', res => $res, err => $err };

            $r->smembers('set_test', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'smembers', res => $res, err => $err };
                $r->disconnect;
            });
        });
    });

    EV::run;

    is scalar(@results), 2, 'SADD/SMEMBERS callbacks called';
    is $results[0]{res}, 3, 'SADD returns number of added elements';
    is ref($results[1]{res}), 'ARRAY', 'SMEMBERS returns array';
    is scalar(@{$results[1]{res}}), 3, 'SMEMBERS returns 3 unique elements';
}

# Test: ZADD/ZRANGE sorted set operations
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->del('zset_test', sub {
        $r->zadd('zset_test', 3, 'three', 1, 'one', 2, 'two', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'zadd', res => $res, err => $err };

            $r->zrange('zset_test', 0, -1, sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'zrange', res => $res, err => $err };
                $r->disconnect;
            });
        });
    });

    EV::run;

    is scalar(@results), 2, 'ZADD/ZRANGE callbacks called';
    is $results[0]{res}, 3, 'ZADD returns number of added elements';
    is_deeply $results[1]{res}, ['one', 'two', 'three'], 'ZRANGE returns sorted order';
}

# Test: EXISTS and DEL
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->set('exists_test', 'value', sub {
        $r->exists('exists_test', sub {
            my ($res, $err) = @_;
            push @results, { cmd => 'exists1', res => $res };

            $r->del('exists_test', sub {
                my ($res, $err) = @_;
                push @results, { cmd => 'del', res => $res };

                $r->exists('exists_test', sub {
                    my ($res, $err) = @_;
                    push @results, { cmd => 'exists2', res => $res };
                    $r->disconnect;
                });
            });
        });
    });

    EV::run;

    is $results[0]{res}, 1, 'EXISTS returns 1 for existing key';
    is $results[1]{res}, 1, 'DEL returns number of deleted keys';
    is $results[2]{res}, 0, 'EXISTS returns 0 after DEL';
}

# Test: TYPE command
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my @results;

    $r->set('type_string', 'value', sub {
        $r->lpush('type_list', 'item', sub {
            $r->sadd('type_set', 'member', sub {
                $r->type('type_string', sub {
                    push @results, shift;
                    $r->type('type_list', sub {
                        push @results, shift;
                        $r->type('type_set', sub {
                            push @results, shift;
                            $r->type('nonexistent', sub {
                                push @results, shift;
                                $r->disconnect;
                            });
                        });
                    });
                });
            });
        });
    });

    EV::run;

    is $results[0], 'string', 'TYPE returns string';
    is $results[1], 'list', 'TYPE returns list';
    is $results[2], 'set', 'TYPE returns set';
    is $results[3], 'none', 'TYPE returns none for nonexistent';
}

done_testing;
