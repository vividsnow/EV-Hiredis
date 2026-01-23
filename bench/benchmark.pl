#!/usr/bin/env perl
use strict;
use warnings;
use Time::HiRes qw(time);
$| = 1; # Autoflush
use Test::RedisServer;
use EV;
use EV::Hiredis;

# Configuration
my $NUM_COMMANDS  = $ENV{BENCH_COMMANDS}  // 10000;
my $VALUE_SIZE    = $ENV{BENCH_VALUE_SIZE} // 100;
my $MAX_PENDING   = $ENV{BENCH_MAX_PENDING}; # undef = unlimited

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or die "redis-server is required for benchmarking: $@";

my %connect_info = $redis_server->connect_info;
my $value = 'x' x $VALUE_SIZE;

print "=" x 60, "\n";
print "EV::Hiredis Benchmark\n";
print "=" x 60, "\n";
print "Commands per test: $NUM_COMMANDS\n";
print "Value size: $VALUE_SIZE bytes\n";
print "Max pending: ", (defined $MAX_PENDING ? $MAX_PENDING : "unlimited"), "\n";
print "=" x 60, "\n\n";

# Benchmark 1: Sequential SET commands (pipelining)
bench_set_pipeline();

# Benchmark 2: Sequential GET commands (pipelining)
bench_get_pipeline();

# Benchmark 3: SET+GET pairs (round-trip latency)
bench_set_get_roundtrip();

# Benchmark 4: Mixed workload
bench_mixed_workload();

# Benchmark 5: Flow control impact (if max_pending set)
bench_flow_control();

# Benchmark 6: Large batch with waiting queue
bench_waiting_queue();

# Cleanup
cleanup_keys();

print "=" x 60, "\n";
print "Benchmark complete.\n";
print "=" x 60, "\n";

sub create_client {
    my %opts = @_;
    return EV::Hiredis->new(
        path => $connect_info{sock},
        on_error => sub { die "Redis error: @_" },
        %opts,
    );
}

sub format_rate {
    my ($count, $elapsed) = @_;
    my $rate = $count / $elapsed;
    if ($rate >= 1_000_000) {
        return sprintf("%.2fM ops/sec", $rate / 1_000_000);
    } elsif ($rate >= 1_000) {
        return sprintf("%.2fK ops/sec", $rate / 1_000);
    } else {
        return sprintf("%.2f ops/sec", $rate);
    }
}

sub format_time {
    my ($seconds) = @_;
    if ($seconds < 0.001) {
        return sprintf("%.2f µs", $seconds * 1_000_000);
    } elsif ($seconds < 1) {
        return sprintf("%.2f ms", $seconds * 1_000);
    } else {
        return sprintf("%.2f s", $seconds);
    }
}

sub bench_set_pipeline {
    print "1. SET Pipeline (fire-and-forget style)\n";
    print "-" x 40, "\n";

    my $r = create_client(max_pending => $MAX_PENDING);
    my $completed = 0;
    my $start = time();

    for my $i (1 .. $NUM_COMMANDS) {
        $r->set("bench:key:$i", $value, sub {
            my ($res, $err) = @_;
            die "SET failed: $err" if $err;
            $completed++;
            if ($completed == $NUM_COMMANDS) {
                $r->disconnect;
            }
        });
    }

    EV::run;
    my $elapsed = time() - $start;

    print "  Completed: $completed commands\n";
    print "  Time: ", format_time($elapsed), "\n";
    print "  Rate: ", format_rate($completed, $elapsed), "\n";
    print "  Avg latency: ", format_time($elapsed / $completed), "\n";
    print "\n";
}

sub bench_get_pipeline {
    print "2. GET Pipeline\n";
    print "-" x 40, "\n";

    my $r = create_client(max_pending => $MAX_PENDING);
    my $completed = 0;
    my $start = time();

    for my $i (1 .. $NUM_COMMANDS) {
        $r->get("bench:key:$i", sub {
            my ($res, $err) = @_;
            die "GET failed: $err" if $err;
            $completed++;
            if ($completed == $NUM_COMMANDS) {
                $r->disconnect;
            }
        });
    }

    EV::run;
    my $elapsed = time() - $start;

    print "  Completed: $completed commands\n";
    print "  Time: ", format_time($elapsed), "\n";
    print "  Rate: ", format_rate($completed, $elapsed), "\n";
    print "  Avg latency: ", format_time($elapsed / $completed), "\n";
    print "\n";
}

sub bench_set_get_roundtrip {
    print "3. SET+GET Round-trip (sequential pairs)\n";
    print "-" x 40, "\n";

    my $r = create_client(max_pending => $MAX_PENDING);
    my $pairs = int($NUM_COMMANDS / 2);
    my $completed = 0;
    my $start = time();
    my $i = 0;

    my $do_next;
    $do_next = sub {
        $i++;
        if ($i > $pairs) {
            $r->disconnect;
            return;
        }
        $r->set("bench:rt:$i", $value, sub {
            my ($res, $err) = @_;
            die "SET failed: $err" if $err;
            $completed++;
            $r->get("bench:rt:$i", sub {
                my ($res, $err) = @_;
                die "GET failed: $err" if $err;
                $completed++;
                $do_next->();
            });
        });
    };

    $do_next->();
    EV::run;
    my $elapsed = time() - $start;

    print "  Completed: $completed commands ($pairs pairs)\n";
    print "  Time: ", format_time($elapsed), "\n";
    print "  Rate: ", format_rate($pairs, $elapsed), "\n";
    print "  Avg round-trip: ", format_time($elapsed / $pairs), "\n";
    print "\n";
}

sub bench_mixed_workload {
    print "4. Mixed Workload (70% GET, 20% SET, 10% INCR)\n";
    print "-" x 40, "\n";

    my $r = create_client(max_pending => $MAX_PENDING);
    my $completed = 0;
    my $start = time();

    for my $i (1 .. $NUM_COMMANDS) {
        my $rand = rand(100);
        my $key_num = int(rand($NUM_COMMANDS)) + 1;

        if ($rand < 70) {
            # 70% GET
            $r->get("bench:key:$key_num", sub {
                $completed++;
                $r->disconnect if $completed == $NUM_COMMANDS;
            });
        } elsif ($rand < 90) {
            # 20% SET
            $r->set("bench:key:$key_num", $value, sub {
                $completed++;
                $r->disconnect if $completed == $NUM_COMMANDS;
            });
        } else {
            # 10% INCR
            $r->incr("bench:counter", sub {
                $completed++;
                $r->disconnect if $completed == $NUM_COMMANDS;
            });
        }
    }

    EV::run;
    my $elapsed = time() - $start;

    print "  Completed: $completed commands\n";
    print "  Time: ", format_time($elapsed), "\n";
    print "  Rate: ", format_rate($completed, $elapsed), "\n";
    print "\n";
}

sub bench_flow_control {
    print "5. Flow Control Comparison\n";
    print "-" x 40, "\n";

    my @limits = (0, 100, 500, 1000);  # 0 = unlimited
    my $cmds = int($NUM_COMMANDS / 2);  # Use fewer commands for this test

    for my $limit (@limits) {
        my $r = create_client(max_pending => ($limit == 0 ? undef : $limit));
        my $completed = 0;
        my $start = time();

        for my $i (1 .. $cmds) {
            $r->set("bench:fc:$i", $value, sub {
                $completed++;
                $r->disconnect if $completed == $cmds;
            });
        }

        EV::run;
        my $elapsed = time() - $start;

        my $limit_str = $limit == 0 ? "unlimited" : sprintf("%d", $limit);
        printf "  max_pending=%-10s %s  (%s)\n",
            $limit_str,
            format_rate($cmds, $elapsed),
            format_time($elapsed);
    }
    print "\n";
}

sub bench_waiting_queue {
    print "6. Waiting Queue (max_pending=50, burst of commands)\n";
    print "-" x 40, "\n";

    my $r = create_client(max_pending => 50);
    my $cmds = int($NUM_COMMANDS / 2);
    my $completed = 0;
    my $max_waiting = 0;
    my $start = time();

    for my $i (1 .. $cmds) {
        $r->set("bench:wq:$i", $value, sub {
            $completed++;
            $r->disconnect if $completed == $cmds;
        });

        my $waiting = $r->waiting_count;
        $max_waiting = $waiting if $waiting > $max_waiting;
    }

    EV::run;
    my $elapsed = time() - $start;

    print "  Completed: $completed commands\n";
    print "  Time: ", format_time($elapsed), "\n";
    print "  Rate: ", format_rate($cmds, $elapsed), "\n";
    print "  Max waiting queue depth: $max_waiting\n";
    print "\n";
}

sub cleanup_keys {
    print "Cleaning up benchmark keys...\n";
    my $r = create_client();
    my $done = 0;

    # Use KEYS to find and delete all bench:* keys
    $r->keys("bench:*", sub {
        my ($keys, $err) = @_;
        if ($err) {
            warn "KEYS failed: $err\n";
            $r->disconnect;
            return;
        }

        if (!$keys || @$keys == 0) {
            print "  No keys to clean up.\n";
            $r->disconnect;
            return;
        }

        my $count = scalar @$keys;
        my $deleted = 0;

        for my $key (@$keys) {
            $r->del($key, sub {
                $deleted++;
                if ($deleted == $count) {
                    print "  Deleted $count keys.\n";
                    $r->disconnect;
                }
            });
        }
    });

    EV::run;
    print "\n";
}
