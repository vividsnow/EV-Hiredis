use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use EV;
use EV::Hiredis;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my %connect_info = $redis_server->connect_info;

# Reproduction of Use-After-Free in skip_pending
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    
    my $cb_count = 0;

    $r->command('set', 'key1', 'val1', sub {
        $cb_count++;
        undef $r; # Trigger destruction
    });

    $r->command('set', 'key2', 'val2', sub {
        $cb_count++;
    });

    $r->command('set', 'key3', 'val3', sub {
        $cb_count++;
    });

    $r->skip_pending();

    is($cb_count, 3, "All callbacks invoked despite destruction in the first one");
}

# Test: undef $redis inside async reply callback (hiredis deferred free path)
# When DESTROY fires inside REDIS_IN_CALLBACK, remaining pending callbacks
# should still be invoked with a disconnect error (not silently dropped).
{
    my @results;
    my $r = EV::Hiredis->new(path => $connect_info{sock});

    $r->command('set', 'uaf_key1', 'val1', sub {
        my ($res, $err) = @_;
        push @results, [$res, $err];
        undef $r;  # Trigger DESTROY inside hiredis reply callback
    });

    $r->command('set', 'uaf_key2', 'val2', sub {
        my ($res, $err) = @_;
        push @results, [$res, $err];
    });

    $r->command('set', 'uaf_key3', 'val3', sub {
        my ($res, $err) = @_;
        push @results, [$res, $err];
    });

    EV::run;

    is(scalar @results, 3, "all 3 callbacks invoked despite DESTROY in first");
    is($results[0][0], 'OK', "first command succeeded");
    ok(!defined $results[1][0], "second command got undef result (destroyed)");
    ok(defined $results[1][1], "second command got error string");
    ok(!defined $results[2][0], "third command got undef result (destroyed)");
    ok(defined $results[2][1], "third command got error string");
}

# Test: multi-channel subscribe + DESTROY outside callback (FREED path)
# When DESTROY fires outside REDIS_IN_CALLBACK with a multi-channel subscribe
# active, __redisAsyncFree iterates the channels dict and fires reply_cb once
# per channel with the same cbt. Without the fix, the second call is UAF.
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my $subscribed = 0;

    $r->on_error(sub {}); # suppress die on disconnect
    $r->command('subscribe', 'mc_ch1', 'mc_ch2', 'mc_ch3', sub {
        my ($res, $err) = @_;
        return if $err;
        if ($res->[0] eq 'subscribe') {
            $subscribed++;
            EV::break if $subscribed == 3;
        }
    });

    EV::run;
    is($subscribed, 3, "multi-channel: subscribed to all 3 channels");
    # $r goes out of scope — DESTROY fires outside callback (FREED path)
}
pass("Survived multi-channel subscribe FREED path");

# Test: multi-channel subscribe + undef inside callback (self==NULL path)
# When DESTROY fires inside REDIS_IN_CALLBACK, ac->data is NULLed.
# __redisAsyncFree fires reply_cb for each channel with self==NULL.
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my $cb_count = 0;

    $r->on_error(sub {}); # suppress die on disconnect
    $r->command('subscribe', 'mc_ch4', 'mc_ch5', 'mc_ch6', sub {
        my ($res, $err) = @_;
        $cb_count++;
        return if $err;
        if ($res->[0] eq 'subscribe' && $res->[2] == 3) {
            undef $r;  # DESTROY inside REDIS_IN_CALLBACK
        }
    });

    EV::run;
    ok($cb_count >= 3, "multi-channel self==NULL: callback invoked at least 3 times (got $cb_count)");
}
pass("Survived multi-channel subscribe self==NULL path");

# Test: multi-channel subscribe + skip_pending + disconnect (skipped path)
# When skip_pending marks a multi-channel subscribe cbt as skipped, then
# disconnect fires, hiredis calls reply_cb N times with reply=NULL for
# each channel. Without the sub_count fix, the first call frees cbt and
# subsequent calls are use-after-free.
{
    my $r = EV::Hiredis->new(path => $connect_info{sock});
    my $skip_cb_count = 0;

    $r->on_error(sub {}); # suppress die on disconnect
    $r->command('subscribe', 'skip_ch1', 'skip_ch2', 'skip_ch3', sub {
        my ($res, $err) = @_;
        $skip_cb_count++;
        return unless defined $res;
        if ($res->[0] eq 'subscribe' && $res->[2] == 3) {
            # All 3 channels subscribed — now skip_pending + disconnect
            $r->skip_pending();
            $r->disconnect;
        }
    });

    EV::run;
    ok($skip_cb_count >= 3, "multi-channel skip_pending: callback invoked at least 3 times (got $skip_cb_count)");
}
pass("Survived multi-channel subscribe skip_pending path");

pass("Survived skip_pending UAF test");
pass("Survived async DESTROY UAF test");
done_testing;
