use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Devel::Peek qw/SvREFCNT/;
use Devel::Refcount qw/refcount/;
my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

plan tests => 13;

my %connect_info = $redis_server->connect_info;

use EV;
use EV::Hiredis;

my $r = EV::Hiredis->new( path => $connect_info{sock} );
my ($get_command, $test);
my $result;
$get_command = sub {
    $r->lrange('foo', 0, -1, sub {
        $result = shift;
        $r->lrange('foo', 0, -1, $test);
        $get_command = undef;
    });
};
$test = sub {
    is refcount($result), 1, 'reference count of array is 1(no_leaks_ok and $test)';
    is SvREFCNT($result->[0]), 1, 'reference count of first element is 1';
    is SvREFCNT($result->[1]), 1, 'reference count of second element is 1';
    $r->disconnect;
};
$r->rpush('foo' => 'bar1', sub {
    $r->rpush('foo' => 'bar2', $get_command);
});
EV::run;

# Test: callback cleanup on Redis error responses
{
    my $r2 = EV::Hiredis->new( path => $connect_info{sock} );
    my $error_result;
    my $error_msg;

    # Create a string key, then try to use list command on it (causes Redis error)
    $r2->set('string_key', 'value', sub {
        $r2->lpush('string_key', 'item', sub {
            # This should fail with WRONGTYPE error
            ($error_result, $error_msg) = @_;
            $r2->disconnect;
        });
    });
    EV::run;

    is $error_result, undef, 'error callback receives undef result';
    like $error_msg, qr/WRONGTYPE/, 'error callback receives error message';
}
pass 'no leak on Redis error response callback';

# Test: callback cleanup when callback throws exception
{
    my $r3 = EV::Hiredis->new(
        path => $connect_info{sock},
        on_error => sub { }, # suppress default die
    );
    my $exception_thrown = 0;
    my $after_exception = 0;

    $r3->set('test_key', 'value', sub {
        $exception_thrown = 1;
        die "intentional exception in callback";
    });

    # Give time for the callback to execute
    my $timer = EV::timer 0.1, 0, sub {
        $after_exception = 1;
        $r3->disconnect;
    };
    EV::run;

    is $exception_thrown, 1, 'callback was executed before exception';
    is $after_exception, 1, 'event loop continued after callback exception';
}
pass 'no leak when callback throws exception';

# Test: callback cleanup on command timeout
{
    my $r4 = EV::Hiredis->new(
        path => $connect_info{sock},
        on_error => sub { }, # suppress default die
        command_timeout => 100, # 100ms timeout
    );
    my $timeout_result;
    my $timeout_error;
    my $callback_called = 0;

    # BLPOP with 10 second wait, but command_timeout is 100ms
    # This should timeout before BLPOP returns
    $r4->blpop('nonexistent_key_for_timeout_test', 10, sub {
        ($timeout_result, $timeout_error) = @_;
        $callback_called = 1;
        $r4->disconnect;
    });

    # Fallback timer in case timeout doesn't work as expected
    my $fallback = EV::timer 2, 0, sub {
        $r4->disconnect unless $callback_called;
    };

    EV::run;

    is $callback_called, 1, 'timeout callback was called';
    is $timeout_result, undef, 'timeout callback receives undef result';
    ok defined($timeout_error), 'timeout callback receives error message';
}
pass 'no leak on command timeout callback';

done_testing;
