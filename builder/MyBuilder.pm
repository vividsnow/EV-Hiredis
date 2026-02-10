package builder::MyBuilder;
use strict;
use warnings FATAL => 'all';
use 5.008005;
use base 'Module::Build::XSUtil';
use Config;
use File::Which qw(which);
use EV::MakeMaker '$installsitearch';

sub _detect_openssl {
    my $openssl_prefix;

    # Check environment override
    if (defined $ENV{OPENSSL_PREFIX}) {
        $openssl_prefix = $ENV{OPENSSL_PREFIX};
        return $openssl_prefix if -d "$openssl_prefix/include/openssl";
        warn "OPENSSL_PREFIX=$openssl_prefix but no include/openssl found\n";
        return undef;
    }

    # Try pkg-config
    if (system("pkg-config --exists openssl 2>/dev/null") == 0) {
        my $cflags = `pkg-config --cflags openssl 2>/dev/null`;
        my $libs = `pkg-config --libs openssl 2>/dev/null`;
        chomp($cflags, $libs);
        return { cflags => $cflags, libs => $libs };
    }

    # macOS: check Homebrew paths
    if ($^O eq 'darwin') {
        for my $path ('/opt/homebrew/opt/openssl', '/usr/local/opt/openssl') {
            if (-d "$path/include/openssl") {
                return $path;
            }
        }
    }

    # Check standard system paths
    for my $inc ('/usr/include', '/usr/local/include') {
        if (-f "$inc/openssl/ssl.h") {
            return { cflags => '', libs => '-lssl -lcrypto' };
        }
    }

    return undef;
}

sub new {
    my ( $class, %args ) = @_;

    my @extra_linker_flags = ("deps/hiredis/libhiredis$Config{lib_ext}");
    my @extra_compiler_flags;

    # Determine SSL support: EV_HIREDIS_SSL=0 to disable, =1 to force, auto-detect otherwise
    my $want_ssl = $ENV{EV_HIREDIS_SSL};
    my $has_ssl = 0;
    my $ssl;
    my ($ssl_cflags, $ssl_libs) = ('', '');

    if (!defined $want_ssl || $want_ssl) {
        $ssl = _detect_openssl();
        if ($ssl) {
            $has_ssl = 1;
            if (ref $ssl eq 'HASH') {
                $ssl_cflags = $ssl->{cflags};
                $ssl_libs = $ssl->{libs};
            } else {
                # prefix path
                $ssl_cflags = "-I$ssl/include";
                $ssl_libs = "-L$ssl/lib -lssl -lcrypto";
            }
        } elsif ($want_ssl) {
            die "EV_HIREDIS_SSL=1 but OpenSSL not found. Install OpenSSL development headers or set OPENSSL_PREFIX.\n";
        }
    }

    if ($has_ssl) {
        push @extra_compiler_flags, '-DEV_HIREDIS_SSL';
        push @extra_compiler_flags, split(/\s+/, $ssl_cflags) if $ssl_cflags;
        push @extra_linker_flags, "deps/hiredis/libhiredis_ssl$Config{lib_ext}";
        push @extra_linker_flags, split(/\s+/, $ssl_libs) if $ssl_libs;
        print "Building with TLS support\n";
    } else {
        print "Building without TLS support (set EV_HIREDIS_SSL=1 to force)\n";
    }

    my $self = $class->SUPER::new(
        %args,
        generate_ppport_h    => 'src/ppport.h',
        c_source             => 'src',
        xs_files             => { 'src/EV__Hiredis.xs' => 'lib/EV/Hiredis.xs' },
        include_dirs         => ['src', 'deps/hiredis', "${installsitearch}/EV", $installsitearch],
        extra_linker_flags   => \@extra_linker_flags,
        (@extra_compiler_flags ? (extra_compiler_flags => \@extra_compiler_flags) : ()),
    );

    my $make;
    if ($^O =~ m/bsd$/ && $^O !~ m/gnukfreebsd$/) {
        my $gmake = which('gmake');
        unless (defined $gmake) {
            print "'gmake' is necessary for BSD platform.\n";
            exit 0;
        }
        $make = $gmake;
    } else {
        $make = $Config{make};
    }

    my @make_args = ('-C', 'deps/hiredis', 'static');
    if ($has_ssl) {
        push @make_args, 'USE_SSL=1';
        if (defined $ssl && !ref $ssl) {
            push @make_args, "OPENSSL_PREFIX=$ssl";
        }
    }

    my $result = $self->do_system($make, @make_args);
    unless ($result) {
        die "Failed to build hiredis static library. Check that you have a C compiler and '$make' installed.\n";
    }

    if ($has_ssl) {
        # Verify SSL static library was built
        my $ssl_lib = "deps/hiredis/libhiredis_ssl$Config{lib_ext}";
        unless (-f $ssl_lib) {
            die "Failed to build hiredis SSL library ($ssl_lib). Check OpenSSL installation.\n";
        }
    }

    return $self;
}

1;
