#! perl

use v5.10.1;

use strict;
use warnings FATAL => 'all';

use Getopt::Long;
use IO::CaptureOutput qw(capture_exec);
use List::MoreUtils qw(uniq);
use Math::Prime::Util qw(factor);

sub reduce_factors
{
    my @flist = @_;
    given(scalar(@flist))
    {
	when(0) { return (1,1); }
	when(1) { return (1,@flist); }
	when(2) { return @flist; }
	default {
	    my $mid = int(scalar(@flist)/2);
	    my @nfl = (1, 1);
	    for my $i (0 .. $#flist)
	    {
		$i < $mid and $nfl[0] *= $flist[$i];
		$i >= $mid and $nfl[1] *= $flist[$i];
	    }
	    return @nfl;
	}
    }
}

sub calc_pw_vector
{
    my ($symbol, $pw) = @_;
    my ($init_prev, $prev, @syms, @pws, @f1, @f2, @back);
    for my $i (0 .. length($pw) - 1 )
    {
	push( @syms, ord(substr($symbol, $i, 1)) );
	push( @pws, ord(substr($pw, $i, 1)) );
	unless( defined($prev) )
	{
	    $prev = $init_prev = int(rand(256));
	}
	my $wanted = $syms[$i] + $pws[$i] - $prev;
	my @af = $wanted < 0 ? reduce_factors( -1, factor(0-$wanted)) : reduce_factors(factor($wanted));
	push( @f1, $af[0] );
	push( @f2, $af[1] );
	push( @back, (($f1[$i] * $f2[$i])+$prev-$syms[$i]) );
	$prev = $pws[$i];
    }

    #printf("%3d\n", $init_prev);
    #my $fmt = join(" ", ("%3d") x scalar(@ofs) );
    #printf( "$fmt\n", @pws );
    #printf( "$fmt\n", @syms );
    #printf( "$fmt\n", @f1 );
    #printf( "$fmt\n", @f2 );
    #printf( "$fmt\n", @back );

    return ($init_prev, \@f1, \@f2);
}

sub gen_pw_objects
{
    my ($symbol, $init_prev, $rf1, $rf2) = @_;
    my @f1 = @$rf1;
    my @f2 = @$rf2;

    my $cpp_src = <<EOH;
#include <limits>
#include <vector>
#include <string>

using namespace std;

class Summarizer
{
public:
    Summarizer(int f1, int f2)
	: m_f1(f1)
	, m_f2(f2)
    {}

    char operator()(char prev) const
    {
	return (m_f1 * m_f2) + prev;
    }

private:
    int m_f1;
    int m_f2;
};

vector<Summarizer>
get_summarizers()
{
    vector<Summarizer> result;

EOH

    for my $i (0 .. $#f1)
    {
	$cpp_src .= <<EOG;
    result.push_back(Summarizer($f1[$i], $f2[$i]));
EOG
    }

    $cpp_src .= <<EOS;

    return result;
}

string
summarize(vector<Summarizer> const &summarizers)
{
    string offsets = "$symbol";
    string result;
    char prev = $init_prev;

    for( size_t i = 0; i < summarizers.size(); ++i )
    {
	prev = summarizers[i](prev) - offsets.at(i);
	result.append( 1, prev );
    }

    return result;
}
EOS

    return $cpp_src;
}

my %opts;
GetOptions( \%opts, "nm-file=s", "password=s", "filter=s" ) or die "Cannot parse options";

$opts{"nm-file"} or die "Missing --nm-file";
-r $opts{"nm-file"} or die "Can't read '" . $opts{"nm-file"} . "': $!";
$opts{"password"} or die "Missing --password";

my $nm_cmd = 'nm "'. $opts{"nm-file"} . '"';
my ($stdout, $stderr, $success, $exit_code) = capture_exec("nm", $opts{"nm-file"});
$success or die "$nm_cmd: $stderr";

my $pwlen = length($opts{"password"});
my @symbols = uniq grep { length($_) >= $pwlen and length($_) <= ($pwlen*2) } map { (split)[-1] } split("\n", $stdout);
$opts{"filter"} and @symbols = grep { $_ =~ m/$opts{filter}/ } @symbols;

srand();
my $basesym = $symbols[rand($#symbols)];
#print "$basesym\n";

my ($init_prev, $rf1, $rf2) = calc_pw_vector($basesym, $opts{"password"});
my $cxx_code = gen_pw_objects($basesym, $init_prev, $rf1, $rf2);

print $cxx_code;
