#!/usr/bin/perl

use strict;
use warnings;
use ApMon;
my $apm = new ApMon(0);
select STDOUT; $| = 1;
select STDERR; $| = 1;
my $monalisahost = $ENV{MONALISAHOST};

if ((!defined $monalisahost) || ( $monalisahost eq "")) {
    $monalisahost = "lxbra0301.cern.ch";
}

my $apmonwarninglevel = $ENV{APMON_DEBUG_LEVEL};
my $apmonconfig = $ENV{APMON_CONFIG};

if ((!defined $apmonwarninglevel) || ($apmonwarninglevel eq "")) {
    $apmonwarninglevel = "WARNING";
}

if ((!defined $apmonconfig) || ($apmonconfig eq "")) {
    $apmonconfig = "['$monalisahost']";
}

$apm->setLogLevel($apmonwarninglevel);
$apm->setDestinations(["$monalisahost"]);

sub convert {
    my $val=shift;
    my $exp=shift;

    if ($exp =~/^K/) {$val *= 1000.0;}
    if ($exp =~/^M/) {$val *= 1000000.0;}
    if ($exp =~/^G/) {$val *= 1000000000.0;}
    if ($exp =~/^T/) {$val *= 1000000000000.0;}
    $val = sprintf("%g", $val);
    return $val;
}


while (1) {
    my $reporturl = "root://lxbra0302.cern.ch:1097//eos/dumper/report";
    my $reporthost = "lxbra0302.cern.ch";

    if (defined $ENV{"EOS_REPORT_URL"}) {
	$reporturl = $ENV{"EOS_REPORT_URL"};
    }
    
    if (defined $ENV{"EOS_REPORT_HOST"}) {
	$reporthost = $ENV{"EOS_REPORT_HOST"};
    }

    printf "Connecting to $reporturl\n";
    if (open NS, "env LD_LIBRARY_PATH=/opt/eos/lib/ /opt/eos/bin/xrdmqdumper $reporturl |") {
	while (<NS>) {
	    my @tags = split "&",$_;
	    #log=1ce392f4-9313-11df-b13d-0030489452c6&path=/eos/user/apeters/passwd&ruid=755&rgid=1338&td=apeters.19906:23@128.142.225.5&host=lxfsra26a02.cern.ch&lid=1&fid=243&fsid=116&ots=1279529614&otms=309&&cts=1279529615&ctms=10&rb=0&wb=1637&srb=0&swb=0&nrc=0&nwc=1&rt=0.00&wt=0.02
	    my $item;
	    my @alltags;
	    my $infohash;
	    foreach $item (@tags) {
		my ($key, $val) = split "=", $item;
		if (defined $key && defined $val) {
#		    printf "key=$key val=$val\n";
		    $infohash->{$key} = $val;
		    push @alltags, $key;
		    push @alltags, $val;
		}
	    }

	    if (defined $infohash->{ruid} && defined $infohash->{rgid} ) {
		my $reporttag1 = "uid::"; $reporttag1 .= $infohash->{ruid};
		my $reporttag2 = "gid::"; $reporttag2 .= $infohash->{rgid};
		$apm->sendParameters('Report', $reporttag1,
				     @alltags);
		
		$apm->sendParameters('Report', $reporttag2,
				     @alltags);
	    }
	}
    }

    close NS;

    sleep(10);
}
