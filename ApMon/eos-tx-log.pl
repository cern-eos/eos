#!/usr/bin/perl

use strict;
use warnings;
use ApMon;
my $apm = new ApMon(0);
select STDOUT; $| = 1;
select STDERR; $| = 1;


while (1) {
    my $totalhash;
    my $transactionlogdir = "/var/log/eos/";
    system("mkdir -p $transactionlogdir");
    my $reporturl = "root://lxbra0302.cern.ch:1097";
    my $reporthost = "lxbra0302.cern.ch";

    if (defined $ENV{"EOS_REPORT_URL"}) {
	$reporturl = $ENV{"EOS_REPORT_URL"};
    }
    
    if (defined $ENV{"EOS_REPORT_HOST"}) {
	$reporthost = $ENV{"EOS_REPORT_HOST"};
    }

    if (defined $ENV{"EOS_TRANSACTION_LOGDIR"}) {
	$transactionlogdir = $ENV{"EOS_TRANSACTION_LOGDIR"};
    }

    $reporturl .= "//eos/";
    $reporturl .= "txlog-";
    my $host = `hostname -f`;
    chomp $host;
    $reporturl .= $host;
    $reporturl .= "/report";

    printf "Connecting to $reporturl\n";

    if (open NS, "env LD_LIBRARY_PATH=/opt/eos/lib/ /opt/eos/bin/xrdmqdumper $reporturl |") {
	select((select(NS), $| = 1)[0]);
	while (<NS>) {
	    my $store = `date +%G`;
	    chomp $store;
	    $store .= `date +%m`;
	    chomp $store;
	    my $logdir;
	    $logdir  = $transactionlogdir ;
	    $logdir .= "/";
	    $logdir .= $store;
	    if ( ! -d "$logdir" ) {
		system("mkdir -p $logdir");
	    }
	    open OUT, ">> $logdir/eostx.log";
	    printf OUT "$_";
	    close OUT;
	}
    }

    close NS;
}
