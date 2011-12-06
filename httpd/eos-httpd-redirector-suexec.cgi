#!/usr/bin/perl -w
use strict;
use IPC::Shareable;
use POSIX qw(setuid getuid);
use CGI;
use Fcntl ':mode';

my $q = CGI->new;


my %xoptions = (
     create    => 0,
     exclusive => 0,
     mode      => 0644,
     destroy   => 0,
	       );

my %options = (
     create    => 'yes',
     exclusive => 0,
     mode      => 0644,
     destroy   => 0,
	       );

##############################################################################################################################
# GRIDMAP Caching
##############################################################################################################################
my %gridmap;

tie %gridmap, 'IPC::Shareable', "gridmap", { %xoptions } or tie %gridmap, 'IPC::Shareable', "gridmap", { %options } or die "eos-httpd-redirector: ipc shareable unreachable";

(tied %gridmap)->shlock;
my $now = time;

if ( (! defined $gridmap{"loadtime"}) ||
     ( ($now > ($gridmap{"loadtime"})))) {
    # reload the grid map file
    if (open GRIDIN, "/etc/grid-security/grid-mapfile") {
	while (<GRIDIN>) {
	    $_ =~ /(\".*\") (.*)/;
	    my $dn = $1;
	    my $user = $2;
	    $dn =~ s/\"//g;
	    $gridmap{"$dn"} = $user;
	}
	close GRIDIN;
	$gridmap{"loadtime"} = $now + 10;
    }
}
##############################################################################################################################
my $dn = $ENV{SSL_CLIENT_S_DN};
my $mappeduser = $gridmap{"$dn"};
my $path = $q->param('path');


if ($mappeduser eq "")  {
    print $q->header,
    $q->start_html('Problems'),
    $q->h2(" DN $dn cannot be mapped"),
    $q->strong(7);
    exit 0;
}

(tied %gridmap)->shunlock;


my ($pwName, $pwCode, $pwUid, $pwGid, $pwQuota, $pwComment, 
    $pwGcos, $pwHome, $pwLogprog) = getpwnam($mappeduser);

if ((defined $pwUid) && (getuid() != $pwUid)) {
    setuid($pwUid);
}
else {
    print $q->header (-status=>1),
    $q->start_html('Problems'),
    $q->h2("Cannot change user id to $pwUid"),
    $q->strong(1);
    exit 0;
}


my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,$ctime,$blksize,$blocks) = stat($path);
my $is_directory  =  S_ISDIR($mode);

my $has_access=0;
if ($is_directory) {
    $has_access = POSIX::access($path, &POSIX::R_OK |&POSIX::X_OK); 
} else {
    $has_access = POSIX::access($path, &POSIX::R_OK); 
}

if (defined $has_access) {
} else {
    print $q->header(-status=>405),
    $q->start_html('Problems'),
    $q->h2("Cannot have access to $path"),
    $q->strong(405);
    exit 0;
}

my $redirectionurl = "http://$ENV{'SERVER_ADDR'}:777/$path";

print $q->redirect("$redirectionurl");

#print $q->header,
#    $q->start_html('Done'),
#    $q->h2("Have Access as $pwUid to $path on server $ENV{'SERVER_ADDR'}"),
#    $q->end_html;





