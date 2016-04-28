=head1 NAME

ApMon - Perl extension for sending application information to MonALISA services.

=head1 SYNOPSIS

  use ApMon;
  # initialize from a URL or from a file
  my $apm = new ApMon::ApMon("http://some.host.com/destinations.conf");
  $apm->sendParameters("Cluster", "Node", "param1", 14.23e-10, "param2", 234);

  # initalize with default xApMon configuration, and send datagrams directly
  # to the given host.
  my $apm = ApMon::ApMon->new(["pcardaab.cern.ch:8884", "localhost"]);
  $apm->sendParameters("Cluster", "Node", {"x" => 12, "y" => 0.3});

  # given xApMon configuration will overwrite the default values.
  my $apm = ApMon::ApMon->new({
	"pcardaab.cern.ch:8884" => 
	    {"sys_monitoring" => 0, "job_monitoring" => 1, "general_info" => 1}, 
	"lcfg.rogrid.pub.ro passwd" => 
	    {"sys_monitoring" => 1, "general_info" => 0}
        });
  $apm->sendParameters("Cluster", "Node", ["name", "some_name", "value", 23]); 

=head1 DESCRIPTION

ApMon is an API that can be used by any application to send monitoring
information to MonALISA services (http://monalisa.cacr.caltech.edu). The
monitoring data is sent as UDP datagrams to one or more hosts running MonALISA.
The MonALISA host may require a password enclosed in each datagram, for
authentication purposes. ApMon can also send datagrams that contain monitoring
information regarding the system or the application.

=head1 METHODS

=over

=cut

package ApMon;

use strict;
use warnings;

use ApMon::Common qw(logger);
use ApMon::ConfigLoader;
use ApMon::BgMonitor;
use IO::Handle;
use POSIX ":sys_wait_h";
use Net::Domain;
use Data::Dumper;

# Here it is kept a list of child processes that have to be killed before finishing.
my @children = (); 

# Temporary files path
my $TMPDIR = (defined $ENV{'TMPDIR'}) ? $ENV{'TMPDIR'} : '/tmp';

=item $apm = new ApMon(@destLocations);

This is the constructor for the ApMon class. It can be used with several types of
arguments: a list of strings (URLs and/or files) - the configuration will be 
read from all; a reference to an ARRAY - each element is a destination ML 
service; for each destination the default options will be used; a reference 
to a HASH - each key is a destination ML service; for each destination you can
define a set of additional options that will overwrite the default ones. You
can also leave it empty and initialize ApMon later using the
$apm->setDestinations() method. This will create the two background processes
(for bg monitoring and configuration update). If you don't want these two
processes to be created ever, you can pass the value 0 as single argument.

=cut

sub new {
	my ($type, @destLocations) = @_;
	
        my $this = {};
	bless $this;
	$this->{CONF_FILE} = "$TMPDIR/confApMon.$$"; # temporary name used to transfer config data from refreshConfig process to the others
	$this->{LAST_VALUES_FILE} = "$TMPDIR/valuesApMon.$$"; #temporary name used to transfer last monitored data from BgMonitor to the main process
	$this->{LAST_CONF_CHECK_TIME} = 0;	# moment when config was checked last time in sec from Epoch
	$this->{CONF_RECHECK} = 1;		# do check if configuration has changed
	$this->{CONF_CHECK_INTERVAL} = 20;	# default interval to check for changes in config files
	$this->{DESTINATIONS} = {};

	my $hostname = Net::Domain::hostfqdn(); 
	$this->{DEFAULT_CLUSTER} = "ApMon_UserSend";
	$this->{DEFAULT_NODE} = $hostname;

	# decide if we will ever have bg processes
	if( @destLocations && ref($destLocations[0]) eq "" && $destLocations[0] eq "0" ){
		$this->{ALLOW_BG_PROCESSES} = 0;
		@destLocations = ();
	}else{
		$this->{ALLOW_BG_PROCESSES} = 1;
	}

	pipe($this->{UPD_RDR}, $this->{UPD_WTR}); # open a pipe to send messages to Config Loader
	$this->{UPD_WTR}->autoflush(1);
	$this->{CONFIG_LOADER} = new ApMon::ConfigLoader($this->{UPD_RDR}, $this->{CONF_FILE});
	
	pipe($this->{BG_RDR}, $this->{BG_WTR}); # open a pipe to send messages to Background Monitor
	$this->{BG_WTR}->autoflush(1);
	$this->{BG_MONITOR} = new ApMon::BgMonitor($this->{BG_RDR}, $this->{CONF_FILE}, $this->{LAST_VALUES_FILE}, $this->{ALLOW_BG_PROCESSES}, $this->{CONFIG_LOADER});
	
	# if the configuration is given in the constructor, load it now
	$this->setDestinations(@destLocations) if @destLocations;
	$SIG{INT} = \&catch_zap;
	$SIG{TERM} = \&catch_zap;
	return $this;
}

=item $apm->setDestinations(@destLocations);

Accept the same parameters as the ApMon constructor

=cut

sub setDestinations {
	my ($this, @destLocations) = @_;
	
	$this->startBgProcesses();
	#logger("INFO", "\$destLocations[0]= .$destLocations[0]. ref = .".ref($destLocations[0]).".");
	if((ref($destLocations[0]) eq "ARRAY") or (ref($destLocations[0]) eq "HASH")) {
		# prevent background Config Loader to change this
		#logger("INFO", "Config is HASH or ARRAY");
		ApMon::Common::writeMessage($this->{UPD_WTR}, "conf_recheck:0\n") if @children;
	}else{
		#logger("INFO", "Config is string = .@destLocations.");
		my $msg = "conf_recheck:1\n";
		for my $dest (@destLocations) {
			$msg .= "dest:$dest\n";
		}
		# send this to background Config Loader for later updates
		ApMon::Common::writeMessage($this->{UPD_WTR}, $msg) if @children; 
	}
	# perform the change now, regardless of the existence of background Config Loader
	$this->{CONFIG_LOADER}->setDestinations(@destLocations);
	$this->enableBgMonitoring(1);
}

=item $apm->addJobToMonitor($pid, $workDir, $clusterName, $nodeName);

Add another job to be monitored. A job is a tree of processes, starting from 
the given PID that has files in workDir directory. If workDir in "", no disk 
measurements will be performed. All produced parameters will be sent to all 
interested destinations using the given cluster and node names.

=cut

sub addJobToMonitor {
	my ($this, $pid, $workDir, $clusterName, $nodeName) = @_;

	ApMon::Common::writeMessage($this->{BG_WTR}, "pid:$pid\nwork_dir:$workDir\nbg_cluster:$clusterName\nbg_node:$nodeName\n") if @children;
	# also set this to the local copy of the BG_MONITOR in case that user decides to stop background processes
	$this->{BG_MONITOR}->addJobToMonitor($pid, $workDir, $clusterName, $nodeName);
}

=item $apm->removeJobToMonitor($pid);

Remove a tree of processes, starting with PID from being monitored.

=cut

sub removeJobToMonitor {
	my ($this, $pid) = @_;

	ApMon::Common::writeMessage($this->{BG_WTR}, "rm_pid:$pid\n") if @children;
	# also set this to the local copy of the BG_MONITOR in case that user decides to stop background processes
	$this->{BG_MONITOR}->removeJobToMonitor($pid);
}

=item $apm->setMonitorClusterNode($clusterName, $nodeName);

This is used to set the cluster and node name for the system-related monitored
data.

=cut

sub setMonitorClusterNode {
	my ($this, $clusterName, $nodeName) = @_;

	ApMon::Common::writeMessage($this->{BG_WTR}, "bg_cluster:$clusterName\nbg_node:$nodeName\n") if @children;
	# also set this to the local copy of the BG_MONITOR in case that user decides to stop background processes
	$this->{BG_MONITOR}->setMonitorClusterNode($clusterName, $nodeName);
}

=item $apm->setConfRecheck($onOff [, $interval]);

Call this function in order to enable or disable the configuration recheck.
If you enable it, you may want to pass a second parameter, that specifies the 
number of seconds between two configuration rechecks. Note that it makes sense
to use configuration recheck only if you get the configuration from (a set of) 
files and/or URLs.

=cut

sub setConfRecheck {
	my $this = shift;
	my $onOff = shift;
	my $interval = shift || 120;

	$this->{CONF_RECHECK} = $onOff;
	$this->{CONF_CHECK_INTERVAL} = $interval;
	ApMon::Common::writeMessage($this->{UPD_WTR}, "conf_recheck:$onOff\nrecheck_interval:$interval\n") if @children;
}

=item $apm->sendParams(@params);

Use this to send a set of parameters without specifying a cluster and a node 
name. In this case, the default values for cluster and node name will be used. 
See the sendParameters function for more details.

=cut

sub sendParams {
	my ($this, @params) = @_;

	$this->sendTimedParams(-1, @params);
}

=item $apm->sendParameters($clusterName, $nodeName, @params);

Use this to send a set of parameters to all given destinations.
The default cluster an node names will be updated with the values given here.
If afterwards you want to send more parameters, you can use the shorter version
of this function, sendParams. The parameters to be sent can be eiter a list, or
a reference to a list. This list should have an even length and should contain 
pairs like (paramName, paramValue). paramValue can be a string, an int or a float.

=cut

sub sendParameters {
	my ($this, $clusterName, $nodeName, @params) = @_;

	$this->sendTimedParameters($clusterName, $nodeName, -1, @params);
}

=item $apm->sendTimedParams($time, @params);

This is the short version of the sendTimedParameters that uses the default
cluster and node name to sent the parameters and allows you to specify a time 
(in seconds from Epoch) for each packet.

=cut

sub sendTimedParams {
	my ($this, $time, @params) = @_;

	$this->sendTimedParameters($this->{DEFAULT_CLUSTER}, $this->{DEFAULT_NODE}, $time, @params);
}

=item $apm->sendTimedParameters($clusterName, $nodeName, $time, @params);

Use this instead of sendParameters to set the time for each packet that is sent.
The time is in seconds from Epoch. If you use the other function, the time for 
these parameters will be sent by the MonALISA serice that receives them.

=cut

sub sendTimedParameters {
	my ($this, $clusterName, $nodeName, $time, @params) = @_;

	ApMon::Common::updateConfig($this);
	if((! defined($clusterName)) || (! defined($nodeName))){
		logger("WARNING", "ClusterName or NodeName are undefined. Not sending given parameters!");
		return;
	}
	$this->{DEFAULT_CLUSTER} = $clusterName;
	$this->{DEFAULT_NODE} = $nodeName;
	if(scalar (keys %{$this->{DESTINATIONS}})){
		for my $dest (keys %{$this->{DESTINATIONS}}){
			ApMon::Common::directSendParameters($dest, $clusterName, $nodeName, $time, \@params);
		}
	}else{
		logger("WARNING", "No destinations defined for sending parameters");
	}
}

=item $apm->sendBgMonitoring();

Send NOW the background monitoring information to the interested destinations. 
Note that this uses the current process and not the background one. So, if you 
stop the background processes you can still use this call to send the 
background information (both about system and jobs) whenever you want. If $mustSend is != 0, 
the bgMonitoring data is sent regardles of when it was last time sent. This allows
sending a 'last result', just before the end of a job, and which can happen anytime.

=cut

sub sendBgMonitoring {
	my $this = shift;
	my $mustSend = shift || 0;
	$this->{BG_MONITOR}->sendBgMonitoring($mustSend);
}

=item $apm->getSysMonInfo('param_name1', 'param_name2', ...);

IF and ONLY IF sendBgMonitoring() was called before, either called by user or by the BgMonitoring process,
the last system monitored values for the requested parameters will be returned. Note that the requested 
parameters must be among the monitored ones. If there is no avaialbe parameter among the requested ones, 
it returns undef.

=cut

sub getSysMonInfo {
	my $this = shift;
	
	$this->update_last_values();
	return $this->filter_params($this->{LAST_VALUES}->{BG_MON_VALUES}, @_);
}

=item $apm->getJobMonInfo($pid, 'param_name1', 'param_name2', ...);

IF and ONLY IF sendBgMonitoring() was called before, either called by user or by the BgMonitoring process,
the last job monitored values for the given PID will be returned. Note that the requested parameters 
must be among the monitored ones. If there is no avaialbe parameter among the requested ones, 
it returns undef.

=cut

sub getJobMonInfo {
	my $this = shift;
	my $pid = shift;
	
	$this->update_last_values();
	return $this->filter_params($this->{LAST_VALUES}->{JOBS}->{$pid}->{BG_MON_VALUES}, @_);
}

=item $apm->enableBgMonitoring($onOff);

This allows enabling and disabling of the background monitoring. Note that this
doesn't stop the background monitor process; Note also that this is called by 
default by setDestinations () to enable the background monitoring once the 
destination is set. It doesn't make sense to call this if you have stopped 
the background processes.

=cut

sub enableBgMonitoring {
	my ($this, $onOff) = @_;

	ApMon::Common::writeMessage($this->{BG_WTR}, "bg_enable:$onOff\n") if @children;
}

=item $apm->refreshConfig();

Call this function to force refreshing the temporary config file and make sure
that at the next send, the new configuration will be used. Note that it makes 
sense to use this only if you load the configuration from (a set of) files 
and/or URLs. Also note that fetching the configuration files from an URL might
take some time, depending on network conditions.

=cut

sub refreshConfig {
	my $this = shift;
	$this->{LAST_CONF_CHECK_TIME} = 0;
	$this->{CONFIG_LOADER}->refreshConfig();
}

=item $apm->startBgProcesses();

This can be called in order to start the background processes (conf loader 
and bg monitor). It is called by default if configuration is read from a
file or from a URL (not when you give a hash or an array for destinations).

=cut

sub startBgProcesses {
	my $this = shift;

	if(! $this->{ALLOW_BG_PROCESSES}){
		logger("DEBUG", "Not starting bg processes since they are not allowed.");
		return;
	}

	if(@children){
		logger("INFO", "Bg processes already started!");
		return;
	}
	logger("INFO", "starting bg processes");
	my $pid;
	# start the Config Loader process and retrieve the config periodically
	$pid = fork();
	if(! defined $pid){
		logger("FATAL", "cannot fork: $!"); die;
	}
	if ($pid == 0) {
		# child
		$this->{CONFIG_LOADER}->run();
		exit(0);
	}
	# parent
	push(@children, $pid);
	undef $pid;
	# start the Background Monitoring process
	$pid = fork();
	if(! defined $pid){
		logger("FATAL", "cannot fork: $!"); die;
	}
	if($pid == 0) {
		# child
		$this->{BG_MONITOR}->run();
		exit(0);
	}
	# parent
	push(@children, $pid);
}

=item $apm->stopBgProcesses();

This can be called to stop all child processes

=cut

sub stopBgProcesses {
	my $this = shift;
	for my $pid (@children) {
		kill 1, $pid;
		waitpid($pid, 0);
	}
	@children = ();
}

=item $apm->setLogLevel($level);

This sets the logging level for all ApMon components.
$level can be one of: "DEBUG", "NOTICE", "INFO", "WARNING", "ERROR", "FATAL".
You can also set the log level from the configuration file by specifying
xApMon_loglevel = one of the above (without quotes).

=cut

sub setLogLevel {
	my ($this, $level) = @_;
	ApMon::Common::setLogLevel($level);
	
	ApMon::Common::writeMessage($this->{UPD_WTR}, "loglevel:$level\n") if @children;
	$this->{CONFIG_LOADER}->setLogLevel($level);
	
	ApMon::Common::writeMessage($this->{BG_WTR}, "loglevel:$level\n") if @children;
	$this->{BG_MONITOR}->setLogLevel($level);
}

=item $apm->setMaxMsgRate($rate);

This sets the maxim number of messages that can be sent to a MonALISA service, per second.
By default, it is 50. This is a very large number, and the idea is to prevent errors from
the user. One can easily put in a for loop, without any sleep, some sendParams calls that
can generate a lot of unnecessary network load.

=cut

sub setMaxMsgRate {
	my ($this, $rate) = @_;

	ApMon::Common::setMaxMsgRate($rate);
	
	ApMon::Common::writeMessage($this->{UPD_WTR}, "maxMsgRate:$rate\n") if @children;
	$this->{CONFIG_LOADER}->setMaxMsgRate($rate);

	ApMon::Common::writeMessage($this->{BG_WTR}, "maxMsgRate:$rate\n") if @children;
	$this->{BG_MONITOR}->setMaxMsgRate($rate);
}

=item $apm->getCpuType();

This returns a hash with the cpu type: cpu_model_name, cpu_MHz, cpu_cache (in KB). This call
is meant to be used together with setCpuSI2k, to establish a SpecInt performance meter.
If it cannot get the cpu type, it returns undef

=cut

sub getCpuType {
	my $this = shift;
	my $cpuType = ApMon::Common::getCpuType();
	ApMon::Common::writeMessage($this->{BG_WTR}, "cpu_mhz:$ApMon::Common::CpuMHz\n") if(@children && $ApMon::Common::CpuMHz);
	return $cpuType;
}

=item $apm->setCpuSI2k(si2k);

This sets the SpecINT2000 meter for the current machine. Consequently, jobs will also report
cpu_ksi2k, based on this value and cpu_time.

=cut

sub setCpuSI2k {
	my ($this, $si2k) = @_;
	ApMon::Common::setCpuSI2k($si2k);
	ApMon::Common::writeMessage($this->{BG_WTR}, "cpu_si2k:$si2k\n") if @children;
}

=item $apm->free();

This function stops the background processes and removes the temporary file. After this 
call, the ApMon object must be recreated in order to be used. It is provided for exceptional 
cases when you have to recreate over and over again the ApMon object; you have to free it 
when you don't need anymore.

=cut

sub free {
	my $this = shift;
	$this->stopBgProcesses();
	#close(ApMon::Common::SOCKET);
	unlink("$TMPDIR/confApMon.$$");
	unlink("$TMPDIR/valuesApMon.$$");
}

##################################################################################################
# The following is internal stuff.

# This is called if uses presses CTRL+C or kill is sent to me
sub catch_zap {
	logger("DEBUG", "Killed! Removing temp files $TMPDIR/{conf,values}ApMon.$$") if defined &logger;
	unlink("$TMPDIR/confApMon.$$");
	unlink("$TMPDIR/valuesApMon.$$");
	stopBgProcesses("dummy");
	exit(0);
}

# from the given hash, based on the givn list of parameters, build a hash will all available
# if the resulting list is empty, return undef.
sub filter_params {
	my $this = shift;
	my $h_src = shift || {};
	my $h_res = {};
	for my $key (@_){
		$h_res->{$key} = $h_src->{$key} if defined($h_src->{$key});
	}
	return (scalar(keys(%$h_res)) == 0 ? undef : $h_res);
}

# Update the last bg monitoring values hash with the contents of the LAST_VALUES_FILE.
# Note that this is produced only after sendBgMonitoring was run, either from the main
# process or the BgMonitor process.
sub update_last_values {
	my $this = shift;
	
	my $now = time;
	return if $this->{LAST_VALUES_TIME} && ($now - $this->{LAST_VALUES_TIME} < 2);
	if(open(F, "<$this->{LAST_VALUES_FILE}")){
		my @lines = <F>;
		close F;
		my $VAR1;
		$this->{LAST_VALUES} = eval join("", @lines);
		logger("ERROR", "Error restoring the last bg monitoring values from file $this->{LAST_VALUES_FILE}:\n$@") if $@;
		$this->{LAST_VALUES_TIME} = $now;
	}else{
		logger("WARNING", "Cannot read the last bg monitoring values from $this->{LAST_VALUES_FILE}");
	}
}

END {
    unlink("$TMPDIR/confApMon.$$");
    unlink("$TMPDIR/valuesApMon.$$");
    stopBgProcesses("dummy");
}

1;

__END__

=back

=head1 AUTHOR

  Catalin Cirstoiu <Catalin.Cirstoiu@cern.ch>

=head1 COPYRIGHT AND LICENSE

This module is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
either expressed or implied. This library is free software; you can
redistribute or modify it under the same terms as Perl itself.

=cut
