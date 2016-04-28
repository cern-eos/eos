package ApMon::BgMonitor;

use strict;
use warnings;

use ApMon::Common qw(logger);
use ApMon::ProcInfo;
use Data::Dumper;
use Net::Domain;

# Settings for Data::Dumper's dump of last values
$Data::Dumper::Indent = 1;
$Data::Dumper::Purity = 1;

# Background Monitor constructor
sub new {
	my ($type, $cmdPipe, $confFile, $lastValuesFile, $allowBgProcs, $confLoader) = @_;
	my $this = {};
	bless $this;
	$this->{CMD_PIPE} = $cmdPipe;
	$this->{CONF_FILE} = $confFile;
	$this->{LAST_VALUES_FILE} = $lastValuesFile;
	$this->{ALLOW_BG_PROCESSES} = $allowBgProcs;
	$this->{CONFIG_LOADER} = $confLoader;
	$this->{LAST_CONF_CHECK_TIME} = 0;
	$this->{CONF_RECHECK} = 1;
	$this->{CONF_CHECK_INTERVAL} = 20;
	$this->{SEND_BG_MONITORING} = 0;
	
	my $hostname = Net::Domain::hostfqdn();
	$this->{BG_MONITOR_CLUSTER} = "ApMon_SysMon";
	$this->{BG_MONITOR_NODE} = $hostname;
	$this->{JOBS} = {};
	

	$this->{PROC_INFO} = new ApMon::ProcInfo();
	return $this;
}

# This call will never return!
# It should be a used just from a child process whose role is just background monitoring.
# In order to report data, user has to send a bg_enable message to enable this.
sub run {
        my $this = shift;
	my $userMsg = "";
	sleep(1);
        while(1) {
		$userMsg = ApMon::Common::readMessage($this->{CMD_PIPE});
                $this->parseParentMessage($userMsg) if $userMsg; # use $this->{CMD_PIPE} channel to get messages from user
                $this->sendBgMonitoring() if $this->{SEND_BG_MONITORING};
                sleep(10);	# updates sould never be more often than this!
        }
}

# Registers another job for monitoring. This can be called by user or from readMessage.
sub addJobToMonitor {
	my ($this, $pid, $workDir, $clusterName, $nodeName) = @_;

	$this->{JOBS}->{$pid}->{CLUSTER} = $clusterName;
	$this->{JOBS}->{$pid}->{NODE} = $nodeName;
	$this->{PROC_INFO}->addJobToMonitor($pid, $workDir);
}

# Removes a job from the monitored processes. This can be called either by user or by readMessage.
sub removeJobToMonitor {
	my ($this, $pid) = @_;

	delete $this->{JOBS}->{$pid};
	$this->{PROC_INFO}->removeJobToMonitor($pid);
}

# Sets the default cluster and node name for the system-related information.
sub setMonitorClusterNode {
	my ($this, $cluster, $node) = @_;

	$this->{BG_MONITOR_CLUSTER} = $cluster;
	$this->{BG_MONITOR_NODE} = $node;
}

# Enables or disables sending of monitoring info
sub enableBgMonitoring {
	my ($this, $enable) = @_;

	$this->{SEND_BG_MONITORING} = $enable;
}

# Sets the log level for BG_MONITOR
sub setLogLevel {
	my ($this, $level) = @_;
	ApMon::Common::setLogLevel($level);
}

# Sets the maximum rate for the messages sent by user
sub setMaxMsgRate {
        my ($this, $rate) = @_;

	ApMon::Common::setMaxMsgRate($rate);
}

# Sets the SI2k meter for this machine
sub setCpuSI2k {
	my ($this, $si2k) = @_;

	ApMon::Common::setCpuSI2k($si2k);
}

# Sets the cpu speed as the one detected when probing cpu type for si2k
sub setCpuMHz {
	my ($this, $mhz) = @_;

	$ApMon::Common::CpuMHz = $mhz;
}

# This is used only if BgMonitor is used as a dedicated monitoring process in order to interpret
# messages from parent process.
sub parseParentMessage {
	my ($this, $msg) = @_;
	
	my ($pid, $workDir, $cluster, $node);
	my @msgs = split(/\n/, $msg);
	for $msg (@msgs){
		$this->setLogLevel($1) if $msg =~ /loglevel:(.*)/;
		$this->setMaxMsgRate($1) if $msg =~ /maxMsgRate:(.*)/;
		$this->enableBgMonitoring($1) if $msg =~ /bg_enable:(.*)/;
		$this->setCpuSI2k($1) if $msg =~ /cpu_si2k:(.*)/;
		$this->setCpuMHz($1) if $msg =~ /cpu_mhz:(.*)/;
		$pid = $1 if $msg =~ /pid:(.*)/;
		$this->removeJobToMonitor($1) if $msg =~ /rm_pid:(.*)/;
		$workDir = $1 if $msg =~ /work_dir:(.*)/;
		$cluster = $1 if $msg =~ /bg_cluster:(.*)/;
		if($msg =~ /bg_node:(.*)/){
			$node = $1;
			if(defined $pid){
				$this->addJobToMonitor($pid, $workDir, $cluster, $node);
				undef $pid;
				undef $cluster;
			}
			if(defined $cluster){
				$this->setMonitorClusterNode($cluster, $node);
				undef $cluster;
			}
		}
	}
}

# This will send the background information to the interested listeners. It is called either from backgroundMonitor
# or directly by the user from time to time to avoid having a sepparate process for this task.
# information is about the system (load, network, memory etc.) and about a number of jobs (PIDs).
#
# If $mustSend is != 0, the bgMonitoring data is sent regardles of when it was last time sent. This allows
# sending a 'last result', just before the end of a job, and which can happen anytime.
sub sendBgMonitoring {
        my $this = shift;
	my $mustSend = shift || 0;

        ApMon::Common::updateConfig($this);
        my (@crtSysParams, @crtJobParams, $now, @sys_results, @job_results, $optsRef, $prevRawData);
        $now = time;
	my $updatedProcInfo = 0;
        for my $dest (keys %{$this->{DESTINATIONS}}) {
                $optsRef = $this->{DESTINATIONS}->{$dest}->{OPTS};
		$prevRawData = $this->{DESTINATIONS}->{$dest}->{PREV_RAW_DATA};
                @crtSysParams = ();
                @crtJobParams = ();
                # for each destination and its options, check if we have to do any background monitoring
                if($optsRef->{'sys_monitoring'} and ($mustSend or $optsRef->{'sys_data_sent'} + $optsRef->{'sys_interval'} <= $now)){
                        for my $param (keys %$optsRef){
                                if($param =~ /^sys_(.+)/ and $optsRef->{$param}){
                                        push(@crtSysParams, $1) unless ($1 eq 'monitoring') or ($1 eq 'interval') or ($1 eq 'data_sent');
                                }
                        }
                        $optsRef->{'sys_data_sent'} = $now;
                }
                if($optsRef->{'job_monitoring'} and ($mustSend or $optsRef->{'job_data_sent'} + $optsRef->{'job_interval'} <= $now)){
                        for my $param (keys %$optsRef){
                                       if($param =~ /^job_(.+)/ and $optsRef->{$param}){
                                               push(@crtJobParams, "$1") unless ($1 eq 'monitoring') or ($1 eq 'interval') or ($1 eq 'data_sent');
                                       }
                        }
                        $optsRef->{'job_data_sent'} = $now;
                }
                if($optsRef->{'general_info'} and ($mustSend or $optsRef->{'general_data_sent'} + 2 * $optsRef->{'sys_interval'} <= $now)){
                        for my $param (keys %$optsRef){
                                if(!($param =~ /^sys_/) and !($param =~ /^job_/) and ($optsRef->{$param})){
                                        push(@crtSysParams, $param) unless ($param eq 'general_info') or ($param eq 'general_data_sent');
                                }
                        }
			$optsRef->{'general_data_sent'} = $now;
                }
		if((! $updatedProcInfo) and (@crtSysParams > 0 or @crtJobParams > 0)){
			$this->{PROC_INFO}->update();
			$updatedProcInfo = 1;
		}
		
		@sys_results = ( @crtSysParams ? $this->{PROC_INFO}->getSystemData(\@crtSysParams, $prevRawData) : () );
		if(@sys_results){
			ApMon::Common::directSendParameters($dest, $this->{BG_MONITOR_CLUSTER}, $this->{BG_MONITOR_NODE}, -1, \@sys_results);
			$this->{LAST_VALUES}->{BG_MON_VALUES} = {} if ! $this->{LAST_VALUES}->{BG_MON_VALUES};
			$this->update_hash($this->{LAST_VALUES}->{BG_MON_VALUES}, \@sys_results);
		}
		for my $pid (keys %{$this->{JOBS}}){
			@job_results = ( @crtJobParams ? $this->{PROC_INFO}->getJobData($pid, \@crtJobParams) : () );
			if(@job_results){
				ApMon::Common::directSendParameters($dest, $this->{JOBS}->{$pid}->{CLUSTER},$this->{JOBS}->{$pid}->{NODE},-1,\@job_results);
				$this->{LAST_VALUES}->{JOBS}->{$pid}->{BG_MON_VALUES} = {} if ! $this->{LAST_VALUES}->{JOBS}->{$pid}->{BG_MON_VALUES};
				$this->update_hash($this->{LAST_VALUES}->{JOBS}->{$pid}->{BG_MON_VALUES}, \@job_results);
			}
		}
        }
	if(open(F, ">$this->{LAST_VALUES_FILE}")){
		print F Dumper($this->{LAST_VALUES});
		close F;
		chmod(0600, $this->{LAST_VALUES_FILE});
	}else{
		logger("WARNING", "Cannot save last BgMonitored values to $this->{LAST_VALUES_FILE}");
	}
}

# update in the given hash the rest of pa
sub update_hash {
	my $this = shift;
	my $hash = shift || {} ;
	my $params = shift;
	@$params & 1 and logger("WARNING", "Odd number of parameters in update_hash") and return;
	while(@$params){
		my $key = shift(@$params);
		my $val = shift(@$params);
		$hash->{$key} = $val;
	}
}

1;

