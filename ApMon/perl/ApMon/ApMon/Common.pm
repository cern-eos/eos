package ApMon::Common;

use strict;
use warnings;

require Exporter;
use Carp qw(cluck);
use Socket;
use ApMon::XDRUtils;
use Data::Dumper;
use Sys::Hostname;

use vars qw(@ISA @EXPORT @EXPORT_OK $APMON_DEFAULT_PORT $VERSION %defaultOptions $KSI2K);

push @ISA, qw(Exporter);
push @EXPORT, qw(logger);
push @EXPORT_OK, qw($APMON_DEFAULT_PORT %defaultOptions);

$VERSION = "2.2.11";
$APMON_DEFAULT_PORT = 8884;

my @LOG_LEVELS = ("DEBUG", "NOTICE", "INFO", "WARNING", "ERROR", "FATAL");
my $CRT_LOGLEVEL = 2; # index in the array above

my $MAX_MSG_RATE = 20; # Default value for max nr. of messages that user is allowed to send, per second

$KSI2K = undef; #kilo spec ints 2k for this machine

# Default options for background monitoring
%defaultOptions = (
        'job_monitoring' => 1,          # perform (or not) job monitoring
        'job_interval' => 10,           # at this interval (in seconds)
        'job_data_sent' => 0,           # time from Epoch when job information was sent; don't touch!

        'job_cpu_time' => 1,            # processor time spent running this job in seconds
	'job_cpu_ksi2k' => 1,		# used CPU power in ksi2k units (see SpecInt2000 for details);
        'job_run_time' => 1,            # elapsed time from the start of this job in seconds
	'job_run_ksi2k' => 1,		# elapsed time in ksi2k units
        'job_cpu_usage' => 1,           # current percent of the processor used for this job, as reported by ps
        'job_virtualmem' => 1,          # size in JB of the virtual memory occupied by the job, as reported by ps
        'job_rss' => 1,                 # size in KB of the resident image size of the job, as reported by ps
        'job_mem_usage' => 1,           # percent of the memory occupied by the job, as reported by ps
        'job_workdir_size' => 1,        # size in MB of the working directory of the job
        'job_disk_total' => 1,          # size in MB of the total size of the disk partition containing the working directory
        'job_disk_used' => 1,           # size in MB of the used disk partition containing the working directory
        'job_disk_free' => 1,           # size in MB of the free disk partition containing the working directory
        'job_disk_usage' => 1,          # percent of the used disk partition containing the working directory
	'job_open_files' => 1,		# number of open file descriptors


        'sys_monitoring' => 1,          # perform (or not) system monitoring
        'sys_interval' => 60,           # at this interval (in seconds)
        'sys_data_sent' => 0,           # time from Epoch when system information was sent; don't touch!

        'sys_cpu_usr' => 1,             # cpu-usage information
        'sys_cpu_sys' => 1,             # all these will produce coresponding paramas without "sys_"
        'sys_cpu_nice' => 1,
        'sys_cpu_idle' => 1,
	'sys_cpu_iowait' => 1,
        'sys_cpu_usage' => 1,
	'sys_interrupts' => 1,
	'sys_context_switches' => 1,
        'sys_load1' => 1,               # system load information
        'sys_load5' => 1,
        'sys_load15' => 1,
        'sys_mem_used' => 1,            # memory usage information
        'sys_mem_free' => 1,
	'sys_mem_actualfree' => 1,	# actually free memory: free + cached + buffers
        'sys_mem_usage' => 1,
	'sys_mem_buffers' => 1,
	'sys_mem_cached' => 1,
        'sys_blocks_in' => 1,
        'sys_blocks_out' => 1,
        'sys_swap_used' => 1,           # swap usage information
        'sys_swap_free' => 1,
        'sys_swap_usage' => 1,
        'sys_swap_in' => 1,
        'sys_swap_out' => 1,
        'sys_net_in' => 1,              # network transfer in kBps
        'sys_net_out' => 1,             # these will produce params called ethX_in, ethX_out, ethX_errs
        'sys_net_errs' => 1,            # for each eth interface
	'sys_net_sockets' => 1,		# number of opened sockets for each proto => sockets_tcp/udp/unix ...
	'sys_net_tcp_details' => 1,	# number of tcp sockets in each state => sockets_tcp_LISTEN, ...
        'sys_processes' => 1,		# total processes and processs in each state (R, S, D ...)
	'sys_uptime' => 1,		# uptime of the machine, in days (float number)
        'sys_eos_disk_space' => 1,      # total space on a disk server
        'sys_eos_disk_free' => 1,       # free space on a disk server
        'sys_eos_disk_used' => 1,       # used space on a disk server
        'sys_eos_disk_usage' => 1,      # usage in %
	'sys_eos_rpm_version' => 1,     # read rpm version
	'sys_xrootd_rpm_version' => 1,  # read xrootd server version
        'general_info' => 1,            # send (or not) general host information once every 2 $sys_interval seconds
        'general_data_sent' => 0,       # time from Epoch when general information was sent; don't touch!

        'hostname' => 1,
        'ip' => 1,                      # will produce ethX_ip params for each interface
	'kernel_version' => 1,
	'platform' => 1,
	'os_type' => 1,
        'cpu_MHz' => 1,
        'no_CPUs' => 1,                 # number of CPUs
        'total_mem' => 1,
        'total_swap' => 1,
	'cpu_vendor_id' => 1,
	'cpu_family' => 1,
	'cpu_model' => 1,
	'cpu_model_name' => 1,
	'cpu_cache' => 1,
	'bogomips' => 1);


# Create a UDP socket through which all information is sent
if(! socket(SOCKET, PF_INET, SOCK_DGRAM, getprotobyname("udp"))){
        logger("FATAL", "Cannot create UDP socket $@");
	die;
}

# Simple logger
sub logger {
        my ($level, $msg) = @_;
        my $i = 0;
        $i++ while (! ($LOG_LEVELS[$i] eq $level) and ($i < @LOG_LEVELS));
        if($CRT_LOGLEVEL <= $i and $i < @LOG_LEVELS){
		my $now =localtime();
		$now =~ s/^\S+\s((\S+\s+){3}).*$/$1/;
                print $now."ApMon[$LOG_LEVELS[$i]]: $msg\n";
        }
}

# Sets the CRT_LOGLEVEL
sub setLogLevel {
	my $level = shift;
	logger("NOTICE", "Setting loglevel to $level");
	if(! defined $level){
		cluck("got undefined level from");
		return;
	}
	my $i = 0;
	$i++ while (! ($LOG_LEVELS[$i] eq $level) and ($i < @LOG_LEVELS));
	if($i < @LOG_LEVELS){
		$CRT_LOGLEVEL = $i;
	}else{
		logger("WARNING", "Unknown log level \"$level\" - ignoring.\n");
	}
}

# Sets the maximum rate for sending messages (see shouldSend subroutine)
sub setMaxMsgRate {
	my $rate = shift;
	
	$MAX_MSG_RATE = $rate;
	logger("INFO", "Setting maxMsgRate to $rate");
}

# For each destination, we'll keep a pair (instance_id, seq_nr) that will identify us
my $senderRef = {};
my $instance_id = getInstanceID();

# This is used internally to send a set of parameters to a given destination.
sub directSendParameters {
        my ($destination, $clusterName, $nodeName, $time, $paramsRef) = @_;

        my @params;
	if(! defined($paramsRef)){
		logger("WARNING", "Not sending undefined parameters!");
		return;
	}
	if(! defined($time)){
		logger("WARNING", "Not sending the parameters for an undefined time!");
		return;
	}

	if(! shouldSend()){
		#logger("WARNING", "Not sending since the messages are too often!");
		return;
	}
	
        if(ref($paramsRef->[0]) eq "ARRAY"){
                @params = @{$paramsRef->[0]};
        }elsif(ref($paramsRef->[0]) eq "HASH"){
                @params = %{$paramsRef->[0]};
        }else{
                @params = @$paramsRef;
        }

        if(@params == 0){
                return;
        }
	
	$senderRef->{$destination} = {INSTANCE_ID => $instance_id, SEQ_NR => 0} if ! $senderRef->{$destination};
	my $sender = $senderRef->{$destination};
	$sender->{INSTANCE_ID} = ($$ << 16) | ($sender->{INSTANCE_ID} && 0xffff);
	$sender->{SEQ_NR} = ($sender->{SEQ_NR} + 1) % 2_000_000_000; # wrap around 2 mld
	
        my ($host, $port, $pass) = split(/:/, $destination);
        logger("NOTICE", "====> $host|$port|$pass/$clusterName/$nodeName".($time != -1 ? " @ $time" : "")." [$sender->{SEQ_NR} # $sender->{INSTANCE_ID}]");
        for(my $i = 0; $i < @params; $i += 2){
		if(defined($params[$i]) && defined($params[$i+1])){
	                logger("NOTICE", "  ==> $params[$i] = $params[$i+1]");
		}else{
			logger("NOTICE", "  ==> ".(defined($params[$i]) ? $params[$i] : "undef name")." = ".(defined($params[$i+1]) ? $params[$i+1] : "undef value")." <== ignoring pair");
			splice(@params, $i, 2);
			$i-=2;
		}
        }
        my $header = "v:${VERSION}_plp:$pass";
        my $msg = ApMon::XDRUtils::encodeString($header)
		. ApMon::XDRUtils::encodeINT32($sender->{INSTANCE_ID})
		. ApMon::XDRUtils::encodeINT32($sender->{SEQ_NR})
		. ApMon::XDRUtils::encodeParameters($clusterName, $nodeName, $time, @params);
        my $in_addr = inet_aton($host);
        my $in_paddr = sockaddr_in($port, $in_addr);
        if(send(SOCKET, $msg, 0, $in_paddr) != length($msg)){
                logger("ERROR", "Could not send UDP datagram to $host:$port");
        }
}

# This is called by child processes to read messages (if they exist) from the parent.
sub readMessage {
        my $PIPE = shift;

        my ($rin, $win, $ein, $rout, $wout, $eout) = ('', '', '');
        my $retMsg = "";
        vec($rin,fileno($PIPE),1) = 1;
        $ein = $rin | $win;
        my ($nfound,$timeleft) = select($rout=$rin, $wout=$win, $eout=$ein, 0);
        if($nfound){
                sysread($PIPE, $retMsg, 1024);
                logger("DEBUG", "readMessage: $retMsg");
	}
	return $retMsg;
}

# This is called by main process to send a message to a child that reads form the given pipe
sub writeMessage {
	my ($PIPE, $msg) = @_;

	if(defined $PIPE){
		logger("DEBUG", "writeMessage: $msg");
                syswrite($PIPE, $msg);
        }else{
                logger("ERROR", "Trying to send '$msg' to child, but the pipe is not defined!");
        }
}

# copy the time when last data was sent
sub updateLastSentTime {
	my ($srcOpts, $dstOpts) = @_;

	$dstOpts->{'general_data_sent'} = $srcOpts->{'general_data_sent'} if $srcOpts->{'general_data_sent'};
	$dstOpts->{'sys_data_sent'} = $srcOpts->{'sys_data_sent'} if $srcOpts->{'sys_data_sent'};
	$dstOpts->{'job_data_sent'} = $srcOpts->{'job_data_sent'} if $srcOpts->{'job_data_sent'};
}

# This is used to update the configuration for an object that has in it's base hash the following elements
# DESTINATIONS, CONF_RECHECK, LAST_CONF_CHECK_TIME, CONF_CHECK_INTERVAL and CONF_FILE.
# In practice, both ApMon and BgMonitor use it to update their configuration.
sub updateConfig {
        my $this = shift;

	if(! $this->{ALLOW_BG_PROCESSES}){
		$this->{DESTINATIONS} = $this->{CONFIG_LOADER}->{DESTINATIONS};
		return;
	}
        my $now = time;
        if((scalar(keys %{$this->{DESTINATIONS}}) > 0 and $this->{CONF_RECHECK} == 0)
                        or ($this->{LAST_CONF_CHECK_TIME} + $this->{CONF_CHECK_INTERVAL} > $now)){
                return;
        }
        logger("DEBUG", "Updating configuration from $this->{CONF_FILE}");
        if(open(CONF, "<$this->{CONF_FILE}")){
		my $prevDest = $this->{DESTINATIONS} || {};
                $this->{DESTINATIONS} = {};     # clear old destinations first
                my ($crtDest, $line);
                while($line = <CONF>){
                        chomp $line;
                        if($line =~ /^(\S+):(\S+):(\S*)$/){
                                # reading a new destination
                                $crtDest = $line;
                                my %defOpts = %defaultOptions;  #get a copy of the default options
                                $this->{DESTINATIONS}->{$crtDest}->{OPTS} = \%defOpts;
				updateLastSentTime($prevDest->{$crtDest}->{OPTS}, $this->{DESTINATIONS}->{$crtDest}->{OPTS});
				$this->{DESTINATIONS}->{$crtDest}->{PREV_RAW_DATA} = 
					($prevDest->{$crtDest}->{PREV_RAW_DATA} ? $prevDest->{$crtDest}->{PREV_RAW_DATA} : {});
                                logger("DEBUG", "Adding destination $line");
                        }elsif($line =~ /^\s(\S+)=(\S+)/) {
                                # reading an attribute for the current destination and modify the current options
                                my ($name, $value) = ($1, $2);
                                logger("DEBUG", "Adding $name=$value");
				if($name eq 'loglevel'){
					$this->setLogLevel($value);
                                }elsif($name eq 'conf_recheck'){
                                        $this->{CONF_RECHECK} = $value;
                                }elsif($name eq 'recheck_interval'){
                                        $this->{CONF_CHECK_INTERVAL} = $value;
				}elsif($name eq 'maxMsgRate'){
                                        $this->setMaxMsgRate($value);
				}else{
                                        $this->{DESTINATIONS}->{$crtDest}->{OPTS}->{$name} = $value;
                                }
                        }else{
                                logger("WARNING", "Unknown line in conf file: $line");
                        }
                }
                close CONF;
        }else{
                logger("ERROR", "Error opening temporary config file $this->{CONF_FILE}. Current config is unchanged.");
                return;
        }
        $this->{LAST_CONF_CHECK_TIME} = time;
}

# don't allow a user to send more than MAX_MSG messages per second, in average
my $prvTime = 0;
my $prvSent = 0;
my $prvDrop = 0;
my $crtTime = 0;
my $crtSent = 0;
my $crtDrop = 0;
my $hWeight = 0.92;

# Decide if the current datagram should be sent.
# This decision is based on the number of messages previously sent.
sub shouldSend {
	my $now = time;

	if($now != $crtTime){
		# new time
		# update previous counters;
		$prvSent = $hWeight * $prvSent + (1 - $hWeight) * $crtSent / ($now - $crtTime); 
		$prvTime = $crtTime;
		logger("DEBUG", "previously sent: $crtSent; dropped: $crtDrop");
		# reset current counter
		$crtTime = $now;
		$crtSent = 0;
		$crtDrop = 0;
	}
	my $valSent = $prvSent * $hWeight + $crtSent * (1 - $hWeight); # compute the history
	
	my $doSend = 1;
	my $level = $MAX_MSG_RATE - $MAX_MSG_RATE / 10; # when we should start dropping messages
	
	if($valSent > $MAX_MSG_RATE - $level){
		$doSend = rand($MAX_MSG_RATE / 10) < ($MAX_MSG_RATE - $valSent);
	}
	
	# counting sent and dropped messages
	if($doSend){
		$crtSent++;
	}else{
		$crtDrop++;
	}
	
	return $doSend;
}

# Try to generate a more random instance id. It takes the process ID and
# combines it with the last digit from the IP addess and a random number
sub getInstanceID {
	my $pid = $$;
	my $ip = int(rand(256));  # last digit of the ip address
	my $host = hostname();    # from Sys::Hostname
	if($host){
		my $addr = inet_ntoa(scalar gethostbyname($host));
		$ip = $1 if $addr =~ /(\d+)$/;
	}
	my $rnd = int(rand(256));
	my $iid = ($pid << 16) | ($ip << 8) | $rnd; # from all this, generate the instance id
	return $iid;
}

# Try to determine the CPU type. Returns a hash with: cpu_model_name, cpu_MHz, cpu_cache (in KB)
# TODO: make this work also for Mac.
sub getCpuType {
	my $cpu_type = {};
	if(open(CPU_INFO, "</proc/cpuinfo")){
		my $line;
		while($line = <CPU_INFO>){
			$cpu_type->{"cpu_MHz"} = $1 if($line =~ /cpu MHz\s+:\s+(\d+\.?\d*)/);
			$cpu_type->{"cpu_model_name"} = $1 if($line =~ /model name\s+:\s+(.+)/ || $line =~ /family\s+:\s+(.+)/);
			$cpu_type->{"cpu_cache"} = $1 if($line =~ /cache size\s+:\s+(\d+)/);
		}
		close(CPU_INFO);
	}else{
		logger("NOTICE", "Cannot open /proc/cpuinfo");
	}
	if(-r "/proc/pal/cpu0/cache_info"){
		if(open(CACHE_INFO, "</proc/pal/cpu0/cache_info")){
			my $line;
			my $level3params = 0;
			while($line = <CACHE_INFO>){
				$level3params = 1 if($line =~/Cache level 3/);
				$cpu_type->{"cpu_cache"} = $1 / 1 if ($level3params && $line =~ /Size\s+:\s+(\d+)/);
			}
			close(CACHE_INFO);
		}else{
			logger("NOTICE", "Cannot open /proc/pal/cpu0/cache_info");
		}
	}
	if(! scalar(keys(%$cpu_type))){
		logger("NOTICE", "Cannot get cpu type");
		return undef;
	}
	return $cpu_type;
}

# Set the SI2K performance meter for this machine. If this function is called then parameter
# cpu_ksi2k will also be reported for the job monitoring with a value computed this way:
# cpu_ksi2k(job) = cpu_time(job) * ( si2k / 1000)
sub setCpuSI2k {
	my $si2k = shift;
	$KSI2K = $si2k / 1000.0 if($si2k);
}

1;

