package ApMon::ProcInfo;

use strict;
use warnings;

use ApMon::Common qw(logger);
use Data::Dumper;
use Net::Domain;
use Time::Local;

# See the end of this file for a set of interesting methods for other modules.

# ProcInfo constructor
sub new {
	my $this = {};
	$this->{DATA} = {};		# monitored data that is going to be reported
	$this->{JOBS} = {};		# jobs that will be monitored 
	# names of the months for ps start time of a process
	$this->{MONTHS} = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
	bless $this;
	$this->readGenericInfo();
	return $this;
}

# this has to be run twice (with the $lastUpdateTime updated) to get some useful results
sub readStat {
	my $this = shift;
	
	if(open(STAT, "</proc/stat")){
		my $line;
		while($line = <STAT>){
			if($line =~ /^cpu\s/) {
				(undef, $this->{DATA}->{"raw_cpu_usr"}, $this->{DATA}->{"raw_cpu_nice"}, 
				        $this->{DATA}->{"raw_cpu_sys"}, $this->{DATA}->{"raw_cpu_idle"},
					$this->{DATA}->{"raw_cpu_iowait"}) = split(/ +/, $line);
			}
			if($line =~ /^page/) { # this doesn't work for kernel >2.5
				(undef, $this->{DATA}->{"raw_blocks_in"}, $this->{DATA}->{"raw_blocks_out"}) = split(/ +/, $line);
			}
			if($line =~/^swap/) { # this also doesn't work in >2.5
				(undef, $this->{DATA}->{"raw_swap_in"}, $this->{DATA}->{"raw_swap_out"}) = split(/ +/, $line);
			}
			$this->{DATA}->{"raw_interrupts"} = $1 if($line =~ /^intr\s(\d+)/);
			$this->{DATA}->{"raw_context_switches"} = $1 if($line =~ /^ctxt\s(\d+)/);
		}
		close STAT;
	}else{
		logger("NOTICE", "ProcInfo: cannot open /proc/stat");
		#disable ..?
	}
	# blocks_in/out and swap_in/out are moved to /proc/vmstat in >2.5 kernels
	if(-r "/proc/vmstat"){
		if(open(VMSTAT, "</proc/vmstat")){
			my $line;
			while($line = <VMSTAT>){
				$this->{DATA}->{"raw_blocks_in"} = $1 if($line =~ /^pgpgin\s(\d+)/);
				$this->{DATA}->{"raw_blocks_out"}= $1 if($line =~ /^pgpgout\s(\d+)/);
				$this->{DATA}->{"raw_swap_in"}  = $1 if($line =~ /^pswpin\s(\d+)/);
				$this->{DATA}->{"raw_swap_out"} = $1 if($line =~ /^pswpout\s(\d+)/);
			}
			close VMSTAT;
		}else{
			logger("NOTICE", "Procinfo: cannot open /proc/vmstat");
		}
	}
}

# sizes are reported in MB (except _usage that is in percent).
sub readMemInfo {
	my $this = shift;
	
	if(open(MEM_INFO, "</proc/meminfo")){
		my $line;
		while($line = <MEM_INFO>){
			if($line =~ /^MemFree:/){
				my (undef, $mem_free) = split(/ +/, $line);
				$this->{DATA}->{"mem_free"} = $mem_free / 1024.0;
			}
			if($line =~ /^MemTotal:/){
				my (undef, $mem_total) = split(/ +/, $line);
				$this->{DATA}->{"total_mem"} = $mem_total / 1024.0;
			}
			if($line =~ /^SwapFree:/){
				my (undef, $swap_free) = split(/ +/, $line);
				$this->{DATA}->{"swap_free"} = $swap_free / 1024.0;
			}
			if($line =~ /^SwapTotal:/){
				my (undef, $swap_total) = split(/ +/, $line);
				$this->{DATA}->{"total_swap"} = $swap_total / 1024.0;
			}
			if($line =~ /^Buffers:/){
				my (undef, $buffers) = split(/ +/, $line);
				$this->{DATA}->{"mem_buffers"} = $buffers / 1024.0;
			}
			if($line =~ /^Cached:/){
				my (undef, $cached) = split(/ +/, $line);
				$this->{DATA}->{"mem_cached"} = $cached / 1024.0;
			}
		}
		close MEM_INFO;
		$this->{DATA}->{"mem_actualfree"} = $this->{DATA}->{"mem_free"} + $this->{DATA}->{"mem_buffers"} + $this->{DATA}->{"mem_cached"}
			if ($this->{DATA}->{"mem_free"} && $this->{DATA}->{"mem_buffers"} && $this->{DATA}->{"mem_cached"});
		$this->{DATA}->{"mem_used"} = $this->{DATA}->{"total_mem"} - $this->{DATA}->{"mem_actualfree"} 
			if ($this->{DATA}->{"total_mem"} && $this->{DATA}->{"mem_actualfree"});
		$this->{DATA}->{"swap_used"} = $this->{DATA}->{"total_swap"} - $this->{DATA}->{"swap_free"} if $this->{DATA}->{"total_swap"};
		$this->{DATA}->{"mem_usage"} = 100.0 * $this->{DATA}->{"mem_used"} / $this->{DATA}->{"total_mem"} 
			if ($this->{DATA}->{"total_mem"} && $this->{DATA}->{"mem_used"});
		$this->{DATA}->{"swap_usage"} = 100.0 * $this->{DATA}->{"swap_used"} / $this->{DATA}->{"total_swap"} if $this->{DATA}->{"total_swap"};
	}else{
		logger("NOTICE", "ProcInfo: cannot open /proc/meminfo");
	}
}

# read the number of processes currently running on the system
# count also the number of runnable, sleeping, zombie, io blocked and traced processes
# works on Darwin
sub countProcesses {
	my $this = shift;
	
	my $total = 0;
	my %states = ('D' => 0, 'R' => 0, 'S' => 0, 'T' => 0, 'Z' => 0);
	if(open(PROC, "ps -A -o state |")){
		my $state = <PROC>;      # ignore the first line - it's the header
		while(<PROC>){
			$state = substr($_, 0, 1);
			$states{$state}++;
			$total++;
		}
		$this->{DATA}->{"processes"} = $total;
		for $state (keys %states){
			next if (($state eq '') || ($state =~ /\s+/));
			$this->{DATA}->{"processes_$state"} = $states{$state};
		}
		close PROC;
	}else{
		logger("NOTICE", "ProcInfo: cannot count the processes using ps.");
	}
}

# reads the IP, hostname, cpu_MHz, kernel_version, os_version, platform
sub readGenericInfo {
	my $this = shift;
	
	my $hostname = Net::Domain::hostfqdn();

	$this->{DATA}->{"hostname"} = $hostname; 
	
	if(open(IF_CFG, "/sbin/ifconfig -a |")){
		my ($eth, $ip, $line);
		while($line = <IF_CFG>){
			if($line =~ /^(eth\d)\s+/){
				$eth = $1;
				undef $ip;
			}
			if(defined($eth) and ($line =~ /\s+inet addr:(\d+\.\d+\.\d+\.\d+)/)){
				$ip = $1;
				$this->{DATA}->{$eth."_ip"} = $ip;
				undef $eth;
			}
		}
		close IF_CFG;
	}else{
		logger("NOTICE", "ProcInfo: couldn't get output from /sbin/ifconfig -a");
	}
	if(open(CPU_INFO, "</proc/cpuinfo")){
		my ($line, $no_cpus);
		while($line = <CPU_INFO>){
			if($line =~ /cpu MHz\s+:\s+(\d+\.?\d*)/){
				$this->{DATA}->{"cpu_MHz"} = $1;
				$no_cpus ++;
			}
			
			if($line =~ /vendor_id\s+:\s+(.+)/ || $line =~ /vendor\s+:\s+(.+)/){
				$this->{DATA}->{"cpu_vendor_id"} = $1;
			}
			
			if($line =~ /cpu family\s+:\s+(.+)/ || $line =~ /revision\s+:\s+(.+)/){
				$this->{DATA}->{"cpu_family"} = $1;
			}

			if($line =~ /model\s+:\s+(.+)/) {
				$this->{DATA}->{"cpu_model"} = $1;
			}

			if($line =~ /model name\s+:\s+(.+)/ || $line =~ /family\s+:\s+(.+)/){
				$this->{DATA}->{"cpu_model_name"} = $1;
			}

			if($line =~ /bogomips\s+:\s+(\d+\.?\d*)/ || $line =~ /BogoMIPS\s+:\s+(\d+\.?\d*)/){
				$this->{DATA}->{"bogomips"} = $1;
			}

			if($line =~ /cache size\s+:\s+(\d+)/){
				$this->{DATA}->{"cpu_cache"} = $1;
			}
		}
		close CPU_INFO;
		$this->{DATA}->{"no_CPUs"} = $no_cpus;
	}else{
		logger("NOTICE", "ProcInfo: cannot open /proc/cpuinfo");
	}
	if(-r "/proc/pal/cpu0/cache_info"){
		if(open(CACHE_INFO, "</proc/pal/cpu0/cache_info")){
			my $line;
			my $level3params = 0;
			while($line = <CACHE_INFO>){
				$level3params = 1 if ($line =~ /Cache level 3/);
				$this->{DATA}->{"cpu_cache"} = $1 / 1024 if ($level3params && $line =~ /Size\s+:\s+(\d+)/);
			}
			close(CACHE_INFO);
		}else{
			logger("NOTICE", "ProcInfo: cannot open /proc/pal/cpu0/cache_info");
		}
	}
	# determine the kernel version
	my $line = `uname -r`;
	chomp $line;
	$this->{DATA}->{"kernel_version"} = $line;
	
	# determine the platform
	$line = `uname -m 2>/dev/null || uname`;
	chomp $line;
	$this->{DATA}->{"platform"} = $line;
	
	# try to determine the OS type
	my $osType = "";
	if(open(LSB_RELEASE, 'env PATH=$PATH:/bin:/usr/bin lsb_release -d 2>/dev/null |')){
		my $line = <LSB_RELEASE>;
		$osType = $1 if ($line && $line =~ /Description:\s*(.*)/);
		close LSB_RELEASE;
	}
	if(! $osType){
		for my $f ("/etc/redhat-release", "/etc/debian_version", "/etc/SuSE-release", 
			   "/etc/slackware-version", "/etc/gentoo-release", "/etc/mandrake-release", 
			   "/etc/mandriva-release", "/etc/issue"){
			if(open(VERF, "$f")){
				$osType = <VERF>;
				chomp $osType;
				close VERF;
				last;
			}
		}
	}
	if(! $osType){
		$osType = `uname -s`;
		chomp $osType;
	}
	$this->{DATA}->{"os_type"} = $osType;
}

# read system's uptime and load average. Time is reported as a floating number, in days.
# It uses the 'uptime' command which's output looks like these:
# 19:55:37 up 11 days, 18:57,  1 user,  load average: 0.00, 0.00, 0.00
# 18:42:31 up 87 days, 18:10,  9 users,  load average: 0.64, 0.84, 0.80
# 6:42pm  up 7 days  3:08,  7 users,  load average: 0.18, 0.14, 0.10
# 6:42pm  up 33 day(s),  1:54,  1 user,  load average: 0.01, 0.00, 0.00
# 18:42  up 7 days,  3:45, 2 users, load averages: 1.10 1.11 1.06
# 18:47:41  up 7 days,  4:35, 19 users,  load average: 0.66, 0.44, 0.41
# 15:10  up 8 days, 12 mins, 2 users, load averages: 1.46 1.27 1.18
# 11:57am  up   2:21,  22 users,  load average: 0.59, 0.93, 0.73
sub readUptimeAndLoadAvg {
	my $this = shift;

	my $line = `uptime`;
	chomp $line;
	if($line =~ /up\s+((\d+)\s+day[ (s),]+)?(\d+)(:(\d+))?[^\d]+(\d+)[^\d]+([\d\.]+)[^\d]+([\d\.]+)[^\d]+([\d\.]+)/){
		my ($days, $hour, $min, $users, $load1, $load5, $load15) = ($2, $3, $5, $6, $7, $8, $9);
		if(! $min){
			$min = $hour;
			$hour = 0;
		}
		$days = 0 if ! $days;
		my $uptime = $days + $hour / 24.0 + $min / 1440.0;
		$this->{DATA}->{"uptime"} = $uptime;
		$this->{DATA}->{"logged_users"} = $users;   # this is currently not reported!
		$this->{DATA}->{"load1"} = $load1;
		$this->{DATA}->{"load5"} = $load5;
		$this->{DATA}->{"load15"}= $load15;
	}else{
		logger("NOTICE", "ProcInfo: got unparsable output from uptime: $line");
	}
}


sub readEosDiskValues {
    my $this = shift;
    if (open IN, "df -P -B 1 | grep data | grep -v Filesystem | awk '{a+=\$2;b+=\$3;c+=\$4;print a,b,c}' | tail -1|") {
	my $all = <IN>;
	if ($all) {
	    my @vals = split (" ",$all);
	    $this->{DATA}->{"eos_disk_space"} = sprintf "%d",$vals[0]/1024.0/1024.0;
	    $this->{DATA}->{"eos_disk_used"}  = sprintf "%d",$vals[1]/1024.0/1024.0;
	    $this->{DATA}->{"eos_disk_free"}  = sprintf "%d",$vals[2]/1024.0/1024.0;
	    $this->{DATA}->{"eos_disk_usage"} = sprintf "%d",100.0 *$vals[1]/$vals[0];
	}
	close(IN);
    }
}

# do a difference with overflow check and repair
# the counter is unsigned 32 or 64 bit
sub diffWithOverflowCheck {
	my ($this, $new, $old) = @_;
	
	if($new >= $old){
		return $new - $old;
	}else{
		my $max = 2 ** 32;	# 32 bits
		if($old >= $max ){
			$max = 2 ** 64;	# 64 bits
		}
		return $new - $old + $max;
	}
}

# read network information like transfered kBps and nr. of errors on each interface
# TODO: find an alternative for MAC OS X
sub readNetworkInfo {
	my $this = shift;
	
	if(open(NET_DEV, "</proc/net/dev")){
		while (my $line = <NET_DEV>) {
			if($line =~ /\s*eth(\d):\s*(\d+)\s+\d+\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+\d+\s+(\d+)/){
				$this->{DATA}->{"raw_eth$1"."_in"} = $2;
				$this->{DATA}->{"raw_eth$1"."_out"} = $4;	
				$this->{DATA}->{"raw_eth$1"."_errs"} = $3 + $5; # in and out errors
			}
		}
		close NET_DEV;
	}else{
		logger("NOTICE", "ProcInfo: cannot open /proc/net/dev");
	}
}

# run nestat 
# Note: this works on MAC OS X
sub readNetStat {
	my $this = shift;

	if(open(NETSTAT, 'env PATH=$PATH:/usr/sbin netstat -an 2>/dev/null |')){
		my %sockets = map { +"sockets_$_" => 0 } ('tcp', 'udp', 'unix'); #icm will be auto added on mac
		my %tcp_details = map { +"sockets_tcp_$_" => 0 } 
			('ESTABLISHED', 'SYN_SENT', 'SYN_RECV', 'FIN_WAIT1', 
			 'FIN_WAIT2', 'TIME_WAIT', 'CLOSED', 'CLOSE_WAIT', 
			 'LAST_ACK', 'LISTEN', 'CLOSING', 'UNKNOWN');
		while (my $line = <NETSTAT>) {
			$line =~ s/\s+$//;
			my $proto = ($line =~ /^([^\s]+)/ ? $1 : "");
			my $state = ($line =~ /([^\s]+)$/ ? $1 : "");
			$proto = "unix" if $line =~ /stream/i || $line =~ /dgram/i;
			if($proto =~ /tcp/){
				$sockets{"sockets_tcp"}++;
				$tcp_details{"sockets_tcp_".$state}++;
			}elsif($proto =~ /udp/){
				$sockets{"sockets_udp"}++;
			}elsif($proto =~ /icm/){
				$sockets{"sockets_icm"}++;
			}elsif($proto =~ /unix/){
				$sockets{"sockets_unix"}++;
			}
		}
		close NETSTAT;
		while(my ($key, $value) = each(%sockets)){ $this->{DATA}->{$key} = $value; }
		while(my ($key, $value) = each(%tcp_details)){ $this->{DATA}->{$key} = $value; }
	}else{
		logger("NOTICE", "ProcInfo: cannot run netstat");
	}
}

# internal function that gets the full list of children (pids) for a process (pid)
# it returns an empty list if the process has died
# Note: This works on MAC OS X
sub getChildren {
	my ($this, $parent) = @_;
	my @children = ();
	my %pidmap = ();
	if(open(PIDS, 'ps -A -o "pid ppid" |')){
		$_ = <PIDS>; # skip header
		while(<PIDS>){
			if(/\s*(\d+)\s+(\d+)/){
				$pidmap{$1} = $2;
				push(@children, $parent) if $1 == $parent;
			}
		}
		close(PIDS);
	}else{
		logger("NOTICE", "ProcInfo: cannot execute ps -A -o \"pid ppid\"");
	}
	for(my $i = 0; $i < @children; $i++){
		my $prnt = $children[$i];
		while( my ($pid, $ppid) = each %pidmap ){
			if($ppid == $prnt){
				push(@children, $pid);
			}
		}
	}
	return @children;
}

# internal function that parses a time formatted like "days-hours:min:sec" and returns the corresponding
# number of seconds.
sub parsePSElapsedTime {
	my ($this, $time) = @_;
	if($time =~ /(\d+)-(\d+):(\d+):(\d+)/){
		return $1 * 24 * 3600 + $2 * 3600 + $3 * 60 + $4;
	}elsif($time =~ /(\d+):(\d+):(\d+)/){
		return $1 * 3600 + $2 * 60 + $3;
	}elsif($time =~ /(\d+):(\d+)/){
		return $1 * 60 + $2;
	}else{
		return 0;
	}
}

# internal function that parses time formatted like "Tue Feb  7 17:13:17 2006" and the returns the
# corresponding number of seconds from EPOCH
sub parsePSStartTime {
	my ($this, $strTime) = @_;
	
	if($strTime !~ /\S+\s+(\S+)\s+(\d+)\s+(\d+):(\d+):(\d+)\s+(\d+)/){
		return 0;
	}else{
		my ($strMonth, $mday, $hour, $min, $sec, $year) = ($1, $2, $3, $4, $5, $6);
		my $mon = 0;
		for my $month (@{$this->{MONTHS}}){
			last if $month eq $strMonth;
			$mon++;
		}
		return timelocal($sec, $min, $hour, $mday, $mon, $year);
	}
}

# read information about this the JOB_PID process
# memory sizes are given in KB
# Note: This works on MAC OS X
sub readJobInfo {
	my ($this, $pid) = @_;
	return unless $pid;
	my @children = $this->getChildren($pid);
	logger("DEBUG", "ProcInfo: Children for pid=$pid; are @children.");
	if(@children == 0){
		logger("INFO", "ProcInfo: Job with pid=$pid terminated; removing it from monitored jobs.");
		$this->removeJobToMonitor($pid);
		return;
	}
	if(open(J_STATUS, 'ps -A -o "pid lstart time %cpu %mem rsz vsz command" |')){
		my $line = <J_STATUS>; # skip header
		my ($etime, $cputime, $pcpu, $pmem, $rsz, $vsz, $comm, $fd) = (0, 0, 0, 0, 0, 0, 0, undef);
		my $cputime_offset = $this->{JOBS}->{$pid}->{DATA}->{'cpu_time_offset'} || 0;
		my %mem_cmd_map = ();  # this contains all $rsz_$vsz_$command as keys for every pid
		# it is used to avoid adding several times processes that have multiple threads and appear in
		# ps as sepparate processes, occupying exacly the same amount of memory. The reason for not adding
		# them multiple times is that that memory is shared as they are threads.
		my $crtTime = time();
		while($line = <J_STATUS>){
			chomp $line; 
			$line =~ s/\s+/ /g; $line =~ s/^\s+//; $line =~ s/\s+$//;
			# line looks like: 
			# "PID              STARTED     TIME    %CPU %MEM RSZ VSZ COMMAND"
			# "6157 Tue Feb 7 22:15:30 2006 00:00:00 0.0 0.0 428 1452 g++ -O -pipe..."
			if($line =~ /(\S+) (\S+ \S+ \S+ \S+ \S+) (\S+) (\S+) (\S+) (\S+) (\S+) (.+)/){
				my($apid, $stime1, $cputime1, $pcpu1, $pmem1, $rsz1, $vsz1, $comm1) 
					= ($1, $2, $3, abs($4), abs($5), $6, $7, $8); # % can be negative on mac!?!
				my $isChild = 0;
				for my $childPid (@children){
					if($apid == $childPid){
						$isChild = 1;
						last;
					}
				}
				next if(! $isChild);
				my $sec = $crtTime - $this->parsePSStartTime($stime1);
				$etime = $sec if $sec > $etime;		# the elapsed time is the maximum of all elapsed
				$sec = $this->parsePSElapsedTime($cputime1);   # times corespornding to all child processes.
				$cputime += $sec; # total cputime is the sum of cputimes for all processes.
				$pcpu += $pcpu1; # total %cpu is the sum of all children %cpu.
				if(! $mem_cmd_map{"$pmem1 $rsz1 $vsz1 $comm1"} ++){
					# it's the first thread/process with this memory footprint; add it.
					$pmem += $pmem1; $rsz += $rsz1; $vsz += $vsz1;
					# the same is true for the number of opened files
					my $thisFD = $this->countOpenFD($apid);
					$fd += $thisFD if (defined $thisFD);
				} # else not adding memory usage.
			}
		}
		close(J_STATUS);
		$cputime += $cputime_offset;
		my $cputime_delta = ($this->{JOBS}->{$pid}->{DATA}->{'cpu_time'} || 0) - $cputime;
		if($cputime_delta > 0){
			# Current time is lower than previous - one of the forked processes finished and
			# its contribution to the cpu_time disappeared.
			# We have to recalculate the cputime_offset. Note that in this case, we lose the 
			# cpu_time of the other processes, consumed between these two reports.
			$cputime_offset += $cputime_delta;
			$cputime += $cputime_delta;
		}
		$this->{JOBS}->{$pid}->{DATA}->{'run_time'} = $etime;
		$this->{JOBS}->{$pid}->{DATA}->{'run_ksi2k'} = $etime * $ApMon::Common::KSI2K if $ApMon::Common::KSI2K;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_time'} = $cputime;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_ksi2k'} = $cputime * $ApMon::Common::KSI2K if $ApMon::Common::KSI2K;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_time_offset'} = $cputime_offset;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_usage'} = $pcpu;
		$this->{JOBS}->{$pid}->{DATA}->{'mem_usage'} = $pmem;
		$this->{JOBS}->{$pid}->{DATA}->{'rss'} = $rsz;
		$this->{JOBS}->{$pid}->{DATA}->{'virtualmem'} = $vsz;
		$this->{JOBS}->{$pid}->{DATA}->{'open_files'} = $fd if (defined $fd);
	}else{
		logger("NOTICE", "ProcInfo: cannot run ps to see job's status for job $pid");
	}
}

# count the number of open files for the given pid
# TODO: find an equivalent for MAC OS X
sub countOpenFD {
	my ($this, $pid) = @_;

	if(opendir(DIR, "/proc/$pid/fd")){
		my @list = readdir(DIR);
		closedir DIR;
		my $open_files = ($pid == $$ ? @list - 4 : @list - 2);
		logger("DEBUG", "Counting open_files for $pid: |@list| => $open_files");
		return $open_files;
	}else{
		logger("NOTICE", "ProcInfo: cannot count the number of opened files for job $pid");
	}
	return undef;
}

# if there is an work directory defined, then compute the used space in that directory
# and the free disk space on the partition to which that directory belongs
# sizes are given in MB
# Note: this works on MAC OS X
sub readJobDiskUsage {
	my ($this, $pid) = @_;
	
	my $workDir = $this->{JOBS}->{$pid}->{WORKDIR};
	return unless $workDir and -d $workDir;
	if(open(DU, "du -Lsck $workDir | tail -1 | cut -f 1 |")){
		my $line = <DU>;
		if($line){
			chomp $line;
			$this->{JOBS}->{$pid}->{DATA}->{'workdir_size'} = $line / 1024.0;
		}else{
			logger("NOTICE", "ProcInfo: cannot get du output for job $pid");
		}
		close(DU);
	}else{
		logger("NOTICE", "ProcInfo: cannot run du to get job's disk usage for job $pid");
	}
	if(open(DF, "df -m $workDir | tail -1 |")){
		my $line = <DF>;
		if($line){
			chomp $line;
			if($line =~ /\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%/){
				$this->{JOBS}->{$pid}->{DATA}->{'disk_total'} = $1;
				$this->{JOBS}->{$pid}->{DATA}->{'disk_used'} = $2;
				$this->{JOBS}->{$pid}->{DATA}->{'disk_free'} = $3;
				$this->{JOBS}->{$pid}->{DATA}->{'disk_usage'} = $4;
			}
		}else{
			logger("NOTICE", "ProcInfo: cannot get df output for job $pid");
		}
		close(DF);
	}else{
		logger("NOTICE", "ProcInfo: cannot run df to get job's disk usage for job $pid");
	}
}

# create cummulative parameters based on raw params like cpu_, blocks_, swap_, or ethX_
sub computeCummulativeParams {
	my ($this, $dataRef, $prevDataRef) = @_;

	if(scalar(keys %$prevDataRef) == 0){
		for my $param (keys %$dataRef){
			next if $param !~ /^raw_/;
			$prevDataRef->{$param} = $dataRef->{$param};
		}
		$prevDataRef->{'TIME'} = $dataRef->{'TIME'};
		return;
	}

	# cpu -related params
	if(defined($dataRef->{'raw_cpu_usr'}) && defined($prevDataRef->{'raw_cpu_usr'})){
		my %diff = ();
		my $cpu_sum = 0;
		for my $param ('cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait') {
			$diff{$param} = $this->diffWithOverflowCheck($dataRef->{"raw_$param"}, $prevDataRef->{"raw_$param"});
			$cpu_sum += $diff{$param};
		}
		for my $param ('cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait') {
			if($cpu_sum != 0){
				$dataRef->{$param} = 100.0 * $diff{$param} / $cpu_sum;
			}else{
				delete $dataRef->{$param};
			}
		}
		if($cpu_sum != 0){
			$dataRef->{'cpu_usage'} = 100.0 * ($cpu_sum - $diff{'cpu_idle'}) / $cpu_sum;
		}else{
			delete $dataRef->{'cpu_usage'};
		}
		# add the other parameters
		for my $param ('interrupts', 'context_switches'){
			if(defined($dataRef->{"raw_$param"}) && defined($prevDataRef->{"raw_$param"})){
				$dataRef->{$param} = $this->diffWithOverflowCheck($dataRef->{"raw_$param"}, $prevDataRef->{"raw_$param"});
			}
		}
	}

	# interrupts, context switches, swap & blocks - related params
	my $interval = $dataRef->{TIME} - $prevDataRef->{TIME};
	for my $param ('blocks_in', 'blocks_out', 'swap_in', 'swap_out', 'interrupts', 'context_switches') {
		if(defined($dataRef->{"raw_$param"}) && defined($prevDataRef->{"raw_$param"}) && ($interval != 0)){
			my $diff = $this->diffWithOverflowCheck($dataRef->{"raw_$param"}, $prevDataRef->{"raw_$param"});
			$dataRef->{$param."_R"} = $diff / $interval;
		}else{
			delete $dataRef->{$param."_R"};
		}
	}

	# eth - related params
	for my $rawParam (keys %$dataRef){
		next if $rawParam !~ /^raw_eth/;
		next if ! defined($prevDataRef->{$rawParam});
		my $param = $1 if($rawParam =~ /raw_(.*)/);
		if($interval != 0){
			$dataRef->{$param} = $this->diffWithOverflowCheck($dataRef->{$rawParam}, $prevDataRef->{$rawParam}); # absolute difference
			$dataRef->{$param} = $dataRef->{$param} / $interval / 1024.0 if($param !~ /_errs$/); # if it's _in or _out, compute in KB/sec
		}else{
			delete $dataRef->{$param};
		}
	}

	# copy contents of the current data values to the 
	for my $param (keys %$dataRef){
		next if $param !~ /^raw_/;
		$prevDataRef->{$param} = $dataRef->{$param};
	}
	$prevDataRef->{'TIME'} = $dataRef->{'TIME'};
}


# Return the array image of a hash with the requested parameters (from paramsRef)
# sorted alphabetically
# The cummulative parameters are computed based on $prevDataRef
# As a side effect, prevDataRef is updated to have the values in dataRef.
sub getFilteredData {
	my ($this, $dataRef, $paramsRef, $prevDataRef) = @_;

	# we don't do this for jobs
	$this->computeCummulativeParams($dataRef, $prevDataRef)	if($prevDataRef);

	my %result = ();
	for my $param (@$paramsRef) {
		if($param eq "net_sockets"){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /sockets_[^_]+$/;
			}
		}elsif($param eq "net_tcp_details"){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /sockets_tcp_/;
			}
		}elsif($param =~ /^net_(.*)$/ or $param =~ /^(ip)$/){
			my $net_param = $1;
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /^eth\d_$net_param/;
			}
		}elsif($param eq "processes"){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /^processes/;
			}
		}elsif($param =~ /blocks_|swap_|interrupts|context_switches/){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /^${param}_R$/;
			}
		}else{
			$result{$param} = $dataRef->{$param} if defined $dataRef->{$param};
		}
	}
	my @sorted_result = ();
	for my $key (sort (keys %result)) {
		push(@sorted_result, $key, $result{$key});
	}
	return @sorted_result;

}

######################################################################################
# Interesting functions for other modules:

# This should be called from time to time to update the monitored data, 
# but not more often than once a second because of the resolution of time()
sub update {
	my $this = shift;
	$this->readStat();
	$this->readMemInfo();
	$this->readUptimeAndLoadAvg();
	$this->countProcesses();
	$this->readNetworkInfo();
	$this->readNetStat();
	$this->readEosDiskValues();
	$this->{DATA}->{TIME} = time;
	for my $pid (keys %{$this->{JOBS}}) {
		$this->readJobInfo($pid);
		$this->readJobDiskUsage($pid);
	}
}

# Call this to add another PID to be monitored
sub addJobToMonitor {
	my ($this, $pid, $workDir) = @_;
	
	$this->{JOBS}->{$pid}->{WORKDIR} = $workDir;
	$this->{JOBS}->{$pid}->{DATA} = {};
}

# Call this to stop monitoring a PID
sub removeJobToMonitor {
	my ($this, $pid) = @_;
	
	delete $this->{JOBS}->{$pid};
}

# Return a filtered hash containting the system-related parameters and values
sub getSystemData {
	my ($this, $paramsRef, $prevDataRef) = @_;

	return $this->getFilteredData($this->{DATA}, $paramsRef, $prevDataRef);
}

# Return a filtered hash containing the job-related parameters and values
sub getJobData {
	my ($this, $pid, $paramsRef) = @_;
	
	return $this->getFilteredData($this->{JOBS}->{$pid}->{DATA}, $paramsRef);
}

1;

