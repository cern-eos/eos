package ApMon::ProcInfo;

use strict;
use warnings;

use ApMon::Common qw(logger);
use Data::Dumper;
use Net::Domain;
use Time::Local;
use Config;

# See the end of this file for a set of interesting methods for other modules.

# ProcInfo constructor
sub new {
	my $this = {};
	$this->{DATA} = {};		# monitored data that is going to be reported
	$this->{JOBS} = {};		# jobs that will be monitored 
	$this->{NETWORKINTERFACES} = {};# network interface names
	# names of the months for ps start time of a process
	$this->{MONTHS} = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
	$this->{readGI} = 0;	# used to read generic information less often
	bless $this;
	return $this;
}

# this has to be run twice (with the $lastUpdateTime updated) to get some useful results
sub readStat {
	my $this = shift;
	
	if ($Config{osname} eq "solaris"){
	    if (open(VMSTAT, "vmstat -s |")){
		my $line;
		
		while ($line = <VMSTAT>){
		    if ($line =~ /(\d+)\s+user\s+cpu/ ){
			$this->{DATA}->{"raw_cpu_usr"} = $1;
		    }
		
		    if ($line =~ /(\d+)\s+system\s+cpu/){
			$this->{DATA}->{"raw_cpu_sys"} = $1;
		    }
		
		    if ($line =~ /(\d+)\s+idle\s+cpu/){
			$this->{DATA}->{"raw_cpu_idle"} = $1;
		    }
		    
		    if ($line =~ /(\d+)\s+wait\s+cpu/){
			$this->{DATA}->{"raw_cpu_iowait"} = $1;
		    }
		
		    if ($line =~ /(\d+)\s+pages\s+swapped\s+in/){
			$this->{DATA}->{"raw_swap_in"} = $1;
		    }
		
		    if ($line =~ /(\d+)\s+pages\s+swapped\s+out/){
			$this->{DATA}->{"raw_swap_out"} = $1;
		    }
		    
		    if ($line =~ /(\d+)\s+device\s+interrupts/){
			$this->{DATA}->{"raw_interrupts"} = $1;
		    }
		
		    if ($line =~ /(\d+)\s+cpu\s+context\s+switches/){
			$this->{DATA}->{"raw_context_switches"} = $1;
		    }
		    
		}
		
		close VMSTAT;
	    }

	    $this->{DATA}->{"raw_blocks_in"} = $this->{DATA}->{"raw_blocks_out"} = 0;
	
	    if (open(IOSTAT, "iostat -xnI |")){
		my $line = <IOSTAT>;
		$line = <IOSTAT>;
		
		my $kbR = 0;
		my $kbW = 0;
		
		while ($line = <IOSTAT>){
		    (undef, $kbR, $kbW) = split(/\s+/, $line);
		    
		    $this->{DATA}->{"raw_blocks_in"} += $kbR;
		    $this->{DATA}->{"raw_blocks_out"} += $kbW;
		}
		
		close IOSTAT;
	    }
	
	    return;
	}
	
	if(open(STAT, "</proc/stat")){
		my $line;
		while($line = <STAT>){
			if($line =~ /^cpu\s/) {
				(undef, $this->{DATA}->{"raw_cpu_usr"}, $this->{DATA}->{"raw_cpu_nice"}, 
				        $this->{DATA}->{"raw_cpu_sys"}, $this->{DATA}->{"raw_cpu_idle"},
					$this->{DATA}->{"raw_cpu_iowait"}, $this->{DATA}->{"raw_cpu_irq"},
					$this->{DATA}->{"raw_cpu_softirq"}, $this->{DATA}->{"raw_cpu_steal"},
					$this->{DATA}->{"raw_cpu_guest"}
				) = split(/ +/, $line);
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
	
	if ($Config{osname} eq "solaris"){
	    if (open(MEM_INFO, "prtconf |")){
		my $line;
		
		while ($line = <MEM_INFO>){
		    if ($line =~ /^Memory size: (\d+) (\w)/){
			$this->{DATA}->{"total_mem"} = $1;
			
			if ($2 eq "G"){
			    $this->{DATA}->{"total_mem"} *= 1024;
			}
		    }
		}
		
		close MEM_INFO;
	    }
	    
	    if (open(MEM_INFO, "vmstat |")){
		my $line;

		# first header line
		$line = <MEM_INFO>;
		# second header line
		$line = <MEM_INFO>;
		# and the contents 
		$line = <MEM_INFO>;
		
		if ($line =~ /^\s*\d+\s+\d+\s+\d+\s+\d+\s+(\d+)/){
		    my $memfree = $1 / 1024;
		
		    $this->{DATA}->{"mem_free"} = $memfree;
		    $this->{DATA}->{"mem_actual_free"} = $memfree;
		    $this->{DATA}->{"mem_used"} = $this->{DATA}->{"total_mem"} - $memfree;
		    $this->{DATA}->{"mem_usage"} = $this->{DATA}->{"mem_used"} * 100 / $this->{DATA}->{"total_mem"} if  $this->{DATA}->{"total_mem"};
		}		
		
		close MEM_INFO;
	    }
	    
	    if (open(MEM_INFO, "swap -l |")){
		my $line;
		
		$line = <MEM_INFO>;
		
		$this->{DATA}->{"total_swap"} = 0;
		$this->{DATA}->{"swap_free"} = 0;
		
		while ($line = <MEM_INFO>){
		    if ($line =~ /(\d+)\s+(\d+)$/){
			$this->{DATA}->{"total_swap"} += $1 / 2048;
			$this->{DATA}->{"swap_free"} += $2 / 2048;
		    }
		}
		
		$this->{DATA}->{"swap_used"} = $this->{DATA}->{"total_swap"} - $this->{DATA}->{"swap_free"};
		
		$this->{DATA}->{"swap_usage"} = 100.0 * $this->{DATA}->{"swap_used"} / $this->{DATA}->{"total_swap"} if $this->{DATA}->{"total_swap"};
	    }
	
	    return;
	}
	
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
	
	my $command = "ps -A -o state |";
	
	if ($Config{osname} eq "solaris"){
	    $command = "ps -A -o s |";
	}
	
	if(open(PROC, $command)){
		my $state = <PROC>;      # ignore the first line - it's the header
		while(<PROC>){
			$state = substr($_, 0, 1);
			$states{$state}++;
			$total++;
		}

		close PROC;
		
		$this->{DATA}->{"processes"} = $total;
		for $state (keys %states){
			next if (($state eq '') || ($state =~ /\s+/));
			$this->{DATA}->{"processes_$state"} = $states{$state};
		}
	}
	else{
		logger("NOTICE", "ProcInfo: cannot count the processes using ps.");
	}
}

#Read information about CPU.
sub readCPUInfo {
	my $this = shift;
	
	if ($Config{osname} eq "solaris"){
	    chomp ($this->{DATA}->{"no_CPUs"} = `psrinfo -p`);

	    return;
	}
	
	if(-r "/proc/cpuinfo"){
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
		}
	}
	# this is for Itanium
	if(-r "/proc/pal/cpu0/cache_info"){
		if(open(CACHE_INFO, "</proc/pal/cpu0/cache_info")){
			my $line;
			my $level3params = 0;
			while($line = <CACHE_INFO>){
				$level3params = 1 if ($line =~ /Cache level 3/);
				$this->{DATA}->{"cpu_cache"} = $1 / 1024 if ($level3params && $line =~ /Size\s+:\s+(\d+)/);
			}
			close(CACHE_INFO);
		}
	}
	# also put the ksi2k factor, if known
	$this->{DATA}->{"ksi2k_factor"} = $ApMon::Common::KSI2K if $ApMon::Common::KSI2K;
}

# reads the IP, hostname, cpu_MHz, kernel_version, os_version, platform
sub readGenericInfo {
	my $this = shift;

	my $hostname = Net::Domain::hostfqdn();

	$this->{DATA}->{"hostname"} = $hostname;
	
	if ($Config{osname} eq "solaris"){
	    chomp ($this->{DATA}->{"os_type"} = `uname -sr`);
	    $this->{DATA}->{"platform"} = "solaris";
	    $this->{DATA}->{"kernel_version"} = $Config{osvers};
	
	    if (open(IF_CFG, "ifconfig -a4 |")){
		my ($eth, $ip, $line);
		
		while ($line = <IF_CFG>){
		    if ($line =~ /^(\w+\d):/){
			$eth = $1;
		    }
		    
		    if (defined($eth) and ($line =~ /\s+inet\s+(\d+\.\d+\.\d+\.\d+)/)){
			$ip = $1;
			
			next if ($eth =~ /^lo/);

			$this->{DATA}->{$eth."_ip"} = $ip;

			# fake eth0 on solaris
			$this->{DATA}->{"eth0_ip"} = $ip unless $this->{DATA}->{"eth0_ip"};
		    }
		}
	    }
	
	    return;
	}
	
	if(open(IF_CFG, "/sbin/ifconfig -a |")){
		my ($eth, $ip, $ipv6, $line);
		while($line = <IF_CFG>){
			if($line =~ /^(\w+):?\s+/ ){
				undef $ip;
				if (exists($this->{NETWORKINTERFACES}->{$1})){
				    $eth = $1;
				    undef $ip;
				    undef $ipv6;
				}
				else{
				    undef $eth;
				}
				next;
			}
			
			if ($line =~ /^\w/){
			    undef $eth;
			    undef $ip;
			    undef $ipv6;
			    next;
			}
			
			if(defined($eth) and ($line =~ /\s+inet( addr:)?\s*(\d+\.\d+\.\d+\.\d+)/) and ! defined($ip)){
				$ip = $2;
				$this->{DATA}->{$eth."_ip"} = $ip;
				undef $ipv6;
			}

			if(defined($eth) and ($line =~ /\s+inet6( addr:)?\s*([0-9a-fA-F:]+).*(Scope:Global|scopeid.*global)/) and ! defined($ipv6)){
				$ipv6 = $2;
				$this->{DATA}->{$eth."_ipv6"} = $ipv6;
			}
		}
		close IF_CFG;
	}else{
		logger("NOTICE", "ProcInfo: couldn't get output from /sbin/ifconfig -a");
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
	}
	else{
		logger("NOTICE", "ProcInfo: got unparsable output from uptime: $line");
	}
}


sub readEosDiskValues {
    my $this = shift;
    my $storagepath=$ENV{"APMON_STORAGEPATH"};

    if ( "$storagepath" eq "" ) {
        $storagepath = "data";
    }

    if (open IN, "df -P -B 1 | grep $storagepath | grep -v Filesystem | awk '{a+=\$2;b+=\$3;c+=\$4;print a,b,c}' | tail -1|") {
        my $all = <IN>;
        if ($all) {
            my @vals = split (" ",$all);
            $this->{DATA}->{"eos_disk_space"} = sprintf "%.03f",$vals[0]/1024.0/1024.0/1024.0/1024.0;
            $this->{DATA}->{"eos_disk_used"}  = sprintf "%.03f",$vals[1]/1024.0/1024.0/1024.0/1024.0;
            $this->{DATA}->{"eos_disk_free"}  = sprintf "%.03f",$vals[2]/1024.0/1024.0/1024.0/1024.0;
            $this->{DATA}->{"eos_disk_usage"} = sprintf "%d",100.0 *$vals[1]/$vals[0];
        }
        close(IN);
    }
}

sub readEosRpmValues {
    my $this = shift;
    if (open IN, "rpm -qa xrootd | cut -d '-' -f2 |") {
        my $all = <IN>;
        if ($all) {
            chomp $all;
            $all =~ s/xrootd-//;
            $this->{DATA}->{"xrootd_rpm_version"} = 'v'.$all;
        }
        close(IN);
    }

    if (open IN, "rpm -qa eos-server |") {
        my $all = <IN>;
        if ($all) {
            chomp $all;
            $all =~ s/eos-server-//;
            $this->{DATA}->{"eos_rpm_version"} = $all;
        }
        close(IN);
    }
}

sub show_call_stack {  
    my ( $path, $line, $subr );  
    my $max_depth = 30; 
    my $i = 1;  

    while ( (my @call_details = (caller($i++))) && ($i<$max_depth) ) {
	print "$call_details[1] line $call_details[2] in function $call_details[3]\n";
    }
}

# do a difference with overflow check and repair
# the counter is unsigned 32 or 64 bit
sub diffWithOverflowCheck {
	my ($this, $new, $old) = @_;

	if($new >= $old){
		return $new - $old;
	}
	else{
		return $new;
	}
}

# read network information like transfered kBps and nr. of errors on each interface
# TODO: find an alternative for MAC OS X
sub readNetworkInfo {
	my $this = shift;

	$this->{NETWORKINTERFACES} = {};
	
	if ($Config{osname} eq "solaris"){
	    my $ifname;
	    my $line; 
	
	    if (open(NET_DEV, "ifconfig -a4 |")){
	    	while ($line = <NET_DEV>){
		    next if ($ifname);
		    
		    if ($line =~ /^(\w+\d):\s+/){
			next if ($line =~ /^lo/);
		    
			$ifname = $1;
		    }
		}
		
		close NET_DEV;
	    }
	    
	    
	    my $bytesIn = 0;
	    my $bytesOut = 0;
	    
	    if (open(NET_DEV,"netstat -P tcp -s |")){
		while ($line = <NET_DEV>){
		    if ($line =~ /tcpOut\w+Bytes\s*=\s*(\d+)/){
			$bytesOut += $1;
		    }
		    
		    if ($line =~ /tcpRetransBytes\s*=\s*(\d+)/){
			$bytesOut += $1;
		    }
		    
		    if ($line =~ /tcpIn\w+Bytes\s*=\s*(\d+)/){
			$bytesIn += $1;
		    }
		}
		
		close NET_DEV;
		
		$this->{DATA}->{"raw_net_".$ifname."_in"} = $bytesIn;
		$this->{DATA}->{"raw_net_".$ifname."_out"} = $bytesOut;
		$this->{DATA}->{"raw_net_".$ifname."_err"} = 0;
		
		#fake eth0 traffic, even if on Solaris the interfaces have weird names
		#and moreover we cannot tell the traffic per each interface...
		$this->{DATA}->{"raw_net_eth0_in"} = $bytesIn;
		$this->{DATA}->{"raw_net_eth0_out"} = $bytesOut;
		$this->{DATA}->{"raw_net_eth0_err"} = 0;

		$this->{DATA}->{"raw_net_total_traffic_in"} = $bytesIn;
		$this->{DATA}->{"raw_net_total_traffic_out"} = $bytesOut;

		$this->{NETWORKINTERFACES}->{"eth0"} = "eth0";
		$this->{NETWORKINTERFACES}->{"total_traffic"} = "total_traffic";
	    }
	    
	    return;
	}

	if (opendir my $dh, "/sys/class/net"){
	    my @things = grep {$_ ne '.' and $_ ne '..' } readdir $dh;
	    foreach my $thing (@things) {
		my $link = readlink("/sys/class/net/".$thing);
		if (defined($link) && index($link, "/virtual/")<0){
		    $this->{NETWORKINTERFACES}->{$thing} = $thing;
		}
	    }
	}

	my $total_traffic_in=0;
	my $total_traffic_out=0;

	if(open(NET_DEV, "</proc/net/dev")){
		while (my $line = <NET_DEV>) {
			if($line =~ /\s*(\w+):\s*(\d+)\s+\d+\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+\d+\s+(\d+)/){
				if ( exists($this->{NETWORKINTERFACES}->{$1}) ){
				    $this->{DATA}->{"raw_net_$1"."_in"} = $2;
				    $this->{DATA}->{"raw_net_$1"."_out"} = $4;
				    $this->{DATA}->{"raw_net_$1"."_errs"} = $3 + $5; # in and out errors

				    $total_traffic_in += $2;
				    $total_traffic_out += $4;
				}
			}
		}
		close NET_DEV;
	}else{
		logger("NOTICE", "ProcInfo: cannot open /proc/net/dev");
	}

	$this->{DATA}->{"raw_net_total_traffic_in" } = $total_traffic_in;
	$this->{DATA}->{"raw_net_total_traffic_out"} = $total_traffic_out;

	$this->{NETWORKINTERFACES}->{"total_traffic"} = "total_traffic";
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
	
	    if ($Config{osname} eq "solaris"){
		my $sockclass;
		
		while (my $line = <NETSTAT>){
		    if ($line =~ /^UDP:/){
			$sockclass = "udp";
			$line = <NETSTAT>;
			$line = <NETSTAT>;
			next;
		    }
		    
		    if ($line =~ /^TCP:/){
			$sockclass = "tcp";
			$line = <NETSTAT>;
			$line = <NETSTAT>;
			next;
		    }
		    
		    if ($line =~ /^SCTP:/){
			$sockclass = "sctp";
			$line = <NETSTAT>;
			$line = <NETSTAT>;
			next;
		    }
		    
		    if ($line =~ /^Active UNIX domain sockets/){
			$sockclass = "unix";
			$line = <NETSTAT>;
			next;
		    }
		    
		    chomp ($line);
		    
		    if (length($line) == 0){
			undef $sockclass;
			next;
		    }
		    
		    if (defined($sockclass)){
			if ($sockclass eq "tcp"){
			    if ($line =~ /\s+(\w+)\s*$/){
				$sockets{"sockets_tcp"}++;

				my $state = uc($1);
				
				if (not defined($tcp_details{"sockets_tcp_".$state})){
				    $tcp_details{"sockets_tcp_".$state} = 0;
				}
				
				$tcp_details{"sockets_tcp_".$state}++;
			    }
			}
			else{
			    if (not defined($sockets{"sockets_$sockclass"})){
				$sockets{"sockets_$sockclass"} = 0;
			    }
			    
			    $sockets{"sockets_$sockclass"}++;
			}
		    }
		}
	    }
	    else{
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
	    }
	
	    close NETSTAT;
	    while(my ($key, $value) = each(%sockets)){ $this->{DATA}->{$key} = $value; }
	    while(my ($key, $value) = each(%tcp_details)){ $this->{DATA}->{$key} = $value; }
	}
	else{
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
		my ($etime, $cputime, $pcpu, $pmem, $rsz, $vsz, $comm, $fd, $minflt, $majflt) = (0, 0, 0, 0, 0, 0, 0, undef, 0, 0);
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
				# Get the number of minor and major page faults
				if(open(STAT, "/proc/$apid/stat")){
					my $line = <STAT>;
					my($pid, $exec, $status, $ppid, $pgrp, $sid, $tty, $tty_grp, $flags, $mflt, $cmflt, $jflt, $cjflt)
						= split(/\s+/, $line);
					$minflt += $mflt;
					$majflt += $jflt;
				}
				close(STAT);
			}
		}
		close(J_STATUS);
		$cputime += $cputime_offset;
		my $cputime_delta = ($this->{JOBS}->{$pid}->{DATA}->{'cpu_time'} || 0) - $cputime; # note this is the other way around!
		if($cputime_delta > 0){
			# Current time is lower than previous - one of the forked processes finished and
			# its contribution to the cpu_time disappeared.
			# We have to recalculate the cputime_offset. Note that in this case, we lose the 
			# cpu_time of the other processes, consumed between these two reports.
			$cputime_offset += $cputime_delta;
			$cputime += $cputime_delta;
		}
		$cputime_delta = $cputime - ($this->{JOBS}->{$pid}->{DATA}->{'cpu_time'} || 0);	# real cpu time delta
		my $etime_delta = $etime - ($this->{JOBS}->{$pid}->{DATA}->{'run_time'} || 0);	# real elapsed time delta
		my $crtCpuSpeed = $this->{DATA}->{'cpu_MHz'} || 1;
		my $orgCpuSpeed = $ApMon::Common::CpuMHz || $crtCpuSpeed;
		#my $freqFact = $crtCpuSpeed / $orgCpuSpeed; # if Cpu speed varies in time, adjust ksi2k factor
		my $freqFact = 1;		
		$this->{JOBS}->{$pid}->{DATA}->{'run_time'} += $etime_delta;
		$this->{JOBS}->{$pid}->{DATA}->{'run_ksi2k'} += $etime_delta * $freqFact * $ApMon::Common::KSI2K if $ApMon::Common::KSI2K;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_time'} += $cputime_delta;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_ksi2k'} += $cputime_delta * $freqFact * $ApMon::Common::KSI2K if $ApMon::Common::KSI2K;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_time_offset'} = $cputime_offset;
		$this->{JOBS}->{$pid}->{DATA}->{'cpu_usage'} = $pcpu;
		$this->{JOBS}->{$pid}->{DATA}->{'mem_usage'} = $pmem;
		$this->{JOBS}->{$pid}->{DATA}->{'rss'} = $rsz;
		$this->{JOBS}->{$pid}->{DATA}->{'virtualmem'} = $vsz;
		$this->{JOBS}->{$pid}->{DATA}->{'open_files'} = $fd if (defined $fd);
		$this->{JOBS}->{$pid}->{DATA}->{'page_faults_min'} = $minflt;
		$this->{JOBS}->{$pid}->{DATA}->{'page_faults_maj'} = $majflt;
	}else{
		logger("NOTICE", "ProcInfo: cannot run ps to see job's status for job $pid");
	}
}

# count the number of open files for the given pid
# TODO: find an equivalent for MAC OS X
sub countOpenFD {
	my ($this, $pid) = @_;
	
	if ($Config{osname} eq "solaris"){
	    return undef;
	}

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
	if(open(DF, "df -k $workDir | tail -1 |")){
		my $line = <DF>;
		if($line){
			chomp $line;
			if($line =~ /\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%/){
				$this->{JOBS}->{$pid}->{DATA}->{'disk_total'} = $1 / 1024.0;
				$this->{JOBS}->{$pid}->{DATA}->{'disk_used'} = $2 / 1024.0;
				$this->{JOBS}->{$pid}->{DATA}->{'disk_free'} = $3 / 1024.0;
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
		for my $param ('cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait', 'cpu_irq', 'cpu_softirq', 'cpu_steal', 'cpu_guest') {
		    if (defined($dataRef->{"raw_$param"}) && defined($prevDataRef->{"raw_$param"})){
			$diff{$param} = $this->diffWithOverflowCheck($dataRef->{"raw_$param"}, $prevDataRef->{"raw_$param"});
			$cpu_sum += $diff{$param};
		    }
		}
		for my $param ('cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait', 'cpu_irq', 'cpu_softirq', 'cpu_steal', 'cpu_guest') {
		    if (defined($dataRef->{"raw_$param"}) && defined($prevDataRef->{"raw_$param"})){
			if($cpu_sum != 0){
				$dataRef->{$param} = 100.0 * $diff{$param} / $cpu_sum;
			}else{
				delete $dataRef->{$param};
			}
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

	# physical network interfaces - related params
	for my $rawParam (keys %$dataRef){
		next if $rawParam !~ /^raw_net_/;
		next if ! defined($prevDataRef->{$rawParam});
		my $param = $1 if($rawParam =~ /raw_net_(.*)/);

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
	my ($this, $dataRef, $paramsRef, $prevDataRef, $networkInterfaces) = @_;

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
				if ($key =~ /^(\w+)_$net_param/ ){
					if ( exists ($networkInterfaces->{$1}) ){
					    $result{$key} = $dataRef->{$key};
					}
				}
			}
		}elsif($param eq "processes"){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /^processes/;
			}
		}elsif($param =~ /blocks_|swap_|interrupts|context_switches/){
			for my $key (keys %$dataRef) {
				$result{$key} = $dataRef->{$key} if $key =~ /^${param}_R$/;
			}
			$result{$param} = $dataRef->{$param} if($param =~/^swap_/ && defined($dataRef->{$param}));
		}
		else{
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
	logger("NOTICE", "ProcInfo: Collecting backgound and ".keys(%{$this->{JOBS}})." PIDs monitoring info.");
	$this->readStat();
	$this->readMemInfo();
	$this->readUptimeAndLoadAvg();
	$this->countProcesses();
	$this->readNetworkInfo();
	$this->readNetStat();
	$this->readEosDiskValues();
        $this->readEosRpmValues();
	$this->{DATA}->{TIME} = time;
	$this->readGenericInfo() if (($this->{readGI}++) % 2 == 0);
	$this->readCPUInfo();
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

	my @ret = $this->getFilteredData($this->{DATA}, $paramsRef, $prevDataRef, $this->{NETWORKINTERFACES});
	
	#print Dumper(@ret);
	
	return @ret;
}

# Return a filtered hash containing the job-related parameters and values
sub getJobData {
	my ($this, $pid, $paramsRef) = @_;
	
	return $this->getFilteredData($this->{JOBS}->{$pid}->{DATA}, $paramsRef, $this->{NETWORKINTERFACES});
}

1;

