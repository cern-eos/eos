package ApMon::ConfigLoader;

use strict;
use warnings;

use ApMon::Common qw(logger $APMON_DEFAULT_PORT %defaultOptions);
use Socket;
use Data::Dumper;
use Carp qw(cluck);


# Config Loader constructor
sub new {
        my ($type, $cmdPipe, $confFile) = @_;
        my $this = {};
        bless $this;
        $this->{CMD_PIPE} = $cmdPipe;
        $this->{CONF_FILE} = $confFile;
	$this->{LAST_CONF_CHECK_TIME} = 0;
        $this->{CONF_RECHECK} = 1;
        $this->{CONF_CHECK_INTERVAL} = 30;
	$this->{DEST_LOCATIONS} = ();		# http/file locations from where to read the config
	$this->{DESTINATIONS} = {};
	return $this;
}

# This call will never return!
# It should be a used just from a child process whose role is only configuration refreshing.
sub run {
	my $this = shift;
	my ($wasSuccess, $userMsg) = (0, undef);
	while(1) {
		$userMsg = ApMon::Common::readMessage($this->{CMD_PIPE});
		$this->parseParentMessage($userMsg) if $userMsg;
		$wasSuccess = $this->refreshConfig($wasSuccess) if $this->{CONF_RECHECK};
		sleep($this->{CONF_CHECK_INTERVAL});
	}
}

# This allows setting the configuration. It can be used with several arguments:
# - list of strings (URLs and/or files) - the configuration will be read from all
# - reference to an ARRAY - each element is a destination ML service; for each destination
#   the default options will be used
# - reference to a HASH - each key is a destination ML service; for each destination you can
#   define a set of additional options that will overwrite the default ones.
sub setDestinations {
	my ($this, @destLocations) = @_;
	
	my $prevDest = $this->{DESTINATIONS};
	$this->{DESTINATIONS} = {};
        # determine the way we were instantiated and initalize accordingly
        if(ref($destLocations[0]) eq "ARRAY"){
                # user gave a reference to an array, each element being a destination (host[:port][ pass])
                # we will send datagrams to all valid destinations (i.e. host can be resolved), with default options
                $this->{CONF_RECHECK} = 0;
                my ($destStr, $dest);
                for $destStr (@{$destLocations[0]}) {
                        $dest = $this->parseDestination($destStr);
                        if($dest){
                                my %defOptsCopy = %defaultOptions;
                                logger("INFO", "Added destination $dest with default options.");
                                $this->{DESTINATIONS}->{$dest}->{OPTS} = \%defOptsCopy;
				ApMon::Common::updateLastSentTime($prevDest->{$dest}->{OPTS}, $this->{DESTINATIONS}->{$dest}->{OPTS});
				$this->{DESTINATIONS}->{$dest}->{PREV_RAW_DATA} = 
					($prevDest->{$dest}->{PREV_RAW_DATA} ? $prevDest->{$dest}->{PREV_RAW_DATA} : {});
				$this->{DESTINATIONS}->{$dest}->{OPTS}->{'conf_recheck'} = 0;
                        }
                }
		$this->writeDestinations();
        }elsif(ref($destLocations[0]) eq "HASH"){
                # user gave a reference to a hash, each key being a destination (host[:port][ pass])
                # we will send datagrams to all valid destinations (i.e. host can be resolved), overwritting the
                # default options with the ones passed by user. Options will be named as in the %defaultOptions.
                $this->{CONF_RECHECK} = 0;
                my ($destStr, $dest);
                for $destStr (keys %{$destLocations[0]}){
                        $dest = $this->parseDestination($destStr);
                        if($dest){
                                my %defOptsCopy = %defaultOptions;
                                $this->{DESTINATIONS}->{$dest}->{OPTS} = \%defOptsCopy;
				ApMon::Common::updateLastSentTime($prevDest->{$dest}->{OPTS}, $this->{DESTINATIONS}->{$dest}->{OPTS});
				$this->{DESTINATIONS}->{$dest}->{PREV_RAW_DATA} = 
					($prevDest->{$dest}->{PREV_RAW_DATA} ? $prevDest->{$dest}->{PREV_RAW_DATA} : {});
				$this->{DESTINATIONS}->{$dest}->{OPTS}->{'conf_recheck'} = 0;
                                logger("INFO", "Added destination $dest with the following additional options:");
                                # now we have to modify default options with the ones given by user
                                my ($key, $value);
                                for $key (keys %{$destLocations[0]->{$destStr}}){
                                        $value = $destLocations[0]->{$destStr}->{$key};
                                        logger("INFO", " -> $key = $value");
                                        $this->{DESTINATIONS}->{$dest}->{OPTS}->{$key} = $value;
                                }
                        }
                }
		$this->writeDestinations();
        }else{
		# we got a list of URLs and/or files. Fetch them and get the configuration
		$this->{DEST_LOCATIONS} = ();
		push(@{$this->{DEST_LOCATIONS}}, @destLocations);
		$this->refreshConfig();
	}
}

# This will fetch all the configuration files and then, if this part was succesful, it 
# will call parseConfig to build the temporary configuration file from which both Main
# and BgMonitor will read the destinations
sub refreshConfig {
	my $this = shift;
	my $wasSuccess = shift || 0;

	if(! $this->{DEST_LOCATIONS} || (! @{$this->{DEST_LOCATIONS}})){
		logger("NOTICE", "No configuration file was given.");
		return $wasSuccess;
	}
	my ($error, $linesRef);
	logger("DEBUG", "Refreshing config from pid $$");
	($error, $linesRef) = $this->fetchConfig(@{$this->{DEST_LOCATIONS}});
	if(! $error){ # or ($error and ($wasSuccess < @$linesRef))){
		# it reading destinations worked ok, or if we had a partial error reading files,
		# but the configuration size is bigger than earlier, apply those new changes
		$wasSuccess = @$linesRef;
		$this->parseConfig($linesRef);
	}else{
		logger("WARNING", "Failed reading destination files/urls. Configuration will remain unchanged.");
	}
	return $wasSuccess;
}
	
# fetch the configuration form all given files/URLs. It returns a pair ($error, $linesRef) where
# $error contains the number of locations from where the retrieval of the configuration failed.
# $linesRef is a reference to an array containing all the lines.
sub fetchConfig {
        my ($this, @dests) = @_;

        my @lines = ();
        my $error = 1;
        for my $dest (@dests){
                if ( $dest =~ /^http:\/\// ) {
                        logger("INFO", "Reading config from url: $dest");
			require LWP::UserAgent;
                        my $ua = LWP::UserAgent->new();
                        $ua->timeout(5);
                        $ua->env_proxy();
                        my $response = $ua->get($dest);
                        if($response->is_success){
                                push(@lines, split("\n", $response->content . "\nEND_PART\n"));
				$error = 0;
                        }else{
                                logger("WARNING", "Error reading url: $dest");
				logger("WARNING", "Got: ".$response->status_line);
                        }
                }else{
                        logger("INFO", "Reading config from file: $dest");
                        if(open(INFILE, "<$dest")){
                                my @newlines = <INFILE>;
                                push(@lines, @newlines);
                                close(INFILE);
                                push(@lines, split("\n", "\nEND_PART\n"));
				$error = 0;
                        }else{
                                logger("WARNING", "Error reading file: $dest");
                        }
                }
        }
        return ($error, \@lines);
}

# This will parse the config lines brought by fetchConfig, creating the local temporary config file
sub parseConfig {
        my ($this, $linesRef) = @_;

        my @lines = @$linesRef;
        my @dests = ();
        my %opts = ();
		
	my $prevDest = $this->{DESTINATIONS};
	$this->{DESTINATIONS} = {};
        for my $line (@lines) {
                chomp $line;
                next if $line =~ /^\s*$/;       # skip empty lines
                next if $line =~ /^\s*#/;       # skip comments
                $line =~ s/\s+/ /g;             # eliminate multiple spaces
                $line =~ s/^ //;                # remove space at the beginning
                $line =~ s/ $//;                # remove space at the end
                if($line =~ /^xApMon_(.*)/){
                        # set an option for the current destinations
                        my $opt = $1;
                        if($opt =~ /(\S+)\s?=\s?(\S+)/){
                                my ($name, $value) = ($1, $2);
                                $value = $value =~ /off/i ? 0 : $value;
                                $value = $value =~ /on/i ? 1 : $value;
                                $opts{$name} = $value;
				$this->setLogLevel($value) if $name eq "loglevel";
				$this->setMaxMsgRate($value) if $name eq "maxMsgRate";
                                $this->{CONF_RECHECK} = $value if $name eq "conf_recheck";
                                $this->{CONF_CHECK_INTERVAL} = $value if $name eq "recheck_interval";
                                #logger("DEBUG", "set option $name <- $value");
                        }
                }elsif($line =~ /END_PART/){
                        #logger("DEBUG", "Storing options into the temp conf file");
                        for my $dest (@dests){
				my %optsCopy = %opts;
				$this->{DESTINATIONS}->{$dest}->{OPTS} = \%optsCopy;
				ApMon::Common::updateLastSentTime($prevDest->{$dest}->{OPTS}, $this->{DESTINATIONS}->{$dest}->{OPTS});
				$this->{DESTINATIONS}->{$dest}->{PREV_RAW_DATA} = 
					($prevDest->{$dest}->{PREV_RAW_DATA} ? $prevDest->{$dest}->{PREV_RAW_DATA} : {});
                        }
                        @dests = ();
                        %opts = ();
                }else{
                        # parse a new destination
			my $dest = $this->parseDestination($line);
                        push(@dests, $dest) if $dest;
                }
        }
	$this->writeDestinations();
}

# Write the destinations to the temporary config file, to be able to get them also from the
# other processes
sub writeDestinations {
	my $this = shift;

        if(open(CONF, ">".$this->{CONF_FILE}.".tmp")) {
        	logger("DEBUG", "Writting config to $this->{CONF_FILE}");
		my ($dest, $opt, $val);
	        for $dest (keys %{$this->{DESTINATIONS}}) {
			print CONF "$dest\n";
			for $opt (keys %{$this->{DESTINATIONS}->{$dest}->{OPTS}}) {
				$val = $this->{DESTINATIONS}->{$dest}->{OPTS}->{$opt};
				print CONF " $opt=$val\n";
			}
		}
		close(CONF);
		chmod(0600, $this->{CONF_FILE}.'.tmp');
	        # this is done in order to keep the interference between the processes as small as possible
        	rename($this->{CONF_FILE}.'.tmp', $this->{CONF_FILE});
	}else{
		logger("ERROR", "Cannot write destinations to file $this->{CONF_FILE}");
	}
}

# Given a destination line (i.e. host[:port][ passwd]), it returns an array containing at most one
# string of the following form: "ip:port:passwd"
# This is what will be used as a destination in sending the directSendParameters.
sub parseDestination {
        my ($this, $line) = @_;

        my $dest = "";
        if($line =~ /([\.\-a-zA-Z0-9]+)\s*:?\s*(\d+)?\s*(.*)?/){
                my ($host, $port, $pass) = ($1, $2, $3);
                $port = (! defined($port) || $port eq "") ? $APMON_DEFAULT_PORT : $port;
                my ($name,$aliases,$type,$len,$addr) = gethostbyname($host);
                if (defined($len) and $len == 4) {
                        my $ip = inet_ntoa($addr);
                        logger("DEBUG", "found destination i=$ip, P=$port, p=$pass");
                        $dest = "$ip:$port:$pass";
                }else{
                        logger("WARNING", "Error resolving host $host");
                }
        }
        return $dest;
}

# This will parse the options sent by functions in ApMon
sub parseParentMessage {
	my ($this, $msg) = @_;
	my @msgs = split(/\n/, $msg);
	my @dests = ();
	logger("DEBUG", "Reading messages from user");
	for $msg (@msgs){
		$this->setLogLevel($1) if $msg =~ /loglevel:(.*)/;
		$this->setMaxMsgRate($1) if $msg =~ /maxMsgRate:(.*)/;
		$this->{CONF_RECHECK} = $1 if $msg =~ /conf_recheck:(.*)/;
		$this->{CONF_CHECK_INTERVAL} = $1 if $msg =~ /recheck_interval:(.*)/;
		push(@dests, $1) if $msg =~/dest:(.*)/;
	}
	$this->setDestinations(@dests) if @dests;
}

# Sets the log level for CONFIG_LOADER
sub setLogLevel {
        my ($this, $level) = @_;

	ApMon::Common::setLogLevel($level);
}

# Sets the maximum rate for the messages sent by user
sub setMaxMsgRate {
	my ($this, $rate) = @_;

	ApMon::Common::setMaxMsgRate($rate);
}

1;

