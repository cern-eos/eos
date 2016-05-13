package ApMon::XDRUtils;

use strict;
use warnings;

my $XDR_STRING = 0;
my $XDR_INT32 = 2;
my $XDR_REAL64 = 5;

my $MAX_INT = 1<<31;

# Encode a set of parameters in the following format:
# |clusterName | nodeName | time | #params |
# | paramName | paramType | paramValue| x #params
# and time, if != -1
sub encodeParameters {
	my ($clusterName, $nodeName, $time, @params) = @_;

	my $encParams = "";
	for(my $i = 0; $i < $#params; $i += 2){
		$encParams .= encodeParameter($params[$i], $params[$i+1]);
	}
	my $encTime = $time == -1 ? "" : encodeINT32($time);
	return encodeString($clusterName) . encodeString($nodeName) . 
	       encodeINT32(@params/2) . $encParams . $encTime;
}

# Encode a parameter pair (paramName, paramValue)
sub encodeParameter {
	my ($name, $value) = @_;
	
	my $type = getType($value);
	my $encValue;
	if ($type == $XDR_INT32) {
		$encValue = encodeINT32($value);
	} elsif ($type == $XDR_REAL64) {
		$encValue = encodeREAL64($value);
	} else {
		$encValue = encodeString($value);
	}
	return encodeString($name).encodeINT32($type).$encValue;
}

# Return the type for a given value (XDR_INT32, XDR_REAL64, XDR_STRING)
sub getType {
        $_ = shift;
	
	return $XDR_INT32 if(/^[+-]?\d+$/ && (abs($_) < $MAX_INT));
	return $XDR_REAL64 if /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/;
	return $XDR_STRING;
}

# Encode a string in XDR format
sub encodeString {
	my $str = shift;
	
	my $enc = encodeINT32(length($str));
	while (length($str) % 4 != 0){
		$str .= "\0";
	}
	return $enc.$str;
}

# Encode a 32 bit signed integer in XDR format
sub encodeINT32 {
	my $val = shift;
	
	return pack("N", int($val));
}

# Encode a 64 bit double in XDR format
sub encodeREAL64 {
	my $val = shift;
	
	my $end = verifyEndian();
	if ($end == 0) {
		return reverse(pack("d",$val));
	} else {
		return pack("d",$val);
	}
}

# Verify if machine is big-endian or little-endian
sub verifyEndian {
	my $foo = pack("s2",1,2);
	if ($foo eq "\1\0\2\0" ) {
		return 0;
	} elsif ( $foo eq "\0\1\0\2" ) {
		return 1;
	}
}

1;

