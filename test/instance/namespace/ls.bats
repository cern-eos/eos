# bats file_tags=ls
load ../instance

lsTestDir="/eos/$EOS_TEST_INSTANCE/test/instancetest/ls_test"
lsOptTestDir="/eos/$EOS_TEST_INSTANCE/test/instancetest/ls_option_test"

@test "set up of the ls directory" {
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p "$lsTestDir"
}

@test "ls of a directory and of a pattern" {
  # General case
  runtest "### Ls          " unix SUCCESS "" eos ls "$lsTestDir"
  runtest "### Ls          " unix SUCCESS "" eos ls "$lsTestDir | wc -l | grep 0"
  runtest "### Touch       " unix SUCCESS "" eos touch "$lsTestDir/fic1.txt"
  runtest "### Touch       " unix SUCCESS "" eos touch "$lsTestDir/fic2.txt"
  runtest "### Touch       " unix SUCCESS "" eos touch "$lsTestDir/fic3.txt"
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir "$lsTestDir/dir1"
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir "$lsTestDir/dir2"
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir "$lsTestDir/dir3"
  runtest "### Ls          " unix SUCCESS "" eos ls "$lsTestDir/ | wc -l | grep 6"
  runtest "### Ls          " unix SUCCESS "" eos ls "$lsTestDir/fic*.txt | wc -l | grep 3"
  runtest "### Ls          " unix ERROR "" eos ls -N "$lsTestDir/fic*.txt"
}

@test "ls with globbing characters" {
  # Globbing cases
  runtest "### Touch       " unix SUCCESS "" command eos touch "$lsTestDir/regexx*p.tt\.ern"
  runtest "### Touch       " unix SUCCESS "" command eos touch "$lsTestDir/"'number{1..3}test.txt'
  runtest "### Ls          " unix SUCCESS "" eos ls "$lsTestDir/number\{1..3\}test.txt | wc -l | grep 1"
  runtest "### Ls          " unix ERROR "" command eos ls "$lsTestDir/regexx*p.tt\.ern"
  runtest "### Ls          " unix SUCCESS "" command eos ls --no-globbing "$lsTestDir/regexx*p.tt\.ern"
  runtest "### Ls          " unix SUCCESS "" command eos ls -N "$lsTestDir/regexx*p.tt\.ern"
}

@test "each short flag reaches the MGM" {
  # Command line flags. The console rebuilds the 'mgm.option' string from the
  # flags it parses, so a flag which is unknown to the client is silently dropped
  # and never reaches the MGM. This is what happened to '-h' and made
  # 'eos ls -alhrt' report raw byte counts instead of readable sizes.
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p "$lsOptTestDir"
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir "$lsOptTestDir/subdir"
}

@test "sizes are scaled by 1000, from 10k on" {
  # Sizes are scaled by a factor of 1000 and only starting at 10k, see
  # eos::common::StringConversion::GetReadableSizeString
  runtest "### Create      " unix SUCCESS "" shell "dd if=/dev/zero of=$EOS_TEST_TESTSYS/ls_file2M bs=1000 count=2000"
  runtest "### Create      " unix SUCCESS "" shell "dd if=/dev/zero of=$EOS_TEST_TESTSYS/ls_file15k bs=1000 count=15"
  runtest "### Create      " unix SUCCESS "" shell "dd if=/dev/zero of=$EOS_TEST_TESTSYS/ls_file5k bs=1000 count=5"
  runtest "### Upload      " unix SUCCESS "" upload "$EOS_TEST_TESTSYS/ls_file2M" "$lsOptTestDir/file2M"
  runtest "### Upload      " unix SUCCESS "" upload "$EOS_TEST_TESTSYS/ls_file15k" "$lsOptTestDir/file15k"
  runtest "### Upload      " unix SUCCESS "" upload "$EOS_TEST_TESTSYS/ls_file5k" "$lsOptTestDir/file5k"
}

@test "-h gives readable sizes in each flag group" {
  # '-h' has to give readable sizes in whatever short flag group it appears
  runtest "### Ls -lh      " unix SUCCESS "" eos ls "-lh $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -hl      " unix SUCCESS "" eos ls "-hl $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -alh     " unix SUCCESS "" eos ls "-alh $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -lha     " unix SUCCESS "" eos ls "-lha $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -alhrt   " unix SUCCESS "" eos ls "-alhrt $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -lhrt    " unix SUCCESS "" eos ls "-lhrt $lsOptTestDir | grep file15k | grep -q '15\.00 k'"
}

@test "Below 10k the size is reported unscaled, with and without '-h'" {
  # Below 10k the size is reported unscaled, with and without '-h'
  runtest "### Ls -lh      " unix SUCCESS "" eos ls "-lh $lsOptTestDir | grep file5k | grep -q ' 5000 '"
}

@test "readable sizes for one file and for flags after the path" {
  # Readable sizes for a single file and for flags given after the path
  runtest "### Ls -lh      " unix SUCCESS "" eos ls "-lh $lsOptTestDir/file2M | grep -q '2\.00 M'"
  runtest "### Ls -lh      " unix SUCCESS "" eos ls "$lsOptTestDir -lh | grep file2M | grep -q '2\.00 M'"
}

@test "Without '-h' the exact number of bytes has to be reported" {
  # Without '-h' the exact number of bytes has to be reported
  runtest "### Ls -l       " unix SUCCESS "" eos ls "-l $lsOptTestDir | grep file2M | grep -q ' 2000000 '"
  runtest "### Ls -l       " unix ERROR   "" eos ls "-l $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
}

@test "-r and -t do not swallow the other flags" {
  # '-r' and '-t' are accepted for convenience, they must not swallow the
  # remaining flags of the command line
  runtest "### Ls -lrt     " unix SUCCESS "" eos ls "-lrt $lsOptTestDir | grep file2M | grep -q ' 2000000 '"
}

@test "-h alone as a flag gives readable sizes" {
  # '-h' given as a flag on its own has to be understood as a readable size
  runtest "### Ls -a-l-h   " unix SUCCESS "" eos ls "-a -l -h -r -t $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -l -h    " unix SUCCESS "" eos ls "-l -h $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
  runtest "### Ls -h -l    " unix SUCCESS "" eos ls "-h -l $lsOptTestDir | grep file2M | grep -q '2\.00 M'"
}

@test "ls -h alone and --help print the usage" {
  # 'ls -h' alone keeps asking for the usage, '--help' always does. Help is
  # printed to stderr by the common console help page and exits successfully.
  runtest "### Ls -h       " unix SUCCESS "" shell "command eos -b ls -h 2>&1 | grep -q 'ls \[OPTIONS\] \[path\.\.\.\]'"
  runtest "### Ls --help   " unix SUCCESS "" shell "command eos -b ls --help 2>&1 | grep -q 'ls \[OPTIONS\] \[path\.\.\.\]'"
  runtest "### Ls --help   " unix SUCCESS "" shell "command eos -b ls --help $lsOptTestDir 2>&1 | grep -q 'ls \[OPTIONS\] \[path\.\.\.\]'"
  runtest "### Ls -h       " unix SUCCESS "" shell "command eos -b ls -h 2>&1 | grep -qx 'USAGE'"
}

@test "'-h' must not silently imply '-l'" {
  # '-h' must not silently imply '-l'
  runtest "### Ls -ah      " unix ERROR   "" eos ls "-ah $lsOptTestDir | grep -qE '^[-d][rwxst+-]{9} '"
}

@test "'-a' shows the hidden entries" {
  # '-a' shows the hidden entries
  runtest "### Touch       " unix SUCCESS "" eos touch "$lsOptTestDir/.hidden_file"
  runtest "### Ls          " unix ERROR   "" eos ls "$lsOptTestDir | grep -q hidden_file"
  runtest "### Ls -a       " unix SUCCESS "" eos ls "-a $lsOptTestDir | grep -q hidden_file"
}

@test "'-d' lists the directory itself instead of its content" {
  # '-d' lists the directory itself instead of its content
  runtest "### Ls -d       " unix SUCCESS "" eos ls "-d $lsOptTestDir | grep -qx ls_option_test"
  runtest "### Ls -ld      " unix SUCCESS "" eos ls "-ld $lsOptTestDir | wc -l | grep -qx 1"
  runtest "### Ls -ld      " unix SUCCESS "" eos ls "-ld $lsOptTestDir | grep -q '^d'"
}

@test "'-F' appends a '/' to the directories" {
  # '-F' appends a '/' to the directories
  runtest "### Ls -F       " unix SUCCESS "" eos ls "-F $lsOptTestDir | grep -qx subdir/"
}

@test "'-i' prepends the inode information" {
  # '-i' prepends the inode information
  runtest "### Ls -li      " unix SUCCESS "" eos ls "-li $lsOptTestDir | grep file2M | grep -qE '^[0-9]+ '"
}

@test "-c adds a checksum column and implies -l" {
  # '-c' prepends a 34 characters wide checksum column and implies '-l'
  runtest "### Ls -c       " unix SUCCESS "" eos ls "-c $lsOptTestDir | grep file2M | grep -qE '^.{34}[-d][rwxst+-]{9} '"
}

@test "'-n' keeps the user and group ids numerical" {
  # '-n' keeps the user and group ids numerical
  runtest "### Ls -ln      " unix SUCCESS "" eos ls "-ln $lsOptTestDir | grep file2M | grep -qE '^[-d][rwxst+-]{9} +[0-9]+ +[0-9]+ +[0-9]+ '"
}

@test "'-y' reports the backend status and implies '-l'" {
  # '-y' reports the backend status and implies '-l'
  runtest "### Ls -y       " unix SUCCESS "" eos ls "-y $lsOptTestDir | grep file2M | grep -qE '[-d][rwxst+-]{9} '"
}

@test "-s checks that the directory exists, without a listing" {
  # '-s' only checks the existence of the directory without listing it
  runtest "### Ls -s       " unix SUCCESS "" eos ls "-s $lsOptTestDir"
  runtest "### Ls -s       " unix ERROR   "" eos ls "-s $lsOptTestDir | grep -q ."
  runtest "### Ls -s       " unix ERROR   "" eos ls "-s $lsOptTestDir/does_not_exist"
}

@test "clean up of the ls tests" {
  # Cleanup everything
  runtest "### Cleanup     " unix SUCCESS "" eos rm -rF $lsOptTestDir/
  runtest "### Cleanup     " unix SUCCESS "" shell "rm -f $EOS_TEST_TESTSYS/ls_file2M $EOS_TEST_TESTSYS/ls_file15k $EOS_TEST_TESTSYS/ls_file5k"
  runtest "### Cleanup     " unix SUCCESS "" eos rm -rF $lsTestDir/
}
