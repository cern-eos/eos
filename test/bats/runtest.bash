# ------------------------------------------------------------------------------
# runtest of eos-instance-test for BATS: a converted test keeps its runtest
# lines, each in an @test, and the test fails as the old line did. Load it
# after eos.bash:
#
#   load ../bats/runtest
#
#   @test "rm of a stall rule" {
#     runtest "### Access" unix IGNORE "" eos "access rm stall r"
#   }
#
# runtest [H] <info> <auth> <expect> <env> <mode> <args> ...
#   H         use $EOS_TEST_FULL_REDIRECTOR instead of $EOS_TEST_REDIRECTOR
#   <info>    a description, only for the output
#   <auth>    unix, sss, krb5 or gsi
#   <expect>  SUCCESS (exit code 0), ERROR (not 0), IGNORE, or an exit code
#   <env>     assignments for the command, e.g. "EOS_APP=x"
#   <mode>    the command: a function of the suite (eos, meta, upload ...) or
#             a program
# As in eos-instance-test, the mode and its arguments are split again into
# words ($*): runtest ... eos "access rm stall r" runs eos access rm stall r.
# ------------------------------------------------------------------------------
runtest() {
  local redir=${EOS_TEST_REDIRECTOR-} info auth expect renv ret=0

  if [ "$1" = H ]; then
    EOS_TEST_REDIRECTOR=${EOS_TEST_FULL_REDIRECTOR-}
    shift
  fi

  info=$1 auth=$2 expect=$3 renv=$4
  shift 4
  echo "# $info: $*"

  case $auth in
    krb5) ( eval export X509_USER_CERT=/tmp/illegal X509_USER_KEY=/tmp/illegal $renv; $* ) || ret=$? ;;
    gsi)  ( eval export KRB5CCNAME=/tmp/illegal $renv; $* ) || ret=$? ;;
    unix) ( eval export XrdSecPROTOCOL=unix $renv; $* ) || ret=$? ;;
    sss)  ( eval export XrdSecPROTOCOL=sss $renv; $* ) || ret=$? ;;
    *)    echo "runtest: unknown auth $auth"; ret=2; expect=0 ;;
  esac

  EOS_TEST_REDIRECTOR=$redir

  case $expect in
    SUCCESS) [ $ret = 0 ] || { echo "expected success, got $ret"; return 1; } ;;
    ERROR)   [ $ret != 0 ] || { echo "expected an error, got 0"; return 1; } ;;
    IGNORE)  ;;
    *)       [ $ret = "$expect" ] || { echo "expected $expect, got $ret"; return 1; } ;;
  esac
}
