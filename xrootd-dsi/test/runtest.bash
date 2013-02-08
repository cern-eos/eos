#!/bin/bash

#### USER PART TO SET UP #######
GRIDFTP_SRV="gadde-slc6.cern.ch"
USERNAME="gadde"      # username for gridftp
TEST_DIR="/tmp/test"  # should be rwx for the above user
GFTP_CLIENT="uberftp" # gridftp client, only uberftp is supported
CKS_TYPE="adler32"     
EOS=0


function cks_func { # should stdout the checksum of the file given ad the only input 
if [ ${EOS} -eq 1 ]; then
  eos-adler32 $1 | grep -o adler32=.* | cut -d = -f 2 |  tr -cd [:xdigit:]
else
  xrdadler32 $1 | cut -d \  -f 1 |  sed -e "s/^ \{1,\}//" | tr -cd [:xdigit:]
fi;
}
##############################


#### USER OPTIONAL PART ######
TMP_FILE="/tmp/gridftp_test_$$.log"
TEST_FILE="/tmp/gridftp_test_$$.test"
TEST_FILENAME=`basename ${TEST_FILE}`

GFTP_CALL_PREFIX="${GFTP_CLIENT} -u ${USERNAME} ${GRIDFTP_SRV}"
GFTP_CALL_SUFFIX="&>${TMP_FILE}"
#############################



function TestCmd {
   local cmd="\"$1\""
   local expected=$2
   echo -n "Executing: ${cmd} -> "
   eval ${GFTP_CALL_PREFIX} ${cmd} ${GFTP_CALL_SUFFIX}
   local retval=$?
   echo -n returned ${retval}
   if [ ${expected} -ne ${retval} ]; then
     echo " error!"
     cat ${TMP_FILE}
     exit $?
   fi;
   echo " as expected"
   return $?
}

echo "*** Test command cd ***"
TestCmd "cd ${TEST_DIR}" 0
echo "*** Test stat ***"
TestCmd "ls ${TEST_DIR}" 0
echo "*** Generate test file ***"
dd if=/dev/urandom of=${TEST_FILE} bs=1M count=10 &> /dev/null
echo "*** Test receive ***"
TestCmd "put ${TEST_FILE} ${TEST_DIR}" 0
echo "*** Test send ***"
TestCmd "get ${TEST_DIR}/${TEST_FILENAME} ${TEST_FILE}.copy" 0
echo "*** Test roundtrip integrity ***"
diff ${TEST_FILE} ${TEST_FILE}.copy
if [ $? -ne 0 ]; then
   echo Error
fi;
echo "*** Test checksum ***"
TestCmd "quote CKSM adler32 0 128 ${TEST_DIR}/${TEST_FILENAME}" 0
if [ ${EOS} -eq 1 ]; then
  rcksm=`grep "213 " ${TMP_FILE}  | cut -d / -f 2   |  tr -cd [:xdigit:] `
else
  rcksm=`grep "213 " ${TMP_FILE}  | cut -d ":" -f 2 |  tr -cd [:xdigit:] `
fi;
echo "remote checksum is ${rcksm}"
lcksm=`cks_func ${TEST_FILE}`
echo "local  checksum is ${lcksm}"
if [ "${rcksm}" != "${lcksm}"  ]; then
   echo Error
fi;
echo "*** Test mkdir ***"
TestCmd "mkdir ${TEST_DIR}/tmpdir" 0
TestCmd "ls ${TEST_DIR}/tmpdir" 0
echo "*** Test rmdir ***"
TestCmd "rmdir ${TEST_DIR}/tmpdir" 0
TestCmd "ls ${TEST_DIR}/tmpdir" 1
#echo "Test rename"
#TestCmd "rename ${TEST_DIR}/${TEST_FILENAME} ${TEST_DIR}/${TEST_FILENAME}.newname" 0
#TestCmd "ls ${TEST_DIR}/${TEST_FILENAME}" 1
#TestCmd "ls ${TEST_DIR}/${TEST_FILENAME}.newname" 0
#TestCmd "rename ${TEST_DIR}/${TEST_FILENAME}.newname ${TEST_DIR}/${TEST_FILENAME}" 0

#rm -f ${TMP_FILE} ${TEST_FILE}

