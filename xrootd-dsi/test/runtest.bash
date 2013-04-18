#!/bin/bash

#### USER PART TO SET UP #######
GRIDFTP_SRV="machine.domain.country"
USERNAME="username"          # username for gridftp
TEST_DIR="/tmp/gridftptest"  # should be rwx for the above user
GFTP_CLIENT="uberftp"        # gridftp client, only uberftp is supported
CKS_TYPE="adler32"     
#EOS=1
EOS=0
STRESSTEST_SIZE=100          # number of operations for the stress test

function cks_func { # should stdout the checksum of the file given as the only input 
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
GFTP_CALL_SUFFIX=">>${TMP_FILE}"
#############################



function TestCmd {
   local cmd="\"$1\""
   local expected=$2
   echo -n "Executing: ${cmd} -> "
   echo "@@@@ ${GFTP_CALL_PREFIX} ${cmd} ${GFTP_CALL_SUFFIX} @@@@" >> ${TMP_FILE}
   eval ${GFTP_CALL_PREFIX} ${cmd} ${GFTP_CALL_SUFFIX}
   local retval=$?
   echo -n returned ${retval}
   if [ ${expected} -ne ${retval} ]; then
     echo -e "\033[31m error!\033[37m"
     cat ${TMP_FILE}
     exit $?
   fi;
   echo -e "\033[32m as expected\033[37m"
   return $?
}

rm -f ${TMP_FILE}
mkdir -p /dev/shm/gridftptest
rm -f /dev/shm/gridftptest/*

echo -e "\033[36m*** Test command cd ***\033[37m"
TestCmd "cd ${TEST_DIR}" 0
echo -e "\033[36m*** Test stat ***\033[37m"
TestCmd "ls ${TEST_DIR}" 0

echo -e "\033[36m*** Generate small test file ***\033[37m"
dd if=/dev/urandom of=${TEST_FILE} bs=1k count=10 &> /dev/null
echo -e "\033[36m*** Test DSI small receive ***\033[37m"
TestCmd "put ${TEST_FILE} ${TEST_DIR}" 0
echo -e "\033[36m*** Test DSI small send ***\033[37m"
TestCmd "get ${TEST_DIR}/${TEST_FILENAME} ${TEST_FILE}.copy" 0
echo -e "\033[36m*** Test roundtrip integrity ***\033[37m"
diff ${TEST_FILE} ${TEST_FILE}.copy
if [ $? -ne 0 ]; then
   echo -e "-> \033[31mError!\033[37m"
else
   echo -e "-> \033[32m Successful \033[37m"
fi;
echo -e "\033[36m*** Stress Test small files ***\033[37m"
# generate a lot of symbolic links to make many copies in one go
for i in $(seq 0 ${STRESSTEST_SIZE})
  do
     ln -s ${TEST_FILE} ${TEST_FILE}_$i
 done
# run the 'put' copy
TestCmd "put \"${TEST_FILE}_*\" ${TEST_DIR}" 0
# remove the symbolic links, don't remove the remote files yet because they will be used for the 'get' stress test
rm ${TEST_FILE}_*
# run the 'get' copy
TestCmd "get \"${TEST_DIR}/${TEST_FILENAME}_*\" /dev/shm/gridftptest " 0
# remove the files locally
#rm /dev/shm/gridftptest/${TEST_FILENAME}_*
# remove the files remotely
TestCmd "rm \"${TEST_DIR}/${TEST_FILENAME}_*\" " 0

echo -e "\033[36m*** Generate big test file ***\033[37m"
dd if=/dev/urandom of=${TEST_FILE} bs=1M count=10 &> /dev/null
echo -e "\033[36m*** Test DSI big receive ***\033[37m"
TestCmd "put ${TEST_FILE} ${TEST_DIR}" 0
echo -e "\033[36m*** Test DSI big send ***\033[37m"
TestCmd "get ${TEST_DIR}/${TEST_FILENAME} ${TEST_FILE}.copy" 0
echo -e "\033[36m*** Test roundtrip integrity ***\033[37m"
diff ${TEST_FILE} ${TEST_FILE}.copy
if [ $? -ne 0 ]; then
   echo -e "-> \033[31mError!\033[37m"i
else
   echo -e "-> \033[32m Successful \033[37m"
fi;
echo -e "\033[36m*** Stress Test big files ***\033[37m"
# generate a lot of symbolic links to make many copies in one go
for i in $(seq 0 ${STRESSTEST_SIZE})
  do
     ln -s ${TEST_FILE} ${TEST_FILE}_$i
 done
# run the 'put' copy
TestCmd "put \"${TEST_FILE}_*\" ${TEST_DIR}" 0
# remove the symbolic links, don't remove the remote files yet because they will be used for the 'get' stress test
rm ${TEST_FILE}_*
# run the 'get' copy
date
TestCmd "get \"${TEST_DIR}/${TEST_FILENAME}_*\" /dev/shm/gridftptest " 0
date
TestCmd "get \"${TEST_DIR}/${TEST_FILENAME}_*\" /dev/shm/gridftptest " 0
date
# remove the files locally
# rm /dev/shm/gridftptest/${TEST_FILENAME}_*
# remove the files remotely
TestCmd "rm \"${TEST_DIR}/${TEST_FILENAME}_*\" " 0
# remove the files in ram 
rm /dev/shm/gridftptest/*



echo -e "\033[36m*** Test checksum ***\033[37m"
TestCmd "quote CKSM adler32 0 128 ${TEST_DIR}/${TEST_FILENAME}" 0
if [ ${EOS} -eq 1 ]; then
  rcksm=`grep "213 " ${TMP_FILE}  | cut -d \  -f 2   |  tr -cd [:xdigit:] | tail --bytes=8`
else
  rcksm=`grep "213 " ${TMP_FILE}  | cut -d ":" -f 2 |  tr -cd [:xdigit:] | tail --bytes=8`
fi;
echo -e "remote checksum is ${rcksm}"
lcksm=`cks_func ${TEST_FILE} | tail --bytes=8`
echo -e "local  checksum is ${lcksm}"
if [ "${rcksm}" != "${lcksm}"  ]; then
   echo -e "\033[31mError\033[37m"
fi;

# clean up the local files
rm ${TEST_FILE} ${TEST_FILE}.copy
# clean up the remote file
TestCmd "rm ${TEST_DIR}/${TEST_FILENAME} " 0

echo -e "\033[36m*** Test mkdir ***\033[37m"
TestCmd "mkdir ${TEST_DIR}/tmpdir" 0
TestCmd "ls ${TEST_DIR}/tmpdir" 0
echo -e "\033[36m*** Test rmdir ***\033[37m"
TestCmd "rmdir ${TEST_DIR}/tmpdir" 0
TestCmd "ls ${TEST_DIR}/tmpdir" 1

