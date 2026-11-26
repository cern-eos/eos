# bats file_tags=rain
load ../instance

@test "RAIN layouts write, read and recover" {
  NUM_FST=$(command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR fs ls | awk '{ if (($6 ~ /booted/) || ($7 ~ /booted/)) c++} END {print c}')
  if [ $NUM_FST -ge 6 ]; then
     # Create temporary RAIN test file outside EOS
     export EOS_TEST_RAIN_TMP=/tmp/rain_test
     mkdir -p $EOS_TEST_RAIN_TMP

     # Create the RAIDDP directory and set the proper attributes
     runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blockchecksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blocksize=1M "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=raiddp "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"

     # Create the RAID6 directory and set the proper attributes
     runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blockchecksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blocksize=1M "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=raid6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"

     # Create the RAID6 directory with 4MB stripe block size
     runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blockchecksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blocksize=4M "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=raid6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M"

     # Qrain test (4 parity stripes )
     runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blockchecksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blocksize=1M "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=qrain "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=6 "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"

     runtest "### RaiddpTest " unix SUCCESS "" eos_rain "raiddp" "$EOS_TEST_RAIN_DIR" "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp" "$EOS_TEST_RAIN_TMP"
     # eos-rain-test switches file systems off and on again: wait until all are back
     runtest "### FS ready   " unix SUCCESS "" wait_for 60 all_fs_ready
     runtest "### Raid6Test  " unix SUCCESS "" eos_rain "raid6" "$EOS_TEST_RAIN_DIR" "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6" "$EOS_TEST_RAIN_TMP"
     runtest "### FS ready   " unix SUCCESS "" wait_for 60 all_fs_ready
     runtest "### QrainTest  " unix SUCCESS "" eos_rain "qrain" "$EOS_TEST_RAIN_DIR" "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain" "$EOS_TEST_RAIN_TMP"
     runtest "### FS ready   " unix SUCCESS "" wait_for 60 all_fs_ready

     # Verify that parallel writers to the same EC file being created are blocked
     runtest "### ECParallelWr" unix SUCCESS "" eos_ec_parallel "/eos/$EOS_TEST_INSTANCE/test/instancetest/ec_parallel"

     # Run the eos-io-test for sequential and non-streaming IO against the RAIN
     # directories
     runtest "### EosIoRaiddp  " unix SUCCESS "" eos_io "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp" "$EOS_TEST_RAIN_DIR"
     runtest "### EosIoRaid6   " unix SUCCESS "" eos_io "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6" "$EOS_TEST_RAIN_DIR"
     runtest "### EosIoQrain   " unix SUCCESS "" eos_io "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain" "$EOS_TEST_RAIN_DIR"

     # Run xrdcpnonstreaming test to make sure the final file checksum which
     # requires re-reading the entire file for computation is correct
     TESTFILE_NONSTREAMING=$EOS_TEST_TESTSYS/cp_non_streaming.dat
     dd if=/dev/urandom of=$TESTFILE_NONSTREAMING bs=1M count=40 > /dev/null 2>&1
     CKS_NONSTREAMING=`eos-checksum crc32c $TESTFILE_NONSTREAMING | awk '{print $4}' | sed s/crc32c=//`
     runtest "### NStreamDp     " unix SUCCESS "" cp_nonstreaming  $TESTFILE_NONSTREAMING "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp/eos_non_stream.dat"
     eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp/eos_non_stream.dat"
     runtest "### XsRaiddp      " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp/eos_non_stream.dat | jq -r '.checksumvalue' | grep $CKS_NONSTREAMING"
     runtest "### NStream6      " unix SUCCESS "" cp_nonstreaming $TESTFILE_NONSTREAMING "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6/eos_non_stream.dat"
     runtest "### XsRaid6       " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6/eos_non_stream.dat | jq -r '.checksumvalue' | grep $CKS_NONSTREAMING"
     runtest "### NStream6_4M   " unix SUCCESS "" cp_nonstreaming $TESTFILE_NONSTREAMING "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M/eos_non_stream.dat"
     runtest "### XsRaid6_4M    " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M/eos_non_stream.dat | jq -r '.checksumvalue' | grep $CKS_NONSTREAMING"
     runtest "### NStreamQ      " unix SUCCESS "" cp_nonstreaming $TESTFILE_NONSTREAMING "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain/eos_non_stream.dat"
     runtest "### XsQrain       " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain/eos_non_stream.dat | jq -r '.checksumvalue' | grep $CKS_NONSTREAMING"

     # Run simple test to check the RAIN PIO mode which is by default enabled when doing eoscp
     runtest "### eoscp pio raiddp      " unix SUCCESS "" shell eoscp "root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp/eos_non_stream.dat" "$EOS_TEST_RAIN_TMP/pio_raiddp_check.dat"
     runtest "### eoscp pio xs raiddp   " unix SUCCESS "" shell eos-checksum crc32c "$EOS_TEST_RAIN_TMP/pio_raiddp_check.dat" "| grep $CKS_NONSTREAMING"
     runtest "### eoscp pio raid6       " unix SUCCESS "" shell eoscp "root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/raid6/eos_non_stream.dat" "$EOS_TEST_RAIN_TMP/pio_raid6_check.dat"
     runtest "### eoscp pio xs raid6    " unix SUCCESS "" shell eos-checksum crc32c "$EOS_TEST_RAIN_TMP/pio_raid6_check.dat" "| grep $CKS_NONSTREAMING"
     runtest "### eoscp pio raid6_4M    " unix SUCCESS "" shell eoscp "root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/raid6_4M/eos_non_stream.dat" "$EOS_TEST_RAIN_TMP/pio_raid6_4M_check.dat"
     runtest "### eoscp pio xs raid6_4M " unix SUCCESS "" shell eos-checksum crc32c "$EOS_TEST_RAIN_TMP/pio_raid6_4M_check.dat" "| grep $CKS_NONSTREAMING"
     runtest "### eoscp pio qrain       " unix SUCCESS "" shell eoscp "root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/qrain/eos_non_stream.dat" "$EOS_TEST_RAIN_TMP/pio_qrain_check.dat"
     runtest "### eoscp pio xs qrain    " unix SUCCESS "" shell eos-checksum crc32c "$EOS_TEST_RAIN_TMP/pio_qrain_check.dat" "| grep $CKS_NONSTREAMING"

     # Clean up the files used for the RAIN test
     runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/raiddp"
     runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/raid6"
     runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/qrain"
     rm -rf $EOS_TEST_RAIN_TMP
  else
    echo "Can not run RAIN test - not enough FSTs!"
  fi
}

@test "eoscp-rain-test on a plain file" {
  # Create PLAIN directory and set the attributes for the eoscp-rain-test
  # Add a file used for testing - this test can be done with just one FST
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blockchecksum=crc32c "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.blocksize=1M "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=plain "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
  runtest "### Upload    " unix SUCCESS "" upload "$EOS_TEST_RAIN_DIR/file3.rain" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir/file.dat"
  runtest "### EoscpRaiddp " unix SUCCESS "" eoscp_rain "raiddp" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir/file.dat"
  runtest "### EoscpRaid6  " unix SUCCESS "" eoscp_rain "reeds" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir/file.dat"
  runtest "### Cleanup     " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp_rain_dir"
}
