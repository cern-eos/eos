# bats file_tags=grpc
load ../instance

@test "eos-grpc-test and eos-grpc-file-test" {
  runtest "### eos-grpc  " unix SUCCESS "" eos_grpc "/eos/$EOS_TEST_INSTANCE/test/instancetest/grpc.dir/" ${EOS_TEST_FULL_REDIRECTOR}
  runtest "### eos-grpc-file " unix SUCCESS "" eos_grpc_file "/eos/$EOS_TEST_INSTANCE/test/instancetest/grpcfile.dir/" ${EOS_TEST_FULL_REDIRECTOR}
}
