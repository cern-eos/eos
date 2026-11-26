# bats file_tags=cgi_vs_proto_file
load ../instance

@test "each eos file subcommand works over CGI and protobuf" {
  # Drives every 'eos file' subcommand over both the legacy opaque-CGI protocol
  # and the Protobuf CLI, so an old client talking to an upgraded MGM stays
  # covered.
  runtest "### eos-file-dual-protocol " unix SUCCESS "" eos_file_dual_protocol "/eos/$EOS_TEST_INSTANCE/test/instancetest/filedual"
}
