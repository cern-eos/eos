# bats file_tags=mirage
load ../instance

@test "the FST synthetic IO bench writes and reads" {
  runtest "### Replica   " unix SUCCESS "" eos_mirage
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/network"
}

@test "eos health reports the group default.0" {
  # HEALTH
  runtest "### Health    " unix SUCCESS "" eos health "-m | grep \"default\.0\""
}
