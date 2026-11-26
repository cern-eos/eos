# bats file_tags=wfe
load ../instance

WFEDIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"

@test "prepare triggers the workflow, with the p ACL" {
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=on"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Touch     " unix SUCCESS "" eos touch "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe/test1.dat"
  runtest "### Prepare   " unix SUCCESS "" meta prepare "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe/test1.dat\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID} | wc -l | grep -w 0"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "755" "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:${NOBODY_UID}:p "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.sync::prepare.default=test "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Prepare   " unix SUCCESS "" meta prepare "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe/test1.dat\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:${NOBODY_UID}:p "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Prepare   " unix SUCCESS "" meta prepare "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe/test1.dat\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Attr      " unix SUCCESS "" eos attr rm sys.acl "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe"
  runtest "### Prepare   " unix SUCCESS "" meta prepare "/eos/$EOS_TEST_INSTANCE/test/instancetest/wfe/test1.dat\?eos.ruid=0\&eos.rgid=0"
}

@test "async events of an unknown action end in g/" {
  # Async events. A 'noop' action is an unknown method, its job ends in g/.
  # Workflow names are unique per run, entries of earlier runs stay for keepTIME.
  WFE_NTX=$(command eos -b root://$EOS_TEST_REDIRECTOR space status default 2>/dev/null | awk '$1 == "wfe.ntx" {print $3}')
  WFE_MAXPENDING=$(command eos -b root://$EOS_TEST_REDIRECTOR space status default 2>/dev/null | awk '$1 == "wfe.maxpending" {print $3}')
  # queued in q/, dispatched and moved to g/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.async$$=noop "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" async$$ asynctest 1
  runtest "### Dispatch  " unix SUCCESS "" wait_for 60 wfe_count g async$$ 1
  # paused: recorded in q/, dispatched only once the engine is on again
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=paused"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.pause$$=noop "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" pause$$ asynctest 1
  runtest "### Queued    " unix SUCCESS "" wfe_count q pause$$ 1
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=on"
  runtest "### Dispatch  " unix SUCCESS "" wait_for 60 wfe_count g pause$$ 1
  # a backlog larger than wfe.ntx is fully dispatched as job slots free up
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=paused"
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe.ntx=1"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.many$$=noop "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" many$$ asynctest 20
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=on"
  runtest "### Dispatch  " unix SUCCESS "" wait_for 60 wfe_count g many$$ 20
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe.ntx=${WFE_NTX:-0}"
  # an entry removed from q/ before dispatch is skipped and the engine goes on
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=paused"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.gone$$=noop "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" gone$$ asynctest 1
  runtest "### Rm        " unix SUCCESS "" wfe_rm q gone$$
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=on"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" gone$$ asynctest 1
  runtest "### Dispatch  " unix SUCCESS "" wait_for 60 wfe_count g gone$$ 1
  # above wfe.maxpending entries stay only in q/ and are loaded by later scans
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=paused"
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe.maxpending=4"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.cap$$=noop "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" cap$$ asynctest 10
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=on"
  runtest "### Dispatch  " unix SUCCESS "" wait_for 120 wfe_count g cap$$ 10
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe.maxpending=${WFE_MAXPENDING:-10000000}"
}

@test "EAGAIN retries through e/, then f/ after retry.max" {
  # EAGAIN from a bash action: retried through e/ with a due time in the future,
  # then moved to f/ once retry.max is reached
  runtest "### Attr      " unix SUCCESS "" eos attr set "'sys.workflow.asynctest.retry$$=bash:shell:wfetest exit 11'" "$WFEDIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.retry$$.retry.max=1 "$WFEDIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.workflow.asynctest.retry$$.retry.delay=1 "$WFEDIR"
  runtest "### Workflow  " unix SUCCESS "" wfe_trigger "$WFEDIR" retry$$ asynctest 1
  runtest "### Retry     " unix SUCCESS "" wait_for 60 wfe_count f retry$$ 1
  runtest "### Retry     " unix SUCCESS "" shell "command eos -b root://$EOS_TEST_REDIRECTOR attr get sys.wfe.retry \$(wfe_entries f retry$$) | grep -w 1"
  runtest "### Space     " unix SUCCESS "" eos space config "default space.wfe=off"
}
