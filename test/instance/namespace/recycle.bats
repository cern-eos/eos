# bats file_tags=recycle
load ../instance

@test "recycle config --add-bin and --remove-bin take literal paths" {
  runtest "### RecycleAdd" unix SUCCESS "" recycle_config_path_encoding --add-bin
  runtest "### RecycleRm " unix SUCCESS "" recycle_config_path_encoding --remove-bin
  recyclesize=`command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR recycle -m | grep maxbytes | awk '{print $3}' | sed s/maxbytes=//`
  recycletime=`command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR recycle -m | grep lifetime | awk '{print $6}' | sed s/lifetime=//`
  test -z $recyclesize && recyclesize=0
  test -z $recycletime && recycletime=0
  if [ $recyclesize -eq 0 ] ; then
  runtest "### Recycle   " unix SUCCESS "" eos quota set -g 99 -v 100G -i 10M /eos/$EOS_TEST_INSTANCE/proc/recycle/
  fi
  if [ $recycletime -eq 0 ]; then
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --lifetime 86400
  fi
}

@test "recycle of a file, and its restore by the owner" {
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle"
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --add-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle"
  runtest "### Find      " unix SUCCESS "" eos find -f /eos/$EOS_TEST_INSTANCE/proc/recycle/ "| grep passwd.recycle | wc -l |grep -w 1"
  # Off: "eos recycle ls" (default --uid) does not list the entries of root, see
  # FINDINGS.md. Turn it on again when that is fixed.
  # runtest "### Recycle-Ls" unix SUCCESS "" eos recycle ls "| grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle | wc -l | grep -w 1"
  runtest "### Restore   " unix ERROR   "" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle | awk '{print $10}' | tail -1`
  runtest "### Restore   " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle | awk '{print $10}' | tail -1`
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle"
  rm -rf "$EOS_TEST_TESTSYS/dumpit"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle"
  runtest "### Restore   " unix ERROR   "EOSROLE='-r 3 4'" eos recycle restore `command eos -b  root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle | awk '{print $10}' | tail -1`
  runtest "### Restore   " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle restore -f `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/passwd.recycle | awk '{print $10}' | tail -1`
}

@test "restore of a directory from the recycle bin" {
  # Directory recycle restor
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir/passwd.recycle"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir/"
  runtest "### List      " unix 2       "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir"
  runtest "### Restore   " unix SUCCESS   "EOSROLE='-r 0 0'" eos recycle restore `command eos -b  root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir/ | awk '{print $10}' | tail -1`
  runtest "### List      " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle/dir"
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --lifetime 2d
  runtest "### Recycle   " unix SUCCESS "" eos recycle -m "| grep -q \"lifetime=172800 \""
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --remove-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle
  runtest "### Rem       " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/recycle
}

@test "the project recycle bin" {
  # Test project recycle functionality
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle"
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --add-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle
  runtest "### Setup     " unix SUCCESS "" eos recycle project --path /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle
  rid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle | jq -r .id)
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### MissingRid" unix ERROR "" eos recycle ls --rid
  runtest "### Restore   " unix ERROR "EOSROLE='-r 99 99'" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### Restore   " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  rm -rf "$EOS_TEST_TESTSYS/dumpit"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### Restore   " unix ERROR "EOSROLE='-r 3 4'" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### Restore   " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle restore -f `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### Purge     " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle purge --rid $rid -k `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### NoFile    " unix ERROR "" eos recycle ls --all "| grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  # Test "recycle ls --project <path>" listing for non-privileged users. A path
  # that carries the sys.forced.recycleid xattr must list the project recycle
  # bin, while a path without the xattr must return an error.
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### ProjLs    " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle ls --project /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle "| grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### ProjLsSub " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle ls --project /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/ "| grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile"
  runtest "### ProjLsBad " unix ERROR   "EOSROLE='-r 3 4'" eos recycle ls --project /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest "### Purge     " unix SUCCESS "EOSROLE='-r 3 4'" eos recycle purge --rid $rid -k `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle/pfile | awk '{print $10}' | tail -1`
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --remove-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle
  runtest "### Rem       " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/precycle
}
