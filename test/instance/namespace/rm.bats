# bats file_tags=rm
load ../instance

RM_TEST_FILE="/eos/$EOS_TEST_INSTANCE/test/instancetest/rm_test.dat"
rmTestDir="/eos/$EOS_TEST_INSTANCE/test/instancetest/rm_test"
RM_MULTI1="/eos/$EOS_TEST_INSTANCE/test/instancetest/multi 1.txt"
RM_MULTI2="/eos/$EOS_TEST_INSTANCE/test/instancetest/multi 2.txt"
RM_MULTI3="/eos/$EOS_TEST_INSTANCE/test/instancetest/ multi3 .txt"
RM_DIR1="/eos/$EOS_TEST_INSTANCE/test/instancetest/rmdir1"
RM_DIR2="/eos/$EOS_TEST_INSTANCE/test/instancetest/rmdir2"
RM_DIR3="/eos/$EOS_TEST_INSTANCE/test/instancetest/rmdir3"
RM_ARROW_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/rm_arrow"
FC_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/filecopy_dir"

@test "rm of a detached file waits for the FST deletion" {
  # These tests assume that the polling interval for deletions on the FST side is
  # less than 7 seconds. Set the EOS_FST_DELETE_QUERY_INTERVAL=5.
  # First test that the removal of a detached file will retrigger the deletion on
  # the diskserver and will wait for the confirmation before dropping the ns entry
  runtest "### Upload      " unix SUCCESS "" eos cp /etc/passwd ${RM_TEST_FILE}
  rm_fxid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${RM_TEST_FILE} | jq -r .fxid)
  rm_fsid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${RM_TEST_FILE} | jq -r .locations[].fsid | head -n 1)
  runtest "### Rm          " unix SUCCESS "" eos rm -F ${RM_TEST_FILE}
  runtest "### Dropdel     " unix SUCCESS "" eos fs dropdeletion "${rm_fsid}"
  # The FST may delete the file at once; else the branch removes it
  runtest "### Wait        " unix IGNORE  "" wait_for 5 file_is_deleted "$rm_fxid"
  if ! file_is_deleted "$rm_fxid"; then
      runtest "### Fileinfo    " unix SUCCESS "" eos fileinfo "fxid:${rm_fxid}"
      runtest "### Rm detached " unix SUCCESS "" eos rm "fxid:${rm_fxid}"
  fi
  # The FST asks for deletions every EOS_FST_DELETE_QUERY_INTERVAL s
  runtest "### File Del    " unix SUCCESS "" wait_for 30 file_is_deleted "$rm_fxid"
  # Secondly test that rm -F on a detached file will clean up the namespace
  # without waiting for a confirmation from the diskservers
  runtest "### Upload      " unix SUCCESS "" eos cp /etc/passwd ${RM_TEST_FILE}
  rm_fxid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${RM_TEST_FILE} | jq -r .fxid)
  rm_fsid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${RM_TEST_FILE} | jq -r .locations[].fsid | head -n 1)
  runtest "### Rm          " unix SUCCESS "" eos rm -F ${RM_TEST_FILE}
  runtest "### Dropdel     " unix SUCCESS "" eos fs dropdeletion "${rm_fsid}"
  # The FST may delete the file at once; else the branch removes it
  runtest "### Wait        " unix IGNORE  "" wait_for 5 file_is_deleted "$rm_fxid"
  if ! file_is_deleted "$rm_fxid"; then
      runtest "### Fileinfo    " unix SUCCESS "" eos fileinfo "fxid:${rm_fxid}"
      runtest "### Rm force    " unix SUCCESS "" eos rm -F "fxid:${rm_fxid}"
  fi
  runtest "### File Del    " unix SUCCESS "" file_is_deleted "$rm_fxid"
}

@test "Create a directory with 10 files in it" {
  # Create a directory with 10 files in it
  eos mkdir -p $rmTestDir
  for ((i=1; i <= 10; i++));
  do
    eos cp /etc/passwd $rmTestDir/passwd$i
  done
}

@test "Deletion with no globbing should not work" {
  # Deletion with no globbing should not work
  runtest "### Rm noglob   " unix ERROR "" eos rm --no-globbing $rmTestDir/*
  runtest "### Rm r noglob " unix ERROR "" eos rm -rF --no-globbing $rmTestDir/*
}

@test "rm of a path with * in the middle fails" {
  # The deletion of a path that contains the "*" character in the middle should not work
  runtest "### Rm glob dir " unix ERROR "" eos rm "/eos/$EOS_TEST_INSTANCE/test/*/rm_test"
  runtest "### Rm r glob dir" unix ERROR "" eos rm -rF "/eos/$EOS_TEST_INSTANCE/test/*/rm_test"
}

@test "rm of the directory contents works by default" {
  # The deletion of the test directory and the 10 files should work by default
  runtest "### Rm all glob " unix SUCCESS "" eos rm $rmTestDir/*
  runtest "### Empty       " unix SUCCESS "" eos ls $rmTestDir/ "| wc -l"
}

@test "Create the same directory with extra non empty directories in it" {
  # Create the same directory with extra non empty directories in it
  for((i = 1; i <= 10; i++));
  do
    eos cp /etc/passwd $rmTestDir/passwd$i
    eos mkdir $rmTestDir/$i
    eos cp /etc/passwd $rmTestDir/$i/passwd$i
  done
}

@test "wildcards select children also with -r" {
  # Wildcards select matching children of the parent directory even with -r.
  runtest "### Touch match        " unix SUCCESS "" eos touch "$rmTestDir/1-remove"
  runtest "### Touch nonmatch     " unix SUCCESS "" eos touch "$rmTestDir/keep-1"
  runtest "### Touch hidden       " unix SUCCESS "" eos touch "$rmTestDir/.1-keep"
  runtest "### Rm glob no match  " unix ERROR   "" eos rm -rF "$rmTestDir/no-such-entry-*"
  runtest "### Rm recursive glob " unix ERROR   "" eos rm -rF "$rmTestDir/[.0-9]*"
  runtest "### Stat removed match " unix ERROR   "" eos stat "$rmTestDir/1-remove"
  runtest "### Stat nonmatch      " unix SUCCESS "" eos stat "$rmTestDir/keep-1"
  runtest "### Stat hidden        " unix SUCCESS "" eos stat "$rmTestDir/.1-keep"
  runtest "### Stat matched child " unix SUCCESS "" eos stat "$rmTestDir/1/passwd1"
  runtest "### Stat matched child " unix SUCCESS "" eos stat "$rmTestDir/10/passwd10"
}

@test "globbing rm fails on non-empty directories" {
  # The deletion with globbing enabled by default should not work as we have non-empty directories
  runtest "### Rm all glob " unix ERROR "" eos rm $rmTestDir/*
  runtest "### Cleanup Fail" unix ERROR "" eos rm -rF $rmTestDir/*
}

@test "set up of the rm pattern tree" {
  for((i = 1; i <= 10; i++));
  do
    eos touch $rmTestDir/$((i+10))
  done
}

@test "Put paths with globbing characters in the namespace" {
  # Put paths with globbing characters in the namespace
  eos touch $rmTestDir/Perso[2ou52]_dgdia.pdf
  command eos touch "$rmTestDir/regexx*p.tt\.ern"
}

@test "set up of the rm number pattern files" {
  eos touch $rmTestDir/number1pattern
  eos touch $rmTestDir/number2pattern
}

@test "No file matches the globbing, the deletion should not work" {
  # No file matches the globbing, the deletion should not work
  runtest "### Rm file     " unix ERROR "" eos rm $rmTestDir/Perso[2ou52]_dgdia.pdf
  runtest "### Stat file" unix SUCCESS "" eos stat $rmTestDir/Perso[2ou52]_dgdia.pdf
}

@test "--no-globbing removes a name with brackets" {
  # Explicitely disabling the globbing should delete the Perso[2ou52]_dgdia.pdf file
  runtest "### Rm file glob" unix SUCCESS "" eos rm --no-globbing $rmTestDir/Perso[2ou52]_dgdia.pdf
  runtest "### Stat file glob" unix ERROR "" eos stat $rmTestDir/Perso[2ou52]_dgdia.pdf
}

@test "a pattern with braces is not a glob: rm fails" {
  # Deleting with wrong globbing pattern (curly braces inside) should not work as it is not a valid
  # globbing character
  runtest "### Bad glob rm " unix ERROR "" eos rm "$rmTestDir/number\{1..9\}pattern"
  runtest "### Stat file   " unix SUCCESS "" eos stat $rmTestDir/number1pattern
  runtest "### Stat file   " unix SUCCESS "" eos stat $rmTestDir/number2pattern
  runtest "### Stat file   " unix SUCCESS "" command eos stat "$rmTestDir/regexx*p.tt\.ern"
}

@test "Deleting number[1-10]pattern without globbing should not work" {
  # Deleting number[1-10]pattern without globbing should not work
  runtest "### Rm noglob   " unix ERROR "" eos rm --no-globbing "$rmTestDir/number[1-9]pattern"
}

@test "Deleting number[1-10]pattern with globbing should work" {
  # Deleting number[1-10]pattern with globbing should work
  runtest "### Rm glob ok  " unix SUCCESS "" eos rm "$rmTestDir/number[1-9]pattern"
  runtest "### Stat file   " unix ERROR "" eos stat $rmTestDir/number1pattern
  runtest "### Stat file   " unix ERROR "" eos stat $rmTestDir/number2pattern
  runtest "### Stat file   " unix SUCCESS "" command eos stat "$rmTestDir/regexx*p.tt\.ern"
}

@test "Files rmTestDir/{11..20} should not have been deleted" {
  # Files $rmTestDir/{11..20} should not have been deleted
  runtest "### Stat dir    " unix SUCCESS "" eos stat $rmTestDir/11
  runtest "### Stat dir    " unix SUCCESS "" eos stat $rmTestDir/20
}

@test "The deletion of the regexx* should not work" {
  # The deletion of the regexx* should not work
  runtest "### Rm regexx*  " unix ERROR "" command eos rm "$rmTestDir/regexx*p.tt\.ern"
  runtest "### Stat file   " unix SUCCESS "" command eos stat "$rmTestDir/regexx*p.tt\.ern"
}

@test "The deletion of the regexx* should work with disabled globbing" {
  # The deletion of the regexx* should work with disabled globbing
  runtest "### Rm file     " unix SUCCESS "" command eos rm --no-globbing "$rmTestDir/regexx*p.tt\.ern"
  runtest "### Stat file   " unix ERROR "" command eos stat "$rmTestDir/regexx*p.tt\.ern"
}

@test "rm of several files with spaces in their names" {
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd "'${RM_MULTI1}'"
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd "'${RM_MULTI2}'"
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd "'${RM_MULTI3}'"
  runtest "### Rm multi    " unix SUCCESS "" eos rm "'${RM_MULTI1}'" "'${RM_MULTI2}'" "'${RM_MULTI3}'"
  for f in "$RM_MULTI1" "$RM_MULTI2" "$RM_MULTI3"; do wait_for 30 eos_path_absent "$f"; done
  runtest "### Stat        " unix ERROR "" eos stat "'${RM_MULTI1}'"
  runtest "### Stat        " unix ERROR "" eos stat "'${RM_MULTI2}'"
  runtest "### Stat        " unix ERROR "" eos stat "'${RM_MULTI3}'"
}

@test "rm -r of several directories" {
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_DIR1}
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_DIR2}
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_DIR3}
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR1}/file1.txt
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR1}/file2.txt
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR2}/file1.txt
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR2}/file2.txt
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR3}/file1.txt
  runtest "### Touch       " unix SUCCESS "" eos touch ${RM_DIR3}/file2.txt
  runtest "### Rm -r multi " unix SUCCESS "" eos rm -r ${RM_DIR1} ${RM_DIR2} ${RM_DIR3}
  for d in ${RM_DIR1} ${RM_DIR2} ${RM_DIR3}; do wait_for 30 eos_path_absent $d; done
  runtest "### Stat        " unix ERROR "" eos stat ${RM_DIR1}
  runtest "### Stat        " unix ERROR "" eos stat ${RM_DIR2}
  runtest "### Stat        " unix ERROR "" eos stat ${RM_DIR3}
}

@test "rm of names with an arrow, with the recycle bin" {
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_ARROW_DIR}
  runtest "### Recycle     " unix SUCCESS "" eos recycle config --add-bin ${RM_ARROW_DIR}
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_ARROW_DIR}/d
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${RM_ARROW_DIR}/cp
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd ${RM_ARROW_DIR}/d/f2
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd "'${RM_ARROW_DIR}/d/v -> y'"
  runtest "### Ln          " unix SUCCESS "" eos ln "'${RM_ARROW_DIR}/d/s -> z'" f2
}

@test "Console cp with wildcard expansion" {
  # Console cp with wildcard expansion
  runtest "### Cp * arrow  " unix SUCCESS "" eos cp "'${RM_ARROW_DIR}/d/*'" ${RM_ARROW_DIR}/cp/
  runtest "### Stat        " unix SUCCESS "" eos stat "'${RM_ARROW_DIR}/cp/v -> y'"
  runtest "### Stat        " unix SUCCESS "" eos stat "'${RM_ARROW_DIR}/cp/s -> z'"
  runtest "### Chown -r arrow" unix SUCCESS "" eos chown -r -h bin:bin ${RM_ARROW_DIR}
}

@test "Recycle: rm -r into the bin, restore, then purge" {
  # Recycle: rm -r into the bin, restore, then purge
  runtest "### Rm -r bin   " unix SUCCESS "" eos rm -r ${RM_ARROW_DIR}/d/
  runtest "### Stat        " unix ERROR "" eos stat ${RM_ARROW_DIR}/d
  runtest "### Restore     " unix SUCCESS "EOSROLE='-r 1 1'" eos recycle restore `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep ${RM_ARROW_DIR}/d/ | awk '{print $10}' | tail -1`
  runtest "### Stat        " unix SUCCESS "" eos stat "'${RM_ARROW_DIR}/d/v -> y'"
  runtest "### Stat        " unix SUCCESS "" eos stat "'${RM_ARROW_DIR}/d/s -> z'"
  runtest "### Rm -r bin   " unix SUCCESS "" eos rm -r ${RM_ARROW_DIR}/d/
  runtest "### Purge       " unix SUCCESS "EOSROLE='-r 0 0'" eos recycle purge -k `command eos -b root://$EOS_TEST_REDIRECTOR recycle ls --all | grep ${RM_ARROW_DIR}/d/ | awk '{print $10}' | tail -1`
  runtest "### NoEntry     " unix ERROR "" eos recycle ls --all "| grep ${RM_ARROW_DIR}/d/"
  runtest "### Recycle     " unix SUCCESS "" eos recycle config --remove-bin ${RM_ARROW_DIR}
}

@test "Direct delete (no recycle) of the copies and of one more symlink" {
  # Direct delete (no recycle) of the copies and of one more symlink
  runtest "### Ln          " unix SUCCESS "" eos ln "'${RM_ARROW_DIR}/cp/l -> z'" f2
  runtest "### Rm -r arrow " unix SUCCESS "" eos rm -rF --no-confirmation ${RM_ARROW_DIR}
  runtest "### Stat        " unix ERROR "" eos stat ${RM_ARROW_DIR}
}

@test "file copy of a directory" {
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${FC_DIR}/src
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p ${FC_DIR}/dst
  runtest "### CP          " unix SUCCESS "" eos cp /etc/passwd ${FC_DIR}/src/passwd
  runtest "### Touch       " unix SUCCESS "" eos touch ${FC_DIR}/src/empty
  runtest "### File copy -r" unix SUCCESS "" eos file copy -f ${FC_DIR}/src/ ${FC_DIR}/dst/
  runtest "### FileInfo    " unix SUCCESS "" eos fileinfo "${FC_DIR}/dst/passwd -m | grep -w size=`stat -c %s /etc/passwd`"
  runtest "### FileInfo    " unix SUCCESS "" eos fileinfo "${FC_DIR}/dst/empty -m | grep -w size=0"
  runtest "### Rm -r       " unix SUCCESS "" eos rm -rF --no-confirmation ${FC_DIR}
}

@test "clean up of the rm tests" {
  # Cleanup everything
  runtest "### Cleanup     " unix SUCCESS "" eos rm -rF $rmTestDir/
}

@test "rm -F of a detached file cleans the namespace" {
  # Rm detached
  runtest "### eos-file-cont-detached-test  " unix SUCCESS "" eos_file_cont_detached "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_cont_detached"
}
