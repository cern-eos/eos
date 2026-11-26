# bats file_tags=gateway
load ../instance

@test "set up of the gateway directory" {
  # Create structure
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1"
}

@test "set up of the bin user" {
  # We will use the "bin user"
  runtest "### Chown     " unix SUCCESS "" eos chown -r bin:bin "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
  runtest "### Access    " unix SUCCESS "" eos access allow user bin
  runtest "### Vid       " unix SUCCESS "" eos vid set membership 1 -sudo
}

@test "set up of a gateway key" {
  # Create a dummy key so that the gateway authentication can work
  runtest "### Vid       " unix SUCCESS "" eos vid set map -https key:1234 vuid:1 vgid:1
}

@test "Try to read the file without impersonation, it should fail" {
  # Try to read the file without impersonation, it should fail
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "bin (uid 1) is no sudoer: it cannot read the file" {
  # As the bin user (uid=1) is not sudoer, the file should not be readable
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:1 --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "bin becomes a sudoer" {
  # Set the bin user as a sudoer
  runtest "### Vid       " unix SUCCESS "" eos vid set membership 1 +sudo
}

@test "impersonation of itself reads the file" {
  # Impersonate by himself should allow to read the file
  runtest "### Http GET  " unix SUCCESS "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:1 --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "impersonation of a user without access cannot read the file" {
  # Impersonate to a non-authorized user will not allow to read the file
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:3 --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1--output $EOS_TEST_TESTSYS/dumpit
}

@test "Change the owner of the directory to the adm user (uid=3)" {
  # Change the owner of the directory to the adm user (uid=3)
  runtest "### Chown     " unix SUCCESS "" eos chown -r adm:adm "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
}

@test "The bin user should not be able to access the file anymore" {
  # The bin user should not be able to access the file anymore
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:1  --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "Allow adm to access the instance" {
  # Allow adm to access the instance
  runtest "### Access    " unix SUCCESS "" eos access allow user adm
}

@test "without sudo, bin cannot impersonate and read the file" {
  # Remove the bin user from the sudoer, we should not be able to read the file as we cannot impersonate
  runtest "### Vid       " unix SUCCESS "" eos vid set membership 1 -sudo
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:3 --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "with sudo, bin can read the file as adm (uid 3)" {
  # Add the bin user to the sudoer and verity the file can be read if we impersonate adm (uid=3)
  runtest "### Vid       " unix SUCCESS "" eos vid set membership 1 +sudo
  runtest "### Http GET  " unix SUCCESS "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:3 --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "Read the file again without any impersonation" {
  # Read the file again without any impersonation
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "Remote-User may give a user name instead of a uid" {
  # Impersonate with the header 'Remote-User' and the username instead of the user id
  runtest "### Http GET  " unix SUCCESS "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:adm --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:bin --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "Remove sudoer and retry the impersonation with the 'Remote-User'" {
  # Remove sudoer and retry the impersonation with the 'Remote-User'
  runtest "### Cleanup   " unix SUCCESS "" eos vid set membership 1 -sudo
  runtest "### Http GET  " unix ERROR "" curl -v -H X-Gateway-Authorization:1234 -H X-Forwarded-For:dummy -H Remote-User:adm --location --fail https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway/file.gateway.1 --output $EOS_TEST_TESTSYS/dumpit
}

@test "clean up of the gateway tests" {
  # Cleanup
  runtest "### Cleanup   " unix SUCCESS "" eos access unallow user bin
  runtest "### Cleanup   " unix SUCCESS "" eos access unallow user adm
  runtest "### Cleanup   " unix SUCCESS "" eos vid rm 'vid:https:\"key:1234\":gid'
  runtest "### Cleanup   " unix SUCCESS "" eos vid rm 'vid:https:\"key:1234\":uid'
  runtest "### Cleanup   " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/gateway"
}
