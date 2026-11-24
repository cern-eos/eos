//------------------------------------------------------------------------------
//! @file FileRegisterCmdTest.cc
//! @brief Reject malformed registration checksums before namespace access.
//------------------------------------------------------------------------------

#include "mgm/proc/admin/FileRegisterCmd.hh"
#include "gtest/gtest.h"

#include <string>

TEST(FileRegisterChecksumTest, RejectsMalformedProtobuf)
{
  const std::string invalidChecksums[] = {
      "z0", "0g", "abxz", "1", "abc", std::string(66, 'a'), "ab cd", "0x12", "00\\00"};

  for (const auto& checksum : invalidChecksums) {
    SCOPED_TRACE(checksum);
    eos::console::RequestProto request;
    request.mutable_record()->set_path("/eos/unconfigured/file");
    request.mutable_record()->set_checksum(checksum);
    auto root = eos::common::VirtualIdentity::Root();
    eos::mgm::FileRegisterCmd command(std::move(request), root);
    const auto reply = command.ProcessRequest();
    EXPECT_EQ(reply.retc(), EINVAL);
    EXPECT_NE(reply.std_err().find("checksum"), std::string::npos);
  }
}
