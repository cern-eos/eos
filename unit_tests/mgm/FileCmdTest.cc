#include "mgm/proc/user/FileCmd.hh"
#include "gtest/gtest.h"

#include <string>

TEST(FileTouchChecksumTest, RejectsMalformedProtobufBeforeNamespaceAccess)
{
  const std::string invalidChecksums[] = {
      "zz", "0g", "abxz", "1", "abc", std::string(66, 'a'), "ab cd", "0x12", "00\\00"};

  for (const auto& checksum : invalidChecksums) {
    SCOPED_TRACE(checksum);
    eos::console::RequestProto request;
    request.mutable_file()->mutable_md()->set_path("/eos/unconfigured/file");
    request.mutable_file()->mutable_touch()->set_checksuminfo(checksum);
    auto root = eos::common::VirtualIdentity::Root();
    eos::mgm::FileCmd command(std::move(request), root);
    const auto reply = command.ProcessRequest();
    EXPECT_EQ(reply.retc(), EINVAL);
    EXPECT_NE(reply.std_err().find("checksum"), std::string::npos);
  }
}
