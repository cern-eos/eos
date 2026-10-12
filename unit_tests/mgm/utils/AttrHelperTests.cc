/************************************************************************
  * EOS - the CERN Disk Storage System                                   *
  * Copyright (C) 2022 CERN/Switzerland                           *
  *                                                                      *
  * This program is free software: you can redistribute it and/or modify *
  * it under the terms of the GNU General Public License as published by *
  * the Free Software Foundation, either version 3 of the License, or    *
  * (at your option) any later version.                                  *
  *                                                                      *
  * This program is distributed in the hope that it will be useful,      *
  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
  * GNU General Public License for more details.                         *
  *                                                                      *
  * You should have received a copy of the GNU General Public License    *
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
  ************************************************************************
*/

#include "common/Constants.hh"
#include "mgm/misc/Constants.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/utils/AttrHelper.hh"
#include "namespace/utils/Attributes.hh"
#include "gtest/gtest.h"
#include <json/json.h>

using namespace eos::mgm;
using eos::mgm::attr::checkDirOwner;

namespace {
class AttrJsonOutput : public IProcCommand {
public:
  using IProcCommand::ConvertOutputToJsonFormat;
};
} // namespace

TEST(AttrOutput, JsonConversionPreservesLiteralQuotesAndBackslashes)
{
  for (const char* value :
       {"plain", "value\"quote", "\"leading", "\"", "\"quoted\"", "back\\slash", "tail\\",
        "space and \"quote\"", "tab\tvalue", "\\\"escaped-looking\\\""}) {
    SCOPED_TRACE(value);
    const std::string output =
        "user.attr.json4=" + attr::formatQuotedValue(value, true) + "\n";
    const auto result = AttrJsonOutput::ConvertOutputToJsonFormat(output);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0]["user"]["attr"]["json4"].asString(), value);
  }
}

TEST(AttrOutput, HumanOutputKeepsItsExistingRepresentation)
{
  const std::string value = "value\"quote\\backslash";
  EXPECT_EQ(attr::formatQuotedValue(value, false), "\"" + value + "\"");
}

TEST(AttrOutput, JsonConversionKeepsExistingNumericAndEmptyValueTypes)
{
  const auto result = AttrJsonOutput::ConvertOutputToJsonFormat(
      "user.number=" + attr::formatQuotedValue("3", true) + "\n" +
      "user.empty=" + attr::formatQuotedValue("", true) + "\n");
  ASSERT_EQ(result.size(), 2u);
  EXPECT_TRUE(result[0]["user"]["number"].isUInt64());
  EXPECT_EQ(result[0]["user"]["number"].asUInt64(), 3u);
  EXPECT_EQ(result[1]["user"]["empty"].asString(), "NULL");
}

TEST(checkDirOwner, EmptyMap)
{
  eos::common::VirtualIdentity vid;
  bool sticky_owner;
  ASSERT_FALSE(checkDirOwner({}, {}, {}, vid, sticky_owner, nullptr));
}

TEST(checkDirOwner, StickyOwner)
{
  eos::common::VirtualIdentity vid;
  eos::IContainerMD::XAttrMap xattrs {{SYS_OWNER_AUTH, "*"}};
  bool sticky_owner;
  ASSERT_TRUE(checkDirOwner(xattrs, {}, {}, vid, sticky_owner, nullptr));
  ASSERT_TRUE(sticky_owner);
}

TEST(checkDirOwner, dirOwner)
{
  eos::common::VirtualIdentity vid;
  vid.uid = 23;
  vid.gid = 23;
  uid_t dir_uid = 46;
  gid_t dir_gid = 46;
  vid.prot = "krb5";
  vid.uid_string = "testuser";
  bool sticky_owner;
  eos::IContainerMD::XAttrMap  xattrs {{SYS_OWNER_AUTH, "sss:operator,krb5:testuser"}};
  EXPECT_TRUE(
    checkDirOwner(xattrs, dir_uid, dir_gid, vid, sticky_owner, nullptr));
  ASSERT_FALSE(sticky_owner);
  ASSERT_EQ(vid.uid, dir_uid);
  ASSERT_EQ(vid.gid, dir_gid);
}

TEST(checkAtomicUpload, EmptyMap)
{
  ASSERT_FALSE(attr::checkAtomicUpload({}));
}

TEST(checkAtomicUpload, sys)
{
  eos::IContainerMD::XAttrMap xattrs {{SYS_FORCED_ATOMIC, "1"}};
  ASSERT_TRUE(attr::checkAtomicUpload(xattrs));
  xattrs[SYS_FORCED_ATOMIC] = "0";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs));
  xattrs[SYS_FORCED_ATOMIC] = "-1";
  ASSERT_TRUE(attr::checkAtomicUpload(xattrs));
  xattrs[SYS_FORCED_ATOMIC] = "garbage";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs));
}

TEST(checkAtomicUpload, user)
{
  eos::IContainerMD::XAttrMap xattrs {{USER_FORCED_ATOMIC, "1"}};
  ASSERT_TRUE(attr::checkAtomicUpload(xattrs));
  xattrs[USER_FORCED_ATOMIC] = "0";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs));
  xattrs[USER_FORCED_ATOMIC] = "-1";
  ASSERT_TRUE(attr::checkAtomicUpload(xattrs));
  xattrs[USER_FORCED_ATOMIC] = "garbage";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs));
}

TEST(checkAtomicUpload, cgi)
{
  eos::IContainerMD::XAttrMap xattrs{};
  ASSERT_TRUE(attr::checkAtomicUpload(xattrs, "foo"));
  // sys overrides everything!
  xattrs[SYS_FORCED_ATOMIC] = "0";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs, "foo"));
  // usr overrides cgi
  xattrs.clear();
  xattrs[USER_FORCED_ATOMIC] = "0";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs, "foo"));
}

TEST(checkAtomicUpload, sysoverride)
{
  eos::IContainerMD::XAttrMap xattrs;
  xattrs[SYS_FORCED_ATOMIC] = "0";
  xattrs[USER_FORCED_ATOMIC] = "1";
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs));
  ASSERT_FALSE(attr::checkAtomicUpload(xattrs, "foo"));
}

TEST(getVersioning, cgi)
{
  eos::IContainerMD::XAttrMap xattrs;
  std::string version {"1"};
  ASSERT_EQ(attr::getVersioning(xattrs, version), 1);
  version = "2";
  ASSERT_EQ(attr::getVersioning(xattrs, version), 2);
}

TEST(getVersioning, invalid_cgi)
{
  ASSERT_FALSE(attr::getVersioning({}, "garbage"));
  eos::IContainerMD::XAttrMap xattrs {{SYS_VERSIONING, "0"},
    {USER_VERSIONING, "1"}};
  ASSERT_FALSE(attr::getVersioning(xattrs, "garbage"));
  xattrs.clear();
  xattrs[SYS_VERSIONING] = "1";
  xattrs[USER_VERSIONING] = "0";
  ASSERT_FALSE(attr::getVersioning(xattrs, "garbage"));
}

TEST(getVersioning, cgi_overrides)
{
  eos::IContainerMD::XAttrMap xattrs {{SYS_VERSIONING, "0"},
    {USER_VERSIONING, "1"}};
  std::string version {"2"};
  ASSERT_EQ(attr::getVersioning(xattrs, version), 2);
}

TEST(getVersioning, sys_overrides)
{
  eos::IContainerMD::XAttrMap xattrs;
  xattrs[SYS_VERSIONING] = "1";
  xattrs[USER_VERSIONING] = "0";
  ASSERT_TRUE(attr::getVersioning(xattrs));
  xattrs[SYS_VERSIONING] = "10";
  ASSERT_EQ(attr::getVersioning(xattrs), 10);
  // sys overrides usr, so a garbage sys value will mean 0 versions!
  xattrs[SYS_VERSIONING] = "garbage";
  xattrs[USER_VERSIONING] = "1";
  ASSERT_FALSE(attr::getVersioning(xattrs));
}

TEST(getVersioning, user)
{
  eos::IContainerMD::XAttrMap xattrs {{USER_VERSIONING, "1"}};
  ASSERT_TRUE(attr::getVersioning(xattrs));
  xattrs[USER_VERSIONING] = "10";
  ASSERT_EQ(attr::getVersioning(xattrs), 10);
}

TEST(getCarryOverXattrs, KeepsStoredXattrs)
{
  eos::IFileMD::XAttrMap old_xattrs{{"user.foo", "1"},
                                    {"sys.acl", "u:99:rx"},
                                    {eos::common::EOS_APP_LOCK_ATTR, "expires:0"}};
  ASSERT_EQ(attr::getCarryOverXattrs(old_xattrs), old_xattrs);
}

TEST(getCarryOverXattrs, DropsPreviousInstanceXattrs)
{
  eos::IFileMD::XAttrMap old_xattrs{{"user.foo", "1"},
                                    {eos::common::EOS_BTIME, "1.2"},
                                    {eos::common::EOS_FS_TRACKING_ATTR, "+1"},
                                    {eos::common::EOS_DTRACE_ATTR, "dtrace"},
                                    {eos::common::EOS_VTRACE_ATTR, "vtrace"},
                                    {eos::common::EOS_UTRACE_ATTR, "utrace"},
                                    {eos::common::EOS_TMP_ATOMIC_ATTR, ".sys.a#.file"}};
  eos::IFileMD::XAttrMap expected{{"user.foo", "1"}};
  ASSERT_EQ(attr::getCarryOverXattrs(old_xattrs), expected);
}

TEST(getCarryOverXattrs, DropsInternalAndTmpXattrs)
{
  // e.g. a new file keeping the link count of a hard link target would be
  // renamed instead of deleted, one keeping a forced ETag would never change it
  eos::IFileMD::XAttrMap old_xattrs{{"user.foo", "1"},
                                    {SYS_HARD_LINK, "4097"},
                                    {SYS_NUM_LINK, "1"},
                                    {"sys.eos.fifo", "1"},
                                    {eos::kAttrTmpEtagKey, "\"etag\""},
                                    {"sys.eosx", "1"},
                                    {"sys.tmpx", "1"}};
  eos::IFileMD::XAttrMap expected{
      {"user.foo", "1"}, {"sys.eosx", "1"}, {"sys.tmpx", "1"}};
  ASSERT_EQ(attr::getCarryOverXattrs(old_xattrs), expected);
}

TEST(getCarryOverXattrs, DropsFusexAndTapeXattrs)
{
  // CTA gives the new file its own archive file id when it is created, the
  // retrieve requests and the FUSE state belong to the previous contents
  eos::IFileMD::XAttrMap old_xattrs{
      {"user.foo", "1"},
      {eos::common::ARCHIVE_FILE_ID_ATTR_NAME, "4242"},
      {eos::common::ARCHIVE_STORAGE_CLASS_ATTR_NAME, "single"},
      {eos::common::ARCHIVE_ERROR_ATTR_NAME, "error"},
      {eos::common::CTA_OBJECTSTORE_ARCHIVE_REQ_ID_NAME, "archive_req"},
      {eos::common::CTA_OBJECTSTORE_REQ_ID_NAME, "retrieve_req"},
      {eos::common::RETRIEVE_REQID_ATTR_NAME, "req_id"},
      {eos::common::RETRIEVE_REQTIME_ATTR_NAME, "1"},
      {eos::common::RETRIEVE_EVICT_COUNTER_NAME, "1"},
      {eos::common::RETRIEVE_ERROR_ATTR_NAME, "error"},
      {"sys.fusex.state", "C|"},
      {"sys.fusex.creator", "uuid"},
      {"sys.archivex", "1"},
      {"sys.ctax", "1"},
      {"sys.retrievex", "1"},
      {"sys.fusexx", "1"}};
  eos::IFileMD::XAttrMap expected{{"user.foo", "1"},
                                  {"sys.archivex", "1"},
                                  {"sys.ctax", "1"},
                                  {"sys.retrievex", "1"},
                                  {"sys.fusexx", "1"}};
  ASSERT_EQ(attr::getCarryOverXattrs(old_xattrs), expected);
}

TEST(getCarryOverXattrs, DropsEncryptionKeys)
{
  // The new file is only encrypted with the keys of its own upload
  eos::IFileMD::XAttrMap old_xattrs{{"user.foo", "1"},
                                    {eos::kAttrObfuscateKey, "old"},
                                    {eos::kAttrEncrypted, "1"},
                                    {eos::kAttrEncryptSpace, "default"},
                                    {eos::kAttrEncryptedFp, "oldfp"}};
  eos::IFileMD::XAttrMap expected{{"user.foo", "1"}};
  ASSERT_EQ(attr::getCarryOverXattrs(old_xattrs), expected);
}
