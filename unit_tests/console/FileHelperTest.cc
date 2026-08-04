//------------------------------------------------------------------------------
//! @file FileHelperTest.cc
//! @author Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
 ************************************************************************/

//------------------------------------------------------------------------------
// FileHelper turns an 'eos file' command line into a FileProto. It is shared
// by the console and by eos-grpc-ns, so the mapping pinned here is what both
// the XRootD and the gRPC transports put on the wire.
//------------------------------------------------------------------------------

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "console/commands/helpers/FileHelper.hh"
#undef IN_TEST_HARNESS

namespace {
GlobalOptions gOpts;

//------------------------------------------------------------------------------
//! Parse a command line and return the resulting FileProto. Uses the default
//! (identity) path resolver, so paths come out exactly as they went in.
//------------------------------------------------------------------------------
bool
Parse(const std::string& cmd, eos::console::FileProto& out)
{
  FileHelper helper(gOpts);

  if (!helper.ParseCommand(cmd.c_str())) {
    return false;
  }

  out = helper.GetRequest().file();
  return true;
}

//------------------------------------------------------------------------------
//! Parse and expect success, returning the FileProto
//------------------------------------------------------------------------------
eos::console::FileProto
ParseOk(const std::string& cmd)
{
  eos::console::FileProto file;
  EXPECT_TRUE(Parse(cmd, file)) << "failed to parse: " << cmd;
  return file;
}

//------------------------------------------------------------------------------
//! Assert that a command line is rejected
//------------------------------------------------------------------------------
void
ExpectReject(const std::string& cmd)
{
  eos::console::FileProto file;
  EXPECT_FALSE(Parse(cmd, file)) << "expected rejection of: " << cmd;
}
} // namespace

//------------------------------------------------------------------------------
// Path / id specifier resolution
//------------------------------------------------------------------------------
TEST(FileHelper, PathIsTakenVerbatimWithoutResolver)
{
  auto file = ParseOk("drop /eos/dir/f.dat 3");
  ASSERT_EQ(file.md().path(), "/eos/dir/f.dat");
  ASSERT_EQ(file.md().id(), 0u);
}

TEST(FileHelper, PathResolverIsApplied)
{
  FileHelper helper(gOpts, [](const char* in) { return std::string("/eos/base/") + in; });
  ASSERT_TRUE(helper.ParseCommand("drop f.dat 3"));
  ASSERT_EQ(helper.GetRequest().file().md().path(), "/eos/base/f.dat");
}

TEST(FileHelper, PathResolverIsNotAppliedToIdSpecifiers)
{
  FileHelper helper(gOpts, [](const char* in) { return std::string("/eos/base/") + in; });
  ASSERT_TRUE(helper.ParseCommand("drop fxid:abc123 3"));
  const auto& md = helper.GetRequest().file().md();
  ASSERT_EQ(md.id(), 0xabc123u);
  ASSERT_TRUE(md.path().empty());
}

TEST(FileHelper, DecimalFidSpecifier)
{
  auto file = ParseOk("drop fid:12345 3");
  ASSERT_EQ(file.md().id(), 12345u);
  ASSERT_TRUE(file.md().path().empty());
}

TEST(FileHelper, MalformedIdSpecifierFallsBackToPath)
{
  // 'fxid:' with a non-hex tail is not an id, it stays a path
  auto file = ParseOk("drop fxid:zzz 3");
  ASSERT_EQ(file.md().path(), "fxid:zzz");
  ASSERT_EQ(file.md().id(), 0u);
}

TEST(FileHelper, IdSpecifierWithEmptyTailIsAPath)
{
  auto file = ParseOk("drop fxid: 3");
  ASSERT_EQ(file.md().path(), "fxid:");
  ASSERT_EQ(file.md().id(), 0u);
}

//------------------------------------------------------------------------------
// ConsoleMain matches the specifier with regexec(), which is unanchored at the
// start, so a trailing 'fxid:<hex>' is recognised even in the middle of a
// token. These pin that behaviour, which the helper reproduces.
//------------------------------------------------------------------------------
TEST(FileHelper, IdSpecifierIsRecognisedAnywhereInTheToken)
{
  auto file = ParseOk("drop /eos/dir/fxid:abc 3");
  ASSERT_EQ(file.md().id(), 0xabcu);
  ASSERT_TRUE(file.md().path().empty());
}

TEST(FileHelper, IdSpecifierMustRunToTheEnd)
{
  // the digits do not reach the end of the token, so this stays a path
  auto file = ParseOk("drop /eos/fxid:abc/sub 3");
  ASSERT_EQ(file.md().path(), "/eos/fxid:abc/sub");
  ASSERT_EQ(file.md().id(), 0u);
}

TEST(FileHelper, LaterIdSpecifierWinsWhenTheFirstDoesNotReachTheEnd)
{
  // the first 'fxid:' is not followed by digits running to the end, the
  // second one is - regexec() finds the leftmost *successful* match
  auto file = ParseOk("drop fxid:zz/fxid:1f 3");
  ASSERT_EQ(file.md().id(), 0x1fu);
  ASSERT_TRUE(file.md().path().empty());
}

TEST(FileHelper, HexPrefixIsPreferredOverDecimal)
{
  // 'fxid:' is tried before 'fid:', matching ConsoleMain's ordering
  auto file = ParseOk("drop fid:10/fxid:10 3");
  ASSERT_EQ(file.md().id(), 0x10u);
}

//------------------------------------------------------------------------------
// rename / rename_with_symlink / symlink
//------------------------------------------------------------------------------
TEST(FileHelper, Rename)
{
  auto file = ParseOk("rename /eos/a /eos/b");
  ASSERT_TRUE(file.has_rename());
  ASSERT_EQ(file.md().path(), "/eos/a");
  ASSERT_EQ(file.rename().new_path(), "/eos/b");
}

TEST(FileHelper, RenameNeedsTwoArguments) { ExpectReject("rename /eos/a"); }

TEST(FileHelper, RenameWithSymlink)
{
  auto file = ParseOk("rename_with_symlink /eos/a/f.dat /eos/b");
  ASSERT_TRUE(file.has_rename_with_symlink());
  ASSERT_EQ(file.md().path(), "/eos/a/f.dat");
  ASSERT_EQ(file.rename_with_symlink().destination_dir(), "/eos/b");
}

TEST(FileHelper, Symlink)
{
  auto file = ParseOk("symlink /eos/link /eos/target");
  ASSERT_TRUE(file.has_symlink());
  ASSERT_EQ(file.md().path(), "/eos/link");
  ASSERT_EQ(file.symlink().target_path(), "/eos/target");
  ASSERT_FALSE(file.symlink().force());
}

TEST(FileHelper, SymlinkForce)
{
  auto file = ParseOk("symlink -f /eos/link /eos/target");
  ASSERT_TRUE(file.symlink().force());
  ASSERT_EQ(file.md().path(), "/eos/link");
  ASSERT_EQ(file.symlink().target_path(), "/eos/target");
}

//------------------------------------------------------------------------------
// drop
//------------------------------------------------------------------------------
TEST(FileHelper, Drop)
{
  auto file = ParseOk("drop /eos/f.dat 7");
  ASSERT_TRUE(file.has_drop());
  ASSERT_EQ(file.drop().fsid(), 7u);
  ASSERT_FALSE(file.drop().force());
  ASSERT_FALSE(file.drop().dropcache());
}

TEST(FileHelper, DropForce)
{
  auto file = ParseOk("drop /eos/f.dat 7 -f");
  ASSERT_EQ(file.drop().fsid(), 7u);
  ASSERT_TRUE(file.drop().force());
}

TEST(FileHelper, DropCache)
{
  // 'cache' is not an fsid, it selects the read-through cache location
  auto file = ParseOk("drop /eos/f.dat cache");
  ASSERT_TRUE(file.drop().dropcache());
  ASSERT_EQ(file.drop().fsid(), 0u);
  ASSERT_FALSE(file.drop().force());
}

TEST(FileHelper, DropRejectsNonNumericFsid)
{
  ExpectReject("drop /eos/f.dat notanfsid");
  ExpectReject("drop /eos/f.dat");
}

//------------------------------------------------------------------------------
// touch
//------------------------------------------------------------------------------
TEST(FileHelper, TouchPlain)
{
  auto file = ParseOk("touch /eos/f.dat");
  ASSERT_TRUE(file.has_touch());
  ASSERT_EQ(file.touch().md().path(), "");
  ASSERT_EQ(file.md().path(), "/eos/f.dat");
  ASSERT_FALSE(file.touch().nolayout());
  ASSERT_FALSE(file.touch().truncate());
  ASSERT_FALSE(file.touch().absorb());
}

TEST(FileHelper, TouchFlags)
{
  auto file = ParseOk("touch -n -0 -a /eos/f.dat");
  ASSERT_TRUE(file.touch().nolayout());
  ASSERT_TRUE(file.touch().truncate());
  ASSERT_TRUE(file.touch().absorb());
}

TEST(FileHelper, TouchCombinedFlags)
{
  // flags may be bundled into a single token
  auto file = ParseOk("touch -n0 /eos/f.dat");
  ASSERT_TRUE(file.touch().nolayout());
  ASSERT_TRUE(file.touch().truncate());
}

TEST(FileHelper, TouchSize)
{
  auto file = ParseOk("touch /eos/f.dat 1024");
  ASSERT_EQ(file.touch().size(), 1024u);
  ASSERT_TRUE(file.touch().hardlinkpath().empty());
}

TEST(FileHelper, TouchHardlinkPath)
{
  // a positional starting with '/' is a link path, not a size
  auto file = ParseOk("touch /eos/f.dat /shared/src.dat");
  ASSERT_EQ(file.touch().hardlinkpath(), "/shared/src.dat");
  ASSERT_EQ(file.touch().size(), 0u);
}

TEST(FileHelper, TouchSizeAndChecksum)
{
  auto file = ParseOk("touch /eos/f.dat 1024 deadbeef");
  ASSERT_EQ(file.touch().size(), 1024u);
  ASSERT_EQ(file.touch().checksuminfo(), "deadbeef");
}

TEST(FileHelper, TouchLock)
{
  auto file = ParseOk("touch -l /eos/f.dat");
  ASSERT_EQ(file.touch().lockop(), "lock");
  ASSERT_TRUE(file.touch().lockop_lifetime().empty());
}

TEST(FileHelper, TouchLockWithLifetime)
{
  auto file = ParseOk("touch -l /eos/f.dat 3600");
  ASSERT_EQ(file.touch().lockop(), "lock");
  ASSERT_EQ(file.touch().lockop_lifetime(), "3600");
  // the lifetime must not be mistaken for a size
  ASSERT_EQ(file.touch().size(), 0u);
}

TEST(FileHelper, TouchLockAudienceIsInverted)
{
  // audience 'app' relaxes the user match, so the wildcard is 'user'
  auto app = ParseOk("touch -l /eos/f.dat 3600 app");
  ASSERT_EQ(app.touch().wildcard(), "user");
  auto user = ParseOk("touch -l /eos/f.dat 3600 user");
  ASSERT_EQ(user.touch().wildcard(), "app");
}

TEST(FileHelper, TouchLockRejectsUnknownAudience)
{
  ExpectReject("touch -l /eos/f.dat 3600 nobody");
}

TEST(FileHelper, TouchUnlock)
{
  auto file = ParseOk("touch -u /eos/f.dat 3600");
  ASSERT_EQ(file.touch().lockop(), "unlock");
  // unlock takes no further arguments
  ASSERT_TRUE(file.touch().lockop_lifetime().empty());
  ASSERT_EQ(file.touch().size(), 0u);
}

TEST(FileHelper, TouchNeedsAPath)
{
  ExpectReject("touch");
  ExpectReject("touch -n");
}

//------------------------------------------------------------------------------
// move / replicate
//------------------------------------------------------------------------------
TEST(FileHelper, Move)
{
  auto file = ParseOk("move /eos/f.dat 1 2");
  ASSERT_TRUE(file.has_move());
  ASSERT_EQ(file.move().fsid1(), 1u);
  ASSERT_EQ(file.move().fsid2(), 2u);
}

TEST(FileHelper, MoveNeedsBothFsids)
{
  ExpectReject("move /eos/f.dat 1");
  ExpectReject("move /eos/f.dat 1 two");
}

TEST(FileHelper, Replicate)
{
  auto file = ParseOk("replicate /eos/f.dat 3 4");
  ASSERT_TRUE(file.has_replicate());
  ASSERT_EQ(file.replicate().fsid1(), 3u);
  ASSERT_EQ(file.replicate().fsid2(), 4u);
}

//------------------------------------------------------------------------------
// copy
//------------------------------------------------------------------------------
TEST(FileHelper, Copy)
{
  auto file = ParseOk("copy /eos/src /eos/dst");
  ASSERT_TRUE(file.has_copy());
  ASSERT_EQ(file.md().path(), "/eos/src");
  ASSERT_EQ(file.copy().dst(), "/eos/dst");
  ASSERT_FALSE(file.copy().force());
  ASSERT_FALSE(file.copy().silent());
  ASSERT_FALSE(file.copy().clone());
}

TEST(FileHelper, CopyFlags)
{
  auto file = ParseOk("copy -f -s -c /eos/src /eos/dst");
  ASSERT_TRUE(file.copy().force());
  ASSERT_TRUE(file.copy().silent());
  ASSERT_TRUE(file.copy().clone());
}

TEST(FileHelper, CopyRejectsUnknownFlag)
{
  ExpectReject("copy -x /eos/src /eos/dst");
  ExpectReject("copy /eos/src");
}

//------------------------------------------------------------------------------
// purge / version / versions
//------------------------------------------------------------------------------
TEST(FileHelper, PurgeDefaultsToMinusOne)
{
  // no explicit count means 'apply the sys.versioning attribute'
  auto file = ParseOk("purge /eos/f.dat");
  ASSERT_TRUE(file.has_purge());
  ASSERT_EQ(file.purge().purge_version(), -1);
}

TEST(FileHelper, PurgeWithCount)
{
  auto file = ParseOk("purge /eos/f.dat 0");
  ASSERT_EQ(file.purge().purge_version(), 0);
}

TEST(FileHelper, Version)
{
  auto file = ParseOk("version /eos/f.dat 3");
  ASSERT_TRUE(file.has_version());
  ASSERT_EQ(file.version().purge_version(), 3);
}

TEST(FileHelper, Versions)
{
  auto file = ParseOk("versions /eos/f.dat");
  ASSERT_TRUE(file.has_versions());
  ASSERT_EQ(file.versions().grab_version(), "-1");
}

TEST(FileHelper, VersionsGrab)
{
  auto file = ParseOk("versions /eos/f.dat 1600000000");
  ASSERT_EQ(file.versions().grab_version(), "1600000000");
}

//------------------------------------------------------------------------------
// layout
//------------------------------------------------------------------------------
TEST(FileHelper, LayoutStripes)
{
  auto file = ParseOk("layout /eos/f.dat -stripes 4");
  ASSERT_TRUE(file.has_layout());
  ASSERT_EQ(file.layout().stripes(), 4u);
}

TEST(FileHelper, LayoutChecksum)
{
  auto file = ParseOk("layout /eos/f.dat -checksum adler");
  ASSERT_EQ(file.layout().checksum(), "adler");
}

TEST(FileHelper, LayoutType)
{
  auto file = ParseOk("layout /eos/f.dat -type 00100112");
  ASSERT_EQ(file.layout().type(), "00100112");
}

TEST(FileHelper, LayoutRejectsUnknownSelector)
{
  ExpectReject("layout /eos/f.dat -bogus 4");
  ExpectReject("layout /eos/f.dat -stripes");
}

//------------------------------------------------------------------------------
// tag
//------------------------------------------------------------------------------
TEST(FileHelper, TagAdd)
{
  auto file = ParseOk("tag /eos/f.dat +5");
  ASSERT_TRUE(file.has_tag());
  ASSERT_TRUE(file.tag().add());
  ASSERT_FALSE(file.tag().remove());
  ASSERT_FALSE(file.tag().unlink());
  ASSERT_EQ(file.tag().fsid(), 5u);
}

TEST(FileHelper, TagRemove)
{
  auto file = ParseOk("tag /eos/f.dat -5");
  ASSERT_TRUE(file.tag().remove());
  ASSERT_EQ(file.tag().fsid(), 5u);
}

TEST(FileHelper, TagUnlink)
{
  auto file = ParseOk("tag /eos/f.dat ~5");
  ASSERT_TRUE(file.tag().unlink());
  ASSERT_EQ(file.tag().fsid(), 5u);
}

TEST(FileHelper, TagBareFsidSetsNoFlag)
{
  // legacy behaviour: an unprefixed fsid is forwarded with no flag set
  auto file = ParseOk("tag /eos/f.dat 5");
  ASSERT_FALSE(file.tag().add());
  ASSERT_FALSE(file.tag().remove());
  ASSERT_FALSE(file.tag().unlink());
  ASSERT_EQ(file.tag().fsid(), 5u);
}

//------------------------------------------------------------------------------
// convert
//------------------------------------------------------------------------------
TEST(FileHelper, ConvertPositionals)
{
  auto file = ParseOk("convert /eos/f.dat replica:2 default scattered adler");
  ASSERT_TRUE(file.has_convert());
  ASSERT_EQ(file.convert().layout(), "replica:2");
  ASSERT_EQ(file.convert().target_space(), "default");
  ASSERT_EQ(file.convert().placement_policy(), "scattered");
  ASSERT_EQ(file.convert().checksum(), "adler");
  ASSERT_FALSE(file.convert().rewrite());
}

TEST(FileHelper, ConvertRewrite)
{
  auto file = ParseOk("convert /eos/f.dat replica:2 --rewrite");
  ASSERT_TRUE(file.convert().rewrite());
  ASSERT_EQ(file.convert().layout(), "replica:2");
}

TEST(FileHelper, ConvertRejectsSync)
{
  ExpectReject("convert /eos/f.dat replica:2 --sync");
}

//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
TEST(FileHelper, VerifyFlags)
{
  auto file = ParseOk(
      "verify /eos/f.dat -checksum -commitchecksum -commitsize -commitfmd -resync");
  ASSERT_TRUE(file.has_verify());
  ASSERT_TRUE(file.verify().checksum());
  ASSERT_TRUE(file.verify().commitchecksum());
  ASSERT_TRUE(file.verify().commitsize());
  ASSERT_TRUE(file.verify().commitfmd());
  ASSERT_TRUE(file.verify().resync());
  ASSERT_EQ(file.md().path(), "/eos/f.dat");
}

TEST(FileHelper, VerifyFsidFilter)
{
  // a numeric positional after the path selects a single replica
  auto file = ParseOk("verify /eos/f.dat 3 -checksum");
  ASSERT_EQ(file.verify().fsid(), 3u);
  ASSERT_TRUE(file.verify().checksum());
}

TEST(FileHelper, VerifyRate)
{
  auto file = ParseOk("verify /eos/f.dat -rate 100");
  ASSERT_EQ(file.verify().rate(), 100u);
}

TEST(FileHelper, VerifyRejectsMissingRateValueAndPath)
{
  ExpectReject("verify /eos/f.dat -rate");
  ExpectReject("verify -checksum");
}

//------------------------------------------------------------------------------
// adjustreplica
//------------------------------------------------------------------------------
TEST(FileHelper, AdjustreplicaBare)
{
  auto file = ParseOk("adjustreplica /eos/f.dat");
  ASSERT_TRUE(file.has_adjustreplica());
  ASSERT_FALSE(file.adjustreplica().nodrop());
  ASSERT_TRUE(file.adjustreplica().space().empty());
}

TEST(FileHelper, AdjustreplicaSpaceAndSubgroup)
{
  auto file = ParseOk("adjustreplica /eos/f.dat default 3");
  ASSERT_EQ(file.adjustreplica().space(), "default");
  ASSERT_EQ(file.adjustreplica().subgroup(), "3");
}

TEST(FileHelper, AdjustreplicaOptions)
{
  auto file = ParseOk("adjustreplica /eos/f.dat --nodrop --exclude-fs 12");
  ASSERT_TRUE(file.adjustreplica().nodrop());
  ASSERT_EQ(file.adjustreplica().exclude_fs(), "12");
}

TEST(FileHelper, AdjustreplicaRejectsDanglingExcludeFs)
{
  ExpectReject("adjustreplica /eos/f.dat --exclude-fs");
}

//------------------------------------------------------------------------------
// share / workflow
//------------------------------------------------------------------------------
TEST(FileHelper, ShareDefaultLifetime)
{
  auto file = ParseOk("share /eos/f.dat");
  ASSERT_TRUE(file.has_share());
  ASSERT_EQ(file.share().expires(), 28u * 86400u);
}

TEST(FileHelper, ShareExplicitLifetime)
{
  auto file = ParseOk("share /eos/f.dat 3600");
  ASSERT_EQ(file.share().expires(), 3600u);
}

TEST(FileHelper, Workflow)
{
  auto file = ParseOk("workflow /eos/f.dat default sync::prepare");
  ASSERT_TRUE(file.has_workflow());
  ASSERT_EQ(file.workflow().workflow(), "default");
  ASSERT_EQ(file.workflow().event(), "sync::prepare");
}

TEST(FileHelper, WorkflowNeedsThreeArguments)
{
  ExpectReject("workflow /eos/f.dat default");
}

//------------------------------------------------------------------------------
// info
//------------------------------------------------------------------------------
TEST(FileHelper, InfoFlags)
{
  auto file = ParseOk("info /eos/f.dat --path --fid --fxid --size --checksum --fullpath "
                      "--proxy --env -m");
  ASSERT_TRUE(file.has_fileinfo());
  const auto& info = file.fileinfo();
  ASSERT_TRUE(info.path());
  ASSERT_TRUE(info.fid());
  ASSERT_TRUE(info.fxid());
  ASSERT_TRUE(info.size());
  ASSERT_TRUE(info.checksum());
  ASSERT_TRUE(info.fullpath());
  ASSERT_TRUE(info.proxy());
  ASSERT_TRUE(info.env());
  ASSERT_TRUE(info.monitoring());
  ASSERT_EQ(info.md().path(), "/eos/f.dat");
}

TEST(FileHelper, InfoSilentIsClientSide)
{
  // -s only suppresses client output, it is not part of the request
  FileHelper helper(gOpts);
  ASSERT_TRUE(helper.ParseCommand("info /eos/f.dat -s"));
  ASSERT_TRUE(helper.mIsSilent);
}

TEST(FileHelper, InfoFileIdSpecifiers)
{
  auto fid = ParseOk("info fid:99");
  ASSERT_EQ(fid.fileinfo().md().id(), 99u);
  ASSERT_EQ(fid.fileinfo().md().type(), eos::console::FILE);
  auto fxid = ParseOk("info fxid:63");
  ASSERT_EQ(fxid.fileinfo().md().id(), 0x63u);
}

TEST(FileHelper, InfoContainerIdSpecifiers)
{
  auto pid = ParseOk("info pid:42");
  ASSERT_EQ(pid.fileinfo().md().id(), 42u);
  ASSERT_EQ(pid.fileinfo().md().type(), eos::console::CONTAINER);
  auto pxid = ParseOk("info pxid:2a");
  ASSERT_EQ(pxid.fileinfo().md().id(), 42u);
  ASSERT_EQ(pxid.fileinfo().md().type(), eos::console::CONTAINER);
}

TEST(FileHelper, InfoInodeSpecifier)
{
  auto file = ParseOk("info inode:12345");
  ASSERT_EQ(file.fileinfo().md().ino(), 12345u);
  ASSERT_EQ(file.fileinfo().md().id(), 0u);
}

TEST(FileHelper, InfoRejectsUnknownFlagAndMissingPath)
{
  ExpectReject("info /eos/f.dat --bogus");
  ExpectReject("info -m");
  ExpectReject("info /eos/a /eos/b");
}

//------------------------------------------------------------------------------
// General rejections
//------------------------------------------------------------------------------
TEST(FileHelper, UnknownSubcommandIsRejected)
{
  ExpectReject("nosuchsubcommand /eos/f.dat");
  ExpectReject("");
}

TEST(FileHelper, QuotedArgumentsArePreserved)
{
  auto file = ParseOk("rename \"/eos/dir with space/a\" /eos/b");
  ASSERT_EQ(file.md().path(), "/eos/dir with space/a");
  ASSERT_EQ(file.rename().new_path(), "/eos/b");
}

TEST(FileHelper, AmpersandIsNotSealed)
{
  // FileProto fields carry raw values, the opaque-CGI '#AND#' convention
  // must not leak in
  auto file = ParseOk("rename /eos/a&b /eos/c");
  ASSERT_EQ(file.md().path(), "/eos/a&b");
}
