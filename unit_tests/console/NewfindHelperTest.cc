//------------------------------------------------------------------------------
//! @file NewfindHelperTest.cc
//! @author Cedric Caffy - CERN
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
// NewfindHelper turns an 'eos find' command line into a FindProto wrapped in a
// RequestProto. 'eos -j find --fileinfo <path>' relies on two independent bits
// of that mapping: FindProto.Fileinfo, and RequestProto.format=JSON which the
// MGM reads back through IProcCommand::WantsJsonOutput(). Both are pinned here
// since the JSON output of find is keyed on them.
//------------------------------------------------------------------------------

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "console/commands/helpers/NewfindHelper.hh"
#undef IN_TEST_HARNESS

namespace {
//------------------------------------------------------------------------------
//! Parse a command line and return the resulting RequestProto
//------------------------------------------------------------------------------
bool
Parse(const GlobalOptions& opts, const std::string& cmd, eos::console::RequestProto& out)
{
  NewfindHelper helper(opts);

  if (!helper.ParseCommand(cmd.c_str())) {
    return false;
  }

  out = helper.GetRequest();
  return true;
}

//------------------------------------------------------------------------------
//! Parse with the default global options and expect success
//------------------------------------------------------------------------------
eos::console::RequestProto
ParseOk(const std::string& cmd)
{
  GlobalOptions opts;
  eos::console::RequestProto req;
  EXPECT_TRUE(Parse(opts, cmd, req)) << "failed to parse: " << cmd;
  return req;
}

//------------------------------------------------------------------------------
//! Parse as if the global -j flag had been given
//------------------------------------------------------------------------------
eos::console::RequestProto
ParseOkJson(const std::string& cmd)
{
  GlobalOptions opts;
  opts.mJsonFormat = true;
  eos::console::RequestProto req;
  EXPECT_TRUE(Parse(opts, cmd, req)) << "failed to parse: " << cmd;
  return req;
}
} // namespace

//------------------------------------------------------------------------------
// --fileinfo mapping
//------------------------------------------------------------------------------
TEST(NewfindHelper, FileinfoFlagIsMapped)
{
  auto req = ParseOk("--fileinfo /eos/dev/dir/");
  ASSERT_TRUE(req.find().fileinfo());
  ASSERT_EQ(req.find().path(), "/eos/dev/dir/");
}

TEST(NewfindHelper, FileinfoIsOffByDefault)
{
  ASSERT_FALSE(ParseOk("/eos/dev/dir/").find().fileinfo());
}

//------------------------------------------------------------------------------
// The global -j flag reaches the MGM as RequestProto::JSON. This is what
// NewfindCmd reads to decide between the monitoring format and JSON.
//------------------------------------------------------------------------------
TEST(NewfindHelper, JsonGlobalOptionSetsRequestFormat)
{
  auto req = ParseOkJson("--fileinfo /eos/dev/dir/");
  ASSERT_EQ(req.format(), eos::console::RequestProto::JSON);
  ASSERT_TRUE(req.find().fileinfo());
}

TEST(NewfindHelper, RequestFormatDefaultsToNonJson)
{
  auto req = ParseOk("--fileinfo /eos/dev/dir/");
  ASSERT_EQ(req.format(), eos::console::RequestProto::DEFAULT);
}

//------------------------------------------------------------------------------
// -j must not disturb anything else - it only selects the output format
//------------------------------------------------------------------------------
TEST(NewfindHelper, JsonGlobalOptionLeavesFindProtoUntouched)
{
  const std::string cmd = "-f --size --checksum --maxdepth 3 /eos/dev/dir/";
  auto plain = ParseOk(cmd);
  auto json = ParseOkJson(cmd);
  ASSERT_EQ(plain.find().SerializeAsString(), json.find().SerializeAsString());
  ASSERT_EQ(plain.format(), eos::console::RequestProto::DEFAULT);
  ASSERT_EQ(json.format(), eos::console::RequestProto::JSON);
}

//------------------------------------------------------------------------------
// --fileinfo composes with the entry selection flags, which decide whether
// directories, files or both get a JSON entry
//------------------------------------------------------------------------------
TEST(NewfindHelper, FileinfoComposesWithSelectionFlags)
{
  auto files = ParseOkJson("-f --fileinfo /eos/dev/dir/");
  ASSERT_TRUE(files.find().fileinfo());
  ASSERT_TRUE(files.find().files());
  ASSERT_FALSE(files.find().directories());
  auto dirs = ParseOkJson("-d --fileinfo /eos/dev/dir/");
  ASSERT_TRUE(dirs.find().fileinfo());
  ASSERT_TRUE(dirs.find().directories());
  ASSERT_FALSE(dirs.find().files());
}

//------------------------------------------------------------------------------
// --fileinfo reports on the entries, --purge, --layoutstripes and -b act on
// them. Each of those only handles one kind of entry, so the other kind would
// still be reported on and the two outputs would interleave
//------------------------------------------------------------------------------
TEST(NewfindHelper, FileinfoRejectsActionFlags)
{
  GlobalOptions opts;
  eos::console::RequestProto req;
  ASSERT_FALSE(Parse(opts, "--fileinfo --purge 3 /eos/dev/dir/", req));
  ASSERT_FALSE(Parse(opts, "--fileinfo --purge atomic /eos/dev/dir/", req));
  ASSERT_FALSE(Parse(opts, "--fileinfo --layoutstripes 2 /eos/dev/dir/", req));
  ASSERT_FALSE(Parse(opts, "--fileinfo -b /eos/dev/dir/", req));
  // the order the flags are given in must not matter
  ASSERT_FALSE(Parse(opts, "--purge 3 --fileinfo /eos/dev/dir/", req));
  ASSERT_FALSE(Parse(opts, "--layoutstripes 2 --fileinfo /eos/dev/dir/", req));
  ASSERT_FALSE(Parse(opts, "-b --fileinfo /eos/dev/dir/", req));
}

TEST(NewfindHelper, ActionFlagsStayValidOnTheirOwn)
{
  GlobalOptions opts;
  eos::console::RequestProto req;
  ASSERT_TRUE(Parse(opts, "--purge 3 /eos/dev/dir/", req));
  ASSERT_EQ(req.find().purge(), "3");
  ASSERT_TRUE(Parse(opts, "--purge atomic /eos/dev/dir/", req));
  ASSERT_EQ(req.find().purge(), "atomic");
  ASSERT_TRUE(Parse(opts, "--layoutstripes 2 /eos/dev/dir/", req));
  ASSERT_TRUE(req.find().dolayoutstripes());
  ASSERT_TRUE(Parse(opts, "-b /eos/dev/dir/", req));
  ASSERT_TRUE(req.find().balance());
}
