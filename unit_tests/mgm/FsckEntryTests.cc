//------------------------------------------------------------------------------
// File: FsckEntryTests.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include <type_traits>
#define IN_TEST_HARNESS
#include "mgm/fsck/FsckEntry.hh"
#undef IN_TEST_HARNESS

static constexpr uint64_t kTimestampSec {1560331003};
static constexpr uint64_t kFileSize {256256};
static std::string kChecksum {"74d77c3a"};

//------------------------------------------------------------------------------
// Test fixture for the FsckEntry
//------------------------------------------------------------------------------
class FsckEntryTest: public ::testing::Test
{
protected:
  //----------------------------------------------------------------------------
  //! Set up
  //------------------------------------------------------------------------------
  void SetUp() override
  {
    mFsckEntry = std::unique_ptr<eos::mgm::FsckEntry>
                 (new eos::mgm::FsckEntry(1234567, 3, "none"));
    PopulateMgmFmd();

    for (auto fsid : mFsckEntry->mMgmFmd.locations()) {
      PopulateFstFmd(fsid);
    }
  }

  //----------------------------------------------------------------------------
  //! Tear down - no needed as everything is already handled by destructor
  //----------------------------------------------------------------------------
  // void TearDown() override;

  //----------------------------------------------------------------------------
  //! Populate with dummy data the MGM fmd structure
  //----------------------------------------------------------------------------
  void PopulateMgmFmd()
  {
    auto& fmd = mFsckEntry->mMgmFmd;
    // Populate FileMd.proto
    fmd.set_id(1234567);
    fmd.set_cont_id(199991);
    fmd.set_uid(1001);
    fmd.set_gid(2002);
    fmd.set_size(kFileSize);
    // Layout with two replicas, adler checkusm
    fmd.set_layout_id(std::stoul("0x0100112", nullptr, 16));
    fmd.set_name("test_file.dat");
    // Date: 06/12/2019 @ 9:16am
    struct timespec ts;
    ts.tv_sec = kTimestampSec;
    ts.tv_nsec = 0;
    fmd.set_ctime(&ts, sizeof(ts));
    fmd.set_mtime(&ts, sizeof(ts));
    size_t xs_sz;
    auto xs_buff = eos::common::StringConversion::Hex2BinDataChar(kChecksum, xs_sz);
    fmd.set_checksum(xs_buff.get(), xs_sz);
    fmd.add_locations(3);
    fmd.add_locations(5);
  }

  //----------------------------------------------------------------------------
  //! Populate with dummy data the FST fmd structure
  //----------------------------------------------------------------------------
  void PopulateFstFmd(eos::common::FileSystem::fsid_t fsid)
  {
    if (mFsckEntry->mFstFileInfo.find(fsid) != mFsckEntry->mFstFileInfo.end()) {
      return;
    }

    std::unique_ptr<eos::mgm::FstFileInfoT> finfo  {
      new eos::mgm::FstFileInfoT("/data01/00000000/0012d687", eos::mgm::FstErr::None)};
    finfo->mDiskSize = kFileSize;
    auto& fmd = finfo->mFstFmd;
    // Populate FmdBase.proto
    fmd.set_fid(123456);
    fmd.set_cid(199991);
    fmd.set_fsid(fsid);
    fmd.set_ctime(kTimestampSec);
    fmd.set_ctime_ns(0);
    fmd.set_mtime(kTimestampSec);
    fmd.set_mtime_ns(0);
    fmd.set_atime(kTimestampSec);
    fmd.set_atime_ns(0);
    // fmd.set_checktime() unset
    fmd.set_size(kFileSize);
    fmd.set_disksize(kFileSize);
    fmd.set_mgmsize(kFileSize);
    fmd.set_checksum(kChecksum);
    fmd.set_diskchecksum(kChecksum);
    fmd.set_mgmchecksum(kChecksum);
    fmd.set_lid(std::stoul("0x0100112", nullptr, 16));
    fmd.set_uid(1001);
    fmd.set_gid(2002);
    fmd.set_filecxerror(0);
    fmd.set_blockcxerror(0);
    fmd.set_layouterror(0);
    fmd.set_locations("3,5,");
    mFsckEntry->mFstFileInfo.insert(std::make_pair(fsid, std::move(finfo)));
  }

  std::unique_ptr<eos::mgm::FsckEntry> mFsckEntry;
};

//------------------------------------------------------------------------------
// Basic checks
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, BasicChecks)
{
  ASSERT_TRUE(mFsckEntry->GenerateRepairWokflow().empty());
  // Got an error reported but the checks don't confirm it
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::MgmXsDiff;
  ASSERT_FALSE(mFsckEntry->GenerateRepairWokflow().empty());
  // Got an error reported but the checks do confirm it
  size_t xs_sz;
  auto xs_buff = eos::common::StringConversion::Hex2BinDataChar("aabbccdd",
                 xs_sz);
  mFsckEntry->mMgmFmd.set_checksum(xs_buff.get(), xs_sz);
  auto repair_ops = mFsckEntry->GenerateRepairWokflow();
  ASSERT_FALSE(repair_ops.empty());
}
