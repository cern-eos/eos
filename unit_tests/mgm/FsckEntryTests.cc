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
#include "gmock/gmock.h"
#include "common/LayoutId.hh"
#define IN_TEST_HARNESS
#include "mgm/fsck/FsckEntry.hh"
#undef IN_TEST_HARNESS

using ::testing::Return;
using eos::common::LayoutId;
using eos::common::FileSystem;
static constexpr uint64_t kTimestampSec {1560331003};
static constexpr uint64_t kFileSize {256256};
static std::string kChecksum {"74d77c3a"};

//------------------------------------------------------------------------------
// MockRepairJob that doesn't trigger any TPC transfer
//------------------------------------------------------------------------------
class MockRepairJob: public eos::mgm::FsckRepairJob
{
public:
  MockRepairJob(eos::common::FileId::fileid_t fid,
                FileSystem::fsid_t fsid_src,
                FileSystem::fsid_t fsid_trg = 0,
                std::set<FileSystem::fsid_t> exclude_srcs = {},
                std::set<FileSystem::fsid_t> exclude_dsts = {},
                bool drop_src = true,
                const std::string& app_tag = "fsck_mock"):
    eos::mgm::FsckRepairJob(fid, fsid_src, fsid_trg, exclude_srcs,
                            exclude_dsts, drop_src, app_tag)
  {}

  MOCK_METHOD0(DoItNoExcept, void());
  virtual void DoIt() noexcept
  {
    return DoItNoExcept();
  }
  MOCK_CONST_METHOD0(GetStatus, eos::mgm::FsckRepairJob::Status());
};

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
    mRepairJob = nullptr;
    mFsckEntry = std::unique_ptr<eos::mgm::FsckEntry>
                 (new eos::mgm::FsckEntry(1234567, 3, "none", nullptr));
    PopulateMgmFmd();

    for (auto fsid : mFsckEntry->mMgmFmd.locations()) {
      PopulateFstFmd(fsid);
    }

    // Redefine the repair factory to return a MockRepairJob
    mFsckEntry->mRepairFactory =
      [&](eos::common::FileId::fileid_t fid,
          FileSystem::fsid_t fsid_src,
          FileSystem::fsid_t fsid_trg ,
          std::set<FileSystem::fsid_t> exclude_srcs,
          std::set<FileSystem::fsid_t> exclude_dsts,
          bool drop_src,
    const std::string & app_tag) {
      if (mRepairJob) {
        return mRepairJob;
      } else {
        mRepairJob.reset(new MockRepairJob(fid, fsid_src, fsid_trg, exclude_srcs,
                                           exclude_dsts, drop_src, app_tag));
      }

      return mRepairJob;
    };
  }

  //----------------------------------------------------------------------------
  //! Tear down - not needed as everything is already handled by destructor
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
  void PopulateFstFmd(FileSystem::fsid_t fsid)
  {
    std::unique_ptr<eos::mgm::FstFileInfoT> finfo  {
      new eos::mgm::FstFileInfoT("/data01/00000000/0012d687", eos::mgm::FstErr::None)};
    finfo->mDiskSize = kFileSize;
    auto& proto_fmd = finfo->mFstFmd.mProtoFmd;
    // Populate FmdBase.proto
    proto_fmd.set_fid(123456);
    proto_fmd.set_cid(199991);
    proto_fmd.set_fsid(fsid);
    proto_fmd.set_ctime(kTimestampSec);
    proto_fmd.set_ctime_ns(0);
    proto_fmd.set_mtime(kTimestampSec);
    proto_fmd.set_mtime_ns(0);
    proto_fmd.set_atime(kTimestampSec);
    proto_fmd.set_atime_ns(0);
    // proto_fmd.set_checktime() unset
    proto_fmd.set_size(kFileSize);
    proto_fmd.set_disksize(kFileSize);
    proto_fmd.set_mgmsize(kFileSize);
    proto_fmd.set_checksum(kChecksum);
    proto_fmd.set_diskchecksum(kChecksum);
    proto_fmd.set_mgmchecksum(kChecksum);
    proto_fmd.set_lid(std::stoul("0x0100112", nullptr, 16));
    proto_fmd.set_uid(1001);
    proto_fmd.set_gid(2002);
    proto_fmd.set_filecxerror(0);
    proto_fmd.set_blockcxerror(0);
    proto_fmd.set_layouterror(0);
    proto_fmd.set_locations("3,5,");
    mFsckEntry->mFstFileInfo.insert(std::make_pair(fsid, std::move(finfo)));
  }

  std::unique_ptr<eos::mgm::FsckEntry> mFsckEntry;
  std::shared_ptr<eos::mgm::FsckRepairJob> mRepairJob;
};

//------------------------------------------------------------------------------
// MGM checksum difference
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, MgmXsDiff)
{
  using eos::common::StringConversion;
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::MgmXsDiff;
  size_t xs_sz;
  auto xs_buff = eos::common::StringConversion::Hex2BinDataChar("aabbccdd",
                 xs_sz);
  auto& mgm_fmd = mFsckEntry->mMgmFmd;
  mgm_fmd.set_checksum(xs_buff.get(), xs_sz);
  // The new MGM FMD chechsum should be different from the initial one
  ASSERT_STRNE(kChecksum.c_str(),
               StringConversion::BinData2HexString
               (mgm_fmd.checksum().c_str(),
                SHA256_DIGEST_LENGTH,
                LayoutId::GetChecksumLen(mgm_fmd.layout_id())).c_str());
  ASSERT_TRUE(mFsckEntry->Repair());
  // After a successful repair the checksum should match the original one
  ASSERT_STREQ(kChecksum.c_str(),
               StringConversion::BinData2HexString
               (mgm_fmd.checksum().c_str(),
                SHA256_DIGEST_LENGTH,
                LayoutId::GetChecksumLen(mgm_fmd.layout_id())).c_str());
}

//------------------------------------------------------------------------------
// MGM checksum difference and one FST replica can not be contacted
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, MgmXsDiffFstNoContact)
{
  using eos::common::StringConversion;
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::MgmXsDiff;
  size_t xs_sz;
  auto xs_buff = eos::common::StringConversion::Hex2BinDataChar("aabbccdd",
                 xs_sz);
  auto& mgm_fmd = mFsckEntry->mMgmFmd;
  mgm_fmd.set_checksum(xs_buff.get(), xs_sz);
  // The new MGM FMD chechsum should be different from the initial one
  ASSERT_STRNE(kChecksum.c_str(),
               StringConversion::BinData2HexString
               (mgm_fmd.checksum().c_str(),
                SHA256_DIGEST_LENGTH,
                LayoutId::GetChecksumLen(mgm_fmd.layout_id())).c_str());
  // Mark one of the FST replicas as NoContact
  auto& finfo = mFsckEntry->mFstFileInfo.begin()->second;
  finfo->mFstErr = eos::mgm::FstErr::NoContact;
  ASSERT_FALSE(mFsckEntry->Repair());
}

//------------------------------------------------------------------------------
// MGM size difference
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, MgmSzDiff)
{
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::MgmSzDiff;
  auto& mgm_fmd = mFsckEntry->mMgmFmd;
  mgm_fmd.set_size(123456789);
  // The new MGM FMD size should be different from the initial one
  ASSERT_NE(kFileSize, mgm_fmd.size());
  ASSERT_TRUE(mFsckEntry->Repair());
  // After a successful repair the size should match the original one
  ASSERT_EQ(kFileSize, mgm_fmd.size());
}

//------------------------------------------------------------------------------
// FST size difference
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, FstSzDiff)
{
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::FstSzDiff;
  // All FST sizes match, repair succeeds - no bad replicas
  ASSERT_TRUE(mFsckEntry->Repair());

  // All FST fmd sizes are different, repair fails - no good replicas
  for (auto& pair : mFsckEntry->mFstFileInfo) {
    auto& finfo = pair.second;
    finfo->mFstFmd.mProtoFmd.set_disksize(1);
  }

  ASSERT_FALSE(mFsckEntry->Repair());
  // Set the first FST fmd disksize to the correct one - repair successful
  std::shared_ptr<eos::mgm::FsckRepairJob> repair_job =
    mFsckEntry->mRepairFactory(0, 0, 0, {}, {}, true, "none");
  MockRepairJob* mock_job = static_cast<MockRepairJob*>(repair_job.get());
  EXPECT_CALL(*mock_job, DoItNoExcept);
  EXPECT_CALL(*mock_job, GetStatus).
  WillOnce(Return(eos::mgm::FsckRepairJob::Status::OK));
  auto& finfo = mFsckEntry->mFstFileInfo.begin()->second;
  finfo->mFstFmd.mProtoFmd.set_disksize(finfo->mFstFmd.mProtoFmd.size());
  ASSERT_TRUE(mFsckEntry->Repair());
}

//------------------------------------------------------------------------------
// FST xs difference
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, FstXsDiff)
{
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::FstXsDiff;
  // All FST xs match, repair succeeds - no bad replicas
  ASSERT_TRUE(mFsckEntry->Repair());

  // All FST fmd xs are different, repair failes - no good replicas
  for (auto& pair : mFsckEntry->mFstFileInfo) {
    auto& finfo = pair.second;
    finfo->mFstFmd.mProtoFmd.set_diskchecksum("abcdefab");
  }

  ASSERT_FALSE(mFsckEntry->Repair());
  // Set the first FST fmd xs to the correct one - repair successful
  // @note the repair factory always returns the same repair job object so that
  // we can easily set expecteations on it
  std::shared_ptr<eos::mgm::FsckRepairJob> repair_job =
    mFsckEntry->mRepairFactory(0, 0, 0, {}, {}, true, "none");
  MockRepairJob* mock_job = static_cast<MockRepairJob*>(repair_job.get());
  EXPECT_CALL(*mock_job, DoItNoExcept);
  EXPECT_CALL(*mock_job, GetStatus).
  WillOnce(Return(eos::mgm::FsckRepairJob::Status::OK));
  auto& finfo = mFsckEntry->mFstFileInfo.begin()->second;
  finfo->mFstFmd.mProtoFmd.set_diskchecksum(kChecksum);
  ASSERT_TRUE(mFsckEntry->Repair());
}

//------------------------------------------------------------------------------
// Unregistered replica when file has enough replicas gets dropped
// Begin:                  Final:
// MGM: 3 5                MGM: 3 5
// FST: 3 5 101(u)         FST: 3 5
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, UnregReplicaDrop)
{
  FileSystem::fsid_t unreg_fsid = 101;
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::UnregRepl;
  // Add one more FST replica which is unregistered
  PopulateFstFmd(unreg_fsid);
  ASSERT_TRUE(mFsckEntry->Repair());
  // The replica on FS 101 should be dropped from the map
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.find(unreg_fsid) ==
              mFsckEntry->mFstFileInfo.end());
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.size() ==
              LayoutId::GetStripeNumber(mFsckEntry->mMgmFmd.layout_id()) + 1);
}

//------------------------------------------------------------------------------
// Unregistered replica when file doesn't have enough replicas gets added
// Begin:                Final:
// MGM: 5                MGM: 5 101
// FST: 5 101(u)         FST: 5 101
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, UnregReplicaAdd)
{
  FileSystem::fsid_t unreg_fsid = 101;
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::UnregRepl;
  // Add one more FST replica which is unregistered
  PopulateFstFmd(unreg_fsid);
  // Drop the replica on fsid 3
  FileSystem::fsid_t drop_fsid = 3;
  ASSERT_EQ(1, mFsckEntry->mFstFileInfo.erase(drop_fsid));
  auto locations = mFsckEntry->mMgmFmd.mutable_locations();

  for (auto it = locations->begin(); it != locations->end(); ++it) {
    if (*it == drop_fsid) {
      locations->erase(it);
      break;
    }
  }

  ASSERT_TRUE(mFsckEntry->Repair());
  // The replica on FS 101 should be added to the map and MGM meta data info
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.find(unreg_fsid) !=
              mFsckEntry->mFstFileInfo.end());
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.size() ==
              LayoutId::GetStripeNumber(mFsckEntry->mMgmFmd.layout_id()) + 1);
}

//------------------------------------------------------------------------------
// Over-replicated files should drop some of their replicas to reach the
// nominal number of replicas of the layout
// Begin:                Final:
// MGM: 3 5 6 7          MGM: 3 5
// FST: 3 5 6 7          FST: 3 5
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, FileOverReplicated)
{
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::DiffRepl;

  for (const auto& elem : {
  6, 7
}) {
    PopulateFstFmd(elem);
    mFsckEntry->mMgmFmd.add_locations(elem);
  }
  // Over-replicated
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.size() >
              LayoutId::GetStripeNumber(mFsckEntry->mMgmFmd.layout_id()) + 1);
  ASSERT_TRUE(mFsckEntry->Repair());
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.size() ==
              LayoutId::GetStripeNumber(mFsckEntry->mMgmFmd.layout_id()) + 1);
}

//------------------------------------------------------------------------------
// Under-replicated files should trigger new FsckRepair jobs that create new
// replicas up to the nominal number of replicas of the layout
// Begin:                Final:
// MGM: 3                MGM: 3 x
// FST: 3                FST: 3 x
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, FileUnderReplicated)
{
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::DiffRepl;
  // Drop the replica on fsid 5
  FileSystem::fsid_t drop_fsid = 5;
  ASSERT_EQ(1, mFsckEntry->mFstFileInfo.erase(drop_fsid));
  auto locations = mFsckEntry->mMgmFmd.mutable_locations();

  for (auto it = locations->begin(); it != locations->end(); ++it) {
    if (*it == drop_fsid) {
      locations->erase(it);
      break;
    }
  }

  // Under-replicated
  ASSERT_TRUE(mFsckEntry->mFstFileInfo.size() <
              LayoutId::GetStripeNumber(mFsckEntry->mMgmFmd.layout_id()) + 1);
  // Set the expectations
  // @note the repair factory always returns the same repair job object so that
  // we can easily set expecteations on it
  std::shared_ptr<eos::mgm::FsckRepairJob> repair_job =
    mFsckEntry->mRepairFactory(0, 0, 0, {}, {}, false, "none");
  MockRepairJob* mock_job = static_cast<MockRepairJob*>(repair_job.get());
  EXPECT_CALL(*mock_job, DoItNoExcept).Times(1);
  EXPECT_CALL(*mock_job, GetStatus).
  WillOnce(Return(eos::mgm::FsckRepairJob::Status::OK));
  ASSERT_TRUE(mFsckEntry->Repair());
}

//------------------------------------------------------------------------------
// Missgin replica should be dropped from the MGM file metadata and a repair
// job shoudl bring the number of replicas back up to nominal number
// Begin:                Final:
// MGM: 3 5              MGM: 3 y
// FST: 3                FST: 3 y
//------------------------------------------------------------------------------
TEST_F(FsckEntryTest, FileMissingReplica)
{
  // Set the desired type of error
  mFsckEntry->mReportedErr = eos::mgm::FsckErr::MissRepl;
  // Mark replica on file system 5 as not on disk
  FileSystem::fsid_t miss_fsid = 5;
  auto it = mFsckEntry->mFstFileInfo.find(miss_fsid);
  it->second->mFstErr = eos::mgm::FstErr::NotOnDisk;
  // Set the expectations
  // @note the repair factory always returns the same repair job object so that
  // we can easily set expecteations on it
  std::shared_ptr<eos::mgm::FsckRepairJob> repair_job =
    mFsckEntry->mRepairFactory(0, 0, 0, {}, {}, false, "none");
  MockRepairJob* mock_job = static_cast<MockRepairJob*>(repair_job.get());
  EXPECT_CALL(*mock_job, DoItNoExcept).Times(1);
  EXPECT_CALL(*mock_job, GetStatus).
  WillOnce(Return(eos::mgm::FsckRepairJob::Status::OK));
  ASSERT_TRUE(mFsckEntry->Repair());
  // The missing replicas should no longer be registered with the MGM fmd
  bool found = false;

  for (const auto& fsid : mFsckEntry->mMgmFmd.locations()) {
    if (fsid == miss_fsid) {
      found = true;
      break;
    }
  }

  ASSERT_FALSE(found);
}
