//------------------------------------------------------------------------------
//! \file FmdAttributeHandlerTest.cc
//! \author Jozsef Makai<jmakai@cern.ch>
//! \brief Tests for the FmdAttributeHandler class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "fst/FmdAttributeHandler.hh"
#include "fst/io/local/FsIo.hh"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

using namespace eos;
using namespace eos::fst;
using namespace ::testing;

class FmdAttributeHandlerTest : public ::testing::Test {
public:
  static constexpr auto testFileName = "/tmp/00000005";
  static constexpr auto nonExistingFileName = "/tmp/non_existing_file";
  static constexpr auto checksum = "1a2b3c4d";
  static constexpr auto mgmChecksum = "M1a2bG3c4dM";
  static constexpr auto mgmLocations= "1,";
  static constexpr auto fid = 1;
  static constexpr auto cid = 2;
  static constexpr auto size = 111;
  static constexpr auto mgmSize = 222;
  const int fsid = 1;

  FileIo* fileIo = nullptr;
  FileIo* nonExistingFileIo = nullptr;

  Fmd fmd, mgmSameFmd, mgmUpdatedFmd;

  FmdAttributeHandlerTest() {
    fmd.set_fid(fid);
    fmd.set_fsid(fsid);
    fmd.set_cid(cid);
    fmd.set_size(size);
    fmd.set_mgmsize(size);
    fmd.set_checksum(checksum);
    fmd.set_mgmchecksum(checksum);

    mgmSameFmd = fmd;
    mgmSameFmd.set_fsid(100); // set an irrelevant field for update

    mgmUpdatedFmd.set_fid(fid);
    mgmUpdatedFmd.set_fsid(fsid);
    mgmUpdatedFmd.set_cid(cid);
    mgmUpdatedFmd.set_size(mgmSize);
    mgmUpdatedFmd.set_mgmsize(mgmSize);
    mgmUpdatedFmd.set_checksum(mgmChecksum);
    mgmUpdatedFmd.set_mgmchecksum(mgmChecksum);
    mgmUpdatedFmd.set_locations(mgmLocations);
  }

  void SetUp() override {
    fileIo = new FsIo(testFileName);
    fileIo->fileOpen(SFS_O_CREAT | SFS_O_RDWR);
    fileIo->fileClose();
    fileIo->attrSet("user.eos.filecxerror", "0");
    fileIo->attrSet("user.eos.blockcxerror", "1");

    nonExistingFileIo = new FsIo(nonExistingFileName);
  }

  void TearDown() override {
    fileIo->fileRemove();
    delete fileIo;
    fileIo = nullptr;

    nonExistingFileIo->fileRemove();
    delete nonExistingFileIo;
    nonExistingFileIo = nullptr;
  }
};

class MockMgmCommunicator : public MgmCommunicator {
public:
  MOCK_METHOD3(GetMgmFmd, int(const char*, eos::common::FileId::fileid_t, struct Fmd&));
};

class MockCompression : public eos::common::Compression {
public:
  void
  Compress(eos::Buffer& record) override {

  }

  void
  Decompress(eos::Buffer& record) override {

  }
};

MockCompression mockCompressor;

TEST_F(FmdAttributeHandlerTest, TestAttrSetAndGet) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  testFmdAttributeHandler.FmdAttrSet(fileIo, fmd);

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestAttrGetWhenNotPresent) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  EXPECT_THROW(testFmdAttributeHandler.FmdAttrGet(fileIo), MDException);
}

TEST_F(FmdAttributeHandlerTest, TestAttrSetWhenFileNotPresent) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  Fmd fmd;
  EXPECT_THROW(testFmdAttributeHandler.FmdAttrSet(nonExistingFileIo, fmd), MDException);
}

TEST_F(FmdAttributeHandlerTest, TestAttrDelete) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  testFmdAttributeHandler.FmdAttrSet(fileIo, fmd);
  EXPECT_NO_THROW(testFmdAttributeHandler.FmdAttrGet(fileIo));
  testFmdAttributeHandler.FmdAttrDelete(fileIo);
  EXPECT_THROW(testFmdAttributeHandler.FmdAttrGet(fileIo), MDException);
}

TEST_F(FmdAttributeHandlerTest, TestAttrDeleteWhenNoFilePresent) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  EXPECT_THROW(testFmdAttributeHandler.FmdAttrDelete(fileIo), MDException);
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmNoData) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(Return(ENODATA));
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, 1, 2, "dummyManager"));
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmError) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(Return(-1));
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, 1, 2, "dummyManager"));
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFilePresent) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(fileIo, fsid, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  /* fsid is also set when there was no fmd on local disk */
  ASSERT_EQ(fsid, newFmd.fsid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFileNotPresent) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, fsid, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(nonExistingFileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  /* fsid is also set when there was no file on local disk */
  ASSERT_EQ(fsid, newFmd.fsid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFmdUpdate) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(mgmUpdatedFmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};

  testFmdAttributeHandler.FmdAttrSet(fileIo, fmd);
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(fileIo, 1, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  /* relevant values are updated from the MGM */
  ASSERT_EQ(mgmUpdatedFmd.fid(), newFmd.fid());
  ASSERT_EQ(mgmUpdatedFmd.fsid(), newFmd.fsid());
  ASSERT_EQ(mgmUpdatedFmd.cid(), newFmd.cid());
  ASSERT_EQ(mgmUpdatedFmd.size(), newFmd.size());
  ASSERT_EQ(mgmUpdatedFmd.checksum(), newFmd.checksum());
  ASSERT_EQ(mgmUpdatedFmd.locations(), newFmd.locations());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFmdUpToDate) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(mgmSameFmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};

  testFmdAttributeHandler.FmdAttrSet(fileIo, fmd);
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(fileIo, 1, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  /* we have the old disk values (especially for fsid) */
  ASSERT_EQ(fmd.fid(), newFmd.fid());
  ASSERT_EQ(fmd.fsid(), newFmd.fsid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.locations(), newFmd.locations());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithBadFile) {
  MockMgmCommunicator mockMgmCommunicator;
  EXPECT_CALL(mockMgmCommunicator, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FsIo badIo("/|this|/is*/a/bad?/<file name>");
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor, &mockMgmCommunicator};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(&badIo, 1, 2, "dummyManager"));
}

TEST_F(FmdAttributeHandlerTest, TestResyncDisk) {
  FmdAttributeHandler testFmdAttributeHandler {&mockCompressor};
  EXPECT_TRUE(testFmdAttributeHandler.ResyncDisk(testFileName, fsid, false));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  ASSERT_EQ(5, newFmd.fid());
  ASSERT_EQ(fsid, newFmd.fsid());
  ASSERT_EQ(0, newFmd.filecxerror());
  ASSERT_EQ(1, newFmd.blockcxerror());
}
