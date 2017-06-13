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

using namespace eos::fst;
using namespace ::testing;

class FmdAttributeHandlerTest : public ::testing::Test {
public:
  static constexpr auto testFileName = "/tmp/fmd_attribute_handler_test_file";
  static constexpr auto nonExistingFileName = "/tmp/non_existing_file";
  static constexpr auto checksum = "1a2b3c4d";
  static constexpr auto fid = 1;
  static constexpr auto cid = 2;
  static constexpr auto size = 111;

  FileIo* fileIo = nullptr;
  FileIo* nonExistingFileIo = nullptr;

  Fmd fmd;

  FmdAttributeHandlerTest() {
    fmd.set_fid(fid);
    fmd.set_cid(cid);
    fmd.set_size(size);
    fmd.set_mgmsize(size);
    fmd.set_checksum(checksum);
    fmd.set_mgmchecksum(checksum);
  }

  void SetUp() override {
    fileIo = new FsIo(testFileName);
    fileIo->fileOpen(SFS_O_CREAT | SFS_O_RDWR);
    fileIo->fileClose();

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

class MockFmdClient : public FmdClient {
public:
  MOCK_METHOD3(GetMgmFmd, int(const char*, eos::common::FileId::fileid_t, struct Fmd&));
};

TEST_F(FmdAttributeHandlerTest, TestAttrSetAndGet) {
  gFmdAttributeHandler.FmdAttrSet(fileIo, fmd);

  Fmd newFmd = gFmdAttributeHandler.FmdAttrGet(fileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestAttrGetWhenNotPresent) {
  EXPECT_THROW(gFmdAttributeHandler.FmdAttrGet(fileIo), fmd_attribute_error);
}

TEST_F(FmdAttributeHandlerTest, TestAttrSetWhenFileNotPresent) {
  Fmd fmd;
  EXPECT_THROW(gFmdAttributeHandler.FmdAttrSet(nonExistingFileIo, fmd), fmd_attribute_error);
}

TEST_F(FmdAttributeHandlerTest, TestAttrDelete) {
  gFmdAttributeHandler.FmdAttrSet(fileIo, fmd);
  EXPECT_NO_THROW(gFmdAttributeHandler.FmdAttrGet(fileIo));
  gFmdAttributeHandler.FmdAttrDelete(fileIo);
  EXPECT_THROW(gFmdAttributeHandler.FmdAttrGet(fileIo), fmd_attribute_error);
}

TEST_F(FmdAttributeHandlerTest, TestAttrDeleteWhenNoFilePresent) {
  EXPECT_THROW(gFmdAttributeHandler.FmdAttrDelete(fileIo), fmd_attribute_error);
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmNoData) {
  MockFmdClient mockFmdClient;
  EXPECT_CALL(mockFmdClient, GetMgmFmd(_, _, _)).WillOnce(Return(ENODATA));
  FmdAttributeHandler testFmdAttributeHandler {&mockFmdClient};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, 1, 2, "dummyManager"));
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmError) {
  MockFmdClient mockFmdClient;
  EXPECT_CALL(mockFmdClient, GetMgmFmd(_, _, _)).WillOnce(Return(-1));
  FmdAttributeHandler testFmdAttributeHandler {&mockFmdClient};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, 1, 2, "dummyManager"));
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFilePresent) {
  MockFmdClient mockFmdClient;
  EXPECT_CALL(mockFmdClient, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockFmdClient};
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(fileIo, 1, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(fileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithFileNotPresent) {
  MockFmdClient mockFmdClient;
  EXPECT_CALL(mockFmdClient, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FmdAttributeHandler testFmdAttributeHandler {&mockFmdClient};
  EXPECT_TRUE(testFmdAttributeHandler.ResyncMgm(nonExistingFileIo, 1, 2, "dummyManager"));

  Fmd newFmd = testFmdAttributeHandler.FmdAttrGet(nonExistingFileIo);

  ASSERT_EQ(fmd.fid(), newFmd.fid());
  ASSERT_EQ(fmd.cid(), newFmd.cid());
  ASSERT_EQ(fmd.size(), newFmd.size());
  ASSERT_EQ(fmd.mgmsize(), newFmd.mgmsize());
  ASSERT_EQ(fmd.checksum(), newFmd.checksum());
  ASSERT_EQ(fmd.mgmchecksum(), newFmd.mgmchecksum());
}

TEST_F(FmdAttributeHandlerTest, TestResyncMgmWithBadFile) {
  MockFmdClient mockFmdClient;
  EXPECT_CALL(mockFmdClient, GetMgmFmd(_, _, _)).WillOnce(DoAll(SetArgReferee<2>(fmd), Return(0)));

  FsIo badIo("/|this|/is*/a/bad?/<file name>");
  FmdAttributeHandler testFmdAttributeHandler {&mockFmdClient};
  EXPECT_FALSE(testFmdAttributeHandler.ResyncMgm(&badIo, 1, 2, "dummyManager"));
}
