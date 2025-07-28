//------------------------------------------------------------------------------
//! @file NfsIoTests.cc
//! @author Robert-Paul Pasca - CERN
//! @brief Unit tests for NfsIo class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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
#define IN_TEST_HARNESS
#ifdef HAVE_NFS
#include "fst/io/nfs/NfsIo.hh"
#endif
#undef IN_TEST_HARNESS
#include "TestEnv.hh"
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#ifdef HAVE_NFS

//------------------------------------------------------------------------------
// Test fixture for NfsIo tests
//------------------------------------------------------------------------------
class NfsIoTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mTestDir = "/tmp/nfsio_test_" + std::to_string(getpid());
    ASSERT_EQ(0, mkdir(mTestDir.c_str(), 0755));
    
    mTestFile = mTestDir + "/test_file.dat";
    mTestPath = "nfs://" + mTestFile;
  }

  void TearDown() override
  {
    unlink(mTestFile.c_str());
    std::string xattr_file = mTestDir + "/.test_file.dat.xattr";
    unlink(xattr_file.c_str());
    rmdir(mTestDir.c_str());
  }

  std::string mTestDir;
  std::string mTestFile;
  std::string mTestPath;
};

//------------------------------------------------------------------------------
// Test NfsIo constructor and basic setup
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, Constructor)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);

  char buffer[10];
  EXPECT_EQ(-1, nfsio.fileRead(0, buffer, sizeof(buffer)));
  EXPECT_EQ(EBADF, errno);
}

//------------------------------------------------------------------------------
// Test file creation and opening
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, FileOpen)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  
  int result = nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0);
  EXPECT_EQ(SFS_OK, result);
  
  std::string test_data = "test";
  EXPECT_GT(nfsio.fileWrite(0, test_data.c_str(), test_data.size()), 0);
  EXPECT_EQ(0, nfsio.fileClose());
  
  char buffer[10];
  EXPECT_EQ(-1, nfsio.fileRead(0, buffer, sizeof(buffer)));
  EXPECT_EQ(EBADF, errno);
}

//------------------------------------------------------------------------------
// Test file write operations
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, FileWrite)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  
  std::string test_data = "Hello, NFS World!";
  int64_t bytes_written = nfsio.fileWrite(0, test_data.c_str(), test_data.size());
  
  EXPECT_EQ(test_data.size(), bytes_written);
  nfsio.fileClose();
  
  struct stat st;
  ASSERT_EQ(0, stat(mTestFile.c_str(), &st));
  EXPECT_EQ(test_data.size(), st.st_size);
}

//------------------------------------------------------------------------------
// Test file read operations
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, FileRead)
{
  std::string test_data = "Test string for NFS read";

  eos::fst::NfsIo nfsio_write(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio_write.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  ASSERT_EQ(test_data.size(), nfsio_write.fileWrite(0, test_data.c_str(), test_data.size()));
  nfsio_write.fileClose();
  
  eos::fst::NfsIo nfsio_read(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio_read.fileOpen(SFS_O_RDONLY, 0, "", 0));
  
  char buffer[1024];
  int64_t bytes_read = nfsio_read.fileRead(0, buffer, sizeof(buffer));
  
  EXPECT_EQ(test_data.size(), bytes_read);
  EXPECT_EQ(0, memcmp(test_data.c_str(), buffer, test_data.size()));
  nfsio_read.fileClose();
}

//------------------------------------------------------------------------------
// Test file stat operations
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, FileStat)
{
  std::string test_data = "Test file for nfs stat";
  
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  ASSERT_EQ(test_data.size(), nfsio.fileWrite(0, test_data.c_str(), test_data.size()));
  
  struct stat st;
  EXPECT_EQ(0, nfsio.fileStat(&st));
  EXPECT_EQ(test_data.size(), st.st_size);
  
  nfsio.fileClose();
}

//------------------------------------------------------------------------------
// Test file truncate operations
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, FileTruncate)
{
  std::string test_data = "This is a longer test string for nfs truncation";
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  ASSERT_EQ(test_data.size(), nfsio.fileWrite(0, test_data.c_str(), test_data.size()));
  
  // Sync the file before truncating to ensure data is written
  EXPECT_EQ(0, nfsio.fileSync());
  EXPECT_EQ(0, nfsio.fileTruncate(10));

  struct stat st;
  ASSERT_EQ(0, nfsio.fileStat(&st));
  EXPECT_EQ(10, st.st_size);
  nfsio.fileClose();
}

//------------------------------------------------------------------------------
// Test attribute operations
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, AttributeOperations)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  
  // Test setting an attribute
  std::string attr_name = "user.test.key";
  std::string attr_value = "test_value_123";
  
  EXPECT_EQ(0, nfsio.attrSet(attr_name, attr_value));
  
  // Test getting the attribute
  std::string retrieved_value;
  EXPECT_EQ(0, nfsio.attrGet(attr_name, retrieved_value));
  EXPECT_EQ(attr_value, retrieved_value);
  
  // Test listing attributes
  std::vector<std::string> attr_list;
  EXPECT_EQ(0, nfsio.attrList(attr_list));
  EXPECT_TRUE(std::find(attr_list.begin(), attr_list.end(), attr_name) != attr_list.end());
  
  // Test deleting an attribute
  EXPECT_EQ(0, nfsio.attrDelete(attr_name.c_str()));
  EXPECT_NE(0, nfsio.attrGet(attr_name, retrieved_value));
  EXPECT_EQ(ENOATTR, errno);
  
  nfsio.fileClose();
}

//------------------------------------------------------------------------------
// Test error conditions
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, ErrorConditions)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  
  char buffer[100];
  EXPECT_EQ(-1, nfsio.fileRead(0, buffer, sizeof(buffer)));
  EXPECT_EQ(EBADF, errno);
  
  std::string test_data = "test";
  EXPECT_EQ(-1, nfsio.fileWrite(0, test_data.c_str(), test_data.size()));
  EXPECT_EQ(EBADF, errno);
  
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  EXPECT_EQ(-1, nfsio.fileWrite(100, test_data.c_str(), test_data.size()));
  EXPECT_EQ(ENOTSUP, errno);
  
  nfsio.fileClose();
}

//------------------------------------------------------------------------------
// Test path parsing
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, PathParsing)
{
  // nfs:// prefix removal
  std::string nfs_path = "nfs:///tmp/test/file.dat";
  eos::fst::NfsIo nfsio(nfs_path, nullptr, nullptr);
  
  EXPECT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  nfsio.fileClose();
  
  unlink("/tmp/test/file.dat");
  rmdir("/tmp/test");
}

//------------------------------------------------------------------------------
// Test sequential write requirement
//------------------------------------------------------------------------------
TEST_F(NfsIoTest, SequentialWriteRequirement)
{
  eos::fst::NfsIo nfsio(mTestPath, nullptr, nullptr);
  ASSERT_EQ(SFS_OK, nfsio.fileOpen(SFS_O_CREAT | SFS_O_RDWR, 0644, "", 0));
  
  std::string data1 = "First chunk";
  std::string data2 = "Second chunk";
  
  EXPECT_EQ(data1.size(), nfsio.fileWrite(0, data1.c_str(), data1.size()));
  EXPECT_EQ(data2.size(), nfsio.fileWrite(data1.size(), data2.c_str(), data2.size()));
  
  // Non-sequential write should fail
  EXPECT_EQ(-1, nfsio.fileWrite(0, data1.c_str(), data1.size()));
  EXPECT_EQ(ENOTSUP, errno);
  
  nfsio.fileClose();
}

#endif // HAVE_NFS

//------------------------------------------------------------------------------
// Test that runs when NFS is not available
//------------------------------------------------------------------------------
TEST(NfsIoTestNoNfs, UnavailableWhenNoNfs)
{
#ifndef HAVE_NFS
  GTEST_SKIP() << "NFS support not compiled in, test skipped";
#else
  GTEST_SKIP() << "NFS support available, this test is for when NFS is not available";
#endif
}
