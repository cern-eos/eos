//------------------------------------------------------------------------------
// File: FuseFsTest.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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


/*----------------------------------------------------------------------------*/
#include <cppunit/extensions/HelperMacros.h>
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <utime.h>
#include <unistd.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <string.h>
#include <algorithm>
/*----------------------------------------------------------------------------*/
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_FUSE_FUSEFSTEST_HH__
#define __EOS_FUSE_FUSEFSTEST_HH__

//------------------------------------------------------------------------------
//! FuseFsTest class
//------------------------------------------------------------------------------
class FuseFsTest: public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(FuseFsTest);
    CPPUNIT_TEST(StatFileTest);
    CPPUNIT_TEST(ChmodFileTest);
    CPPUNIT_TEST(ChownFileTest);
    CPPUNIT_TEST(CreateRmDirTest);
    CPPUNIT_TEST(XAttrTest);
    CPPUNIT_TEST(RenameFileTest);
    CPPUNIT_TEST(DirListTest);  
    CPPUNIT_TEST(CreatTruncRmFileTest);
    CPPUNIT_TEST(StatVFSTest);
    CPPUNIT_TEST(UtimesTest);  
  CPPUNIT_TEST_SUITE_END();

 public:

  //----------------------------------------------------------------------------
  //! setUp function called before each test is done
  //----------------------------------------------------------------------------
  void setUp(void);

  
  //----------------------------------------------------------------------------
  //! tearDown function after each test is done
  //----------------------------------------------------------------------------
  void tearDown(void);

  
  //----------------------------------------------------------------------------
  //! Stat unopened file through the file system
  //----------------------------------------------------------------------------
  void StatFileTest();


  //----------------------------------------------------------------------------
  //! Chmod file
  //----------------------------------------------------------------------------
  void ChmodFileTest();


  //----------------------------------------------------------------------------
  //! Chown file
  //----------------------------------------------------------------------------
  void ChownFileTest();


  //----------------------------------------------------------------------------
  //! Create and remove file
  //----------------------------------------------------------------------------
  void CreateRmFileTest();


  //----------------------------------------------------------------------------
  //! Create and remove directory
  //----------------------------------------------------------------------------
  void CreateRmDirTest();


  //----------------------------------------------------------------------------
  //! Extended attributes test
  //----------------------------------------------------------------------------
  void XAttrTest();


  //----------------------------------------------------------------------------
  //! Rename file test
  //----------------------------------------------------------------------------
  void RenameFileTest();


  //----------------------------------------------------------------------------
  //! Directory list test
  //----------------------------------------------------------------------------
  void DirListTest();


  //----------------------------------------------------------------------------
  //! Create, truncate, remove unopened file through the file system
  //----------------------------------------------------------------------------
  void CreatTruncRmFileTest();


  //----------------------------------------------------------------------------
  //! Get file system information 
  //----------------------------------------------------------------------------
  void StatVFSTest();


  //----------------------------------------------------------------------------
  //! utimes test - change the last access/modification time
  //----------------------------------------------------------------------------
  void UtimesTest();

  
 private:

  eos::fuse::test::TestEnv* mEnv; ///< testing environment object
};

CPPUNIT_TEST_SUITE_REGISTRATION(FuseFsTest);


//------------------------------------------------------------------------------
// setUp function called before each test is done
//------------------------------------------------------------------------------
void
FuseFsTest::setUp()
{
  mEnv = new eos::fuse::test::TestEnv();
}


//----------------------------------------------------------------------------
// tearDown function after each test is done
//----------------------------------------------------------------------------
void
FuseFsTest::tearDown()
{
  delete mEnv;
}


//------------------------------------------------------------------------------
// Stat unopened file through the file system
//------------------------------------------------------------------------------
void
FuseFsTest::StatFileTest()
{
  struct stat buf;
  std::string fname = mEnv->GetMapping("file_path");
  off_t file_size = atoi(mEnv->GetMapping("file_size").c_str());
  mode_t perm_mask = S_IRWXU | S_IRWXG | S_IRWXO;
  mode_t expect_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  CPPUNIT_ASSERT(buf.st_size == file_size);
  CPPUNIT_ASSERT(S_ISREG(buf.st_mode));
  CPPUNIT_ASSERT((buf.st_mode & perm_mask) == expect_mode);
}


//------------------------------------------------------------------------------
// Chmod on file
//------------------------------------------------------------------------------
void
FuseFsTest::ChmodFileTest()
{
  struct stat buf;
  std::string fname = mEnv->GetMapping("file_path");
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  mode_t perm_mask = S_IRWXU | S_IRWXG | S_IRWXO;
  mode_t old_mode = buf.st_mode & perm_mask;
  mode_t new_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
  // Set the new mode - this has no effect as in EOS we don't enforce
  // permissions on file level but on directory level
  CPPUNIT_ASSERT(!chmod(fname.c_str(), new_mode));
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  CPPUNIT_ASSERT((buf.st_mode & perm_mask ) == old_mode);
}


//------------------------------------------------------------------------------
// Chown file
//------------------------------------------------------------------------------
void
FuseFsTest::ChownFileTest()
{
  struct stat buf;
  std::string fname = mEnv->GetMapping("file_path");
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  uid_t old_uid = buf.st_uid;
  gid_t old_gid = buf.st_gid;
  uid_t new_uid = 3; // adm
  gid_t new_gid = 4; // adm
  CPPUNIT_ASSERT(!chown(fname.c_str(), new_uid, new_gid));
  // Chown is not allowed from FUSE at the moment
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  CPPUNIT_ASSERT(buf.st_uid == old_uid);
  CPPUNIT_ASSERT(buf.st_gid == old_gid);
}


//------------------------------------------------------------------------------
// Create and remove directory
//------------------------------------------------------------------------------
void
FuseFsTest::CreateRmDirTest()
{
  std::string dummy_dir = mEnv->GetMapping("dir_dummy");
  mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH;
  CPPUNIT_ASSERT(!mkdir(dummy_dir.c_str(), mode));
  CPPUNIT_ASSERT(!rmdir(dummy_dir.c_str()));     
}


//------------------------------------------------------------------------------
// Extended attributes test
//------------------------------------------------------------------------------
void
FuseFsTest::XAttrTest()
{
  // List xattr names - see TestEnv.cc for expected attributes
  ssize_t sz_real = 0;
  ssize_t sz_xattr = 16384;
  char listx[sz_xattr];
  std::string dir = mEnv->GetMapping("dir_path");
  CPPUNIT_ASSERT((sz_real = listxattr(dir.c_str(), listx, sz_xattr)) != -1);

  // Get the individual xattrs
  std::vector<std::string> vxattr;
  std::istringstream iss(std::string(listx, listx + sz_real - 1));
  std::string xattr;

  while (std::getline(iss, xattr, '\0'))
  {
    vxattr.push_back(xattr);
    //std::cout << "XAtrr elem: " << xattr << std::endl;
  }

  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.blockchecksum") != vxattr.end());
  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.blocksize") != vxattr.end());
  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.checksum") != vxattr.end());
  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.layout") != vxattr.end());
  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.nstripes") != vxattr.end());
  CPPUNIT_ASSERT(find(vxattr.begin(), vxattr.end(), "user.admin.forced.space") != vxattr.end());

  // Get xattr values and compare to expected values
  ssize_t sz_val_real = 0;
  ssize_t sz_val = 4096;
  char value[sz_val];

  // TODO: rewrite this when we have a better compiler
  std::map<std::string, std::string> expect_map;
  expect_map["user.admin.forced.blockchecksum"] = "crc32c";
  expect_map["user.admin.forced.blocksize"] = "4k";
  expect_map["user.admin.forced.checksum"] = "adler";
  expect_map["user.admin.forced.layout"] = "replica";
  expect_map["user.admin.forced.nstripes"] = "2";
  expect_map["user.admin.forced.space"] = "default";
  
  for (auto elem = vxattr.begin(); elem != vxattr.begin(); ++elem)
  {
    CPPUNIT_ASSERT((sz_val_real = getxattr(dir.c_str(), elem->c_str(), value, sz_val)) != -1);
    CPPUNIT_ASSERT(strncmp(value, expect_map[*elem].c_str(), sz_val_real) == 0);   
  }

  // Set an extended attribute
  std::string new_xattr = "user.fuse.test";
  std::string new_val = "test_val";

  CPPUNIT_ASSERT(!setxattr(dir.c_str(), new_xattr.c_str(),
                           new_val.c_str(), new_val.length() + 1, 0));

  // Check the newly set xattr 
  CPPUNIT_ASSERT((sz_val_real = getxattr(dir.c_str(), new_xattr.c_str(), value, sz_val)) != -1);
  CPPUNIT_ASSERT(strncmp(value, new_val.c_str(), sz_val_real) == 0);

  // Remove the newly added xattr
  CPPUNIT_ASSERT(!removexattr(dir.c_str(), new_xattr.c_str()));
}


//------------------------------------------------------------------------------
// Rename file test
//------------------------------------------------------------------------------
void
FuseFsTest::RenameFileTest()
{
  int fd = -1;
  struct stat buf;
  std::string old_path = mEnv->GetMapping("file_dummy");
  std::string new_path = mEnv->GetMapping("file_rename");
  old_path += "_rft";
  
  CPPUNIT_ASSERT((fd = creat(old_path.c_str(), S_IRWXU)) != -1);
  CPPUNIT_ASSERT(!close(fd));
  CPPUNIT_ASSERT(!rename(old_path.c_str(), new_path.c_str()));
  CPPUNIT_ASSERT(stat(old_path.c_str(), &buf)); // fails
  CPPUNIT_ASSERT(!stat(new_path.c_str(), &buf));
  CPPUNIT_ASSERT(!rename(new_path.c_str(), old_path.c_str()));
  CPPUNIT_ASSERT(stat(new_path.c_str(), &buf)); // fails
  CPPUNIT_ASSERT(!stat(old_path.c_str(), &buf));
  CPPUNIT_ASSERT(!remove(old_path.c_str()));
}


//------------------------------------------------------------------------------
// Directory list test
//------------------------------------------------------------------------------
void
FuseFsTest::DirListTest()
{
  // Create dummy file
  std::string dir_path = mEnv->GetMapping("dir_path");
  std::string fdummy = mEnv->GetMapping("file_dummy");
  fdummy += "_dlt";
  int fd = -1;
  
  CPPUNIT_ASSERT((fd = creat(fdummy.c_str(), S_IRUSR | S_IWUSR)) != -1);
  CPPUNIT_ASSERT(!close(fd));

  // Open, read and close directory
  struct dirent* dirent;
  std::vector<struct dirent> vdirent;
  DIR* dir = NULL;
  CPPUNIT_ASSERT((dir = opendir(dir_path.c_str())) != NULL);

  while ((dirent = readdir(dir)))
  {
    vdirent.push_back(*dirent);
    //std::cout << "Name: " << dirent->d_name << std::endl;
  }

  CPPUNIT_ASSERT(!closedir(dir));
  
  // We expect 4 files including . and ..
  CPPUNIT_ASSERT(vdirent.size() == 4);
  CPPUNIT_ASSERT(!remove(fdummy.c_str()));
}



//------------------------------------------------------------------------------
// Create, truncate, remove unopened file through the file system
//------------------------------------------------------------------------------
void
FuseFsTest::CreatTruncRmFileTest()
{
  // Fill buffer with random characters
  off_t sz_buff = 105;
  char* buff = new char[sz_buff];
  std::ifstream urandom ("/dev/urandom", std::fstream::in | std::fstream::binary);
  urandom.read(buff, sz_buff);
  urandom.close();

  // Write 4.5 MB file using filled buffer
  int fd = -1;
  off_t offset = 0;
  size_t count = sz_buff;
  ssize_t nwrite = sz_buff;
  off_t file_size = 4.5 * 1024;
  std::string fdummy = mEnv->GetMapping("file_dummy"); fdummy += "_ctrft";
  CPPUNIT_ASSERT((fd = creat(fdummy.c_str(), S_IRUSR | S_IWUSR)) != -1);

  while (offset != file_size)
  {
    if (file_size - offset < sz_buff)
      count = file_size - offset;
    
    CPPUNIT_ASSERT((nwrite = write(fd, buff, count)) == (ssize_t)count);
    offset += count;    
  }

  CPPUNIT_ASSERT(!close(fd));

  // Truncate the file to 1MB and stat it
  struct stat buf;
  CPPUNIT_ASSERT(!stat(fdummy.c_str(), &buf));
  CPPUNIT_ASSERT(buf.st_size == file_size);
  offset = 1024 * 1024;
  CPPUNIT_ASSERT(!truncate(fdummy.c_str(), offset));
  CPPUNIT_ASSERT(!stat(fdummy.c_str(), &buf));
  CPPUNIT_ASSERT(buf.st_size == offset);
  CPPUNIT_ASSERT(!remove(fdummy.c_str()));
  delete[] buff;
}


//------------------------------------------------------------------------------
// Get file system information 
//------------------------------------------------------------------------------
void
FuseFsTest::StatVFSTest()
{
  struct statvfs buf;
  std::string dir_path = mEnv->GetMapping("dir_path");
  CPPUNIT_ASSERT(!statvfs(dir_path.c_str(), &buf));
  CPPUNIT_ASSERT(buf.f_bsize == 4096);
  CPPUNIT_ASSERT(buf.f_frsize == 4096);
}


//------------------------------------------------------------------------------
// utimes test - change the last access/modification time
//------------------------------------------------------------------------------
void
FuseFsTest::UtimesTest()
{
  struct stat fbuf, nfbuf; // file stat
  struct stat dbuf, ndbuf; // dir stat
  std::string dir = mEnv->GetMapping("dir_path");
  std::string fname = mEnv->GetMapping("file_path");
  
  CPPUNIT_ASSERT(!stat(fname.c_str(), &fbuf));
  CPPUNIT_ASSERT(!stat(dir.c_str(), &dbuf));
  struct utimbuf* times = NULL;
  CPPUNIT_ASSERT(!utime(fname.c_str(), times));
  CPPUNIT_ASSERT(!stat(fname.c_str(), &nfbuf));
  CPPUNIT_ASSERT(!stat(dir.c_str(), &ndbuf));

  // Check updated file atime and mtime
  CPPUNIT_ASSERT(fbuf.st_atime != nfbuf.st_atime);
  CPPUNIT_ASSERT(fbuf.st_mtime != nfbuf.st_mtime);

  // Checke updated parent dir atime and mtime
  // TODO: FIX THIS TEST 
  //CPPUNIT_ASSERT(dbuf.st_atime != ndbuf.st_atime);
  //CPPUNIT_ASSERT(dbuf.st_mtime != ndbuf.st_mtime);
}

#endif // __EOS_FUSE_FUSEFSTEST_HH__

