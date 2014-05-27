//------------------------------------------------------------------------------
// File: FuseFileTest.hh
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
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
/*----------------------------------------------------------------------------*/
#include "TestEnv.hh"
#include "fuse/FuseCache/CacheEntry.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOS_FUSE_FUSEFILETEST_HH__
#define __EOS_FUSE_FUSEFILETEST_HH__


//------------------------------------------------------------------------------
//! FuseFileTest class
//------------------------------------------------------------------------------
class FuseFileTest: public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(FuseFileTest);
    CPPUNIT_TEST(WriteStatTest);
    CPPUNIT_TEST(MultiProcessTest);
    CPPUNIT_TEST(WriteReadTest);
    CPPUNIT_TEST(SparseWriteTest);
    CPPUNIT_TEST(ManyWriteFilesTest);
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
  //! Write and stat file in a loop. We also sync every two steps so we would
  //! expect that the size obtained by doing stat on the opened file to be
  //! correct every time we do the sync.
  //----------------------------------------------------------------------------
  void WriteStatTest();


  //----------------------------------------------------------------------------
  //! Access the same file from two different processes. The parent proceess
  //! creates the file and the child process writes and closes it. At the end 
  //! the parent proceess reopens the newly created file to check that the 
  //! contents is correct.
  //----------------------------------------------------------------------------
  void MultiProcessTest();


  //----------------------------------------------------------------------------
  //! Test to ensure all data is flushed from cache before doing a reading
  //----------------------------------------------------------------------------
  void WriteReadTest();


  //----------------------------------------------------------------------------
  //! Test that doing sparse write operations does not block the cache by
  //! filling up with partial cache entries and never evicting them.
  //----------------------------------------------------------------------------
  void SparseWriteTest();


  //----------------------------------------------------------------------------
  //! Have many files opened for writing which only contain an incomplete 
  //! cache entry such that the write cache is rapidly filled with parital 
  //! entries. This should trigger the automatic eviction of some cache entries
  //! such that the writing does not block.
  //!
  //----------------------------------------------------------------------------
  void ManyWriteFilesTest();

 private:

  eos::fuse::test::TestEnv* mEnv; ///< testing environment object
};

CPPUNIT_TEST_SUITE_REGISTRATION(FuseFileTest);


//------------------------------------------------------------------------------
// setUp function called before each test is done
//------------------------------------------------------------------------------
void
FuseFileTest::setUp()
{
  mEnv = new eos::fuse::test::TestEnv();
}


//----------------------------------------------------------------------------
// tearDown function after each test is done
//----------------------------------------------------------------------------
void
FuseFileTest::tearDown()
{
  delete mEnv;
}


//------------------------------------------------------------------------------
// Write and stat open file in a loop
//------------------------------------------------------------------------------
void
FuseFileTest::WriteStatTest()
{
  // Fill buffer with random characters
  off_t sz_buff = 1024*1024 + 3; // ~ 1MB
  char* buff = new char[sz_buff];
  std::ifstream urandom ("/dev/urandom", std::fstream::in | std::fstream::binary);
  urandom.read(buff, sz_buff);
  urandom.close();

  // Create file
  int fd = -1;
  struct stat buf;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  std::string fname = mEnv->GetMapping("file_dummy"); fname += "_wst";
  CPPUNIT_ASSERT((fd = creat(fname.c_str(), mode)) != -1);
  
  // Write-(sync)-stat the file
  int count = 0;
  off_t offset = 0;
  off_t sz_file = 10.3 * 1024 *1024; // ~ 10MB

  while (offset < sz_file)
  {                 
    CPPUNIT_ASSERT(!fstat(fd, &buf));
    // Expect correct real size all the time
    CPPUNIT_ASSERT(buf.st_size == offset);
    CPPUNIT_ASSERT(S_ISREG(buf.st_mode));
    CPPUNIT_ASSERT((buf.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) == mode);
    CPPUNIT_ASSERT(write(fd, buff, sz_buff) == (ssize_t) sz_buff);
    offset += sz_buff;
   
    if (count % 2)
      CPPUNIT_ASSERT(!fsync(fd));
       
    if (offset + sz_buff > sz_file)
      sz_buff = sz_file - offset;

    count++;
  }
  
  CPPUNIT_ASSERT(!close(fd));
  CPPUNIT_ASSERT(!stat(fname.c_str(), &buf));
  CPPUNIT_ASSERT(buf.st_size == sz_file);               
  CPPUNIT_ASSERT(!remove(fname.c_str()));
  delete[] buff;
}


//------------------------------------------------------------------------------
// Access the same file from two different processes. The parent proceess
// creates the file and the child process writes and closes it. At the end 
// the parent proceess reopens the newly created file to check that the 
// contents is correct.
//------------------------------------------------------------------------------
void 
FuseFileTest::MultiProcessTest()
{
  // Fill buffer with random characters
  off_t sz_buff = 1024*1024 + 137; // ~ 1MB
  char* buff = new char[sz_buff];
  std::ifstream urandom ("/dev/urandom", std::fstream::in | std::fstream::binary);
  urandom.read(buff, sz_buff);
  urandom.close();

  int fd = -1;
  std::string fname = mEnv->GetMapping("file_dummy"); fname += "_mpt";
  CPPUNIT_ASSERT((fd = creat(fname.c_str(), S_IRUSR | S_IWUSR)) != -1);
  pid_t pid = fork();

  if (pid == -1)
  {
    // Error while forking
    CPPUNIT_ASSERT_MESSAGE("Error while forking", pid == -1 );    
  }
  else if (pid == 0)
  {
    // Child
    CPPUNIT_ASSERT(write(fd, buff, sz_buff) == (ssize_t)sz_buff);
    CPPUNIT_ASSERT(!close(fd));
    delete[] buff;
  }
  else 
  {
    // Parent 
    int status;
    CPPUNIT_ASSERT(wait(&status) == pid);
    CPPUNIT_ASSERT(WIFEXITED(status));
    CPPUNIT_ASSERT((fd = open(fname.c_str(), O_RDONLY, 0)) != -1);
    char* rbuff = new char[sz_buff];
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // *** TODO ***
    // Retry this when we merge the modifications from the beryl branch also
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //CPPUNIT_ASSERT(read(fd, rbuff, sz_buff) == (ssize_t)sz_buff);
    ssize_t nread = pread(fd, rbuff, sz_buff, 0);
    //std::cout << "Rd_sz= " << nread << " expected size=" << sz_buff << std::endl;
    // TODO: also replace below nread with sz_buff
    CPPUNIT_ASSERT_MESSAGE("WR/RD buffer missmatch", !strncmp(buff, rbuff, nread));
    CPPUNIT_ASSERT(!close(fd));
    CPPUNIT_ASSERT(!remove(fname.c_str()));
    delete[] rbuff;
    delete[] buff;
  }
}


//------------------------------------------------------------------------------
// Test to ensure all data is flushed from cache before doing a reading
//------------------------------------------------------------------------------
void
FuseFileTest::WriteReadTest()
{
  int fd = -1;
  size_t buff_sz = 10240;
  char *buf = new char[buff_sz];
  memset(buf, 7, buff_sz);

  std::string fname = mEnv->GetMapping("file_dummy"); fname += "_wrt";
  CPPUNIT_ASSERT((fd = open(fname.c_str(), O_CREAT | O_RDWR, S_IRWXU)) != -1);
  CPPUNIT_ASSERT(write(fd, buf, buff_sz) == (ssize_t)buff_sz);
  ssize_t nread = pread(fd, buf, 30, 10200);
  CPPUNIT_ASSERT( nread == 30);
  CPPUNIT_ASSERT(!close(fd));
  CPPUNIT_ASSERT(!remove(fname.c_str()));
  delete[] buf;
}


//------------------------------------------------------------------------------
// Test doing sparse write operations does not block the cache by filling up
// with partial cache entries and never evicting them.
//------------------------------------------------------------------------------
void
FuseFileTest::SparseWriteTest()
{
  // Start writing sparsely into a file filling all the cache
  int fd = -1;
  off_t offset = 0;
  off_t sz_buff = 1024;
  char buff[sz_buff];
  memset(&buff, 13, sz_buff);
  int sz_cache = atoi(mEnv->GetMapping("fuse_cache_size").c_str());
  std::string fname = mEnv->GetMapping("file_dummy"); fname += "_swt";
  CPPUNIT_ASSERT((fd = creat(fname.c_str(), S_IRWXU)) != -1);
  off_t sz_gap = 4 * 1024 *1024;
  off_t sz_final = 1.5 * sz_cache; // fill all cache and beyond
  
  while (offset < sz_final)
  {
    if (sz_final - offset < sz_buff)
      sz_buff = sz_final - offset;
    
    CPPUNIT_ASSERT(pwrite(fd, buff, sz_buff, offset) == sz_buff);
    offset += sz_gap; // write 1KB every 4MB
  }

  CPPUNIT_ASSERT(!close(fd));

  // Check size of the file
  struct stat info;
  CPPUNIT_ASSERT(!stat(fname.c_str(), &info));
  CPPUNIT_ASSERT(info.st_size == (offset - sz_gap + sz_buff) );  
  CPPUNIT_ASSERT(!remove(fname.c_str()));
}


//------------------------------------------------------------------------------
// Have many files opened for writing which only contain an incomplete cache
// entry such that the write cache is rapidly filled with parital entries. This
// should trigger the automatic eviction of some cache entries such that the
// writing does not block.
//------------------------------------------------------------------------------
void
FuseFileTest::ManyWriteFilesTest()
{
  int fd = -1;
  struct stat info;
  std::vector<int> vfd;
  std::vector<std::string> vfname;
  off_t sz_buff = 4 * 1024 + 19;
  char buff[sz_buff];
  memset(&buff, 13, sz_buff);
  std::string base_fname = mEnv->GetMapping("file_dummy");
  int sz_cache = atoi(mEnv->GetMapping("fuse_cache_size").c_str());
  int num_files = (2 * sz_cache) / CacheEntry::GetMaxSize();
  std::ostringstream oss;

  // Open files and write to them
  for (int findx = 0; findx < num_files; ++findx)
  {
    oss.str("");
    oss << base_fname << findx;
    CPPUNIT_ASSERT((fd = creat(oss.str().c_str(), S_IRWXU)) != -1);
    CPPUNIT_ASSERT(pwrite(fd, buff, sz_buff, 0) == sz_buff);
    vfd.push_back(fd);
    vfname.push_back(oss.str());
  }

  // Close files and check the expected size
  for (int findx = 0; findx < num_files; ++findx)
  {
    CPPUNIT_ASSERT(!close(vfd[findx]));
    CPPUNIT_ASSERT(!stat(vfname[findx].c_str(), &info));
    // std::cout << "Fname:" << vfname[findx] << " stat size: "
    //          <<  info.st_size << " expect size:" << sz_buff << std::endl;
    // TODO: fix in case we are listing at the same time this leads to a
    // missmatch between the size expected and the size returned from the stat
    // we get the cached value
    CPPUNIT_ASSERT(info.st_size == sz_buff);
  }
  
  // Remove all files
  for (int findx = 0; findx < num_files; ++findx)
    CPPUNIT_ASSERT(!remove(vfname[findx].c_str()));
}
      
#endif // __EOS_FUSE_FUSEFILETEST_HH__


