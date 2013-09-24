//------------------------------------------------------------------------------
// File: FileSystemTest.cc
// Author: Elvin Sindrilaru  <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include "XrdCl/XrdClFileSystem.hh"
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! FileSystemTest class 
//------------------------------------------------------------------------------
class FileSystemTest: public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(FileSystemTest);
    CPPUNIT_TEST(StatTest);
    CPPUNIT_TEST(StatFailTest);
    CPPUNIT_TEST(TruncateTest);
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
  //! File stat test
  //----------------------------------------------------------------------------
  void StatTest();
  
  //----------------------------------------------------------------------------
  //! File stat test
  //----------------------------------------------------------------------------
  void StatFailTest();


  //----------------------------------------------------------------------------
  //! Truncate test
  //----------------------------------------------------------------------------
  void TruncateTest();

  //----------------------------------------------------------------------------
  //! Rename test
  //----------------------------------------------------------------------------
  void RenameTest();

  //----------------------------------------------------------------------------
  //! Remdir test
  //----------------------------------------------------------------------------
  void RemdirTest();

  //----------------------------------------------------------------------------
  //! Rem test
  //----------------------------------------------------------------------------
  void RemTest();

  //----------------------------------------------------------------------------
  //! Prepare test
  //----------------------------------------------------------------------------
  void PrepareTest();

  //----------------------------------------------------------------------------
  //! Mkdir test
  //----------------------------------------------------------------------------
  void MkdirTest();

  //----------------------------------------------------------------------------
  //! fsctl test
  //----------------------------------------------------------------------------
  void fsctlTest();

  //----------------------------------------------------------------------------
  //! FSctl test
  //----------------------------------------------------------------------------
  void FScltTest();

  //----------------------------------------------------------------------------
  //! Exists test
  //----------------------------------------------------------------------------
  void ExistsTest();

  //----------------------------------------------------------------------------
  //! Chksum test
  //----------------------------------------------------------------------------
  void ChksumTest();

  //----------------------------------------------------------------------------
  //! Chmod test
  //----------------------------------------------------------------------------
  void ChmodTest();

 private:

  XrdCl::FileSystem* mFs; ///< XrdCl::FileSystem instance used in the tests
  eos::auth::test::TestEnv* mEnv; ///< testing envirionment object
};


CPPUNIT_TEST_SUITE_REGISTRATION(FileSystemTest);


//------------------------------------------------------------------------------
// setUp function called before each test is done
//------------------------------------------------------------------------------
void
FileSystemTest::setUp()
{
  // Initialise
  mEnv = new eos::auth::test::TestEnv();
  std::string address = "root://" + mEnv->GetMapping("server");
  XrdCl::URL url(address);
  CPPUNIT_ASSERT(url.IsValid());
  
  mFs = new XrdCl::FileSystem(url);
  CPPUNIT_ASSERT(mFs != 0);
}


//----------------------------------------------------------------------------
// tearDown function after each test is done
//----------------------------------------------------------------------------
void
FileSystemTest::tearDown()
{
  delete mEnv;
  delete mFs;
  mFs = 0;
}


//------------------------------------------------------------------------------
// File stat test - for a file which exists in EOS
//------------------------------------------------------------------------------
void
FileSystemTest::StatTest()
{
  XrdCl::StatInfo *stat = 0;
  std::string file_path = mEnv->GetMapping("file");
  XrdCl::XRootDStatus status = mFs->Stat(file_path, stat);
  CPPUNIT_ASSERT(status.IsOK());
  CPPUNIT_ASSERT(stat);
  CPPUNIT_ASSERT(stat->GetSize() == 1048576); // 1MB
  CPPUNIT_ASSERT(stat->TestFlags(XrdCl::StatInfo::IsReadable)) ;
  delete stat;
  stat = 0;
}


//------------------------------------------------------------------------------
// File stat test - for a file which does not exits in EOS
//------------------------------------------------------------------------------
void
FileSystemTest::StatFailTest()
{
  XrdCl::StatInfo *stat = 0;
  std::string file_path = mEnv->GetMapping("file_missing");
  XrdCl::XRootDStatus status = mFs->Stat(file_path, stat);
  CPPUNIT_ASSERT(!status.IsOK());
  CPPUNIT_ASSERT(stat == 0);
  delete stat;
  stat = 0;
}


//------------------------------------------------------------------------------
// Truncate test
//------------------------------------------------------------------------------
void
FileSystemTest::TruncateTest()
{
  std::string file_path = mEnv->GetMapping("file");
  XrdCl::XRootDStatus status = mFs->Truncate(file_path, 1024);
  CPPUNIT_ASSERT(status.IsError());
  CPPUNIT_ASSERT(status.code == XrdCl::errErrorResponse);
}


//------------------------------------------------------------------------------
// Rename test
//------------------------------------------------------------------------------
void
FileSystemTest::RenameTest()
{


}


//------------------------------------------------------------------------------
// Remdir test
//------------------------------------------------------------------------------
void
FileSystemTest::RemdirTest()
{


}


//------------------------------------------------------------------------------
// Rem test
//------------------------------------------------------------------------------
void
FileSystemTest::RemTest()
{


}


//------------------------------------------------------------------------------
// Prepare test
//------------------------------------------------------------------------------
void
FileSystemTest::PrepareTest()
{


}


//------------------------------------------------------------------------------
// Mkdir test
//------------------------------------------------------------------------------
void
FileSystemTest::MkdirTest()
{


}


//------------------------------------------------------------------------------
// fsctl test
//------------------------------------------------------------------------------
void
FileSystemTest::fsctlTest()
{


}


//------------------------------------------------------------------------------
// FSctl test
//------------------------------------------------------------------------------
void
FileSystemTest::FScltTest()
{


}


//------------------------------------------------------------------------------
// Exists test
//------------------------------------------------------------------------------
void
FileSystemTest::ExistsTest()
{


}


//------------------------------------------------------------------------------
// Chksum test
//------------------------------------------------------------------------------
void
FileSystemTest::ChksumTest()
{


}


//------------------------------------------------------------------------------
// Chmod test
//------------------------------------------------------------------------------
void
FileSystemTest::ChmodTest()
{


}



