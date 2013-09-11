//------------------------------------------------------------------------------
// File: FileTest.cc
// Author: Elvin Sindrilaru <esindril@cern.ch> CERN
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
#include "XrdCl/XrdClFile.hh"
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Declaration of FileTest class 
//------------------------------------------------------------------------------
class FileTest: public CppUnit::TestCase
{
 public:
  CPPUNIT_TEST_SUITE(FileTest);
    CPPUNIT_TEST(StatTest);
  CPPUNIT_TEST_SUITE_END();

 
  //----------------------------------------------------------------------------
  //! File stat test
  //----------------------------------------------------------------------------
  void StatTest();

 private:

  XrdCl::File* file; ///< XrdCl::File instance used in the tests
};


CPPUNIT_TEST_SUITE_REGISTRATION(FileTest);


//------------------------------------------------------------------------------
// File stat test
//------------------------------------------------------------------------------
void
FileTest::StatTest()
{

  // Initialise
  XrdCl::XRootDStatus status;
  std::string address = "root://localhost:1099/";
  std::string data_path = "/eos/plain/";
  std::string file_name = "file1";

  XrdCl::URL url(address);
  CPPUNIT_ASSERT(url.IsValid());

  std::string file_url = address + data_path + file_name;

  file = new XrdCl::File();
  status = file->Open(file_url, XrdCl::OpenFlags::Read);
  CPPUNIT_ASSERT(status.IsOK());

  // Do the stat
  XrdCl::StatInfo *stat;
  status = file->Stat(false, stat);
  CPPUNIT_ASSERT(status.IsOK());
  CPPUNIT_ASSERT(stat);
  CPPUNIT_ASSERT(stat->GetSize() == 1048576); // 1MB
  CPPUNIT_ASSERT(stat->TestFlags(XrdCl::StatInfo::IsReadable)) ;

  // Clean up
  delete stat;
  stat = 0;

  status = file->Close();
  CPPUNIT_ASSERT(status.IsOK());

  delete file;
  file = 0;
}

