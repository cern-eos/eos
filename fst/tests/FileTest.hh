//------------------------------------------------------------------------------
//! @file FileTest.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @brief Class containing unit test for the FST component
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

#ifndef __EOSFSTTEST_FILETEST_HH__
#define __EOSFSTTEST_FILETEST_HH__

/*----------------------------------------------------------------------------*/
#include <cppunit/extensions/HelperMacros.h>
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/

//! Ugly hack to expose the private functions for testing
#define protected public
#define private   public
#include "fst/XrdFstOssFile.hh"
#include "fst/layout/RaidMetaLayout.hh"
#undef protected
#undef private

//------------------------------------------------------------------------------
//! Declaration of FileTest class
//------------------------------------------------------------------------------
class FileTest: public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(FileTest);
    CPPUNIT_TEST(ReadVTest);
    CPPUNIT_TEST(SplitReadVTest);
    CPPUNIT_TEST(AlignBufferTest);
    CPPUNIT_TEST(DeleteFlagTest);
  CPPUNIT_TEST_SUITE_END();
  
 public:

  //----------------------------------------------------------------------------
  //! setUp function
  //----------------------------------------------------------------------------
  void setUp(void);

  
  //----------------------------------------------------------------------------
  //! tearDown function
  //----------------------------------------------------------------------------
  void tearDown(void);
  
 protected:
  
  //----------------------------------------------------------------------------
  //! ReadV test
  //----------------------------------------------------------------------------
  void ReadVTest();

  
  //----------------------------------------------------------------------------
  //! SplitReadV test used for the RAIN like files to distribute the inital
  //! readV request to all the corresponding stripe files
  //----------------------------------------------------------------------------
  void SplitReadVTest();

  
  //----------------------------------------------------------------------------
  //! Test the align method used in ht XrdFstOssFile to align requests to the
  //! block checksum size
  //----------------------------------------------------------------------------
  void AlignBufferTest();


  //----------------------------------------------------------------------------
  //! Test the deletion of a file to which the delete flag is sent using the
  //! fctl function on the file object
  //----------------------------------------------------------------------------
  void DeleteFlagTest();


  
 private:
  
  XrdCl::File* mFile; ///< XrdCl::File instance used in the tests
  eos::fst::test::TestEnv* mEnv; ///< test environment object
};

#endif // __EOSFSTTEST_FILETEST_HH__
