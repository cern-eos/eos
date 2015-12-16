/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief File metadata service class test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>
#include <unistd.h>
#include "namespace/utils/TestHelpers.hh"
#include "namespace/ns_on_redis/persistency/FileMDSvc.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"

//------------------------------------------------------------------------------
// FileMDSvcTest class
//------------------------------------------------------------------------------
class FileMDSvcTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE(FileMDSvcTest);
    CPPUNIT_TEST(reloadTest);
    CPPUNIT_TEST_SUITE_END();

    void reloadTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION(FileMDSvcTest);

//------------------------------------------------------------------------------
// Tests implementation
//------------------------------------------------------------------------------
void FileMDSvcTest::reloadTest()
{
  eos::ContainerMDSvc* contSvc = new eos::ContainerMDSvc;
  eos::FileMDSvc*      fileSvc = new eos::FileMDSvc;
  fileSvc->setContainerService(contSvc);
  std::map<std::string, std::string> config;
  CPPUNIT_ASSERT_NO_THROW(fileSvc->initialize());

  eos::IFileMD* file1 = fileSvc->createFile();
  eos::IFileMD* file2 = fileSvc->createFile();
  eos::IFileMD* file3 = fileSvc->createFile();
  eos::IFileMD* file4 = fileSvc->createFile();
  eos::IFileMD* file5 = fileSvc->createFile();
  CPPUNIT_ASSERT(file1 != 0);
  CPPUNIT_ASSERT(file2 != 0);
  CPPUNIT_ASSERT(file3 != 0);
  CPPUNIT_ASSERT(file4 != 0);
  CPPUNIT_ASSERT(file5 != 0);
  file1->setName("file1");
  file2->setName("file2");
  file3->setName("file3");
  file4->setName("file4");
  file5->setName("file5");

  eos::IFileMD::id_t id1 = file1->getId();
  eos::IFileMD::id_t id2 = file2->getId();
  eos::IFileMD::id_t id3 = file3->getId();
  eos::IFileMD::id_t id4 = file4->getId();
  eos::IFileMD::id_t id5 = file5->getId();
  fileSvc->updateStore(file1);
  fileSvc->updateStore(file2);
  fileSvc->updateStore(file3);
  fileSvc->updateStore(file4);
  fileSvc->updateStore(file5);
  fileSvc->removeFile(file2);
  fileSvc->removeFile(file4);
  fileSvc->finalize();
  CPPUNIT_ASSERT_NO_THROW(fileSvc->initialize());

  eos::IFileMD* fileRec1 = fileSvc->getFileMD(id1);
  eos::IFileMD* fileRec3 = fileSvc->getFileMD(id3);
  eos::IFileMD* fileRec5 = fileSvc->getFileMD(id5);
  CPPUNIT_ASSERT(fileRec1 != 0);
  CPPUNIT_ASSERT(fileRec3 != 0);
  CPPUNIT_ASSERT(fileRec5 != 0);
  CPPUNIT_ASSERT(fileRec1->getName() == "file1");
  CPPUNIT_ASSERT(fileRec3->getName() == "file3");
  CPPUNIT_ASSERT(fileRec5->getName() == "file5");
  CPPUNIT_ASSERT_THROW(fileSvc->getFileMD(id2), eos::MDException);
  CPPUNIT_ASSERT_THROW(fileSvc->getFileMD(id4), eos::MDException);
  fileSvc->finalize();
  delete fileSvc;
  unlink(fileName.c_str());
}
