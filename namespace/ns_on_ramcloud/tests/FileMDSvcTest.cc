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
#include "namespace/ns_on_ramcloud/persistency/FileMDSvc.hh"
#include "namespace/ns_on_ramcloud/persistency/ContainerMDSvc.hh"
#include <memory>

namespace eos
{
  static std::string sRamCloudConfigFile = "/etc/ramcloud.client.config";
}

//------------------------------------------------------------------------------
// FileMDSvcTest class
//------------------------------------------------------------------------------
class FileMDSvcTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE(FileMDSvcTest);
    CPPUNIT_TEST(loadTest);
    CPPUNIT_TEST_SUITE_END();

    void loadTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION(FileMDSvcTest);

//------------------------------------------------------------------------------
// Tests implementation
//------------------------------------------------------------------------------
void FileMDSvcTest::loadTest()
{
  std::unique_ptr<eos::IContainerMDSvc> contSvc {new eos::ContainerMDSvc};
  std::unique_ptr<eos::IFileMDSvc> fileSvc {new eos::FileMDSvc};
  fileSvc->setContMDService(contSvc.get());
  std::map<std::string, std::string> config = {};
  fileSvc->configure(config);
  CPPUNIT_ASSERT_NO_THROW(fileSvc->initialize());

  std::unique_ptr<eos::IFileMD> file1 = fileSvc->createFile();
  std::unique_ptr<eos::IFileMD> file2 = fileSvc->createFile();
  std::unique_ptr<eos::IFileMD> file3 = fileSvc->createFile();
  std::unique_ptr<eos::IFileMD> file4 = fileSvc->createFile();
  std::unique_ptr<eos::IFileMD> file5 = fileSvc->createFile();
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
  fileSvc->updateStore(file1.get());
  fileSvc->updateStore(file2.get());
  fileSvc->updateStore(file3.get());
  fileSvc->updateStore(file4.get());
  fileSvc->updateStore(file5.get());
  CPPUNIT_ASSERT(fileSvc->getNumFiles() == 5);
  fileSvc->removeFile(file2.get());
  fileSvc->removeFile(file4.get());
  CPPUNIT_ASSERT(fileSvc->getNumFiles() == 3);
  fileSvc->finalize();
  CPPUNIT_ASSERT_NO_THROW(fileSvc->initialize());

  std::unique_ptr<eos::IFileMD> fileRec1 = fileSvc->getFileMD(id1);
  std::unique_ptr<eos::IFileMD> fileRec3 = fileSvc->getFileMD(id3);
  std::unique_ptr<eos::IFileMD> fileRec5 = fileSvc->getFileMD(id5);
  CPPUNIT_ASSERT(fileRec1 != 0);
  CPPUNIT_ASSERT(fileRec3 != 0);
  CPPUNIT_ASSERT(fileRec5 != 0);
  CPPUNIT_ASSERT(fileRec1->getName() == "file1");
  CPPUNIT_ASSERT(fileRec3->getName() == "file3");
  CPPUNIT_ASSERT(fileRec5->getName() == "file5");
  CPPUNIT_ASSERT_THROW(fileSvc->getFileMD(id2), eos::MDException);
  CPPUNIT_ASSERT_THROW(fileSvc->getFileMD(id4), eos::MDException);
  CPPUNIT_ASSERT_NO_THROW(fileSvc->removeFile(fileRec1.get()));
  CPPUNIT_ASSERT_NO_THROW(fileSvc->removeFile(fileRec3.get()));
  CPPUNIT_ASSERT_NO_THROW(fileSvc->removeFile(fileRec5.get()));
  CPPUNIT_ASSERT(fileSvc->getNumFiles() == 0);
  fileSvc->finalize();
}
