/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include <cppunit/extensions/HelperMacros.h>
#include <memory>
#include <gtest/gtest.h>

// Hack to expose all members of FileSystemView to this test unit
#define private public
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#undef private

//------------------------------------------------------------------------------
// Tests implementation
//------------------------------------------------------------------------------
TEST(FileMDSvc, LoadTest)
{
  std::unique_ptr<eos::IContainerMDSvc> contSvc{new eos::ContainerMDSvc};
  std::unique_ptr<eos::IFileMDSvc> fileSvc{new eos::FileMDSvc};
  fileSvc->setContMDService(contSvc.get());
  std::map<std::string, std::string> config = {{"qdb_host", "localhost"},
    {"qdb_port", "7777"}
  };
  fileSvc->configure(config);
  ASSERT_NO_THROW(fileSvc->initialize());
  std::shared_ptr<eos::IFileMD> file1 = fileSvc->createFile();
  std::shared_ptr<eos::IFileMD> file2 = fileSvc->createFile();
  std::shared_ptr<eos::IFileMD> file3 = fileSvc->createFile();
  std::shared_ptr<eos::IFileMD> file4 = fileSvc->createFile();
  std::shared_ptr<eos::IFileMD> file5 = fileSvc->createFile();
  ASSERT_TRUE(file1 != nullptr);
  ASSERT_TRUE(file2 != nullptr);
  ASSERT_TRUE(file3 != nullptr);
  ASSERT_TRUE(file4 != nullptr);
  ASSERT_TRUE(file5 != nullptr);
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
  ASSERT_TRUE(fileSvc->getNumFiles() == 5);
  fileSvc->removeFile(file2.get());
  fileSvc->removeFile(file4.get());
  ASSERT_TRUE(fileSvc->getNumFiles() == 3);
  fileSvc->finalize();
  ASSERT_NO_THROW(fileSvc->initialize());
  std::shared_ptr<eos::IFileMD> fileRec1 = fileSvc->getFileMD(id1);
  std::shared_ptr<eos::IFileMD> fileRec3 = fileSvc->getFileMD(id3);
  std::shared_ptr<eos::IFileMD> fileRec5 = fileSvc->getFileMD(id5);
  ASSERT_TRUE(fileRec1 != nullptr);
  ASSERT_TRUE(fileRec3 != nullptr);
  ASSERT_TRUE(fileRec5 != nullptr);
  ASSERT_TRUE(fileRec1->getName() == "file1");
  ASSERT_TRUE(fileRec3->getName() == "file3");
  ASSERT_TRUE(fileRec5->getName() == "file5");
  ASSERT_THROW(fileSvc->getFileMD(id2), eos::MDException);
  ASSERT_THROW(fileSvc->getFileMD(id4), eos::MDException);
  ASSERT_NO_THROW(fileSvc->removeFile(fileRec1.get()));
  ASSERT_NO_THROW(fileSvc->removeFile(fileRec3.get()));
  ASSERT_NO_THROW(fileSvc->removeFile(fileRec5.get()));
  ASSERT_TRUE(fileSvc->getNumFiles() == 0);
  fileSvc->finalize();
}

TEST(FileMDSvc, CheckFileTest)
{
  std::map<std::string, std::string> config = {
    {"qdb_host", "localhost"},
    {"qdb_port", "7777"}
  };
  std::unique_ptr<eos::ContainerMDSvc> contSvc{new eos::ContainerMDSvc()};
  std::unique_ptr<eos::FileMDSvc> fileSvc{new eos::FileMDSvc()};
  std::unique_ptr<eos::IView> view{new eos::HierarchicalView()};
  std::unique_ptr<eos::IFsView> fsView{new eos::FileSystemView()};
  fileSvc->setContMDService(contSvc.get());
  contSvc->setFileMDService(fileSvc.get());
  contSvc->configure(config);
  fileSvc->configure(config);
  fsView->configure(config);
  view->setContainerMDSvc(contSvc.get());
  view->setFileMDSvc(fileSvc.get());
  view->configure(config);
  view->initialize();
  fileSvc->addChangeListener(fsView.get());
  // Create test container and file
  std::shared_ptr<eos::IContainerMD> cont =
    view->createContainer("/test_dir", true);
  std::shared_ptr<eos::IFileMD> file =
    view->createFile("/test_dir/test_file1.dat");
  eos::IFileMD::id_t fid = file->getId();
  std::string sfid = std::to_string(fid);
  ASSERT_TRUE(file != nullptr);

  // Add some replica and unlink locations
  for (int i = 1; i <= 4; ++i) {
    file->addLocation(i);
  }

  file->unlinkLocation(3);
  file->unlinkLocation(4);
  view->updateFileStore(file.get());
  // Corrupt the backend KV store
  std::string key;
  qclient::QClient* qcl = eos::BackendClient::getInstance(
                            config["qdb_host"], std::stoi(config["qdb_port"]));
  key = "1" + eos::fsview::sFilesSuffix;
  qclient::QSet fs_set(*qcl, key);
  ASSERT_TRUE(fs_set.srem(sfid));
  key = "4" + eos::fsview::sUnlinkedSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.srem(sfid));
  key = eos::fsview::sNoReplicaPrefix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sadd(sfid));
  key = "5" + eos::fsview::sFilesSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sadd(sfid));
  // Need to add fsid by hand
  fs_set.setKey(eos::fsview::sSetFsIds);
  ASSERT_TRUE(fs_set.sadd("5"));
  // Introduce file in the set to be checked and trigger a check
  fs_set.setKey(eos::constants::sSetCheckFiles);
  ASSERT_NO_THROW(fs_set.sadd(sfid));
  ASSERT_TRUE(fileSvc->checkFiles());
  // Check that the back-end KV store is consistent
  key = "1" + eos::fsview::sFilesSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sismember(sfid));
  key = "2" + eos::fsview::sFilesSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sismember(sfid));
  key = "5" + eos::fsview::sFilesSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(!fs_set.sismember(sfid));
  key = "3" + eos::fsview::sUnlinkedSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sismember(sfid));
  key = "4" + eos::fsview::sUnlinkedSuffix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.sismember(sfid));
  key = eos::fsview::sNoReplicaPrefix;
  fs_set.setKey(key);
  ASSERT_TRUE(fs_set.scard() == 0);
  file->unlinkAllLocations();
  file->removeAllLocations();
  view->removeFile(file.get());
  view->removeContainer("/test_dir", true);
}
