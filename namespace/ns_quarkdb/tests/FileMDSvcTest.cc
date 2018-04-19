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
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <memory>
#include <gtest/gtest.h>

// Hack to expose all members of FileSystemView to this test unit
#define private public
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#undef private

class FileMDSvcF : public eos::ns::testing::NsTestsFixture {};

//------------------------------------------------------------------------------
// Tests implementation
//------------------------------------------------------------------------------
TEST_F(FileMDSvcF, LoadTest)
{
  std::shared_ptr<eos::IFileMD> file1 = fileSvc()->createFile();
  std::shared_ptr<eos::IFileMD> file2 = fileSvc()->createFile();
  std::shared_ptr<eos::IFileMD> file3 = fileSvc()->createFile();
  std::shared_ptr<eos::IFileMD> file4 = fileSvc()->createFile();
  std::shared_ptr<eos::IFileMD> file5 = fileSvc()->createFile();
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
  fileSvc()->updateStore(file1.get());
  fileSvc()->updateStore(file2.get());
  fileSvc()->updateStore(file3.get());
  fileSvc()->updateStore(file4.get());
  fileSvc()->updateStore(file5.get());
  mdFlusher()->synchronize();
  ASSERT_EQ(fileSvc()->getNumFiles(), 5);
  fileSvc()->removeFile(file2.get());
  fileSvc()->removeFile(file4.get());
  mdFlusher()->synchronize();
  ASSERT_EQ(fileSvc()->getNumFiles(), 3);
  fileSvc()->finalize();
  ASSERT_NO_THROW(fileSvc()->initialize());
  std::shared_ptr<eos::IFileMD> fileRec1 = fileSvc()->getFileMD(id1);
  std::shared_ptr<eos::IFileMD> fileRec3 = fileSvc()->getFileMD(id3);
  std::shared_ptr<eos::IFileMD> fileRec5 = fileSvc()->getFileMD(id5);
  ASSERT_TRUE(fileRec1 != nullptr);
  ASSERT_TRUE(fileRec3 != nullptr);
  ASSERT_TRUE(fileRec5 != nullptr);
  ASSERT_TRUE(fileRec1->getName() == "file1");
  ASSERT_TRUE(fileRec3->getName() == "file3");
  ASSERT_TRUE(fileRec5->getName() == "file5");
  ASSERT_THROW(fileSvc()->getFileMD(id2), eos::MDException);
  ASSERT_THROW(fileSvc()->getFileMD(id4), eos::MDException);
  ASSERT_NO_THROW(fileSvc()->removeFile(fileRec1.get()));
  ASSERT_NO_THROW(fileSvc()->removeFile(fileRec3.get()));
  ASSERT_NO_THROW(fileSvc()->removeFile(fileRec5.get()));
  mdFlusher()->synchronize();
  ASSERT_EQ(fileSvc()->getNumFiles(), 0);
  fileSvc()->finalize();
}
