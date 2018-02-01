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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Various namespace tests
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "TestUtils.hh"
#include <cppunit/extensions/HelperMacros.h>
#include <gtest/gtest.h>
#include <memory>

class VariousTests : public eos::ns::testing::NsTestsFixture {};

TEST_F(VariousTests, BasicSanity) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IContainerMD> cont1 = view()->createContainer("/eos/", true);
  ASSERT_EQ(cont1->getId(), 2);
  ASSERT_THROW(view()->createFile("/eos/", true), eos::MDException);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/eos/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 0u);
  file1->addLocation(1);
  file1->addLocation(7);
  ASSERT_EQ(file1->getNumLocation(), 2u);

  containerSvc()->updateStore(root.get());
  containerSvc()->updateStore(cont1.get());
  fileSvc()->updateStore(file1.get());

  shut_down_everything();

  file1 = view()->getFile("/eos/my-file.txt");
  ASSERT_EQ(view()->getUri(file1.get()), "/eos/my-file.txt");
  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 2u);
  ASSERT_EQ(file1->getLocation(0), 1);
  ASSERT_EQ(file1->getLocation(1), 7);

  root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Ensure fsview for location 1 contains file1
  std::shared_ptr<eos::ICollectionIterator<eos::IFileMD::id_t>> it = fsview()->getFileList(1);
  ASSERT_TRUE(it->valid());
  ASSERT_EQ(it->getElement(), file1->getId());
  it->next();
  ASSERT_FALSE(it->valid());


}
