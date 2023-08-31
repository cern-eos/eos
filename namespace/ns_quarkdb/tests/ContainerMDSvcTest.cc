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
//! @brief File metadata service tests
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include <gtest/gtest.h>
#include <memory>

class ContainerMDSvcF : public eos::ns::testing::NsTestsFixture {};

TEST_F(ContainerMDSvcF, BasicSanity)
{
  std::shared_ptr<eos::IContainerMD> container1 = containerSvc()->getContainerMD(1);
  std::shared_ptr<eos::IContainerMD> container2 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container3 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container4 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container5 = containerSvc()->createContainer(0);

  eos::IContainerMD::id_t id = container1->getId();
  container1->setName("root");
  container1->setParentId(container1->getId());
  container2->setName("subContLevel1-1");
  container3->setName("subContLevel1-2");
  container4->setName("subContLevel2-1");
  container5->setName("subContLevel2-2");
  container5->setCUid(17);
  container5->setCGid(17);
  container5->setMode(0750);
  ASSERT_TRUE(container5->access(17, 12, X_OK | R_OK | W_OK));
  ASSERT_TRUE(container5->access(17, 12, X_OK | R_OK));
  ASSERT_TRUE(!container5->access(12, 17, X_OK | R_OK | W_OK));
  ASSERT_TRUE(!container5->access(12, 17, X_OK | W_OK));
  ASSERT_TRUE(container5->access(12, 17, X_OK | R_OK));
  ASSERT_TRUE(!container5->access(12, 12, X_OK | R_OK));
  container1->addContainer(container2.get());
  container1->addContainer(container3.get());
  container3->addContainer(container4.get());
  container3->addContainer(container5.get());
  ASSERT_EQ((size_t)2, container1->getNumContainers());
  ASSERT_EQ((size_t)0, container1->getNumFiles());
  containerSvc()->updateStore(container1.get());
  containerSvc()->updateStore(container2.get());
  containerSvc()->updateStore(container3.get());
  containerSvc()->updateStore(container4.get());
  containerSvc()->updateStore(container5.get());
  mdFlusher()->synchronize();

  ASSERT_EQ((size_t)5, containerSvc()->getNumContainers());
  container3->removeContainer("subContLevel2-2");
  containerSvc()->removeContainer(container5.get());

  mdFlusher()->synchronize();
  ASSERT_EQ((size_t)1, container3->getNumContainers());
  ASSERT_EQ((size_t)4, containerSvc()->getNumContainers());
  std::shared_ptr<eos::IContainerMD> container6 = containerSvc()->createContainer(0);
  container6->setName("subContLevel2-3");
  container3->addContainer(container6.get());
  containerSvc()->updateStore(container6.get());
  eos::IContainerMD::id_t idAttr = container4->getId();
  container4->setAttribute("test1", "test1");
  container4->setAttribute("test1", "test11");
  container4->setAttribute("test2", "test2");
  container4->setAttribute("test3", "test3");
  containerSvc()->updateStore(container4.get());
  ASSERT_EQ((size_t)3, container4->numAttributes());
  ASSERT_TRUE(container4->getAttribute("test1") == "test11");
  ASSERT_TRUE(container4->getAttribute("test3") == "test3");
  ASSERT_THROW(container4->getAttribute("test15"), eos::MDException);

  shut_down_everything();

  std::shared_ptr<eos::IContainerMD> cont1 = containerSvc()->getContainerMD(id);
  ASSERT_EQ(cont1->getName(), "root");
  std::shared_ptr<eos::IContainerMD> cont2 = cont1->findContainer("subContLevel1-1");
  ASSERT_NE(cont2, nullptr);
  ASSERT_EQ(cont2->getName(), "subContLevel1-1");
  cont2 = cont1->findContainer("subContLevel1-2");
  ASSERT_NE(cont2, nullptr);
  ASSERT_EQ(cont2->getName(), "subContLevel1-2");
  cont1 = cont2->findContainer("subContLevel2-1");
  ASSERT_NE(cont1, nullptr);
  ASSERT_EQ(cont1->getName(), "subContLevel2-1");
  cont1 = cont2->findContainer("subContLevel2-2");
  ASSERT_EQ(cont1, nullptr);
  cont1 = cont2->findContainer("subContLevel2-3");
  ASSERT_NE(cont1, nullptr);
  ASSERT_EQ(cont1->getName(), "subContLevel2-3");
  std::shared_ptr<eos::IContainerMD> contAttrs = containerSvc()->getContainerMD(idAttr);
  ASSERT_EQ(contAttrs->numAttributes(), 3);
  ASSERT_EQ(contAttrs->getAttribute("test1"), "test11");
  ASSERT_EQ(contAttrs->getAttribute("test3"), "test3");
  ASSERT_THROW(contAttrs->getAttribute("test15"), eos::MDException);
  // Clean up all the containers
  container1 = containerSvc()->getContainerMD(1);
  container2 = containerSvc()->getContainerMD(2);
  container3 = containerSvc()->getContainerMD(3);
  container4 = containerSvc()->getContainerMD(4);

  container3->removeContainer(container6->getName());
  container3->removeContainer(container4->getName());
  container1->removeContainer(container3->getName());
  container1->removeContainer(container2->getName());
  containerSvc()->removeContainer(container6.get());
  containerSvc()->removeContainer(container4.get());
  containerSvc()->removeContainer(container3.get());
  containerSvc()->removeContainer(container2.get());
  containerSvc()->removeContainer(container1.get());
  mdFlusher()->synchronize();
  ASSERT_EQ((uint64_t)0, containerSvc()->getNumContainers());
}

TEST_F(ContainerMDSvcF, getContainerLocked) {
  auto cont = view()->createContainer("/root/");
  auto contId = cont->getId();
  ASSERT_THROW(containerSvc()->getContainerMDReadLocked(42), eos::MDException);
  {
    auto contReadLocked = containerSvc()->getContainerMDReadLocked(contId);
    ASSERT_NE(nullptr, contReadLocked);
    ASSERT_EQ(contId, contReadLocked->getUnderlyingPtr()->getId());
  }
  ASSERT_THROW(containerSvc()->getContainerMDWriteLocked(42), eos::MDException);
  {
    auto contWriteLocked = containerSvc()->getContainerMDWriteLocked(contId);
    auto contWriteLocked2 = containerSvc()->getContainerMDWriteLocked(contId);
    ASSERT_NE(nullptr, contWriteLocked);
    ASSERT_EQ(contId, contWriteLocked->getUnderlyingPtr()->getId());
    ASSERT_EQ(contWriteLocked->getUnderlyingPtr().get(),contWriteLocked2->getUnderlyingPtr().get());
  }
}