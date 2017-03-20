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
#include <cppunit/extensions/HelperMacros.h>
#include <memory>

//------------------------------------------------------------------------------
// ContainerMDSvcTest class
//------------------------------------------------------------------------------
class ContainerMDSvcTest : public CppUnit::TestCase
{
public:
  CPPUNIT_TEST_SUITE(ContainerMDSvcTest);
  CPPUNIT_TEST(loadTest);
  CPPUNIT_TEST_SUITE_END();

  void loadTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ContainerMDSvcTest);

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void
ContainerMDSvcTest::loadTest()
{
  try {
    std::unique_ptr<eos::IContainerMDSvc> containerSvc{new eos::ContainerMDSvc()};
    std::unique_ptr<eos::IFileMDSvc> fileSvc{new eos::FileMDSvc()};
    std::map<std::string, std::string> config = {{"qdb_host", "localhost"},
      {"qdb_port", "6380"}
    };
    containerSvc->setFileMDService(fileSvc.get());
    containerSvc->configure(config);
    containerSvc->initialize();
    std::shared_ptr<eos::IContainerMD> container1 =
      containerSvc->createContainer();
    std::shared_ptr<eos::IContainerMD> container2 =
      containerSvc->createContainer();
    std::shared_ptr<eos::IContainerMD> container3 =
      containerSvc->createContainer();
    std::shared_ptr<eos::IContainerMD> container4 =
      containerSvc->createContainer();
    std::shared_ptr<eos::IContainerMD> container5 =
      containerSvc->createContainer();
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
    CPPUNIT_ASSERT(container5->access(17, 12, X_OK | R_OK | W_OK));
    CPPUNIT_ASSERT(container5->access(17, 12, X_OK | R_OK));
    CPPUNIT_ASSERT(!container5->access(12, 17, X_OK | R_OK | W_OK));
    CPPUNIT_ASSERT(!container5->access(12, 17, X_OK | W_OK));
    CPPUNIT_ASSERT(container5->access(12, 17, X_OK | R_OK));
    CPPUNIT_ASSERT(!container5->access(12, 12, X_OK | R_OK));
    container1->addContainer(container2.get());
    container1->addContainer(container3.get());
    container3->addContainer(container4.get());
    container3->addContainer(container5.get());
    CPPUNIT_ASSERT_EQUAL((size_t)2, container1->getNumContainers());
    CPPUNIT_ASSERT_EQUAL((size_t)0, container1->getNumFiles());
    containerSvc->updateStore(container1.get());
    containerSvc->updateStore(container2.get());
    containerSvc->updateStore(container3.get());
    containerSvc->updateStore(container4.get());
    containerSvc->updateStore(container5.get());
    CPPUNIT_ASSERT_EQUAL((size_t)5, containerSvc->getNumContainers());
    container3->removeContainer("subContLevel2-2");
    containerSvc->removeContainer(container5.get());
    CPPUNIT_ASSERT_EQUAL((size_t)1, container3->getNumContainers());
    CPPUNIT_ASSERT_EQUAL((size_t)4, containerSvc->getNumContainers());
    std::shared_ptr<eos::IContainerMD> container6 =
      containerSvc->createContainer();
    container6->setName("subContLevel2-3");
    container3->addContainer(container6.get());
    containerSvc->updateStore(container6.get());
    eos::IContainerMD::id_t idAttr = container4->getId();
    container4->setAttribute("test1", "test1");
    container4->setAttribute("test1", "test11");
    container4->setAttribute("test2", "test2");
    container4->setAttribute("test3", "test3");
    containerSvc->updateStore(container4.get());
    CPPUNIT_ASSERT_EQUAL((size_t)3, container4->numAttributes());
    CPPUNIT_ASSERT(container4->getAttribute("test1") == "test11");
    CPPUNIT_ASSERT(container4->getAttribute("test3") == "test3");
    CPPUNIT_ASSERT_THROW(container4->getAttribute("test15"), eos::MDException);
    containerSvc->finalize();
    containerSvc->initialize();
    std::shared_ptr<eos::IContainerMD> cont1 = containerSvc->getContainerMD(id);
    CPPUNIT_ASSERT(cont1->getName() == "root");
    std::shared_ptr<eos::IContainerMD> cont2 =
      cont1->findContainer("subContLevel1-1");
    CPPUNIT_ASSERT(cont2 != nullptr);
    CPPUNIT_ASSERT(cont2->getName() == "subContLevel1-1");
    cont2 = cont1->findContainer("subContLevel1-2");
    CPPUNIT_ASSERT(cont2 != nullptr);
    CPPUNIT_ASSERT(cont2->getName() == "subContLevel1-2");
    cont1 = cont2->findContainer("subContLevel2-1");
    CPPUNIT_ASSERT(cont1 != nullptr);
    CPPUNIT_ASSERT(cont1->getName() == "subContLevel2-1");
    cont1 = cont2->findContainer("subContLevel2-2");
    CPPUNIT_ASSERT(cont1 == nullptr);
    cont1 = cont2->findContainer("subContLevel2-3");
    CPPUNIT_ASSERT(cont1 != nullptr);
    CPPUNIT_ASSERT(cont1->getName() == "subContLevel2-3");
    std::shared_ptr<eos::IContainerMD> contAttrs =
      containerSvc->getContainerMD(idAttr);
    CPPUNIT_ASSERT(contAttrs->numAttributes() == 3);
    CPPUNIT_ASSERT(contAttrs->getAttribute("test1") == "test11");
    CPPUNIT_ASSERT(contAttrs->getAttribute("test3") == "test3");
    CPPUNIT_ASSERT_THROW(contAttrs->getAttribute("test15"), eos::MDException);
    // Clean up all the containers
    container3->removeContainer(container6->getName());
    container3->removeContainer(container4->getName());
    container1->removeContainer(container3->getName());
    container1->removeContainer(container2->getName());
    containerSvc->removeContainer(container6.get());
    containerSvc->removeContainer(container4.get());
    containerSvc->removeContainer(container3.get());
    containerSvc->removeContainer(container2.get());
    containerSvc->removeContainer(container1.get());
    CPPUNIT_ASSERT_EQUAL((uint64_t)0, containerSvc->getNumContainers());
    containerSvc->finalize();
  } catch (eos::MDException& e) {
    CPPUNIT_ASSERT_MESSAGE(e.getMessage().str(), false);
  }
}
