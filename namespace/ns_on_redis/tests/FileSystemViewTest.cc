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
//! @brief FileSystemView test
//------------------------------------------------------------------------------
#include "namespace/ns_on_redis/accounting/FileSystemView.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include "namespace/ns_on_redis/persistency/FileMDSvc.hh"
#include "namespace/ns_on_redis/views/HierarchicalView.hh"
#include "namespace/utils/TestHelpers.hh"
#include <cppunit/extensions/HelperMacros.h>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <sstream>
#include <unistd.h>

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class FileSystemViewTest : public CppUnit::TestCase
{
public:
  CPPUNIT_TEST_SUITE(FileSystemViewTest);
  CPPUNIT_TEST(fileSystemViewTest);
  CPPUNIT_TEST_SUITE_END();

  void fileSystemViewTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION(FileSystemViewTest);

//------------------------------------------------------------------------------
// Randomize a location
//------------------------------------------------------------------------------
eos::IFileMD::location_t
getRandomLocation()
{
  return 1 + random() % 50;
}

//------------------------------------------------------------------------------
// Count replicas
//------------------------------------------------------------------------------
size_t
countReplicas(eos::IFsView* fs)
{
  size_t replicas = 0;

  for (size_t i = 1; i <= fs->getNumFileSystems(); ++i) {
    replicas += fs->getFileList(i).size();
  }

  return replicas;
}

//------------------------------------------------------------------------------
// Count unlinked
//------------------------------------------------------------------------------
size_t
countUnlinked(eos::IFsView* fs)
{
  size_t unlinked = 0;

  for (size_t i = 1; i <= fs->getNumFileSystems(); ++i) {
    unlinked += fs->getUnlinkedFileList(i).size();
  }

  return unlinked;
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void
FileSystemViewTest::fileSystemViewTest()
{
  srandom(time(nullptr));

  try {
    std::map<std::string, std::string> config = {{"qdb_host", "localhost"},
      {"qdb_port", "6380"}
    };
    std::unique_ptr<eos::ContainerMDSvc> contSvc{new eos::ContainerMDSvc()};
    std::unique_ptr<eos::FileMDSvc> fileSvc{new eos::FileMDSvc()};
    std::unique_ptr<eos::IView> view{new eos::HierarchicalView()};
    std::unique_ptr<eos::IFsView> fsView{new eos::FileSystemView()};
    fileSvc->setContMDService(contSvc.get());
    contSvc->setFileMDService(fileSvc.get());
    contSvc->configure(config);
    fileSvc->configure(config);
    view->setContainerMDSvc(contSvc.get());
    view->setFileMDSvc(fileSvc.get());
    view->configure(config);
    view->initialize();
    dynamic_cast<eos::FileSystemView*>(fsView.get())->initialize(config);
    fileSvc->addChangeListener(fsView.get());
    view->createContainer("/test/embed/embed1", true);
    std::shared_ptr<eos::IContainerMD> c =
      view->createContainer("/test/embed/embed2", true);
    view->createContainer("/test/embed/embed3", true);

    // Create some files
    for (int i = 0; i < 1000; ++i) {
      std::ostringstream o;
      o << "file" << i;
      std::shared_ptr<eos::IFileMD> files[4];
      files[0] = view->createFile(std::string("/test/embed/") + o.str());
      files[1] = view->createFile(std::string("/test/embed/embed1/") + o.str());
      files[2] = view->createFile(std::string("/test/embed/embed2/") + o.str());
      files[3] = view->createFile(std::string("/test/embed/embed3/") + o.str());

      for (int j = 0; j < 4; ++j) {
        while (files[j]->getNumLocation() != 5) {
          files[j]->addLocation(getRandomLocation());
        }

        view->updateFileStore(files[j].get());
      }
    }

    // Create some file without replicas assigned
    for (int i = 0; i < 500; ++i) {
      std::ostringstream o;
      o << "noreplicasfile" << i;
      view->createFile(std::string("/test/embed/embed1/") + o.str());
    }

    // Sum up all the locations
    size_t numReplicas = countReplicas(fsView.get());
    CPPUNIT_ASSERT(numReplicas == 20000);
    size_t numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 0);
    CPPUNIT_ASSERT(fsView->getNoReplicasFileList().size() == 500);

    // Unlink replicas
    for (int i = 100; i < 500; ++i) {
      std::ostringstream o;
      o << "file" << i;
      // Unlink some replicas
      std::shared_ptr<eos::IFileMD> f = c->findFile(o.str());
      f->unlinkLocation(f->getLocation(0));
      f->unlinkLocation(f->getLocation(0));
      view->updateFileStore(f.get());
    }

    numReplicas = countReplicas(fsView.get());
    CPPUNIT_ASSERT(numReplicas == 19200);
    numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 800);
    std::list<eos::IFileMD::id_t> file_ids;

    for (int i = 500; i < 900; ++i) {
      std::ostringstream o;
      o << "file" << i;
      // Unlink some replicas
      std::shared_ptr<eos::IFileMD> f{c->findFile(o.str())};
      f->unlinkAllLocations();
      c->removeFile(o.str());
      f->setContainerId(0);
      file_ids.push_back(f->getId());
      view->updateFileStore(f.get());
    }

    numReplicas = countReplicas(fsView.get());
    CPPUNIT_ASSERT(numReplicas == 17200);
    numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 2800);
    // Restart
    view->finalize();
    fsView->finalize();
    view->initialize();
    fsView->initialize();
    numReplicas = countReplicas(fsView.get());
    CPPUNIT_ASSERT(numReplicas == 17200);
    numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 2800);
    CPPUNIT_ASSERT(fsView->getNoReplicasFileList().size() == 500);
    std::shared_ptr<eos::IFileMD> f{
      view->getFile(std::string("/test/embed/embed1/file1"))};
    f->unlinkAllLocations();
    numReplicas = countReplicas(fsView.get());
    CPPUNIT_ASSERT(numReplicas == 17195);
    numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 2805);
    f->removeAllLocations();
    numUnlinked = countUnlinked(fsView.get());
    CPPUNIT_ASSERT(numUnlinked == 2800);
    view->updateFileStore(f.get());
    CPPUNIT_ASSERT(fsView->getNoReplicasFileList().size() == 501);
    view->removeFile(f.get());
    CPPUNIT_ASSERT(fsView->getNoReplicasFileList().size() == 500);
    view->finalize();
    fsView->finalize();

    // Cleanup - remove all files
    for (int i = 0; i < 1000; ++i) {
      std::ostringstream o;
      o << "file" << i;
      std::list<std::string> paths;
      paths.push_back("/test/embed/" + o.str());
      paths.push_back("/test/embed/embed1/" + o.str());
      paths.push_back("/test/embed/embed2/" + o.str());
      paths.push_back("/test/embed/embed3/" + o.str());

      for (auto && elem : paths) {
        // Skip the files that have already been removed
        if ((elem == "/test/embed/embed1/file1") ||
            (i >= 500 && i < 900 && elem.find("/test/embed/embed2/") == 0)) {
          continue;
        }

        std::shared_ptr<eos::IFileMD> file{view->getFile(elem)};
        CPPUNIT_ASSERT_NO_THROW(view->unlinkFile(file.get()));
        CPPUNIT_ASSERT_NO_THROW(file->removeAllLocations());
        CPPUNIT_ASSERT_NO_THROW(view->removeFile(file.get()));
      }
    }

    // Remove the files that were unlinked only
    for (auto && id : file_ids) {
      std::shared_ptr<eos::IFileMD> file = fileSvc->getFileMD(id);
      CPPUNIT_ASSERT_NO_THROW(file->removeAllLocations());
      CPPUNIT_ASSERT_NO_THROW(view->removeFile(file.get()));
    }

    for (int i = 0; i < 500; ++i) {
      std::ostringstream o;
      o << "noreplicasfile" << i;
      std::string path = "/test/embed/embed1/" + o.str();
      std::shared_ptr<eos::IFileMD> file{view->getFile(path)};
      CPPUNIT_ASSERT_NO_THROW(view->unlinkFile(file.get()));
      CPPUNIT_ASSERT_NO_THROW(view->removeFile(file.get()));
    }

    // Remove all containers
    CPPUNIT_ASSERT_NO_THROW(view->removeContainer("/test/", true));
    // Remove the root container
    std::shared_ptr<eos::IContainerMD> root{view->getContainer("/")};
    CPPUNIT_ASSERT_NO_THROW(contSvc->removeContainer(root.get()));
    view->finalize();
  } catch (eos::MDException& e) {
    CPPUNIT_ASSERT_MESSAGE(e.getMessage().str(), false);
  }
}
