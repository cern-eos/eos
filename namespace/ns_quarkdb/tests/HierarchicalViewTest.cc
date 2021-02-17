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
//! @breif HierarchicalView tests
//------------------------------------------------------------------------------
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include "namespace/ns_quarkdb/utils/QuotaRecomputer.hh"
#include "namespace/utils/RmrfHelper.hh"
#include "namespace/Resolver.hh"
#include "namespace/utils/RenameSafetyCheck.hh"
#include "common/LayoutId.hh"
#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <pthread.h>
#include <sstream>
#include <unistd.h>
#include <gtest/gtest.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <vector>

class HierarchicalViewF : public eos::ns::testing::NsTestsFixture {};

TEST_F(HierarchicalViewF, LoadTest)
{
  std::shared_ptr<eos::IContainerMD> cont1 =
    view()->createContainer("/test/embed/embed1", true);
  std::shared_ptr<eos::IContainerMD> cont2 =
    view()->createContainer("/test/embed/embed2", true);
  std::shared_ptr<eos::IContainerMD> cont3 =
    view()->createContainer("/test/embed/embed3", true);
  std::shared_ptr<eos::IContainerMD> cont4 =
    view()->createContainer("/test/embed/embed4", true);
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  std::shared_ptr<eos::IContainerMD> test = view()->getContainer("/test");
  std::shared_ptr<eos::IContainerMD> embed = view()->getContainer("/test/embed");
  ASSERT_THROW(embed->setName("with/slashes"), eos::MDException);
  ASSERT_TRUE(root != nullptr);
  ASSERT_TRUE(root->getId() == root->getParentId());
  ASSERT_TRUE(test != nullptr);
  ASSERT_TRUE(test->findContainer("embed") != nullptr);
  ASSERT_TRUE(embed != nullptr);
  ASSERT_EQ(root->getId(), 1);
  ASSERT_NE(test->getId(), 1);
  ASSERT_NE(embed->getId(), 1);
  ASSERT_TRUE(embed->findContainer("embed1") != nullptr);
  ASSERT_TRUE(embed->findContainer("embed2") != nullptr);
  ASSERT_TRUE(embed->findContainer("embed3") != nullptr);
  ASSERT_TRUE(cont1->getName() ==
              embed->findContainer("embed1")->getName());
  ASSERT_TRUE(cont2->getName() ==
              embed->findContainer("embed2")->getName());
  ASSERT_TRUE(cont3->getName() ==
              embed->findContainer("embed3")->getName());
  view()->removeContainer("/test/embed/embed2");
  ASSERT_TRUE(embed->findContainer("embed2") == nullptr);
  view()->createFile("/test/embed/file1");
  view()->createFile("/test/embed/file2");
  view()->createFile("/test/embed/embed1/file1");
  view()->createFile("/test/embed/embed1/file2");
  view()->createFile("/test/embed/embed1/file3");
  std::shared_ptr<eos::IFileMD> fileR =
    view()->createFile("/test/embed/embed1/fileR");
  ASSERT_THROW(fileR->setName("has/slashes"), eos::MDException);
  ASSERT_TRUE(view()->getFile("/test/embed/file1"));
  ASSERT_TRUE(view()->getFile("/test/embed/file2"));
  ASSERT_TRUE(view()->getFile("/test/embed/embed1/file1"));
  ASSERT_TRUE(view()->getFile("/test/embed/embed1/file2"));
  ASSERT_TRUE(view()->getFile("/test/embed/embed1/file3"));
  // Rename
  view()->renameContainer(cont4.get(), "embed4.renamed");
  ASSERT_TRUE(cont4->getName() == "embed4.renamed");
  ASSERT_THROW(view()->renameContainer(cont4.get(), "embed1"),
               eos::MDException);
  ASSERT_THROW(view()->renameContainer(cont4.get(), "embed1/asd"),
               eos::MDException);
  view()->getContainer("/test/embed/embed4.renamed");
  view()->renameFile(fileR.get(), "fileR.renamed");
  ASSERT_TRUE(fileR->getName() == "fileR.renamed");
  ASSERT_THROW(view()->renameFile(fileR.get(), "file1"),
               eos::MDException);
  ASSERT_THROW(view()->renameFile(fileR.get(), "file1/asd"),
               eos::MDException);
  view()->getFile("/test/embed/embed1/fileR.renamed");
  ASSERT_THROW(view()->renameContainer(root.get(), "rename"),
               eos::MDException);
  // Test the "reverse" lookup
  std::shared_ptr<eos::IFileMD> file =
    view()->getFile("/test/embed/embed1/file3");
  std::shared_ptr<eos::IContainerMD> container =
    view()->getContainer("/test/embed/embed1");
  ASSERT_EQ(view()->getUri(container.get()), "/test/embed/embed1/");
  ASSERT_EQ(view()->getUriFut(container->getIdentifier()).get(),
            "/test/embed/embed1/");
  ASSERT_EQ(view()->getUri(file.get()), "/test/embed/embed1/file3");
  ASSERT_EQ(view()->getUriFut(file->getIdentifier()).get(),
            "/test/embed/embed1/file3");
  ASSERT_THROW(view()->getUri((eos::IFileMD*)nullptr), eos::MDException);
  ASSERT_THROW(view()->getUriFut(eos::FileIdentifier(9999999)).get(),
               eos::MDException);
  std::shared_ptr<eos::IFileMD> toBeDeleted =
    view()->getFile("/test/embed/embed1/file2");
  toBeDeleted->addLocation(12);
  // This should not succeed since the file should have a replica
  ASSERT_THROW(view()->removeFile(toBeDeleted.get()), eos::MDException);
  // We unlink the file - at this point the file should not be attached to the
  // hierarchy but should still be accessible by id and thus the md pointer
  // should stay valid
  view()->unlinkFile("/test/embed/embed1/file2");
  ASSERT_THROW(view()->getFile("/test/embed/embed1/file2"),
               eos::MDException);
  ASSERT_TRUE(cont1->findFile("file2") == nullptr);
  // We remove the replicas and the file but we need to reload the toBeDeleted
  // pointer
  eos::IFileMD::id_t id = toBeDeleted->getId();
  toBeDeleted = fileSvc()->getFileMD(id);
  toBeDeleted->clearUnlinkedLocations();
  view()->removeFile(toBeDeleted.get());
  ASSERT_THROW(fileSvc()->getFileMD(id), eos::MDException);
  shut_down_everything();
  ASSERT_TRUE(view()->getContainer("/"));
  ASSERT_TRUE(view()->getContainer("/test"));
  ASSERT_TRUE(view()->getContainer("/test/embed"));
  ASSERT_TRUE(view()->getContainer("/test/embed/embed1"));
  ASSERT_TRUE(view()->getFile("/test/embed/file1"));
  ASSERT_TRUE(view()->getFile("/test/embed/file2"));
  ASSERT_TRUE(view()->getFile("/test/embed/embed1/file1"));
  ASSERT_TRUE(view()->getFile("/test/embed/embed1/file3"));
  view()->getContainer("/test/embed/embed4.renamed");
  view()->getFile("/test/embed/embed1/fileR.renamed");
  // Cleanup
  // Unlink files - need to do it in this order since the unlink removes the
  // file from the container and then getFile by path won't work anymore
  std::shared_ptr<eos::IFileMD> file1 = view()->getFile("/test/embed/file1");
  std::shared_ptr<eos::IFileMD> file2 = view()->getFile("/test/embed/file2");
  std::shared_ptr<eos::IFileMD> file11 =
    view()->getFile("/test/embed/embed1/file1");
  std::shared_ptr<eos::IFileMD> file13 =
    view()->getFile("/test/embed/embed1/file3");
  view()->unlinkFile("/test/embed/file1");
  view()->unlinkFile("/test/embed/file2");
  view()->unlinkFile("/test/embed/embed1/file1");
  view()->unlinkFile("/test/embed/embed1/file3");
  view()->unlinkFile("/test/embed/embed1/fileR.renamed");
  // Remove files
  view()->removeFile(fileSvc()->getFileMD(file1->getId()).get());
  view()->removeFile(fileSvc()->getFileMD(file2->getId()).get());
  view()->removeFile(fileSvc()->getFileMD(file11->getId()).get());
  view()->removeFile(fileSvc()->getFileMD(file13->getId()).get());
  view()->removeFile(fileSvc()->getFileMD(fileR->getId()).get());
  // Remove all containers
  eos::RmrfHelper::nukeDirectory(view(), "/test/");
}

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t
mapSize(const eos::IFileMD* file)
{
  eos::IFileMD::layoutId_t lid = file->getLayoutId();

  if (lid > 3) {
    eos::MDException e(ENOENT);
    e.getMessage() << "Location does not exist" << std::endl;
    throw (e);
  }

  return lid * file->getSize();
}

//------------------------------------------------------------------------------
// Create files at given path
//------------------------------------------------------------------------------
static void
createFiles(const std::string& path, eos::IView* view,
            std::map<uid_t, eos::QuotaNodeCore::UsageInfo>* users,
            std::map<gid_t, eos::QuotaNodeCore::UsageInfo>* groups)
{
  eos::IQuotaNode* node = view->getQuotaNode(view->getContainer(path).get());

  for (int i = 0; i < 1000; ++i) {
    std::ostringstream p;
    p << path << "file" << i;
    std::shared_ptr<eos::IFileMD> file{view->createFile(p.str())};
    file->setCUid(random() % 10 + 1);
    file->setCGid(random() % 3 + 1);
    file->setSize(random() % 1000000 + 1);
    file->setLayoutId(random() % 3 + 1);
    view->updateFileStore(file.get());
    node->addFile(file.get());
    uint64_t size = mapSize(file.get());
    eos::QuotaNodeCore::UsageInfo& user = (*users)[file->getCUid()];
    eos::QuotaNodeCore::UsageInfo& group = (*groups)[file->getCGid()];
    user.space += file->getSize();
    user.physicalSpace += size;
    user.files++;
    group.space += file->getSize();
    group.physicalSpace += size;
    group.files++;
  }
}

TEST_F(HierarchicalViewF, ZeroSizedFilenames)
{
  eos::IContainerMDPtr cont1 = view()->createContainer("/test/dir1", true);
  eos::IContainerMDPtr cont2 = view()->createContainer("/dir2", true);
  eos::IFileMDPtr file1 = view()->createFile("/file1", true);
  file1->setName("");
  ASSERT_THROW(cont1->addFile(file1.get()), eos::MDException);
  cont2->setName("");
  ASSERT_THROW(cont1->addContainer(cont2.get()), eos::MDException);
}

//------------------------------------------------------------------------------
// Test namespace resolver based on (path, cid, cxid)
//------------------------------------------------------------------------------
TEST_F(HierarchicalViewF, Resolver)
{
  // Make a lot of containers
  for (size_t i = 0; i < 50; i++) {
    view()->createContainer(SSTR("/dir" << i), true);
  }

  eos::ContainerSpecificationProto spec;
  ASSERT_THROW(eos::Resolver::resolveContainer(view(), spec), eos::MDException);
  spec.set_path("/dir49");
  eos::IContainerMDPtr cont = eos::Resolver::resolveContainer(view(), spec);
  ASSERT_EQ(cont->getName(), "dir49");
  spec.set_cid("48");
  cont = eos::Resolver::resolveContainer(view(), spec);
  ASSERT_EQ(cont->getName(), "dir46");
  spec.set_cxid("30");
  cont = eos::Resolver::resolveContainer(view(), spec);
  ASSERT_EQ(cont->getName(), "dir46");
  spec.set_path("/chicken");
  ASSERT_THROW(eos::Resolver::resolveContainer(view(), spec), eos::MDException);
  spec.set_cid("chicken chicken");
  ASSERT_THROW(eos::Resolver::resolveContainer(view(), spec), eos::MDException);
  spec.set_cxid("chicken");
  ASSERT_THROW(eos::Resolver::resolveContainer(view(), spec), eos::MDException);
}

TEST_F(HierarchicalViewF, QuotaTest)
{
  srandom(time(nullptr));
  // Initialize the system
  setSizeMapper(mapSize);
  // Create some structures, insert quota nodes and test their correctness
  std::shared_ptr<eos::IContainerMD> cont1{
    view()->createContainer("/test/embed/embed1", true)};
  std::shared_ptr<eos::IContainerMD> cont2{
    view()->createContainer("/test/embed/embed2", true)};
  std::shared_ptr<eos::IContainerMD> cont3{
    view()->createContainer("/test/embed/embed3", true)};
  std::shared_ptr<eos::IContainerMD> cont4{view()->getContainer("/test/embed")};
  std::shared_ptr<eos::IContainerMD> cont5{view()->getContainer("/test")};
  eos::IQuotaNode* qnCreated1 = view()->registerQuotaNode(cont1.get());
  eos::IQuotaNode* qnCreated2 = view()->registerQuotaNode(cont3.get());
  eos::IQuotaNode* qnCreated3 = view()->registerQuotaNode(cont5.get());
  ASSERT_THROW(view()->registerQuotaNode(cont1.get()), eos::MDException);
  ASSERT_TRUE(qnCreated1);
  ASSERT_TRUE(qnCreated2);
  ASSERT_TRUE(qnCreated3);
  eos::IQuotaNode* qn1 = view()->getQuotaNode(cont1.get());
  eos::IQuotaNode* qn2 = view()->getQuotaNode(cont2.get());
  eos::IQuotaNode* qn3 = view()->getQuotaNode(cont3.get());
  eos::IQuotaNode* qn4 = view()->getQuotaNode(cont4.get());
  eos::IQuotaNode* qn5 = view()->getQuotaNode(cont5.get());
  ASSERT_TRUE(qn1);
  ASSERT_TRUE(qn2);
  ASSERT_TRUE(qn3);
  ASSERT_TRUE(qn4);
  ASSERT_TRUE(qn5);
  ASSERT_TRUE(qn2 == qn5);
  ASSERT_TRUE(qn4 == qn5);
  ASSERT_TRUE(qn1 != qn5);
  ASSERT_TRUE(qn3 != qn5);
  ASSERT_TRUE(qn3 != qn2);
  // Create some files
  std::map<uid_t, eos::QuotaNodeCore::UsageInfo> users1;
  std::map<gid_t, eos::QuotaNodeCore::UsageInfo> groups1;
  std::string path1 = "/test/embed/embed1/";
  createFiles(path1, view(), &users1, &groups1);
  std::map<uid_t, eos::QuotaNodeCore::UsageInfo> users2;
  std::map<gid_t, eos::QuotaNodeCore::UsageInfo> groups2;
  std::string path2 = "/test/embed/embed2/";
  createFiles(path2, view(), &users2, &groups2);
  std::map<uid_t, eos::QuotaNodeCore::UsageInfo> users3;
  std::map<gid_t, eos::QuotaNodeCore::UsageInfo> groups3;
  std::string path3 = "/test/embed/embed3/";
  createFiles(path3, view(), &users3, &groups3);
  // Verify correctness
  eos::IQuotaNode* node1 = view()->getQuotaNode(view()->getContainer(
                             path1).get());
  eos::IQuotaNode* node2 = view()->getQuotaNode(view()->getContainer(
                             path2).get());

  for (int i = 1; i <= 10; ++i) {
    ASSERT_EQ(node1->getPhysicalSpaceByUser(i), users1[i].physicalSpace);
    ASSERT_EQ(node2->getPhysicalSpaceByUser(i), users2[i].physicalSpace);
    ASSERT_EQ(node1->getUsedSpaceByUser(i), users1[i].space);
    ASSERT_EQ(node2->getUsedSpaceByUser(i), users2[i].space);
    ASSERT_EQ(node1->getNumFilesByUser(i), users1[i].files);
    ASSERT_EQ(node2->getNumFilesByUser(i), users2[i].files);
  }

  for (int i = 1; i <= 3; ++i) {
    ASSERT_EQ(node1->getPhysicalSpaceByGroup(i),
              groups1[i].physicalSpace);
    ASSERT_EQ(node2->getPhysicalSpaceByGroup(i),
              groups2[i].physicalSpace);
    ASSERT_EQ(node1->getUsedSpaceByGroup(i), groups1[i].space);
    ASSERT_EQ(node2->getUsedSpaceByGroup(i), groups2[i].space);
    ASSERT_EQ(node1->getNumFilesByGroup(i), groups1[i].files);
    ASSERT_EQ(node2->getNumFilesByGroup(i), groups2[i].files);
  }

  // Restart and check if the quota stats are reloaded correctly
  shut_down_everything();
  node1 = view()->getQuotaNode(view()->getContainer(path1).get());
  node2 = view()->getQuotaNode(view()->getContainer(path2).get());
  ASSERT_TRUE(node1);
  ASSERT_TRUE(node2);

  for (int i = 1; i <= 10; ++i) {
    ASSERT_EQ(node1->getPhysicalSpaceByUser(i), users1[i].physicalSpace);
    ASSERT_EQ(node2->getPhysicalSpaceByUser(i), users2[i].physicalSpace);
    ASSERT_EQ(node1->getUsedSpaceByUser(i), users1[i].space);
    ASSERT_EQ(node2->getUsedSpaceByUser(i), users2[i].space);
    ASSERT_EQ(node1->getNumFilesByUser(i), users1[i].files);
    ASSERT_EQ(node2->getNumFilesByUser(i), users2[i].files);
  }

  for (int i = 1; i <= 3; ++i) {
    ASSERT_EQ(node1->getPhysicalSpaceByGroup(i), groups1[i].physicalSpace);
    ASSERT_EQ(node2->getPhysicalSpaceByGroup(i), groups2[i].physicalSpace);
    ASSERT_EQ(node1->getUsedSpaceByGroup(i), groups1[i].space);
    ASSERT_EQ(node2->getUsedSpaceByGroup(i), groups2[i].space);
    ASSERT_EQ(node1->getNumFilesByGroup(i), groups1[i].files);
    ASSERT_EQ(node2->getNumFilesByGroup(i), groups2[i].files);
  }

  // Remove the quota nodes on /test/embed/embed1 and /dest/embed/embed2
  // and check if the quota on /test has been updated
  eos::IQuotaNode* parentNode = nullptr;
  parentNode = view()->getQuotaNode(view()->getContainer("/test").get());
  view()->removeQuotaNode(view()->getContainer(path1).get());

  for (int i = 1; i <= 10; ++i) {
    ASSERT_EQ(parentNode->getPhysicalSpaceByUser(i),
              users1[i].physicalSpace + users2[i].physicalSpace);
    ASSERT_EQ(parentNode->getUsedSpaceByUser(i),
              users1[i].space + users2[i].space);
    ASSERT_EQ(parentNode->getNumFilesByUser(i),
              users1[i].files + users2[i].files);
  }

  for (int i = 1; i <= 3; ++i) {
    ASSERT_EQ(parentNode->getPhysicalSpaceByGroup(i),
              groups1[i].physicalSpace + groups2[i].physicalSpace);
    ASSERT_EQ(parentNode->getUsedSpaceByGroup(i),
              groups1[i].space + groups2[i].space);
    ASSERT_EQ(parentNode->getNumFilesByGroup(i),
              groups1[i].files + groups2[i].files);
  }

  view()->removeQuotaNode(view()->getContainer(path3).get());
  ASSERT_THROW(view()->removeQuotaNode(view()->getContainer(path3).get()),
               eos::MDException);

  for (int i = 1; i <= 10; ++i) {
    ASSERT_EQ(parentNode->getPhysicalSpaceByUser(i),
              users1[i].physicalSpace + users2[i].physicalSpace +
              users3[i].physicalSpace);
    ASSERT_EQ(parentNode->getUsedSpaceByUser(i),
              users1[i].space + users2[i].space + users3[i].space);
    ASSERT_EQ(parentNode->getNumFilesByUser(i),
              users1[i].files + users2[i].files + users3[i].files);
  }

  for (int i = 1; i <= 3; ++i) {
    ASSERT_EQ(parentNode->getPhysicalSpaceByGroup(i),
              groups1[i].physicalSpace + groups2[i].physicalSpace +
              groups3[i].physicalSpace);
    ASSERT_EQ(parentNode->getUsedSpaceByGroup(i),
              groups1[i].space + groups2[i].space + groups3[i].space);
    ASSERT_EQ(parentNode->getNumFilesByGroup(i),
              groups1[i].files + groups2[i].files + groups3[i].files);
  }

  // Clean up
  // Remove all the quota nodes
  ASSERT_THROW(view()->removeQuotaNode(view()->getContainer(path1).get()),
               eos::MDException);
  ASSERT_THROW(view()->removeQuotaNode(view()->getContainer(path2).get()),
               eos::MDException);
  ASSERT_THROW(view()->removeQuotaNode(view()->getContainer(path3).get()),
               eos::MDException);
  ASSERT_THROW(view()->removeQuotaNode(view()->getContainer("/test/embed").get()),
               eos::MDException);
  view()->removeQuotaNode(cont5.get());
  // Remove all the files
  std::list<std::string> paths{path1, path2, path3};

  for (auto && path_elem : paths) {
    for (int i = 0; i < 1000; ++i) {
      std::ostringstream p;
      p << path_elem << "file" << i;
      std::shared_ptr<eos::IFileMD> file{view()->getFile(p.str())};
      view()->unlinkFile(p.str());
      view()->removeFile(fileSvc()->getFileMD(file->getId()).get());
    }
  }

  // Remove all containers
  ASSERT_NO_THROW(eos::RmrfHelper::nukeDirectory(view(), "/test/"));
  // Remove the root container
  std::shared_ptr<eos::IContainerMD> root{view()->getContainer("/")};
  ASSERT_NO_THROW(containerSvc()->removeContainer(root.get()));
  ASSERT_NO_THROW(view()->finalize());
}

TEST_F(HierarchicalViewF, LostContainerTest)
{
  std::shared_ptr<eos::IContainerMD> cont1 =
    view()->createContainer("/test/embed/embed1", true);
  std::shared_ptr<eos::IContainerMD> cont2 =
    view()->createContainer("/test/embed/embed2", true);
  std::shared_ptr<eos::IContainerMD> cont3 =
    view()->createContainer("/test/embed/embed3", true);
  std::shared_ptr<eos::IContainerMD> cont4 =
    view()->createContainer("/test/embed/embed1/embedembed", true);
  std::shared_ptr<eos::IContainerMD> cont5 =
    view()->createContainer("/test/embed/embed3.conflict", true);

  // Create some files
  for (int i = 0; i < 1000; ++i) {
    std::ostringstream s1;
    s1 << "/test/embed/embed1/file" << i;
    std::ostringstream s2;
    s2 << "/test/embed/embed2/file" << i;
    std::ostringstream s3;
    s3 << "/test/embed/embed3/file" << i;
    std::ostringstream s4;
    s4 << "/test/embed/embed1/embedembed/file" << i;
    std::ostringstream s5;
    s5 << "/test/embed/embed3.conflict/file" << i;
    std::ostringstream s6;
    s6 << "/test/embed/embed2/conflict_file" << i;
    eos::IFileMDPtr embed1F = view()->createFile(s1.str());
    ASSERT_EQ(view()->getParentContainer(embed1F.get()).get(), cont1);
    view()->createFile(s2.str());
    view()->createFile(s3.str());
    view()->createFile(s4.str());
    view()->createFile(s5.str());
    view()->createFile(s6.str());
    std::shared_ptr<eos::IFileMD> file = view()->getFile(s6.str());

    if (i != 0) {
      ASSERT_THROW(view()->renameFile(file.get(), "conflict_file"),
                   eos::MDException);
    } else {
      view()->renameFile(file.get(), "conflict_file");
    }
  }

  // Trying to remove a non-empty container should result in an exception
  ASSERT_THROW(view()->getContainerMDSvc()->removeContainer(cont1.get()),
               eos::MDException);
  // Trying to rename a container to an already existing one should result in
  // an exception
  ASSERT_NO_THROW(cont5->setName("embed3"));

  // Cleanup
  for (int i = 0; i < 1000; ++i) {
    std::ostringstream s1;
    s1 << "/test/embed/embed1/file" << i;
    std::ostringstream s2;
    s2 << "/test/embed/embed2/file" << i;
    std::ostringstream s3;
    s3 << "/test/embed/embed3/file" << i;
    std::ostringstream s4;
    s4 << "/test/embed/embed1/embedembed/file" << i;
    std::ostringstream s5;
    s5 << "/test/embed/embed3.conflict/file" << i;
    std::ostringstream s6;
    s6 << "/test/embed/embed2/conflict_file" << i;
    std::list<std::string> paths{s1.str(), s2.str(), s3.str(), s4.str(),
                                 s5.str()};

    if (i != 0) {
      paths.insert(paths.end(), s6.str());
    }

    for (auto && elem : paths) {
      std::shared_ptr<eos::IFileMD> file = view()->getFile(elem);
      view()->unlinkFile(elem);
      view()->removeFile(fileSvc()->getFileMD(file->getId()).get());
    }
  }

  // Remove the conflict_file
  std::string path = "test/embed/embed2/conflict_file";
  std::shared_ptr<eos::IFileMD> file = view()->getFile(path);
  view()->unlinkFile(path);
  view()->removeFile(fileSvc()->getFileMD(file->getId()).get());
  // Remove all containers
  // TODO(gbitzes): Something wrong is here, this should succeed, investigate.
  // eos::RmrfHelper::nukeDirectory(view(), "/test/");
}

TEST_F(HierarchicalViewF, RenameDirectoryAsSubdirOfItself)
{
  std::shared_ptr<eos::IContainerMD> cont1 =
    view()->createContainer("/eos/dev/my-dir", true);
  std::shared_ptr<eos::IContainerMD> cont2 =
    view()->createContainer("/eos/dev/my-dir/subdir1", true);
  std::shared_ptr<eos::IContainerMD> cont3 =
    view()->createContainer("/eos/dev/my-dir/subdir1/subdir2", true);
  ASSERT_TRUE(eos::isSafeToRename(view(), cont3.get(), cont1.get()));
  ASSERT_FALSE(eos::isSafeToRename(view(), cont1.get(), cont3.get()));
  ASSERT_TRUE(eos::isSafeToRename(view(), cont2.get(),
                                  cont1.get())); // non-sensical to do, but safe (no-op)
  ASSERT_FALSE(eos::isSafeToRename(view(), cont1.get(), cont2.get()));
}

TEST_F(HierarchicalViewF, AddFileWithConflicts)
{
  eos::IContainerMDPtr cont1 = view()->createContainer("/test/dir1", true);
  view()->createContainer("/test/dir1/dir2", true);
  eos::IContainerMDPtr cont2 = view()->createContainer("/dir1", true);
  eos::IFileMDPtr file1 = view()->createFile("/test/dir1/file1", true);
  eos::IFileMDPtr file2 = view()->createFile("/file1", true);
  ASSERT_THROW(cont1->addFile(file2.get()),
               eos::MDException); // conflicts with file
  file2->setName("dir2");
  ASSERT_THROW(cont1->addFile(file2.get()),
               eos::MDException); // conflicts with directory
  cont1->addFile(file1.get()); // conflicts with itself, thus, no conflict
}

TEST_F(HierarchicalViewF, AddContainerWithConflicts)
{
  eos::IContainerMDPtr cont1 = view()->createContainer("/test/", true);
  eos::IContainerMDPtr cont4 = view()->createContainer("/test/dir1", true);
  eos::IContainerMDPtr cont2 = view()->createContainer("/dir1", true);
  ASSERT_THROW(cont1->addContainer(cont2.get()),
               eos::MDException); // conflicts with container
  view()->createFile("/test/file1", true);
  eos::IContainerMDPtr cont3 = view()->createContainer("/file1", true);
  ASSERT_THROW(cont1->addContainer(cont3.get()),
               eos::MDException); // conflicts with file
  cont1->addContainer(cont4.get()); // conflicts with itself, thus, no conflict
}

TEST_F(HierarchicalViewF, QuotaRecomputation)
{
  eos::IContainerMDPtr quota1 = view()->createContainer("/quota1", true);
  eos::IContainerMDPtr quota2 = view()->createContainer("/quota2", true);
  eos::IContainerMDPtr quota3 = view()->createContainer("/quota1/quota3", true);
  eos::IContainerMDPtr notquota1 = view()->createContainer("/not-a-quota", true);
  eos::IContainerMDPtr notquota2 =
    view()->createContainer("/quota1/not-a-quota-either", true);
  containerSvc()->updateStore(quota1.get());
  containerSvc()->updateStore(quota2.get());
  containerSvc()->updateStore(quota3.get());
  unsigned long layoutId = eos::common::LayoutId::GetId(
                             eos::common::LayoutId::kReplica,
                             eos::common::LayoutId::kMD5,
                             2,
                             eos::common::LayoutId::k4k);

  for (size_t i = 0; i < 10; i++) {
    eos::IFileMDPtr file = view()->createFile(SSTR("/quota1/f" << i), true);
    file->setSize(1337);
    file->setLayoutId(layoutId);
    file->setCUid(i % 4);
    file->setCGid(i % 2);
    fileSvc()->updateStore(file.get());
  }

  layoutId = eos::common::LayoutId::GetId(
               eos::common::LayoutId::kReplica,
               eos::common::LayoutId::kMD5,
               3,
               eos::common::LayoutId::k4k);

  for (size_t i = 0; i < 15; i++) {
    eos::IFileMDPtr file = view()->createFile(SSTR("/quota1/quota3/f" << i), true);
    file->setSize(1338);
    file->setLayoutId(layoutId);
    file->setCUid(100);
    file->setCGid(200);
    fileSvc()->updateStore(file.get());
  }

  layoutId = eos::common::LayoutId::GetId(
               eos::common::LayoutId::kReplica,
               eos::common::LayoutId::kMD5,
               5,
               eos::common::LayoutId::k4k);

  for (size_t i = 0; i < 17; i++) {
    eos::IFileMDPtr file = view()->createFile(SSTR("/quota2/f" << i), true);
    file->setSize(133);
    file->setLayoutId(layoutId);
    file->setCUid(i);
    file->setCGid(9000);
    fileSvc()->updateStore(file.get());
  }

  mdFlusher()->synchronize();
  eos::QuotaNodeCore qnc;
  eos::QuotaRecomputer recomputer(&(qcl()), executor());
  // Simple, non-nested case first: quota2
  eos::IQuotaNode* qn2 = view()->registerQuotaNode(quota2.get());
  ASSERT_NE(qn2, nullptr);
  eos::MDStatus status = recomputer.recompute(view()->getUri(quota2.get()),
                         quota2->getId(), qnc);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(status.getErrno(), 0);
  ASSERT_EQ(status.getError(), "");

  for (size_t i = 0; i < 17; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByUser(i), 133);
    ASSERT_EQ(qnc.getPhysicalSpaceByUser(i), 133 * 5);
    ASSERT_EQ(qnc.getNumFilesByUser(i), 1);
    ASSERT_EQ(qnc.getUsedSpaceByGroup(i), 0);
    ASSERT_EQ(qnc.getPhysicalSpaceByGroup(i), 0);
    ASSERT_EQ(qnc.getNumFilesByGroup(i), 0);
  }

  ASSERT_EQ(qnc.getUsedSpaceByGroup(9000), 17 * 133);
  ASSERT_EQ(qnc.getPhysicalSpaceByGroup(9000), 17 * 133 * 5);
  ASSERT_EQ(qnc.getNumFilesByGroup(9000), 17);
  // quota1 + quota3
  eos::IQuotaNode* qn1p3 = view()->registerQuotaNode(quota1.get());
  ASSERT_NE(qn1p3, nullptr);
  status = recomputer.recompute(view()->getUri(quota1.get()),
                                quota1->getId(), qnc);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(status.getErrno(), 0);
  ASSERT_EQ(status.getError(), "");

  // uid0 and uid1 have 3 files each
  for (size_t i = 0; i < 2; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByUser(i), 1337 * 3);
    ASSERT_EQ(qnc.getPhysicalSpaceByUser(i), 1337 * 3 * 2);
    ASSERT_EQ(qnc.getNumFilesByUser(i), 3);
  }

  // uid2 and uid3 and 2 files each
  for (size_t i = 2; i < 4; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByUser(i), 1337 * 2);
    ASSERT_EQ(qnc.getPhysicalSpaceByUser(i), 1337 * 2 * 2);
    ASSERT_EQ(qnc.getNumFilesByUser(i), 2);
  }

  // gid0 and gid1 have 5 each
  for (size_t i = 0; i < 2; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByGroup(i), 1337 * 5);
    ASSERT_EQ(qnc.getPhysicalSpaceByGroup(i), 1337 * 2 * 5);
    ASSERT_EQ(qnc.getNumFilesByGroup(i), 5);
  }

  ASSERT_EQ(qnc.getUsedSpaceByUser(100), 1338 * 15);
  ASSERT_EQ(qnc.getPhysicalSpaceByUser(100), 1338 * 15 * 3);
  ASSERT_EQ(qnc.getNumFilesByUser(100), 15);
  ASSERT_EQ(qnc.getUsedSpaceByGroup(200), 1338 * 15);
  ASSERT_EQ(qnc.getPhysicalSpaceByGroup(200), 1338 * 15 * 3);
  ASSERT_EQ(qnc.getNumFilesByGroup(200), 15);
  // register quota3, measure
  eos::IQuotaNode* qn3 = view()->registerQuotaNode(quota3.get());
  ASSERT_NE(qn3, nullptr);
  status = recomputer.recompute(view()->getUri(quota3.get()),
                                quota3->getId(), qnc);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(status.getErrno(), 0);
  ASSERT_EQ(status.getError(), "");
  ASSERT_EQ(qnc.getUsedSpaceByUser(100), 1338 * 15);
  ASSERT_EQ(qnc.getPhysicalSpaceByUser(100), 1338 * 15 * 3);
  ASSERT_EQ(qnc.getNumFilesByUser(100), 15);
  ASSERT_EQ(qnc.getUsedSpaceByGroup(200), 1338 * 15);
  ASSERT_EQ(qnc.getPhysicalSpaceByGroup(200), 1338 * 15 * 3);
  ASSERT_EQ(qnc.getNumFilesByGroup(200), 15);
  // measure quota1 _on its own_, without embedded quota3
  status = recomputer.recompute(view()->getUri(quota1.get()),
                                quota1->getId(), qnc);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(status.getErrno(), 0);
  ASSERT_EQ(status.getError(), "");

  // uid0 and uid1 have 3 files each
  for (size_t i = 0; i < 2; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByUser(i), 1337 * 3);
    ASSERT_EQ(qnc.getPhysicalSpaceByUser(i), 1337 * 3 * 2);
    ASSERT_EQ(qnc.getNumFilesByUser(i), 3);
  }

  // uid2 and uid3 and 2 files each
  for (size_t i = 2; i < 4; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByUser(i), 1337 * 2);
    ASSERT_EQ(qnc.getPhysicalSpaceByUser(i), 1337 * 2 * 2);
    ASSERT_EQ(qnc.getNumFilesByUser(i), 2);
  }

  // gid0 and gid1 have 5 each
  for (size_t i = 0; i < 2; i++) {
    ASSERT_EQ(qnc.getUsedSpaceByGroup(i), 1337 * 5);
    ASSERT_EQ(qnc.getPhysicalSpaceByGroup(i), 1337 * 2 * 5);
    ASSERT_EQ(qnc.getNumFilesByGroup(i), 5);
  }

  ASSERT_EQ(qnc.getUsedSpaceByUser(100), 0);
  ASSERT_EQ(qnc.getPhysicalSpaceByUser(100), 0);
  ASSERT_EQ(qnc.getNumFilesByUser(100), 0);
  ASSERT_EQ(qnc.getUsedSpaceByGroup(200), 0);
  ASSERT_EQ(qnc.getPhysicalSpaceByGroup(200), 0);
  ASSERT_EQ(qnc.getNumFilesByGroup(200), 0);
}

TEST_F(HierarchicalViewF, CustomContainerId)
{
  eos::IContainerMDPtr c32 = view()->createContainer("/c32", false, 32);
  ASSERT_EQ(c32->getId(), 32);
  eos::IContainerMDPtr root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);
  eos::IContainerMDPtr child = root->findContainer("c32");
  ASSERT_EQ(child.get(), c32.get());
  eos::IContainerMDPtr c33 = view()->createContainer("/c33", true);
  ASSERT_EQ(c33->getId(), 33);
}

TEST_F(HierarchicalViewF, CustomFileId)
{
  eos::IFileMDPtr f999 = view()->createFile("/f999", 5, 5, 999);
  ASSERT_EQ(f999->getId(), 999);
  eos::IFileMDPtr f1000 = view()->createFile("/f1000", 0, 0, 0);
  ASSERT_EQ(f1000->getId(), 1000);
}
