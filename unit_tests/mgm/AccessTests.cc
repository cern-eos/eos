//------------------------------------------------------------------------------
// File: AccessTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "mgm/Access.hh"
#include "mgm/Acl.hh"
#include "mgm/auth/AccessChecker.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"

using namespace eos;

//------------------------------------------------------------------------------
// Test basic access functionality
//------------------------------------------------------------------------------
TEST(Access, SetRule)
{
  using namespace eos::mgm;
  Access::StallInfo old_stall;
  Access::StallInfo new_stall("*", "60", "test stall", true);
  ASSERT_EQ(false, Access::gStallGlobal);
  // Set new stall state
  Access::SetStallRule(new_stall, old_stall);
  // Do checks without taking the lock as this is just for test purposes
  ASSERT_STREQ("60", Access::gStallRules[new_stall.mType].c_str());
  ASSERT_STREQ("test stall", Access::gStallComment[new_stall.mType].c_str());
  ASSERT_EQ(new_stall.mIsGlobal, Access::gStallGlobal);
  Access::StallInfo empty_stall;
  // Setting an empty stall should not change anything
  Access::SetStallRule(empty_stall, old_stall);
  ASSERT_STREQ("60", Access::gStallRules[new_stall.mType].c_str());
  ASSERT_STREQ("test stall", Access::gStallComment[new_stall.mType].c_str());
  ASSERT_EQ(new_stall.mIsGlobal, Access::gStallGlobal);
  // Revert to initial state
  Access::StallInfo tmp_stall;
  Access::SetStallRule(old_stall, tmp_stall);
  ASSERT_TRUE(Access::gStallRules.count(old_stall.mType) == 0);
  ASSERT_TRUE(Access::gStallComment.count(old_stall.mType) == 0);
  ASSERT_EQ(old_stall.mIsGlobal, Access::gStallGlobal);
}

IContainerMDPtr makeContainer(uid_t uid, gid_t gid, int mode)
{
  IContainerMDPtr cont(new eos::QuarkContainerMD());
  cont->setCUid(uid);
  cont->setCGid(gid);
  cont->setMode(mode);
  return cont;
}

eos::common::VirtualIdentity makeIdentity(uid_t uid, gid_t gid)
{
  eos::common::VirtualIdentity vid;
  vid.uid = uid;
  vid.gid = gid;
  return vid;
}

TEST(AccessChecker, UserRWX)
{
  IContainerMDPtr cont = makeContainer(1234, 9999,
                                       S_IFDIR | S_IRWXU);
  // No access for "other"
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), W_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), X_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(3333, 3333)));
  // No access for "group"
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK, makeIdentity(3333, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), W_OK, makeIdentity(3333, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), X_OK, makeIdentity(3333, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(3333, 9999)));
  // Allow access for user
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), W_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), X_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(1234, 8888)));
}

TEST(AccessChecker, rwxrwxrx)
{
  IContainerMDPtr cont = makeContainer(1234, 9999,
                                       S_IFDIR | S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  // rwx for user
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), W_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), X_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(1234, 8888)));
  // rwx for group
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK, makeIdentity(3333, 9999)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), W_OK, makeIdentity(3333, 9999)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), X_OK, makeIdentity(3333, 9999)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(3333, 9999)));
  // rx for other
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), W_OK, makeIdentity(3333, 3333)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), X_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK | W_OK, makeIdentity(3333, 3333)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), X_OK | W_OK, makeIdentity(3333, 3333)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), mgm::Acl(), R_OK | X_OK, makeIdentity(3333, 3333)));
}

TEST(AccessChecker, WithAclUserRWX)
{
  IContainerMDPtr cont = makeContainer(5555, 9999,
                                       S_IFDIR | S_IRWXU);
  // no access for other
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK, makeIdentity(1234, 8888)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), W_OK, makeIdentity(1234, 8888)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), X_OK, makeIdentity(1234, 8888)));
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), mgm::Acl(), R_OK | W_OK | X_OK, makeIdentity(1234, 8888)));
  // .. unless we have an acl
  eos::common::VirtualIdentity vid1 = makeIdentity(1234, 8888);
  vid1.allowed_gids = { 8888 };
  eos::mgm::Acl acl("u:1234:rwx", "", "", vid1, true);
  ASSERT_TRUE(acl.HasAcl());
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, R_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, W_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, X_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, R_OK | W_OK | X_OK, makeIdentity(1234, 8888)));
  // .. try passing the extended attributes, instead of the Acl object
  eos::IContainerMD::XAttrMap xattrmap;
  xattrmap["sys.acl"] = "u:1234:rwx";
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), xattrmap, R_OK, vid1));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), xattrmap, W_OK, vid1));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), xattrmap, X_OK, vid1));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), xattrmap, R_OK | W_OK | X_OK, vid1));
  // try a group acl ...
  vid1.allowed_gids = { 8888 };
  eos::mgm::Acl acl2("g:8888:rwx", "", "", vid1, true);
  ASSERT_TRUE(acl.HasAcl());
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, R_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, W_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, X_OK, makeIdentity(1234, 8888)));
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, R_OK | W_OK | X_OK, makeIdentity(1234, 8888)));
}

TEST(AccessChecker, WithPrepare)
{
  IContainerMDPtr cont = makeContainer(19229, 9999,
                                       S_IFDIR | S_IRWXU);
  eos::common::VirtualIdentity vid1 = makeIdentity(19229, 1489);
  vid1.allowed_gids = {1489};
  eos::mgm::Acl acl("u:19227:rwx+d,u:19229:rwx+dp,u:19230:rwx+dp", "", "", vid1,
                    true);
  ASSERT_TRUE(acl.HasAcl());
  ASSERT_TRUE(mgm::AccessChecker::checkContainer(
                cont.get(), acl, P_OK, vid1));
  // no prepare flag for uid 19229
  acl = eos::mgm::Acl("u:19227:rwx+d,u:19229:rwx+d,u:19230:rwx+dp", "", "", vid1,
                      true);
  ASSERT_TRUE(acl.HasAcl());
  ASSERT_FALSE(mgm::AccessChecker::checkContainer(
                 cont.get(), acl, P_OK, vid1));
}

IFileMDPtr makeFile(uid_t uid, gid_t gid, int mode)
{
  IFileMDPtr file(new eos::QuarkFileMD());
  file->setCUid(uid);
  file->setCGid(gid);
  file->setFlags(mode);
  return file;
}

TEST(AccessChecker, FileUserRWX)
{
  IFileMDPtr file = makeFile(5555, 9999, S_IRWXU);
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), X_OK,
              makeIdentity(5555, 1111)));
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), R_OK | X_OK,
              makeIdentity(5555, 1111)));
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), W_OK | X_OK,
              makeIdentity(5555, 1111)));
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), R_OK | W_OK | X_OK,
              makeIdentity(5555, 1111)));
  // different uid than the file in question
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(9999, 1111)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), R_OK | X_OK,
               makeIdentity(9999, 1111)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), W_OK | X_OK,
               makeIdentity(9999, 1111)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), R_OK | W_OK | X_OK,
               makeIdentity(9999, 1111)));
  // different uid, same gid, still deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(9999, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), R_OK | X_OK,
               makeIdentity(9999, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), W_OK | X_OK,
               makeIdentity(9999, 9999)));
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), R_OK | W_OK | X_OK,
               makeIdentity(9999, 9999)));
}

TEST(AccessChecker, FileGroupRWX)
{
  // file only allows group access
  IFileMDPtr file = makeFile(5555, 9999, S_IRWXG);
  // file has same uid, and group as file - allow
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), X_OK,
              makeIdentity(5555, 9999)));
  // file has same uid - deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(5555, 8888)));
  // others - deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(1111, 2222)));
}

TEST(AccessChecker, FileOtherRWX)
{
  // file only allows group access - weird, but possible
  IFileMDPtr file = makeFile(5555, 9999, S_IRWXO);
  // file has same uid/gid, deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(5555, 9999)));
  // file has same uid, deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(5555, 8888)));
  // file has same gid, deny
  ASSERT_FALSE(mgm::AccessChecker::checkFile(file.get(), X_OK,
               makeIdentity(6666, 9999)));
  // different uid, different gid, grant
  ASSERT_TRUE(mgm::AccessChecker::checkFile(file.get(), X_OK,
              makeIdentity(2222, 3333)));
}

