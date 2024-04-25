//------------------------------------------------------------------------------
// File: MappingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "Namespace.hh"
#define IN_TEST_HARNESS
#include "common/Mapping.hh"
#undef IN_TEST_HARNESS
#include <XrdSec/XrdSecEntity.hh>
#include <memory>

EOSCOMMONTESTING_BEGIN

void FreeXrdSecEntity(XrdSecEntity* client)
{
  free(client->name);
  free(client->host);
  free(client->vorg);
  free(client->role);
  free(client->grps);
  free(client->caps);
  free(client->endorsements);
  free(client->moninfo);
  free(client->creds);
  free((char*)client->tident);
}

TEST(Mapping, VidAssignOperator)
{
  using namespace eos::common;
  VirtualIdentity vid;
  vid.uid = 99;
  vid.gid = 99;
  vid.uid_string = "99";
  vid.gid_string = "99";
  vid.allowed_uids = {2, 3, 99};
  vid.allowed_gids = {2, 4, 99};
  vid.tident = "client:process_id:sockd_fd";
  vid.name = "dummy_user";
  vid.prot = "xrootd";
  vid.host = "localhost.localdomain";
  vid.grps = "some_random_grps";
  vid.role = "some_random_role";
  vid.dn = "some_random_dn";
  vid.geolocation = "some_random_geoloacation";
  vid.app = "some_random_app";
  vid.sudoer = true;
  VirtualIdentity copy_vid = vid;
  ASSERT_EQ(vid.uid, copy_vid.uid);
  ASSERT_EQ(vid.gid, copy_vid.gid);
  ASSERT_EQ(vid.uid_string, copy_vid.uid_string);
  ASSERT_EQ(vid.gid_string, copy_vid.gid_string);
  ASSERT_EQ(vid.allowed_uids, copy_vid.allowed_uids);
  ASSERT_EQ(vid.allowed_gids, copy_vid.allowed_gids);
  ASSERT_STREQ(vid.tident.c_str(), copy_vid.tident.c_str());
  ASSERT_STREQ(vid.name.c_str(),  copy_vid.name.c_str());
  ASSERT_STREQ(vid.prot.c_str(), copy_vid.prot.c_str());
  ASSERT_EQ(vid.host, copy_vid.host);
  ASSERT_EQ(vid.grps, copy_vid.grps);
  ASSERT_EQ(vid.role, copy_vid.role);
  ASSERT_EQ(vid.dn, copy_vid.dn);
  ASSERT_EQ(vid.geolocation, copy_vid.geolocation);
  ASSERT_EQ(vid.app, copy_vid.app);
  ASSERT_EQ(vid.sudoer, copy_vid.sudoer);
  ASSERT_TRUE(vid.hasUid(2));
  ASSERT_TRUE(copy_vid.hasUid(2));
  ASSERT_TRUE(vid.hasUid(3));
  ASSERT_TRUE(copy_vid.hasUid(3));
  ASSERT_TRUE(vid.hasUid(99));
  ASSERT_TRUE(copy_vid.hasUid(99));
  ASSERT_FALSE(vid.hasUid(4));
  ASSERT_FALSE(copy_vid.hasUid(4));
  ASSERT_TRUE(vid.hasGid(4));
  ASSERT_TRUE(copy_vid.hasGid(4));
  ASSERT_FALSE(vid.hasGid(3));
  ASSERT_FALSE(copy_vid.hasGid(3));
}

TEST(VirtualIdentity, IsLocalhost)
{
  VirtualIdentity vid;
  vid.host = "localhost";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost6";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost.localdomain";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost6.localdomain6";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "pickles";
  ASSERT_FALSE(vid.isLocalhost());
  vid.host = "asdf";
  ASSERT_FALSE(vid.isLocalhost());
}


TEST(VirtualIdentity, HandleKEYS)
{
  using namespace eos::common;
  const std::string secret_key = "xyz_my_secret_key_xyz";
  VirtualIdentity vid;
  auto EntityDeleter = [](XrdSecEntity * client) {
    FreeXrdSecEntity(client);
    delete client;
  };
  std::unique_ptr<XrdSecEntity, decltype(EntityDeleter)>
  client(new XrdSecEntity("gsi"), EntityDeleter);
  client->name = strdup("random");
  client->host = strdup("[::ffff:172.24.76.44]");
  client->vorg = strdup("cms cms cms");
  client->role = strdup("production NULL NULL NULL");
  client->grps = strdup("/cms /cms /cms/country /cms/country/us /cms/uscms");
  client->caps = nullptr;
  client->endorsements = strdup(secret_key.c_str());
  client->moninfo = nullptr;
  client->creds = nullptr;
  client->credslen = 0;
  client->ueid = 0xdead;
  client->addrInfo = nullptr;
  client->tident = strdup("http");
  client->pident = nullptr;
  client->sessvar = nullptr;
  client->uid = 0;
  client->gid = 0;
  // This is happenndin in the IdMap for sss/grpc/https
  vid.key = client->endorsements;
  // Add VOMS mapping
  const std::string_view uid_voms = "voms:\"/cms:production\":uid";
  const std::string_view gid_voms = "voms:\"/cms:production\":gid";
  uid_t mapped_uid = 81;
  gid_t mapped_gid = 81;
  Mapping::gVirtualUidMap.emplace(uid_voms, mapped_uid);
  Mapping::gVirtualGidMap.emplace(gid_voms, mapped_gid);
  // Add specific EOS KEY mapping
  std::string uid_key = "https:\"key:abbabeefdeadabba\":uid";
  std::string gid_key = "https:\"key:abbabeefdeadabba\":gid";
  mapped_uid = 32;
  mapped_gid = 32;
  Mapping::gVirtualUidMap.emplace(uid_key, mapped_uid);
  Mapping::gVirtualGidMap.emplace(gid_key, mapped_gid);
  Mapping::HandleVOMS(client.get(), vid);
  ASSERT_EQ(81, vid.uid);
  ASSERT_EQ(81, vid.gid);
  // The key should not match so there should be no change
  Mapping::HandleKEYS(client.get(), vid);
  ASSERT_EQ(81, vid.uid);
  ASSERT_EQ(81, vid.gid);
  // Add a new key mapping to match the given endorsements
  uid_key = "https:\"key:";
  uid_key += secret_key;
  uid_key += "\":uid";
  gid_key = "https:\"key:";
  gid_key += secret_key;
  gid_key += "\":gid";
  Mapping::gVirtualUidMap.emplace(uid_key, mapped_uid);
  Mapping::gVirtualGidMap.emplace(gid_key, mapped_gid);
  // The key matches so the mapped identity should also match
  Mapping::HandleKEYS(client.get(), vid);
  ASSERT_EQ(32, vid.uid);
  ASSERT_EQ(32, vid.gid);
}

EOSCOMMONTESTING_END
