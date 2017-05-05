//------------------------------------------------------------------------------
// File: ProtoTests.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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
#include <gtest/gtest.h>
#include "MockFileMDSvc.hh"
#include "MockContainerMDSvc.hh"
#include "Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include <iostream>

using ::testing::_;
using ::testing::Return;

EOSNSTESTING_BEGIN

//------------------------------------------------------------------------------
// Test FileMd serialisation, deserialization and checksumming
//------------------------------------------------------------------------------
TEST(NsQuarkdb, FileMd)
{
  MockFileMDSvc file_svc;
  EXPECT_CALL(file_svc, notifyListeners(_)).WillRepeatedly(Return());
  eos::IFileMD::id_t id = 12345;
  eos::FileMD file(id, (eos::IFileMDSvc*)&file_svc);
  file.setName("ns_test_file");
  eos::IContainerMD::id_t cont_id = 9876;
  uint64_t size = 4 * 1024 * 1024;
  eos::IFileMD::ctime_t tnow;
  clock_gettime(CLOCK_REALTIME, &tnow);
  file.setCTime(tnow);
  file.setMTime(tnow);
  file.setSize(size);
  file.setContainerId(cont_id);
  uid_t uid = 123;
  file.setCUid(uid);
  file.setCGid(uid);
  eos::IFileMD::layoutId_t lid = 1243567;
  file.setLayoutId(lid);
  std::string file_cksum = "abcdefgh";
  file.setChecksum(file_cksum.data(), file_cksum.size());
  std::vector<eos::IFileMD::location_t> locations = {2, 23, 3736, 3871, 21, 47, 55};

  for (auto && elem : locations) {
    file.addLocation(elem);
  }

  // Unlink all the even locations
  for (auto && elem : locations) {
    if (elem % 2 == 0) {
      file.unlinkLocation(elem);
    }
  }

  // Serialize
  eos::Buffer buffer;
  file.serialize(buffer);
  // Deserialize
  eos::FileMD rfile(0, (eos::IFileMDSvc*)&file_svc);
  rfile.deserialize(buffer);
  std::string orig_rep, new_rep;
  file.getEnv(orig_rep);
  rfile.getEnv(new_rep);
  ASSERT_EQ(orig_rep, new_rep);
  // Force a checksum corruption and check if it's detected
  uint32_t cksum = 0;
  (void) memcpy(&cksum, buffer.getDataPtr(), sizeof(cksum));
  cksum += 11;
  (void) memcpy(buffer.getDataPtr(), &cksum, sizeof(cksum));
  ASSERT_THROW(rfile.deserialize(buffer), eos::MDException);
}

//------------------------------------------------------------------------------
// Test ContainerMd serialisation, deserialization and checksumming
//------------------------------------------------------------------------------
TEST(NsQuarkdb, ContainerMd)
{
  MockFileMDSvc file_svc;
  MockContainerMDSvc cont_svc;
  EXPECT_CALL(file_svc, notifyListeners(_)).WillRepeatedly(Return());
  EXPECT_CALL(cont_svc, notifyListeners(_, _)).WillRepeatedly(Return());
  eos::IContainerMD::id_t id = 98765;
  eos::ContainerMD cont(id, (eos::IFileMDSvc*)&file_svc,
                        (eos::IContainerMDSvc*)&cont_svc);
  cont.setName("ns_test_cont");
  eos::IContainerMD::id_t parent_id = 34567;
  cont.setParentId(parent_id);
  eos::IContainerMD::ctime_t tnow;
  clock_gettime(CLOCK_REALTIME, &tnow);
  cont.setCTime(tnow);
  cont.setMTime(tnow);
  cont.setTMTime(tnow);
  uid_t uid = 123;
  cont.setCUid(uid);
  cont.setCGid(uid);
  int32_t mode = (1025 << 6);
  cont.setMode(mode);
  std::map<std::string, std::string> xattrs = {
    {"attr_key1", "attr_val1" },
    {"attr_key1", "attr_val2" },
    {"attr_key1", "attr_val3" },
    {"attr_key1", "attr_val4" },
    {"attr_key1", "attr_val5" }
  };

  for (const auto& elem : xattrs) {
    cont.setAttribute(elem.first, elem.second);
  }

  // Serialize
  eos::Buffer buffer;
  cont.serialize(buffer);
  // Deserialize
  eos::ContainerMD rcont(0, (eos::IFileMDSvc*)&file_svc,
                         (eos::IContainerMDSvc*)&cont_svc);
  rcont.deserialize(buffer);
  // Force a checksum corruption and check if it's detected
  uint32_t cksum = 0;
  (void) memcpy(&cksum, buffer.getDataPtr(), sizeof(cksum));
  cksum += 11;
  (void) memcpy(buffer.getDataPtr(), &cksum, sizeof(cksum));
  ASSERT_THROW(rcont.deserialize(buffer), eos::MDException);
}

EOSNSTESTING_END
