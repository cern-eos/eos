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
#include "Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
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
  eos::IContainerMD::id_t cont_id = 9876;
  uint64_t size = 4 * 1024 * 1024;
  eos::IFileMD::ctime_t tnow;
  clock_gettime(CLOCK_REALTIME, &tnow);
  file.setCTime(tnow);
  file.setMTime(tnow);
  file.setSize(size);
  file.setContainerId(cont_id);
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

  eos::Buffer buffer;
  file.serialize(buffer);
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

EOSNSTESTING_END
