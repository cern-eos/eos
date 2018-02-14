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
//! @brief Class to retrieve metadata from the backend - no caching!
//------------------------------------------------------------------------------

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/utils/Buffer.hh"
#include "namespace/utils/DataHelper.hh"
#include "proto/ContainerMd.pb.h"
#include "proto/FileMd.pb.h"

EOSNSNAMESPACE_BEGIN

MDStatus
Serialization::deserializeNoThrow(const Buffer& buffer, eos::ns::FileMdProto &proto)
{
  uint32_t cksum_expected = 0;
  uint32_t obj_size = 0;
  size_t sz = sizeof(cksum_expected);
  const char* ptr = buffer.getDataPtr();
  (void) memcpy(&cksum_expected, ptr, sz);
  ptr += sz;
  (void) memcpy(&obj_size, ptr, sz);
  size_t msg_size = buffer.getSize();
  uint32_t align_size = msg_size - 2 * sz;
  ptr += sz; // now pointing to the serialized object
  uint32_t cksum_computed = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum_computed = DataHelper::finalizeCRC32C(cksum_computed);

  if (cksum_expected != cksum_computed) {
    return MDStatus(EIO, "FileMD object checksum mismatch");
  }

  google::protobuf::io::ArrayInputStream ais(ptr, obj_size);

  if (!proto.ParseFromZeroCopyStream(&ais)) {
    return MDStatus(EIO, "Failed while deserializing FileMD buffer");
  }

  return {};
}

MDStatus
Serialization::deserializeNoThrow(const Buffer& buffer, eos::ns::ContainerMdProto &proto)
{
  uint32_t cksum_expected = 0;
  uint32_t obj_size = 0;
  size_t sz = sizeof(cksum_expected);
  const char* ptr = buffer.getDataPtr();
  (void) memcpy(&cksum_expected, ptr, sz);
  ptr += sz;
  (void) memcpy(&obj_size, ptr, sz);
  size_t msg_size = buffer.getSize();
  uint32_t align_size = msg_size - 2 * sz;
  ptr += sz; // now pointing to the serialized object
  uint32_t cksum_computed = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum_computed = DataHelper::finalizeCRC32C(cksum_computed);

  if (cksum_expected != cksum_computed) {
    return MDStatus(EIO, "ContainerMD object checksum mismatch");
  }

  google::protobuf::io::ArrayInputStream ais(ptr, obj_size);

  if (!proto.ParseFromZeroCopyStream(&ais)) {
    return MDStatus(EIO, "Failed while deserializing ContainerMD buffer");
  }

  return {};
}

MDStatus
Serialization::deserializeNoThrow(const Buffer& buffer, int64_t &ret) {
  char *endptr = NULL;
  ret = strtoll(buffer.getDataPtr(), &endptr, 10);
  if(endptr != buffer.getDataPtr() + buffer.getSize() || ret == LLONG_MIN || ret == LONG_LONG_MAX) {
    return MDStatus(EFAULT, SSTR("Unable to deserialize into int64_t: " << std::string(buffer.getDataPtr(), buffer.getSize())));
  }
  return {};
}



void Serialization::deserializeFile(const Buffer& buffer, eos::ns::FileMdProto &proto) {
  MDStatus status = deserializeNoThrow(buffer, proto);
  status.throwIfNotOk();
}

void Serialization::deserializeContainer(const Buffer& buffer, eos::ns::ContainerMdProto &proto) {
  MDStatus status = deserializeNoThrow(buffer, proto);
  status.throwIfNotOk();
}


EOSNSNAMESPACE_END
