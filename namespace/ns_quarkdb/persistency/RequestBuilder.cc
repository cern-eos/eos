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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Single class which builds redis requests towards the backend.
//------------------------------------------------------------------------------

#include "RequestBuilder.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/utils/StringConvertion.hh"
#include "namespace/utils/Buffer.hh"

EOSNSNAMESPACE_BEGIN

std::uint64_t RequestBuilder::sNumContBuckets = 128 * 1024;
std::uint64_t RequestBuilder::sNumFileBuckets = 1024 * 1024;

//----------------------------------------------------------------------------
//! Write container protobuf metadata.
//----------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeContainerProto(IContainerMD *obj)
{
  eos::Buffer ebuff;
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());
  std::string sid = stringify(obj->getId());

  return { "HSET", RequestBuilder::getContainerBucketKey(obj->getId()), sid, buffer };
}

//----------------------------------------------------------------------------
//! Write file protobuf metadata.
//----------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeFileProto(IFileMD *obj)
{
  eos::Buffer ebuff;
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());
  std::string sid = stringify(obj->getId());
  return { "HSET", RequestBuilder::getFileBucketKey(obj->getId()), sid, buffer };
}

//------------------------------------------------------------------------------
// Get container bucket
//------------------------------------------------------------------------------
std::string
RequestBuilder::getContainerBucketKey(IContainerMD::id_t id)
{
  id = id & (sNumContBuckets - 1);
  std::string bucket_key = stringify(id);
  bucket_key += constants::sContKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
// Get file bucket
//------------------------------------------------------------------------------
std::string
RequestBuilder::getFileBucketKey(IContainerMD::id_t id)
{
  id = id & (sNumFileBuckets - 1);
  std::string bucket_key = stringify(id);
  bucket_key += constants::sFileKeySuffix;
  return bucket_key;
}


EOSNSNAMESPACE_END
