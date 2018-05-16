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

//------------------------------------------------------------------------------
//! Write container protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeContainerProto(IContainerMD *obj)
{
  eos::Buffer ebuff;
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());
  return writeContainerProto(ContainerIdentifier(obj->getId()), obj->getLocalityHint(), buffer);
}

//------------------------------------------------------------------------------
//! Write container protobuf metadata - low level API.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeContainerProto(ContainerIdentifier id, const std::string &hint, const std::string &blob)
{
  std::string sid = stringify(id.getUnderlyingUInt64());
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHSET-AND-DEL-FALLBACK", constants::sContainerKey, sid, hint, blob, RequestBuilder::getContainerBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Write file protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeFileProto(IFileMD *obj)
{
  eos::Buffer ebuff;
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());

  return writeFileProto(FileIdentifier(obj->getId()), obj->getLocalityHint(), buffer);
}

//------------------------------------------------------------------------------
//! Write file protobuf metadata - low level API.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::writeFileProto(FileIdentifier id, const std::string &hint, const std::string &blob)
{
  std::string sid = stringify(id.getUnderlyingUInt64());
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHSET-AND-DEL-FALLBACK", constants::sFileKey, sid, hint, blob, RequestBuilder::getFileBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Read container protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::readContainerProto(ContainerIdentifier id)
{
  // TODO(gbitzes): Pass locality hint when available.
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHGET-WITH-FALLBACK", constants::sContainerKey, SSTR(id.getUnderlyingUInt64()), RequestBuilder::getContainerBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Read file protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::readFileProto(FileIdentifier id)
{
  // TODO(gbitzes): Pass locality hint when available.
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHGET-WITH-FALLBACK", constants::sFileKey, SSTR(id.getUnderlyingUInt64()), RequestBuilder::getFileBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Delete container protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::deleteContainerProto(ContainerIdentifier id)
{
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHDEL-WITH-FALLBACK", constants::sContainerKey, SSTR(id.getUnderlyingUInt64()), RequestBuilder::getContainerBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Delete file protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::deleteFileProto(FileIdentifier id)
{
  // TODO(gbitzes): Remove compatibility hack eventually.
  return { "LHDEL-WITH-FALLBACK", constants::sFileKey, SSTR(id.getUnderlyingUInt64()), RequestBuilder::getFileBucketKey(id) };
}

//------------------------------------------------------------------------------
//! Calculate number of containers.
//------------------------------------------------------------------------------
RedisRequest RequestBuilder::getNumberOfContainers()
{
  return { "LHLEN", constants::sContainerKey };
}

//------------------------------------------------------------------------------
//! Calculate number of files.
//------------------------------------------------------------------------------
RedisRequest RequestBuilder::getNumberOfFiles()
{
  return { "LHLEN", constants::sFileKey };
}

//------------------------------------------------------------------------------
// Get container bucket
//------------------------------------------------------------------------------
std::string
RequestBuilder::getContainerBucketKey(ContainerIdentifier identifier)
{
  uint64_t id = identifier.getUnderlyingUInt64();
  id = id & (sNumContBuckets - 1);
  std::string bucket_key = stringify(identifier.getUnderlyingUInt64());
  bucket_key += constants::sContKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
// Get file bucket
//! Calculate number of files.
//------------------------------------------------------------------------------
std::string
RequestBuilder::getFileBucketKey(FileIdentifier identifier)
{
  uint64_t id = identifier.getUnderlyingUInt64();
  id = id & (sNumFileBuckets - 1);
  std::string bucket_key = stringify(identifier.getUnderlyingUInt64());
  bucket_key += constants::sFileKeySuffix;
  return bucket_key;
}

EOSNSNAMESPACE_END
