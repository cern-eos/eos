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
  return { "LHSET", constants::sContainerKey, sid, hint, blob };
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
  return { "LHSET", constants::sFileKey, sid, hint, blob };
}

//------------------------------------------------------------------------------
//! Read container protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::readContainerProto(ContainerIdentifier id)
{
  // TODO(gbitzes): Pass locality hint when available.
  return { "LHGET", constants::sContainerKey, SSTR(id.getUnderlyingUInt64()) };
}

//------------------------------------------------------------------------------
//! Read file protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::readFileProto(FileIdentifier id)
{
  // TODO(gbitzes): Pass locality hint when available.
  return { "LHGET", constants::sFileKey, SSTR(id.getUnderlyingUInt64()) };
}

//------------------------------------------------------------------------------
//! Delete container protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::deleteContainerProto(ContainerIdentifier id)
{
  return { "LHDEL", constants::sContainerKey, SSTR(id.getUnderlyingUInt64()) };
}

//------------------------------------------------------------------------------
//! Delete file protobuf metadata.
//------------------------------------------------------------------------------
RedisRequest
RequestBuilder::deleteFileProto(FileIdentifier id)
{
  return { "LHDEL", constants::sFileKey, SSTR(id.getUnderlyingUInt64()) };
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

EOSNSNAMESPACE_END
