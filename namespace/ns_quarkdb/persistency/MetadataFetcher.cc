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

#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSNSNAMESPACE_BEGIN

int64_t MetadataFetcher::extractInteger(redisReplyPtr &reply) {
  if(!reply) {
    MDException e(EINVAL);
    e.getMessage() << "Received null response from qclient, metadata backend not available?";
    throw e;
  }

  if(reply->type == REDIS_REPLY_NIL) {
    MDException e(ENOENT);
    e.getMessage() << "Not found";
    throw e;
  }

  if(reply->type != REDIS_REPLY_STRING) {
    MDException e(EINVAL);
    e.getMessage() << "Received unexpected reply type when contacting metadata backend";
    throw e;
  }

  return strtoll(reply->str, nullptr, 10);
}

std::string MetadataFetcher::extractString(redisReplyPtr &reply, id_t forId) {
  if(!reply) {
    MDException e(EINVAL);
    e.getMessage() << "Received null response from qclient, metadata backend not available?";
    throw e;
  }

  if(reply->type == REDIS_REPLY_NIL) {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << forId << " not found";
    throw e;
  }

  if(reply->type != REDIS_REPLY_STRING) {
    MDException e(EINVAL);
    e.getMessage() << "Received unexpected reply type when contacting metadata backend";
    throw e;
  }

  return std::string(reply->str, reply->len);
}

// TODO: We should return std::future<eos::ns::ContainerMDProto> here...
// But std::future has no continuations. :( Find a solution.
eos::ns::ContainerMdProto MetadataFetcher::getContainerFromId(qclient::QClient &qcl, id_t id) {
  std::string blob;

  try {
    std::string sid = SSTR(id);
    qclient::QHash bucket_map(qcl, ContainerMDSvc::getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  if (blob.empty()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__ << " Container #" << id << " not found";
    throw e;
  }

  eos::Buffer ebuff;
  ebuff.putData(blob.c_str(), blob.length());

  eos::ns::ContainerMdProto proto;
  Serialization::deserializeContainer(ebuff, proto);
  return proto;
}

eos::ns::FileMdProto MetadataFetcher::getFileFromId(qclient::QClient &qcl, id_t id) {
  std::string blob;

  try {
    std::string sid = SSTR(id);
    qclient::QHash bucket_map(qcl, FileMDSvc::getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  if (blob.empty()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__ << " File #" << id << " not found";
    throw e;
  }

  eos::Buffer ebuff;
  ebuff.putData(blob.c_str(), blob.length());

  eos::ns::FileMdProto proto;
  Serialization::deserializeFile(ebuff, proto);
  return proto;
}

std::string MetadataFetcher::keySubContainers(id_t id) {
  return SSTR(id << constants::sMapDirsSuffix);
}

std::string MetadataFetcher::keySubFiles(id_t id) {
  return SSTR(id << constants::sMapFilesSuffix);
}

id_t MetadataFetcher::getContainerIDFromName(qclient::QClient &qcl, const std::string &name, id_t parentID) {
  redisReplyPtr reply = qcl.exec(
    "HGET",
    keySubContainers(parentID),
    name
  ).get();

  if(!reply) {
    MDException e(EINVAL);
    e.getMessage() << "Received null response from qclient when retrieving child ID with name " << name << " of #" << parentID << ", metadata backend not available?";
    throw e;
  }

  if(reply->type == REDIS_REPLY_NIL) {
    MDException e(ENOENT);
    e.getMessage() << "Not found: Child container with name " << name << " of #" << parentID;
    throw e;
  }

  if(reply->type != REDIS_REPLY_STRING) {
    MDException e(EINVAL);
    e.getMessage() << "Received unexpected reply type when contacting metadata backend to retrieve child container with name " << name << " of #" << parentID;
    throw e;
  }

  return strtoll(reply->str, nullptr, 10);
}

void MetadataFetcher::getFilesInContainer(qclient::QClient &qcl, id_t container, IContainerMD::FileMap &fileMap) {
  qclient::QHash hash(qcl, keySubFiles(container));

  for(auto it = hash.getIterator(500000); it.valid(); it.next()) {
    fileMap.insert(std::make_pair(it.getKey(), std::stoull(it.getValue())));
  }
}

void MetadataFetcher::getSubContainers(qclient::QClient &qcl, id_t container, IContainerMD::ContainerMap &containerMap) {
  qclient::QHash hash(qcl, keySubContainers(container));

  for(auto it = hash.getIterator(500000); it.valid(); it.next()) {
    containerMap.insert(std::make_pair(it.getKey(), std::stoull(it.getValue())));
  }
}

EOSNSNAMESPACE_END
