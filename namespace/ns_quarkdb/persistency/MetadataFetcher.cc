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

std::string describeRedisReply(redisReplyPtr &reply) {
  std::stringstream ss;

  if(reply->type == REDIS_REPLY_NIL) {
    return "(nil)";
  }
  else if(reply->type == REDIS_REPLY_STRING) {
    return SSTR("\"" << std::string(reply->str, reply->len) << "\"");
  }
  else {
    return SSTR("could not determine response type, fix me pls");
  }

  // .... TODO! And move inside QClient, as these utilities are useful to everyone!
}


// This class should become the one and only codepath to retrieve a file
// protomd from QuarkDB! (and be backed by individual tests targeting just this
// class)
class FileMdFetcher : public qclient::QCallback {
public:
  FileMdFetcher(qclient::QClient &qcl, id_t d) : id(d) {
    qcl.execCB(
      this,
      "HGET", FileMDSvc::getBucketKey(id), SSTR(id)
    );
  }

  // You can only call this _once_ !
  std::future<eos::ns::FileMdProto> getFuture() {
    return promise.get_future();
  }

  virtual void handleResponse(redisReplyPtr &&reply) {
    if(!reply) {
      return set_exception(MAKE_MDEXCEPTION(EFAULT, "QuarkDB backend not available!"));
    }

    if(reply->type == REDIS_REPLY_NIL || (reply->type == REDIS_REPLY_STRING && reply->len == 0) ) {
      return set_exception(
        MAKE_MDEXCEPTION(ENOENT, "File with id #" << id << " not found")
      );
    }

    if(reply->type != REDIS_REPLY_STRING) {
      return set_exception(
        MAKE_MDEXCEPTION(EFAULT, "Received unexpected response type: " << describeRedisReply(reply))
      );
    }

    eos::Buffer ebuff;
    ebuff.putData(reply->str, reply->len);

    eos::ns::FileMdProto proto;

    std::exception_ptr exc = Serialization::deserializeFileNoThrow(ebuff, proto);
    if(exc) {
      return set_exception(exc);
    }

    // If we've made it this far, it's a success!
    return set_value(proto);
  }

private:

  void set_value(const eos::ns::FileMdProto &proto) {
    promise.set_value(proto);
    delete this; // harakiri
  }

  void set_exception(std::exception_ptr exc) {
    promise.set_exception(exc);
    delete this; // harakiri
  }

  id_t id;
  std::promise<eos::ns::FileMdProto> promise;
};

std::future<eos::ns::FileMdProto> MetadataFetcher::getFileFromId(qclient::QClient &qcl, id_t id) {
  FileMdFetcher *fetcher = new FileMdFetcher(qcl, id);
  return fetcher->getFuture();
}

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
