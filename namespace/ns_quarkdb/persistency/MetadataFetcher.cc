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

class FileMdFetcher : public qclient::QCallback {
public:
  FileMdFetcher() {}

  std::future<eos::ns::FileMdProto> initialize(qclient::QClient &qcl, id_t d) {
    std::future<eos::ns::FileMdProto> fut = promise.get_future();
    id = d;

    // There's a particularly evil race condition here: From the point we call
    // execCB and onwards, we must assume that the callback has arrived,
    // and *this has been destroyed already.
    // Not safe to access member variables after execCB, so we fetch the future
    // beforehand.

    qcl.execCB(
      this,
      "HGET", FileMDSvc::getBucketKey(id), SSTR(id)
    );

    // fut is a stack object, safe to access.
    return fut;
  }

  virtual void handleResponse(redisReplyPtr &&reply) {
    if(!reply) {
      return set_exception(EFAULT, "QuarkDB backend not available!");
    }

    if(reply->type == REDIS_REPLY_NIL || (reply->type == REDIS_REPLY_STRING && reply->len == 0) ) {
      return set_exception(ENOENT, "Empty response");
    }

    if(reply->type != REDIS_REPLY_STRING) {
      return set_exception(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
    }

    eos::Buffer ebuff;
    ebuff.putData(reply->str, reply->len);

    eos::ns::FileMdProto proto;

    Serialization::Status status = Serialization::deserializeFileNoThrow(ebuff, proto);
    if(!status.ok()) {
      return set_exception(EIO, status.getError());
    }

    // If we've made it this far, it's a success
    return set_value(proto);
  }

private:

  void set_value(const eos::ns::FileMdProto &proto) {
    promise.set_value(proto);
    delete this; // harakiri
  }

  void set_exception(int err, const std::string &msg) {
    promise.set_exception(
      make_mdexception(err, "Error while fetching FileMD #" << id << " protobuf from QDB: " << msg)
    );
    delete this; // harakiri
  }

  id_t id;
  std::promise<eos::ns::FileMdProto> promise;
};

class ContainerMdFetcher : public qclient::QCallback {
public:
  ContainerMdFetcher() {}

  std::future<eos::ns::ContainerMdProto> initialize(qclient::QClient &qcl, id_t d) {
    std::future<eos::ns::ContainerMdProto> fut = promise.get_future();
    id = d;

    // There's a particularly evil race condition here: From the point we call
    // execCB and onwards, we must assume that the callback has arrived,
    // and *this has been destroyed already.
    // Not safe to access member variables after execCB, so we fetch the future
    // beforehand.

    qcl.execCB(
      this,
      "HGET", ContainerMDSvc::getBucketKey(id), SSTR(id)
    );

    // fut is a stack object, safe to access.
    return fut;
  }

  virtual void handleResponse(redisReplyPtr &&reply) {
    if(!reply) {
      return set_exception(EFAULT, "QuarkDB backend not available!");
    }

    if(reply->type == REDIS_REPLY_NIL || (reply->type == REDIS_REPLY_STRING && reply->len == 0) ) {
      return set_exception(ENOENT, "Empty response");
    }

    if(reply->type != REDIS_REPLY_STRING) {
      return set_exception(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
    }

    eos::Buffer ebuff;
    ebuff.putData(reply->str, reply->len);

    eos::ns::ContainerMdProto proto;

    Serialization::Status status = Serialization::deserializeContainerNoThrow(ebuff, proto);
    if(!status.ok()) {
      return set_exception(EIO, status.getError());
    }

    // If we've made it this far, it's a success
    return set_value(proto);
  }

private:

  void set_value(const eos::ns::ContainerMdProto &proto) {
    promise.set_value(proto);
    delete this; // harakiri
  }

  void set_exception(int err, const std::string &msg) {
    promise.set_exception(
      make_mdexception(err, "Error while fetching ContainerMD #" << id << " protobuf from QDB: " << msg)
    );
    delete this; // harakiri
  }

  id_t id;
  std::promise<eos::ns::ContainerMdProto> promise;
};

std::future<eos::ns::FileMdProto> MetadataFetcher::getFileFromId(qclient::QClient &qcl, id_t id) {
  FileMdFetcher *fetcher = new FileMdFetcher();
  return fetcher->initialize(qcl, id);
}

std::future<eos::ns::ContainerMdProto> MetadataFetcher::getContainerFromId(qclient::QClient &qcl, id_t id) {
  ContainerMdFetcher *fetcher = new ContainerMdFetcher();
  return fetcher->initialize(qcl, id);
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
