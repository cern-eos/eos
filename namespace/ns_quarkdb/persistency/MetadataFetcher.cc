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

MDStatus ensureStringReply(redisReplyPtr &reply) {
  if(!reply) {
    return MDStatus(EFAULT, "QuarkDB backend not available!");
  }

  if(reply->type == REDIS_REPLY_NIL || (reply->type == REDIS_REPLY_STRING && reply->len == 0) ) {
    return MDStatus(ENOENT, "Empty response");
  }

  if(reply->type != REDIS_REPLY_STRING) {
    return MDStatus(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
  }

  return MDStatus();
}

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
    MDStatus status = ensureStringReply(reply);
    if(!status.ok()) {
      return set_exception(status);
    }

    eos::ns::FileMdProto proto;
    status = Serialization::deserialize(reply->str, reply->len, proto);
    if(!status.ok()) {
      return set_exception(status);
    }

    // If we've made it this far, it's a success
    return set_value(proto);
  }

private:

  void set_value(const eos::ns::FileMdProto &proto) {
    promise.set_value(proto);
    delete this; // harakiri
  }

  void set_exception(const MDStatus &status) {
    promise.set_exception(
      make_mdexception(status.getErrno(), "Error while fetching FileMD #" << id << " protobuf from QDB: " << status.getError())
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
    MDStatus status = ensureStringReply(reply);
    if(!status.ok()) {
      return set_exception(status);
    }

    eos::ns::ContainerMdProto proto;
    status = Serialization::deserialize(reply->str, reply->len, proto);
    if(!status.ok()) {
      return set_exception(status);
    }

    // If we've made it this far, it's a success
    return set_value(proto);
  }

private:

  void set_value(const eos::ns::ContainerMdProto &proto) {
    promise.set_value(proto);
    delete this; // harakiri
  }

  void set_exception(const MDStatus &status) {
    promise.set_exception(
      make_mdexception(status.getErrno(), "Error while fetching ContainerMD #" << id << " protobuf from QDB: " << status.getError())
    );
    delete this; // harakiri
  }

  id_t id;
  std::promise<eos::ns::ContainerMdProto> promise;
};

struct MapFetcherFileTrait {
  static std::string getKey(id_t id) {
    return SSTR(id << constants::sMapFilesSuffix);
  }

  using ContainerType = IContainerMD::FileMap;
};

struct MapFetcherContainerTrait {
  static std::string getKey(id_t id) {
    return SSTR(id << constants::sMapDirsSuffix);
  }

  using ContainerType = IContainerMD::ContainerMap;
};


// Fetch maps (ContainerMap, FileMap) of a particular container.
template<typename Trait>
class MapFetcher : public qclient::QCallback {
public:
  using ContainerType = typename Trait::ContainerType;
  static constexpr size_t kCount = 250000;

  MapFetcher() {}

  std::future<ContainerType> initialize(qclient::QClient &qcli, id_t trg) {
    qcl = &qcli;

    contents.set_deleted_key("");
    contents.set_empty_key("##_EMPTY_##");

    std::future<ContainerType> fut = promise.get_future();
    target = trg;

    // There's a particularly evil race condition here: From the point we call
    // execCB and onwards, we must assume that the callback has arrived,
    // and *this has been destroyed already.
    // Not safe to access member variables after execCB, so we fetch the future
    // beforehand.

    qcl->execCB(
      this,
      "HSCAN", Trait::getKey(target), "0", "COUNT", SSTR(kCount)
    );

    // fut is a stack object, safe to access.
    return fut;
  }

  virtual void handleResponse(redisReplyPtr &&reply) override {
    if(!reply) {
      return set_exception(EFAULT, "QuarkDB backend not available!");
    }

    if(reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 || (reply->element[0]->type != REDIS_REPLY_STRING)
       || (reply->element[1]->type != REDIS_REPLY_ARRAY) || ( (reply->element[1]->elements % 2) != 0)) {
      return set_exception(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
    }

    std::string cursor = std::string(reply->element[0]->str, reply->element[0]->len);

    for(size_t i = 0; i < reply->element[1]->elements; i += 2) {
      redisReply *element = reply->element[1]->element[i];
      if(element->type != REDIS_REPLY_STRING) {
        return set_exception(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
      }

      std::string filename = std::string(element->str, element->len);
      element = reply->element[1]->element[i+1];

      if(element->type != REDIS_REPLY_STRING) {
        return set_exception(EFAULT, SSTR("Received unexpected response: " << qclient::describeRedisReply(reply)));
      }

      int64_t value;
      MDStatus st = Serialization::deserialize(element->str, element->len, value);
      if(!st.ok()) {
        return set_exception(st);
      }

      contents[filename] = value;
    }

    // Fire off next request?
    if(cursor == "0") {
      promise.set_value(std::move(contents));
      delete this;
      return;
    }

    qcl->execCB(
      this,
      "HSCAN", Trait::getKey(target), cursor, "COUNT", SSTR(kCount)
    );
  }

private:

  void set_exception(const MDStatus &status) {
    return set_exception(status.getErrno(), status.getError());
  }

  void set_exception(int err, const std::string &msg) {
    promise.set_exception(
      make_mdexception(err, SSTR("Error while fetching file/container map for container #" << target << " from QDB: " << msg))
    );
    delete this; // harakiri
  }

  qclient::QClient *qcl;
  id_t target;
  ContainerType contents;
  std::promise<ContainerType> promise;
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

class IDFromNameFetcher : public qclient::QCallback {
public:
  IDFromNameFetcher(bool cont) : container(cont) {}

  std::future<id_t> initialize(qclient::QClient &qcl, id_t prnt, const std::string &nm) {
    parentID = prnt;
    name = nm;

    std::future<id_t> fut = promise.get_future();

    if(container) {
      qcl.execCB(this, "HGET", SSTR(parentID << constants::sMapDirsSuffix), name);
    }
    else {
      qcl.execCB(this, "HGET", SSTR(parentID << constants::sMapFilesSuffix), name);
    }

    return fut;
  }

  virtual void handleResponse(redisReplyPtr &&reply) override {
    MDStatus status = ensureStringReply(reply);
    if(!status.ok()) {
      return set_exception(status);
    }

    int64_t retval;
    status = Serialization::deserialize(reply->str, reply->len, retval);
    if(!status.ok()) {
      return set_exception(status);
    }

    // If we've made it this far, it's a success
    return set_value(retval);
  }

private:
  std::promise<id_t> promise;
  bool container;

  id_t parentID;
  std::string name;

  void set_value(const int64_t retval) {
    promise.set_value(retval);
    delete this; // harakiri
  }

  void set_exception(const MDStatus &status) {
    promise.set_exception(
      make_mdexception(status.getErrno(), "Error while fetching Container/File ID out of parent id " << parentID << " and name " << name << ": " << status.getError())
    );
    delete this; // harakiri
  }
};

std::future<id_t> MetadataFetcher::getContainerIDFromName(qclient::QClient &qcl, id_t parentID, const std::string &name) {
  IDFromNameFetcher *fetcher = new IDFromNameFetcher(true);
  return fetcher->initialize(qcl, parentID, name);
}

std::future<id_t> MetadataFetcher::getFileIDFromName(qclient::QClient &qcl, id_t parentID, const std::string &name) {
  IDFromNameFetcher *fetcher = new IDFromNameFetcher(false);
  return fetcher->initialize(qcl, parentID, name);
}

std::future<IContainerMD::FileMap> MetadataFetcher::getFilesInContainer(qclient::QClient &qcl, id_t container) {
  MapFetcher<MapFetcherFileTrait> *fetcher = new MapFetcher<MapFetcherFileTrait>();
  return fetcher->initialize(qcl, container);
}

std::future<IContainerMD::ContainerMap> MetadataFetcher::getSubContainers(qclient::QClient &qcl, id_t container) {
  MapFetcher<MapFetcherContainerTrait> *fetcher = new MapFetcher<MapFetcherContainerTrait>();
  return fetcher->initialize(qcl, container);
}

EOSNSNAMESPACE_END
