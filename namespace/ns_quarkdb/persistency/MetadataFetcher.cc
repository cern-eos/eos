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
//! @brief Class to retrieve metadata from the backend - no caching!
//------------------------------------------------------------------------------

#include <functional>
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/utils/PathProcessor.hh"
#include "qclient/QClient.hh"

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " \
  << #message << " = " << message << std::endl

using redisReplyPtr = qclient::redisReplyPtr;
using std::placeholders::_1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper method to check that the redis reply is a string reply
//!
//! @param reply redis reply
//------------------------------------------------------------------------------
MDStatus ensureStringReply(redisReplyPtr& reply)
{
  if (!reply) {
    return MDStatus(EFAULT, "QuarkDB backend not available!");
  }

  if ((reply->type == REDIS_REPLY_NIL) ||
      (reply->type == REDIS_REPLY_STRING && reply->len == 0)) {
    return MDStatus(ENOENT, "Empty response");
  }

  if (reply->type != REDIS_REPLY_STRING) {
    return MDStatus(EFAULT,
                    SSTR("Received unexpected response, was expecting string: "
                         << qclient::describeRedisReply(reply)));
  }

  return MDStatus();
}

//------------------------------------------------------------------------------
// Helper method to check that a redis reply is 0/1
//------------------------------------------------------------------------------
MDStatus ensureBoolReply(redisReplyPtr& reply)
{
  if (!reply) {
    return MDStatus(EFAULT, "QuarkDB backend not available!");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    return MDStatus(EFAULT,
                    SSTR("Received unexpected response, was expecting integer: "
                         << qclient::describeRedisReply(reply)));
  }

  if (reply->integer != 0 && reply->integer != 1) {
    return MDStatus(EFAULT,
                    SSTR("Received unexpected integer, was expecting {0,1}: "
                         << qclient::describeRedisReply(reply)));
  }

  return MDStatus();
}

//------------------------------------------------------------------------------
// Helper method to check that a redis reply is uint64_t
//------------------------------------------------------------------------------
MDStatus ensureUInt64Reply(redisReplyPtr& reply)
{
  if (!reply) {
    return MDStatus(EFAULT, "QuarkDB backend not available!");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    return MDStatus(EFAULT,
                    SSTR("Received unexpected response, was expecting integer: "
                         << qclient::describeRedisReply(reply)));
  }

  if (reply->integer < 0) {
    return MDStatus(EFAULT,
                    SSTR("Received unexpected value, was expecting a uint64_t: "
                         << qclient::describeRedisReply(reply)));
  }

  return MDStatus();
}

//------------------------------------------------------------------------------
//! Struct MapFetcherFileTrait
//------------------------------------------------------------------------------
struct MapFetcherFileTrait {
  static std::string getKey(IContainerMD::id_t id)
  {
    return SSTR(id << constants::sMapFilesSuffix);
  }

  using ContainerType = IContainerMD::FileMap;
};

//------------------------------------------------------------------------------
//! Struct MapFetcherContainerTrait
//------------------------------------------------------------------------------
struct MapFetcherContainerTrait {
  static std::string getKey(IFileMD::id_t id)
  {
    return SSTR(id << constants::sMapDirsSuffix);
  }

  using ContainerType = IContainerMD::ContainerMap;
};


//------------------------------------------------------------------------------
//! Class MapFetcher - fetches maps (ContainerMap, FileMap) of a particular
//! container.
//------------------------------------------------------------------------------
template<typename Trait>
class MapFetcher : public qclient::QCallback
{
public:
  using ContainerType = typename Trait::ContainerType;
  static constexpr size_t kCount = 250000;
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MapFetcher()
  {
    mContents.set_deleted_key("");
    mContents.set_empty_key("##_EMPTY_##");
  }

  //----------------------------------------------------------------------------
  //! Initialize
  //!
  //! @param qcl qclient object
  //! @param trg target container id
  //----------------------------------------------------------------------------
  folly::Future<ContainerType> initialize(qclient::QClient& qcl,
                                          ContainerIdentifier trg)
  {
    mQcl = &qcl;
    mTarget = trg;
    folly::Future<ContainerType> fut = mPromise.getFuture();
    // There's a particularly evil race condition here: From the point we call
    // execCB and onwards, we must assume that the callback has arrived,
    // and *this has been destroyed already.
    // Not safe to access member variables after execCB, so we fetch the future
    // beforehand.
    mQcl->execCB(this, "HSCAN", Trait::getKey(mTarget.getUnderlyingUInt64()), "0",
                 "COUNT", SSTR(kCount));
    // fut is a stack object, safe to access.
    return fut;
  }

  //----------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param reply holds a redis reply object
  //----------------------------------------------------------------------------
  virtual void handleResponse(redisReplyPtr&& reply) override
  {
    if (!reply) {
      return set_exception(EFAULT, "QuarkDB backend not available!");
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
        (reply->element[0]->type != REDIS_REPLY_STRING) ||
        (reply->element[1]->type != REDIS_REPLY_ARRAY) ||
        ((reply->element[1]->elements % 2) != 0)) {
      return set_exception(EFAULT, SSTR("Received unexpected response: "
                                        << qclient::describeRedisReply(reply)));
    }

    std::string cursor = std::string(reply->element[0]->str,
                                     reply->element[0]->len);

    for (size_t i = 0; i < reply->element[1]->elements; i += 2) {
      redisReply* element = reply->element[1]->element[i];

      if (element->type != REDIS_REPLY_STRING) {
        return set_exception(EFAULT, SSTR("Received unexpected response: "
                                          << qclient::describeRedisReply(reply)));
      }

      std::string filename = std::string(element->str, element->len);
      element = reply->element[1]->element[i + 1];

      if (element->type != REDIS_REPLY_STRING) {
        return set_exception(EFAULT, SSTR("Received unexpected response: "
                                          << qclient::describeRedisReply(reply)));
      }

      int64_t value;
      MDStatus st = Serialization::deserialize(element->str, element->len, value);

      if (!st.ok()) {
        return set_exception(st);
      }

      mContents.insert(std::make_pair(filename, value));
    }

    // Fire off next request?
    if (cursor == "0") {
      mPromise.setValue(std::move(mContents));
      delete this;
      return;
    }

    mQcl->execCB(this, "HSCAN", Trait::getKey(mTarget.getUnderlyingUInt64()),
                 cursor, "COUNT",
                 SSTR(kCount));
  }

private:
  //----------------------------------------------------------------------------
  //! Return exception by passing it to the promise
  //!
  //! @param status error return status
  //----------------------------------------------------------------------------
  void set_exception(const MDStatus& status)
  {
    return set_exception(status.getErrno(), status.getError());
  }

  //----------------------------------------------------------------------------
  //! Return exception by passing it to the promise
  //!
  //! @param status error return status
  //----------------------------------------------------------------------------
  void set_exception(int err, const std::string& msg)
  {
    mPromise.setException(
      make_mdexception(err, SSTR("Error while fetching file/container map for "
                                 "container #" << mTarget.getUnderlyingUInt64() << " from QDB: "
                                 << msg)));
    delete this; // harakiri
  }

  qclient::QClient* mQcl;
  ContainerIdentifier mTarget;
  ContainerType mContents;
  folly::Promise<ContainerType> mPromise;
};

//------------------------------------------------------------------------------
// Parse FileMDProto from a redis response, throw on error.
//------------------------------------------------------------------------------
static eos::ns::FileMdProto
parseFileMdProtoResponse(redisReplyPtr reply, FileIdentifier id)
{
  ensureStringReply(reply).throwIfNotOk(SSTR("Error while fetching FileMD #"
                                        << id.getUnderlyingUInt64()
                                        << " protobuf from QDB: "));
  eos::ns::FileMdProto proto;
  Serialization::deserialize(reply->str, reply->len, proto)
  .throwIfNotOk(SSTR("Error while deserializing FileMD #"
                     << id.getUnderlyingUInt64()
                     << " protobuf: "));
  return proto;
}

//------------------------------------------------------------------------------
// Fetch file metadata info for current id
//------------------------------------------------------------------------------
folly::Future<eos::ns::FileMdProto>
MetadataFetcher::getFileFromId(qclient::QClient& qcl, FileIdentifier id)
{
  return qcl.follyExec(RequestBuilder::readFileProto(id))
         .thenValue(std::bind(parseFileMdProtoResponse, _1, id));
}

//----------------------------------------------------------------------------
// Fetch file metadata info for current id
//------------------------------------------------------------------------------
static bool
checkFileMdProtoExistence(redisReplyPtr reply, FileIdentifier id)
{
  MDStatus st = ensureStringReply(reply);

  if (st.getErrno() == ENOENT) {
    return false;
  }

  st.throwIfNotOk(SSTR("Error while fetching FileMD #"
                       << id.getUnderlyingUInt64()
                       << " protobuf from QDB: "));
  return true;
}

//------------------------------------------------------------------------------
// Fetch container metadata info for current id
//------------------------------------------------------------------------------
static bool
checkContainerMdProtoExistence(redisReplyPtr reply, ContainerIdentifier id)
{
  MDStatus st = ensureStringReply(reply);

  if (st.getErrno() == ENOENT) {
    return false;
  }

  st.throwIfNotOk(SSTR("Error while fetching ContainerMD #"
                       << id.getUnderlyingUInt64()
                       << " protobuf from QDB: "));
  return true;
}

//----------------------------------------------------------------------------
// Check if given container id exists on the namespace
//----------------------------------------------------------------------------
folly::Future<bool>
MetadataFetcher::doesContainerMdExist(qclient::QClient& qcl,
                                      ContainerIdentifier id)
{
  return qcl.follyExec(RequestBuilder::readContainerProto(id))
         .thenValue(std::bind(checkContainerMdProtoExistence, _1, id));
}

//----------------------------------------------------------------------------
// Check if given file id exists on the namespace
//----------------------------------------------------------------------------
folly::Future<bool>
MetadataFetcher::doesFileMdExist(qclient::QClient& qcl, FileIdentifier id)
{
  return qcl.follyExec(RequestBuilder::readFileProto(id))
         .thenValue(std::bind(checkFileMdProtoExistence, _1, id));
}

//------------------------------------------------------------------------------
// Parse ContainerMdProto from a redis response, throw on error.
//------------------------------------------------------------------------------
static eos::ns::ContainerMdProto
parseContainerMdProtoResponse(redisReplyPtr reply, ContainerIdentifier id)
{
  ensureStringReply(reply).throwIfNotOk(SSTR("Error while fetching ContainerMD #"
                                        << id.getUnderlyingUInt64()
                                        << " protobuf from QDB: "));
  eos::ns::ContainerMdProto proto;
  Serialization::deserialize(reply->str, reply->len, proto)
  .throwIfNotOk(SSTR("Error while deserializing ContainerMd #"
                     << id.getUnderlyingUInt64()
                     << " protobuf: "));
  return proto;
}

//------------------------------------------------------------------------------
// Fetch container metadata info for current id
//------------------------------------------------------------------------------
folly::Future<eos::ns::ContainerMdProto>
MetadataFetcher::getContainerFromId(qclient::QClient& qcl,
                                    ContainerIdentifier id)
{
  return qcl.follyExec(RequestBuilder::readContainerProto(id))
         .thenValue(std::bind(parseContainerMdProtoResponse, _1, id));
}

//------------------------------------------------------------------------------
// Class MetadataFetcher
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Construct hmap key of subcontainers in container
//------------------------------------------------------------------------------
std::string MetadataFetcher::keySubContainers(IContainerMD::id_t id)
{
  return SSTR(id << constants::sMapDirsSuffix);
}

//------------------------------------------------------------------------------
// Construct hmap key of files in container
//------------------------------------------------------------------------------
std::string MetadataFetcher::keySubFiles(IContainerMD::id_t id)
{
  return SSTR(id << constants::sMapFilesSuffix);
}

//------------------------------------------------------------------------------
// Fetch all files for current id
//------------------------------------------------------------------------------
folly::Future<IContainerMD::FileMap>
MetadataFetcher::getFileMap(qclient::QClient& qcl,
                            ContainerIdentifier container)
{
  MapFetcher<MapFetcherFileTrait>* fetcher = new
  MapFetcher<MapFetcherFileTrait>();
  return fetcher->initialize(qcl, container);
}

//------------------------------------------------------------------------------
// Fetch all file metadata within the given container.
//------------------------------------------------------------------------------
folly::Future<std::vector<folly::Future<eos::ns::FileMdProto>>>
MetadataFetcher::getFileMDsInContainer(qclient::QClient& qcl,
                                       ContainerIdentifier container, folly::Executor* executor)
{
  folly::Future<IContainerMD::FileMap> filemap = getFileMap(qcl, container);
  return filemap.via(executor)
         .thenValue(std::bind(MetadataFetcher::getFilesFromFilemapV, std::ref(qcl), _1));
}

//----------------------------------------------------------------------------
// Fetch all container metadata within the given container.
//----------------------------------------------------------------------------
folly::Future<std::vector<folly::Future<eos::ns::ContainerMdProto>>>
MetadataFetcher::getContainerMDsInContainer(qclient::QClient& qcl,
    ContainerIdentifier container, folly::Executor* executor)
{
  folly::Future<IContainerMD::ContainerMap> containerMap =
    getContainerMap(qcl, container);
  return containerMap.via(executor)
         .thenValue(std::bind(MetadataFetcher::getContainersFromContainerMapV,
                              std::ref(qcl), _1));
}

//------------------------------------------------------------------------------
// Fetch all FileMDs contained within the given FileMap. Vector is sorted by
// filename.
//------------------------------------------------------------------------------
std::vector<folly::Future<eos::ns::FileMdProto>>
    MetadataFetcher::getFilesFromFilemap(qclient::QClient& qcl,
        const IContainerMD::FileMap& fileMap)
{
  // FileMap is a hashmap, thus unsorted.. We want the results to be sorted
  // based on filename, though.
  std::map<std::string, IFileMD::id_t> sortedFileMap;

  for (auto it = fileMap.begin(); it != fileMap.end(); ++it) {
    sortedFileMap[it->first] = it->second;
  }

  std::vector<folly::Future<eos::ns::FileMdProto>> retval;

  for (auto it = sortedFileMap.begin(); it != sortedFileMap.end(); it++) {
    retval.emplace_back(getFileFromId(qcl, FileIdentifier(it->second)));
  }

  return retval;
}

//------------------------------------------------------------------------------
// Same as above, but fileMap is passed as a value.
//------------------------------------------------------------------------------
std::vector<folly::Future<eos::ns::FileMdProto>>
    MetadataFetcher::getFilesFromFilemapV(qclient::QClient& qcl,
        IContainerMD::FileMap fileMap)
{
  return getFilesFromFilemap(qcl, fileMap);
}

//------------------------------------------------------------------------------
// Fetch all ContainerMDs contained within the given ContainerMap.
// Vector is sorted by filename.
//------------------------------------------------------------------------------
std::vector<folly::Future<eos::ns::ContainerMdProto>>
    MetadataFetcher::getContainersFromContainerMap(qclient::QClient& qcl,
        const IContainerMD::ContainerMap& containerMap)
{
  // ContainerMap is a hashmap, thus unsorted.. We want the results to be
  // sorted based on filename, though.
  std::map<std::string, IContainerMD::id_t> sortedContainerMap;

  for (auto it = containerMap.begin(); it != containerMap.end(); ++it) {
    sortedContainerMap[it->first] = it->second;
  }

  std::vector<folly::Future<eos::ns::ContainerMdProto>> retval;

  for (auto it = sortedContainerMap.cbegin(); it != sortedContainerMap.cend();
       it++) {
    retval.emplace_back(getContainerFromId(qcl, ContainerIdentifier(it->second)));
  }

  return retval;
}

//------------------------------------------------------------------------------
// Same as above, but containerMap is passed as a value.
//------------------------------------------------------------------------------
std::vector<folly::Future<eos::ns::ContainerMdProto>>
    MetadataFetcher::getContainersFromContainerMapV(qclient::QClient& qcl,
        IContainerMD::ContainerMap containerMap)
{
  return getContainersFromContainerMap(qcl, containerMap);
}

//------------------------------------------------------------------------------
// Fetch all subcontainers for current id
//------------------------------------------------------------------------------
folly::Future<IContainerMD::ContainerMap>
MetadataFetcher::getContainerMap(qclient::QClient& qcl,
                                 ContainerIdentifier container)
{
  MapFetcher<MapFetcherContainerTrait>* fetcher =
    new MapFetcher<MapFetcherContainerTrait>();
  return fetcher->initialize(qcl, container);
}

//------------------------------------------------------------------------------
// Parse response when looking up a ContainerID / FileID from (parent id, name)
//------------------------------------------------------------------------------
static int64_t parseIDFromNameResponse(redisReplyPtr reply,
                                       ContainerIdentifier parentID, const std::string& name)
{
  std::string errorPrefix =
    SSTR("Error while fetching FileID / ContainerID out of (parent id, name) = "
         "(" << parentID.getUnderlyingUInt64() << ", " << name << "): ");
  ensureStringReply(reply).throwIfNotOk(errorPrefix);
  int64_t retval;
  Serialization::deserialize(reply->str, reply->len, retval)
  .throwIfNotOk(errorPrefix);
  return retval;
}

//------------------------------------------------------------------------------
// int64_t -> FileIdentifier
//------------------------------------------------------------------------------
static FileIdentifier convertInt64ToFileIdentifier(int64_t id)
{
  return FileIdentifier(id);
}

//------------------------------------------------------------------------------
// int64_t -> ContainerIdentifier
//------------------------------------------------------------------------------
static ContainerIdentifier convertInt64ToContainerIdentifier(int64_t id)
{
  return ContainerIdentifier(id);
}

//------------------------------------------------------------------------------
// Fetch a file id given its parent and its name
//------------------------------------------------------------------------------
folly::Future<FileIdentifier>
MetadataFetcher::getFileIDFromName(qclient::QClient& qcl,
                                   ContainerIdentifier parent_id,
                                   const std::string& name)
{
  return qcl.follyExec("HGET",
                       SSTR(parent_id.getUnderlyingUInt64() << constants::sMapFilesSuffix), name)
         .thenValue(std::bind(parseIDFromNameResponse, _1, parent_id, name))
         .thenValue(convertInt64ToFileIdentifier);
}

//------------------------------------------------------------------------------
// Fetch a container id given its parent and its name
//------------------------------------------------------------------------------
folly::Future<ContainerIdentifier>
MetadataFetcher::getContainerIDFromName(qclient::QClient& qcl,
                                        ContainerIdentifier parent_id,
                                        const std::string& name)
{
  return qcl.follyExec("HGET",
                       SSTR(parent_id.getUnderlyingUInt64() << constants::sMapDirsSuffix), name)
         .thenValue(std::bind(parseIDFromNameResponse, _1, parent_id, name))
         .thenValue(convertInt64ToContainerIdentifier);
}

//------------------------------------------------------------------------------
// Resolve FileMdProto from parent ID + name
//------------------------------------------------------------------------------
folly::Future<eos::ns::FileMdProto>
MetadataFetcher::getFileFromName(qclient::QClient& qcl,
                                 ContainerIdentifier parent_id,
                                 const std::string& name)
{
  return getFileIDFromName(qcl, parent_id, name)
         .thenValue(std::bind(getFileFromId, std::ref(qcl), _1));
}

//----------------------------------------------------------------------------
//! Resolve ContainerMdProto from parent ID + name
//----------------------------------------------------------------------------
folly::Future<eos::ns::ContainerMdProto>
MetadataFetcher::getContainerFromName(qclient::QClient& qcl,
                                      ContainerIdentifier parent_id,
                                      const std::string& name)
{
  return getContainerIDFromName(qcl, parent_id, name)
         .thenValue(std::bind(getContainerFromId, std::ref(qcl), _1));
}

//------------------------------------------------------------------------------
// Parse bool response
//------------------------------------------------------------------------------
bool parseBoolResponse(redisReplyPtr reply)
{
  ensureBoolReply(reply).throwIfNotOk("");

  if (reply->integer == 0) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse uint64_t response
//------------------------------------------------------------------------------
uint64_t parseUInt64Response(redisReplyPtr reply)
{
  ensureUInt64Reply(reply).throwIfNotOk("");
  return reply->integer;
}

//------------------------------------------------------------------------------
// Is the given location of a FileID contained in the FsView?
//------------------------------------------------------------------------------
folly::Future<bool>
MetadataFetcher::locationExistsInFsView(qclient::QClient& qcl,
                                        FileIdentifier id,
                                        int64_t location, bool unlinked)
{
  std::string key;

  if (!unlinked) {
    key = SSTR("fsview:" << location << ":files");
  } else {
    key = SSTR("fsview:" << location << ":unlinked");
  }

  return qcl.follyExec("SISMEMBER",
                       key, SSTR(id.getUnderlyingUInt64())).thenValue(parseBoolResponse);
}

//------------------------------------------------------------------------------
// Helper class to resolve a Container's full path
//------------------------------------------------------------------------------
class FullPathResolver : public qclient::QCallback
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  FullPathResolver(qclient::QClient& qcl, ContainerIdentifier cont)
    : mQcl(qcl), mContainerID(cont) {}

  //----------------------------------------------------------------------------
  // Initialize, fire off first request
  //----------------------------------------------------------------------------
  folly::Future<std::string> initialize()
  {
    folly::Future<std::string> fut = mPromise.getFuture();

    if (mContainerID == ContainerIdentifier(1)) {
      // Short-circuit lookup, return "/"
      set_value();
      return fut;
    }

    mQcl.execCB(this, RequestBuilder::readContainerProto(mContainerID));
    return fut;
  }

  //----------------------------------------------------------------------------
  //! Handle response
  //----------------------------------------------------------------------------
  virtual void handleResponse(redisReplyPtr&& reply) override
  {
    if (!reply) {
      return set_exception(EFAULT, "QuarkDB backend not available!");
    }

    if (reply->type != REDIS_REPLY_STRING) {
      return set_exception(EFAULT, SSTR("Received unexpected response: "
                                        << qclient::describeRedisReply(reply)));
    }

    eos::ns::ContainerMdProto proto;
    MDStatus status = Serialization::deserialize(reply->str, reply->len, proto);

    if (!status.ok()) {
      return set_exception(status);
    }

    mPathStack.emplace_front(proto.name());

    if (proto.parent_id() == 1) {
      // We're done
      return set_value();
    }

    // Lookup next chunk
    mQcl.execCB(this, RequestBuilder::readContainerProto(ContainerIdentifier(
                  proto.parent_id())));
  }

  //----------------------------------------------------------------------------
  // Return exception by passing it to the promise
  //----------------------------------------------------------------------------
  void set_exception(const MDStatus& status)
  {
    return set_exception(status.getErrno(), status.getError());
  }

  //----------------------------------------------------------------------------
  // Return exception by passing it to the promise
  //----------------------------------------------------------------------------
  void set_exception(int err, const std::string& msg)
  {
    mPromise.setException(
      make_mdexception(err, SSTR("Error while reconstructing full path of container #"
                                 << mContainerID.getUnderlyingUInt64()
                                 << " from QDB: " << msg)));
    delete this; // harakiri
  }

  //----------------------------------------------------------------------------
  // Set value, we're done
  //----------------------------------------------------------------------------
  void set_value()
  {
    std::ostringstream ss;
    ss << "/";

    for (size_t i = 0; i < mPathStack.size(); i++) {
      ss << mPathStack[i] << "/";
    }

    mPromise.setValue(ss.str());
    delete this;
  }

private:
  qclient::QClient& mQcl;
  ContainerIdentifier mContainerID;
  std::deque<std::string> mPathStack;
  folly::Promise<std::string> mPromise;
};



//------------------------------------------------------------------------------
// Resolve container's full path. Throws an exception if this is a container
// detached from "/".
//------------------------------------------------------------------------------
folly::Future<std::string>
MetadataFetcher::resolveFullPath(qclient::QClient& qcl,
                                 ContainerIdentifier containerID)
{
  FullPathResolver* resolver = new FullPathResolver(qcl, containerID);
  return resolver->initialize();
}

//------------------------------------------------------------------------------
// Helper class to resolve a path to an ID - no symlink support yet.
//------------------------------------------------------------------------------
class ReversePathResolver
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ReversePathResolver(qclient::QClient& qcl, const std::string& path)
    : mQcl(qcl), mPath(path)
  {
    eos::PathProcessor::insertChunksIntoDeque(mPathStack, path);
  }

  //----------------------------------------------------------------------------
  // Initialize, fire off first request
  //----------------------------------------------------------------------------
  folly::Future<FileOrContainerIdentifier> initialize()
  {
    folly::Future<FileOrContainerIdentifier> fut = mPromise.getFuture();

    if (mPathStack.size() == 0) {
      set_value(ContainerIdentifier(1));
    } else {
      startNextRound(ContainerIdentifier(1));
    }

    return fut;
  }

  void handleIncomingFile(eos::ns::FileMdProto proto)
  {
    return set_value(FileIdentifier(proto.id()));
  }

  void handleIncomingContainer(eos::ns::ContainerMdProto proto)
  {
    mPathStack.pop_front();

    if (mPathStack.empty()) {
      return set_value(ContainerIdentifier(proto.id()));
    }

    startNextRound(ContainerIdentifier(proto.id()));
  }

  void handleIncomingFileError(const folly::exception_wrapper& e)
  {
    mPromise.setException(e);
    delete this; // harakiri
  }

  void handleIncomingContainerError(ContainerIdentifier parent,
                                    const folly::exception_wrapper& e)
  {
    if (mPathStack.size() == 1) {
      MetadataFetcher::getFileFromName(mQcl, parent, mPathStack.front())
      .thenValue(std::bind(&ReversePathResolver::handleIncomingFile, this, _1))
      .thenError([this](const folly::exception_wrapper & e) {
        handleIncomingFileError(e);
      });
      return;
    }

    mPromise.setException(e);
    delete this; // harakiri
  }

  //----------------------------------------------------------------------------
  // Return exception by passing it to the promise
  //----------------------------------------------------------------------------
  void set_exception(int err, const std::string& msg)
  {
    mPromise.setException(
      make_mdexception(err, SSTR("Error while resolving path " << mPath <<
                                 " from QDB: " << msg)));
    delete this; // harakiri
  }

  //----------------------------------------------------------------------------
  // Set value, we're done
  //----------------------------------------------------------------------------
  void set_value(FileOrContainerIdentifier outcome)
  {
    mPromise.setValue(outcome);
    delete this;
  }

private:
  //----------------------------------------------------------------------------
  // Start next asynchronous round
  //----------------------------------------------------------------------------
  void startNextRound(ContainerIdentifier parent)
  {
    MetadataFetcher::getContainerFromName(mQcl, parent, mPathStack.front())
    .thenValue(std::bind(&ReversePathResolver::handleIncomingContainer, this, _1))
    .thenError([this, parent](const folly::exception_wrapper & e) {
      handleIncomingContainerError(parent, e);
    });
  }

  qclient::QClient& mQcl;
  std::string mPath;
  std::deque<std::string> mPathStack;
  folly::Promise<FileOrContainerIdentifier> mPromise;
};

//------------------------------------------------------------------------------
// Resolve a path to an ID.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerIdentifier>
MetadataFetcher::resolvePathToID(qclient::QClient& qcl,
                                 const std::string& path)
{
  ReversePathResolver* resolver = new ReversePathResolver(qcl, path);
  return resolver->initialize();
}

//------------------------------------------------------------------------------
// Count how many files and containers are in the given directory
//------------------------------------------------------------------------------
std::pair<folly::Future<uint64_t>, folly::Future<uint64_t>>
    MetadataFetcher::countContents(qclient::QClient& qcl,
                                   ContainerIdentifier containerID)
{
  return std::make_pair<folly::Future<uint64_t>, folly::Future<uint64_t>>(
           qcl.follyExec("HLEN", keySubFiles(containerID.getUnderlyingUInt64()))
           .thenValue(parseUInt64Response),
           qcl.follyExec("HLEN", keySubContainers(containerID.getUnderlyingUInt64()))
           .thenValue(parseUInt64Response)
         );
}

EOSNSNAMESPACE_END
