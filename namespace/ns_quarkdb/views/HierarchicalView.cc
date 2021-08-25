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

#include "common/Logging.hh"
#include "common/Assert.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/Constants.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/utils/PathProcessor.hh"
#include <cerrno>
#include <ctime>
#include <functional>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <cstdlib>

using std::placeholders::_1;

#ifdef __APPLE__
#define EBADFD 77
#endif

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkHierarchicalView::QuarkHierarchicalView(qclient::QClient *qcl, MetadataFlusher *flusher)
  : pQcl(qcl), pQuotaFlusher(flusher), pContainerSvc(nullptr), pFileSvc(nullptr),
    pQuotaStats(new QuarkQuotaStats(pQcl, pQuotaFlusher)), pRoot(nullptr)
{
  pExecutor.reset(new folly::IOThreadPoolExecutor(32));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkHierarchicalView::~QuarkHierarchicalView()
{
  delete pQuotaStats;
}

//------------------------------------------------------------------------------
// Configure the view
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::configure(const std::map<std::string, std::string>& config)
{
  if (pContainerSvc == nullptr) {
    MDException e(EINVAL);
    e.getMessage() << "Container MD Service was not set";
    throw e;
  }

  if (pFileSvc == nullptr) {
    MDException e(EINVAL);
    e.getMessage() << "File MD Service was not set";
    throw e;
  }

  delete pQuotaStats;
  pQuotaStats = new QuarkQuotaStats(pQcl, pQuotaFlusher);
  pQuotaStats->configure(config);
}

//------------------------------------------------------------------------------
// Initialize the view
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::initialize()
{
  initialize1();
  initialize2();
  initialize3();
}

void
QuarkHierarchicalView::initialize1()
{
  pContainerSvc->initialize();

  // Get root container
  try {
    pRoot = pContainerSvc->getContainerMD(1);
  } catch (MDException& e) {
    pRoot = pContainerSvc->createContainer(0);

    if (pRoot->getId() != 1) {
      eos_static_crit("Error when creating root '/' path - directory inode is not 1, but %d!",
                      pRoot->getId());
      std::quick_exit(1);
    }

    pRoot->setName("/");
    pRoot->setParentId(pRoot->getId());
    updateContainerStore(pRoot.get());
  }
}

//------------------------------------------------------------------------------
// Initialize phase 2
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::initialize2()
{
  pFileSvc->initialize();
}

//------------------------------------------------------------------------------
// Initialize phase 3
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::initialize3()
{
  //--------------------------------------------------------------------------
  // Scan all the files to reattach them to containers - THIS SHOULD NOT
  // BE DONE! THE INFO NEEDS TO BE STORED WITH CONTAINERS
  //--------------------------------------------------------------------------
  // FileVisitor visitor( pContainerSvc, pQuotaStats, this );
  // pFileSvc->visit( &visitor );
}

//------------------------------------------------------------------------------
// Finalize the view
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::finalize()
{
  pContainerSvc->finalize();
  pFileSvc->finalize();
  delete pQuotaStats;
  pQuotaStats = nullptr;
}

//------------------------------------------------------------------------------
// Extract FileMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static folly::Future<IFileMDPtr> extractFileMD(FileOrContainerMD ptr)
{
  if (!ptr.file) {
    return folly::makeFuture<IFileMDPtr>(make_mdexception(ENOENT,
                                         "No such file or directory"));
  }

  return ptr.file;
}

//------------------------------------------------------------------------------
// Extract ContainerMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static folly::Future<IContainerMDPtr> extractContainerMD(FileOrContainerMD ptr)
{
  if (!ptr.container) {
    return folly::makeFuture<IContainerMDPtr>(make_mdexception(ENOENT,
           "No such file or directory"));
  }

  return ptr.container;
}

//------------------------------------------------------------------------------
// Lookup a given path.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
QuarkHierarchicalView::getItem(const std::string& uri, bool follow)
{
  //----------------------------------------------------------------------------
  // Build our deque of pending chunks...
  //----------------------------------------------------------------------------
  std::deque<std::string> pendingChunks;
  eos::PathProcessor::insertChunksIntoDeque(pendingChunks, uri);
  //----------------------------------------------------------------------------
  // Initial state: We're at "/", and have to look up all chunks.
  //----------------------------------------------------------------------------
  FileOrContainerMD initialState {nullptr, pRoot};
  return getPathInternal(initialState, pendingChunks, follow, 0);
}

//------------------------------------------------------------------------------
// Convert a ContainerMDPtr to FileOrContainerMD.
//------------------------------------------------------------------------------
static FileOrContainerMD toFileOrContainerMD(IContainerMDPtr ptr)
{
  return {nullptr, ptr};
}

//------------------------------------------------------------------------------
// Lookup a given path - deferred function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
QuarkHierarchicalView::getPathDeferred(folly::Future<FileOrContainerMD> fut,
                                  std::deque<std::string> pendingChunks,
                                  bool follow, size_t expendedEffort)
{
  //----------------------------------------------------------------------------
  // We're blocked on a network request. "Pause" execution of getPathInternal
  // for now, return a pending folly::Future to caller.
  //
  // The Executor pool will "resume" computation later, once the network
  // request is completed.
  //----------------------------------------------------------------------------
  return fut.via(pExecutor.get())
         .thenValue(std::bind(&QuarkHierarchicalView::getPathInternal, this, _1, pendingChunks,
                         follow, expendedEffort));
}

//------------------------------------------------------------------------------
// Lookup a given path - deferred function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
QuarkHierarchicalView::getPathDeferred(folly::Future<IContainerMDPtr> fut,
                                  std::deque<std::string> pendingChunks,
                                  bool follow, size_t expendedEffort)
{
  //----------------------------------------------------------------------------
  // Same as getPathDeferred taking FileOrContainerMD.
  //----------------------------------------------------------------------------
  return fut.via(pExecutor.get())
         .thenValue(toFileOrContainerMD)
         .thenValue(std::bind(&QuarkHierarchicalView::getPathInternal, this, _1, pendingChunks,
                         follow, expendedEffort));
}

//------------------------------------------------------------------------------
// Lookup a given path - internal function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
QuarkHierarchicalView::getPathInternal(FileOrContainerMD state,
                                  std::deque<std::string> pendingChunks,
                                  bool follow, size_t expendedEffort)
{
  //----------------------------------------------------------------------------
  // Our goal is to consume pendingChunks until it's empty.
  // If everything we need is in memory, we keep executing.
  //
  // However, if a network request is necessary to continue lookup, we bail,
  // and do it asynchronously. Execution of the function will "resume" as soon
  // as the network request is complete.
  //----------------------------------------------------------------------------
  while (true) {
    //--------------------------------------------------------------------------
    // Protection against symbolic link loops.
    //--------------------------------------------------------------------------
    expendedEffort++;

    if (expendedEffort > 255) {
      return folly::makeFuture<FileOrContainerMD>(make_mdexception(
               ELOOP, "Too many symbolic links were encountered in translating the pathname"));
    }

    if (!state.container && !state.file) {
      //------------------------------------------------------------------------
      // The previous iteration of the loop resulted in an empty state: Only one
      // way to get here, looking up a non-existent chunk.
      //------------------------------------------------------------------------
      return folly::makeFuture<FileOrContainerMD>(make_mdexception(ENOENT,
             "No such file or directory"));
    }

    if (pendingChunks.empty()) {
      if (!(follow && state.file && state.file->isLink())) {
        //------------------------------------------------------------------------
        // We're done. Our current state contains the desired output.
        //------------------------------------------------------------------------
        return state;
      } else {
        //----------------------------------------------------------------------
        // Edge case: State is actually a symlink we must follow, not done yet.
        //----------------------------------------------------------------------
      }
    }

    if (state.container) {
      //------------------------------------------------------------------------
      // Handle special cases, "." and ".."
      //------------------------------------------------------------------------
      if (pendingChunks.front() == ".") {
        pendingChunks.pop_front();
        continue;
      }

      if (pendingChunks.front() == "..") {
        pendingChunks.pop_front();
        folly::Future<IContainerMDPtr> fut = pContainerSvc->getContainerMDFut(
                                               state.container->getParentId());

        if (!fut.isReady() || fut.hasException()) {
          //--------------------------------------------------------------------
          // We're blocked, "pause" execution, unblock caller.
          //--------------------------------------------------------------------
          return getPathDeferred(std::move(fut), pendingChunks, follow, expendedEffort);
        }

        state.container = std::move(fut).get();
        continue;
      }

      //------------------------------------------------------------------------
      // Normal case: Our current state contains a container, and we're simply
      // looking up the next chunk.
      //------------------------------------------------------------------------
      folly::Future<FileOrContainerMD> next = state.container->findItem(
          pendingChunks.front());
      pendingChunks.pop_front();

      //------------------------------------------------------------------------
      // If we're lucky, the result is ready immediately. Update state, and
      // carry on.
      //------------------------------------------------------------------------
      if (next.isReady() && !next.hasException()) {
        state = std::move(next).get();
        continue;
      } else {
        //----------------------------------------------------------------------
        // We're blocked, "pause" execution, unblock caller.
        //----------------------------------------------------------------------
        return getPathDeferred(std::move(next), pendingChunks, follow, expendedEffort);
      }
    }

    if (state.file) {
      //------------------------------------------------------------------------
      // This is unusual.. How come a file came up in the middle of a path
      // lookup?
      //
      // 1. We've hit a symlink.
      // 2. Caller is drunk, and doing "ls /eos/dir1/file1/not/existing".
      //------------------------------------------------------------------------
      if (!state.file->isLink()) {
        return folly::makeFuture<FileOrContainerMD>(make_mdexception(ENOTDIR,
               "Not a directory"));
      }

      //------------------------------------------------------------------------
      // Ok, this is definitely a symlink. Should we follow it?
      //------------------------------------------------------------------------
      if (pendingChunks.size() == 0u && !follow) {
        //----------------------------------------------------------------------
        // Nope, we're interested in the symlink itself, we're done.
        //----------------------------------------------------------------------
        return state;
      }

      //------------------------------------------------------------------------
      // Populate our pendingChunks with the updated target.
      //------------------------------------------------------------------------
      const std::string& symlinkTarget = state.file->getLink();
      eos::PathProcessor::insertChunksIntoDeque(pendingChunks, symlinkTarget);

      if (!symlinkTarget.empty() && symlinkTarget[0] == '/') {
        //----------------------------------------------------------------------
        // This is an absolute symlink: Our state becomes pRoot again.
        //----------------------------------------------------------------------
        state = FileOrContainerMD {nullptr, pRoot};
      } else {
        //----------------------------------------------------------------------
        // This is a relative symlink: State becomes symlink's parent container.
        //----------------------------------------------------------------------
        folly::Future<IContainerMDPtr> fut = pContainerSvc->getContainerMDFut(
                                               state.file->getContainerId());

        if (!fut.isReady() || fut.hasException()) {
          //--------------------------------------------------------------------
          // We're blocked, "pause" execution, unblock caller.
          //--------------------------------------------------------------------
          return getPathDeferred(std::move(fut), pendingChunks, follow, expendedEffort);
        }

        state.container = std::move(fut).get();
        state.file.reset();
      }

      continue;
    }
  }
}

//------------------------------------------------------------------------------
// Retrieve a file for given uri, asynchronously
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
QuarkHierarchicalView::getFileFut(const std::string& uri, bool follow)
{
  return getItem(uri, follow).thenValue(extractFileMD);
}

//------------------------------------------------------------------------------
// Retrieve a file for given uri
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
QuarkHierarchicalView::getFile(const std::string& uri, bool follow,
                          size_t* link_depths)
{
  return getFileFut(uri, follow).get();
}

//------------------------------------------------------------------------------
// Create a file for given uri
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
QuarkHierarchicalView::createFile(const std::string& uri, uid_t uid, gid_t gid, IFileMD::id_t id)
{
  if (uri == "/") {
    throw_mdexception(EEXIST, "File exists");
  }

  // Split the path and find the last container
  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  if (chunks.size() == 0u) {
    throw_mdexception(EEXIST, "File exists");
  }

  std::string lastChunk = chunks.back();
  chunks.pop_back();
  FileOrContainerMD item = getPathInternal(FileOrContainerMD {nullptr, pRoot},
                           chunks, true, 0).get();

  if (item.file) {
    throw_mdexception(ENOTDIR, "Not a directory");
  }

  IContainerMDPtr parent = item.container;
  FileOrContainerMD potentialConflict = parent->findItem(lastChunk).get();

  if (potentialConflict.file || potentialConflict.container) {
    throw_mdexception(EEXIST, "File exists");
  }

  IFileMDPtr file = pFileSvc->createFile(id);

  if (!file) {
    eos_static_crit("File creation failed for %s", uri.c_str());
    throw_mdexception(EIO, "File creation failed");
  }

  file->setName(lastChunk);
  file->setCUid(uid);
  file->setCGid(gid);
  file->setCTimeNow();
  file->setMTimeNow();
  file->clearChecksum(0);
  parent->addFile(file.get());
  updateFileStore(file.get());
  return file;
}

//------------------------------------------------------------------------
//! Create a link for given uri
//------------------------------------------------------------------------
void
QuarkHierarchicalView::createLink(const std::string& uri, const std::string& linkuri,
                             uid_t uid, gid_t gid)
{
  std::shared_ptr<IFileMD> file = createFile(uri, uid, gid);

  if (file) {
    file->setLink(linkuri);
    file->setSize(linkuri.length());
    updateFileStore(file.get());
  }
}

//------------------------------------------------------------------------------
// Remove link
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::removeLink(const std::string& uri)
{
  return unlinkFile(uri);
}

//------------------------------------------------------------------------------
// Unlink the file for given uri
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::unlinkFile(const std::string& uri)
{
  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  if (chunks.size() == 0) {
    MDException e(ENOENT);
    e.getMessage() << "Not a file";
    throw e;
  }

  std::string lastChunk = chunks[chunks.size() - 1];
  chunks.pop_back();
  IContainerMDPtr parent = getPathExpectContainer(chunks).get();
  std::shared_ptr<IFileMD> file = parent->findFile(lastChunk);

  if (!file) {
    MDException e(ENOENT);
    e.getMessage() << "File does not exist";
    throw e;
  }

  unlinkFile(file.get());
}

//------------------------------------------------------------------------------
// Unlink the file - this is only used for testing
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::unlinkFile(eos::IFileMD* file)
{
  std::shared_ptr<IContainerMD> cont =
    pContainerSvc->getContainerMD(file->getContainerId());
  file->setContainerId(0);
  file->unlinkAllLocations();
  cont->removeFile(file->getName());
  updateFileStore(file);
}

//------------------------------------------------------------------------------
// Remove the file
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::removeFile(IFileMD* file)
{
  // Check if the file can be removed
  if (file->getNumLocation() != 0 || file->getNumUnlinkedLocation() != 0) {
    MDException ex(EBADFD);
    ex.getMessage() << "Cannot remove the record. Unlinked replicas ";
    ex.getMessage() << "still exist";
    throw ex;
  }

  if (file->getContainerId() != 0) {
    std::shared_ptr<IContainerMD> cont =
      pContainerSvc->getContainerMD(file->getContainerId());
    cont->removeFile(file->getName());
  }

  pFileSvc->removeFile(file);
}

//------------------------------------------------------------------------------
// Get a container (directory) asynchronously
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
QuarkHierarchicalView::getContainerFut(const std::string& uri, bool follow)
{
  if (uri == "/") {
    return std::shared_ptr<IContainerMD> {pContainerSvc->getContainerMD(1)};
  }

  return getItem(uri, follow).thenValue(extractContainerMD);
}

//------------------------------------------------------------------------------
// Get a container (directory)
//------------------------------------------------------------------------------
IContainerMDPtr
QuarkHierarchicalView::getContainer(const std::string& uri, bool follow,
                               size_t* link_depth)
{
  return getContainerFut(uri, follow).get();
}

//------------------------------------------------------------------------------
// UpdateStoreGuard helper class
//------------------------------------------------------------------------------
class UpdateStoreGuard
{
public:
  UpdateStoreGuard(eos::QuarkHierarchicalView* v) : view(v) {}

  ~UpdateStoreGuard()
  {
    for (auto it = ptrs.begin(); it != ptrs.end(); it++) {
      view->updateContainerStore((*it).get());
    }
  }

  void add(IContainerMDPtr cont)
  {
    ptrs.insert(cont);
  }

private:
  eos::QuarkHierarchicalView* view;
  std::set<IContainerMDPtr> ptrs;
};

//------------------------------------------------------------------------------
// Create container - method eventually consistent
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
QuarkHierarchicalView::createContainer(const std::string& uri, bool createParents, uint64_t cid)
{
  // Split the path
  if (uri == "/") {
    throw_mdexception(EEXIST, uri << ": Container exists");
  }

  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  if (chunks.empty()) {
    throw_mdexception(EEXIST, uri << ": File exists");
  }

  // Resolve path chunks one by one
  FileOrContainerMD state = {nullptr, pRoot};
  UpdateStoreGuard updateGuard(this);

  while (true) {
    if (state.file) {
      throw_mdexception(ENOTDIR, uri << ": Not a directory");
    }

    if (!state.container) {
      throw_mdexception(ENOENT, uri << ": No such file or directory");
    }

    if (chunks.empty()) {
      return state.container;
    }

    std::string nextChunk = chunks.front();
    std::deque<std::string> nextChunkDeque { nextChunk }; // yes, this is stupid
    chunks.pop_front();

    // Lookup next chunk ..
    try {
      state = getPathInternal(state, nextChunkDeque, true, 0).get();
    } catch (const eos::MDException& e) {
      if (e.getErrno() != ENOENT) {
        // Something's wrong, rethrow
        throw;
      }

      if (!createParents && !chunks.empty()) {
        throw_mdexception(ENOENT, uri << ": No such file or directory");
      }

      // Wait.. what if "ENOENT" is actually due to failed symlink lookup?
      // We'd screw up namespace consistency if we attempt to add a container
      // with the same name as the broken symlink.
      FileOrContainerMD item = state.container->findItem(nextChunk).get();

      if(item.file || item.container) {
        throw_mdexception(ENOTDIR, uri << ": Not a directory");
      }

      IContainerMDPtr newContainer = pContainerSvc->createContainer(chunks.empty()?cid:0);
      newContainer->setName(nextChunk);
      newContainer->setCTimeNow();
      state.container->addContainer(newContainer.get());
      updateGuard.add(state.container);
      updateGuard.add(newContainer);
      state.container = newContainer;
    }
  }
}

//------------------------------------------------------------------------------
// Lookup a given path, expect a container there.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
QuarkHierarchicalView::getPathExpectContainer(const std::deque<std::string>& chunks)
{
  if (chunks.size() == 0u) {
    return pRoot;
  }

  return getPathInternal(FileOrContainerMD {nullptr, pRoot}, chunks, true, 0)
         .thenValue(extractContainerMD);
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::removeContainer(const std::string& uri)
{
  // Find the container
  if (uri == "/") {
    MDException e(EPERM);
    e.getMessage() << "Permission denied.";
    throw e;
  }

  //----------------------------------------------------------------------------
  // Lookup last container
  //----------------------------------------------------------------------------
  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);
  eos_assert(chunks.size() != 0);
  std::string lastChunk = chunks[chunks.size() - 1];
  chunks.pop_back();
  IContainerMDPtr parent = getPathExpectContainer(chunks).get();
  // Check if the container exist and remove it
  auto cont = parent->findContainer(lastChunk);

  if (!cont) {
    MDException e(ENOENT);
    e.getMessage() << uri << ": No such file or directory";
    throw e;
  }

  if (cont->getNumContainers() != 0 || cont->getNumFiles() != 0) {
    MDException e(ENOTEMPTY);
    e.getMessage() << uri << ": Container is not empty";
    throw e;
  }

  // This is a two-step delete
  pContainerSvc->removeContainer(cont.get());
  parent->removeContainer(cont->getName());
}

//------------------------------------------------------------------------------
// Concatenate a deque of chunks into a string
//------------------------------------------------------------------------------
static std::string concatenateDeque(const std::deque<std::string> &chunks) {
  std::ostringstream ss;

  for(size_t i = 0; i < chunks.size(); i++) {
    ss << "/" << chunks[i];
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Concatenate a deque of chunks into a string, with an ending slash
//------------------------------------------------------------------------------
static std::string concatenateDequeWithEndingSlash(const std::deque<std::string> &chunks) {
  std::ostringstream ss;

  for(size_t i = 0; i < chunks.size(); i++) {
    ss << "/" << chunks[i];
  }

  ss << "/";
  return ss.str();
}

//------------------------------------------------------------------------------
// Get uri for the container
//------------------------------------------------------------------------------
std::string
QuarkHierarchicalView::getUri(const IContainerMD* container) const
{
  // Check the input
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  return getUriFut(container->getIdentifier()).get();
}

//------------------------------------------------------------------------------
// Get uri for the container - asynchronous version
//------------------------------------------------------------------------------
folly::Future<std::string>
QuarkHierarchicalView::getUriFut(ContainerIdentifier id) const
{
  return getUriInternalCid({}, id)
    .thenValue(concatenateDequeWithEndingSlash);
}

//------------------------------------------------------------------------------
// Build the URL of the given container, as a deque of chunks. Primary
// "resumable" function.
//------------------------------------------------------------------------------
folly::Future<std::deque<std::string>>
QuarkHierarchicalView::getUriInternal(std::deque<std::string> currentChunks,
  IContainerMDPtr nextToLookup) const {

  while(true) {
    //--------------------------------------------------------------------------
    // Null nextToLookup with an empty deque? ENOENT
    //--------------------------------------------------------------------------
    if(!nextToLookup && currentChunks.empty()) {
      return folly::makeFuture<std::deque<std::string>>(
        make_mdexception(ENOENT, "No such file or directory"));
    }

    //--------------------------------------------------------------------------
    // Null nextToLookup with non-empty deque? Huh.. that shouldn't happen.
    //--------------------------------------------------------------------------
    if(!nextToLookup) {
      std::string err = SSTR("Potential namespace corruption, received null nextToLookup in getUri. " <<
        "Current state: " << concatenateDeque(currentChunks));

      eos_static_crit(err.c_str());
      return folly::makeFuture<std::deque<std::string>>(make_mdexception(EFAULT, err.c_str()));
    }

    //--------------------------------------------------------------------------
    // Reached the end?
    //--------------------------------------------------------------------------
    if(nextToLookup->getIdentifier()  == ContainerIdentifier(1)) {
      return currentChunks;
    }

    //--------------------------------------------------------------------------
    // Potential cycle?
    //--------------------------------------------------------------------------
    if(currentChunks.size() > 255) {
      std::string err = SSTR("Potential namespace corruption, detected loop in getUri. Current container: "
        << nextToLookup->getId() << ", current state: " << concatenateDeque(currentChunks));

      eos_static_crit(err.c_str());
      return folly::makeFuture<std::deque<std::string>>(make_mdexception(EFAULT, err.c_str()));
    }

    //--------------------------------------------------------------------------
    // Add nextToLookup's name into the deque..
    //--------------------------------------------------------------------------
    currentChunks.emplace_front(nextToLookup->getName());

    //--------------------------------------------------------------------------
    // Lookup parent chunk..
    //--------------------------------------------------------------------------
    folly::Future<IContainerMDPtr> pending = pContainerSvc->getContainerMDFut(nextToLookup->getParentId());

    if(pending.isReady()) {
      //------------------------------------------------------------------------
      // Cache hit, carry on.
      //------------------------------------------------------------------------
      nextToLookup = std::move(pending).get();
      continue;
    }

    //--------------------------------------------------------------------------
    // Cache miss, "pause" execution until we receive the necessary metadata
    // from QDB.
    //--------------------------------------------------------------------------
    return pending.via(pExecutor.get())
      .thenValue(std::bind(&QuarkHierarchicalView::getUriInternal, this, std::move(currentChunks),
        _1));
  }
}

//------------------------------------------------------------------------------
// Build the URL of the given container ID.
//------------------------------------------------------------------------------
folly::Future<std::deque<std::string>>
QuarkHierarchicalView::getUriInternalCid(std::deque<std::string> currentChunks,
  ContainerIdentifier cid) const
{
  folly::Future<IContainerMDPtr> pending = pContainerSvc->getContainerMDFut(cid.getUnderlyingUInt64());

  if(pending.isReady() && !pending.hasException()) {
    //--------------------------------------------------------------------------
    // Cache hit
    //--------------------------------------------------------------------------
    return getUriInternal(currentChunks, std::move(pending).get());
  }

  //----------------------------------------------------------------------------
  // Pause execution, give back future.
  //----------------------------------------------------------------------------
  return pending.via(pExecutor.get())
    .thenValue(std::bind(&QuarkHierarchicalView::getUriInternal, this, currentChunks, _1));
}

//------------------------------------------------------------------------------
// Build the URL of the given file, as a deque of chunks.
//------------------------------------------------------------------------------
folly::Future<std::deque<std::string>>
QuarkHierarchicalView::getUriInternalFmd(const IFileMD *fmd) const
{
  if(!fmd) {
    //--------------------------------------------------------------------------
    // ENOENT
    //--------------------------------------------------------------------------
    return folly::makeFuture<std::deque<std::string>>(
      make_mdexception(ENOENT, "No such file or directory"));
  }

  std::deque<std::string> chunks;
  chunks.emplace_front(fmd->getName());
  return getUriInternalCid(chunks, ContainerIdentifier(fmd->getContainerId()));
}

//------------------------------------------------------------------------------
// Build the URL of the given file, as a deque of chunks.
//------------------------------------------------------------------------------
folly::Future<std::deque<std::string>>
QuarkHierarchicalView::getUriInternalFmdPtr(IFileMDPtr fmd) const
{
  return getUriInternalFmd(fmd.get());
}

//------------------------------------------------------------------------------
// Build the URL of the given fid
//------------------------------------------------------------------------------
folly::Future<std::deque<std::string>>
QuarkHierarchicalView::getUriInternalFid(FileIdentifier fid) const
{
  folly::Future<IFileMDPtr> pending = pFileSvc->getFileMDFut(fid.getUnderlyingUInt64());

  if(pending.isReady() && !pending.hasException()) {
    //--------------------------------------------------------------------------
    // Cache hit
    //--------------------------------------------------------------------------
    return getUriInternalFmdPtr(std::move(pending).get());
  }

  //----------------------------------------------------------------------------
  // Pause execution, give back future.
  //----------------------------------------------------------------------------
  return pending.via(pExecutor.get())
    .thenValue(std::bind(&QuarkHierarchicalView::getUriInternalFmdPtr, this, _1));
}

//------------------------------------------------------------------------------
// Get uri for container id
//------------------------------------------------------------------------------
std::string
QuarkHierarchicalView::getUri(const IContainerMD::id_t cid) const
{
  return getUriFut(ContainerIdentifier(cid)).get();
}

//------------------------------------------------------------------------------
// Get uri for the container - asynchronous version
//------------------------------------------------------------------------------
folly::Future<std::string>
QuarkHierarchicalView::getUriFut(FileIdentifier id) const
{
  return getUriInternalFid(id)
    .thenValue(concatenateDeque);
}

//------------------------------------------------------------------------------
// Get uri for the file
//------------------------------------------------------------------------------
std::string
QuarkHierarchicalView::getUri(const IFileMD* file) const
{
  return getUriInternalFmd(file)
    .thenValue(concatenateDeque)
    .get();
}

//------------------------------------------------------------------------
// Get real path translating existing symlink
//------------------------------------------------------------------------
std::string QuarkHierarchicalView::getRealPath(const std::string& uri)
{
  if (uri == "/") {
    MDException e(ENOENT);
    e.getMessage() << " is not a file";
    throw e;
  }

  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);
  eos_assert(chunks.size() != 0);

  if (chunks.size() == 1) {
    return chunks[0];
  }

  //----------------------------------------------------------------------------
  // Remove last chunk
  //----------------------------------------------------------------------------
  std::string lastChunk = chunks[chunks.size() - 1];
  chunks.pop_back();
  //----------------------------------------------------------------------------
  // Lookup parent container..
  //----------------------------------------------------------------------------
  IContainerMDPtr cont = getPathExpectContainer(chunks).get();
  return SSTR(getUri(cont.get()) << lastChunk);
}

//------------------------------------------------------------------------------
// Get quota node id concerning given container
//------------------------------------------------------------------------------
IQuotaNode*
QuarkHierarchicalView::getQuotaNode(const IContainerMD* container, bool search)
{
  // Initial sanity check
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  if (pQuotaStats == nullptr) {
    MDException ex;
    ex.getMessage() << "No QuotaStats placeholder registered";
    throw ex;
  }

  std::shared_ptr<IContainerMD> current;

  // Search for the node
  try {
    current = pContainerSvc->getContainerMD(container->getId());

    if (search) {
      while (current->getName() != pRoot->getName() &&
             (current->getFlags() & QUOTA_NODE_FLAG) == 0) {
        current = pContainerSvc->getContainerMD(current->getParentId());
      }
    }
  }
  catch(...) {
    eos_static_crit("Attempted to get quota node of possibly detached container with cid=%llu", container->getId());
    return nullptr;
  }

  // We have either found a quota node or reached root without finding one
  // so we need to double check whether the current container has an
  // associated quota node
  if ((current->getFlags() & QUOTA_NODE_FLAG) == 0) {
    return nullptr;
  }

  IQuotaNode* node = pQuotaStats->getQuotaNode(current->getId());

  if (node != nullptr) {
    return node;
  }

  return pQuotaStats->registerNewNode(current->getId());
}

//------------------------------------------------------------------------------
// Register the container to be a quota node
//------------------------------------------------------------------------------
IQuotaNode*
QuarkHierarchicalView::registerQuotaNode(IContainerMD* container)
{
  // Initial sanity check
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  if (pQuotaStats == nullptr) {
    MDException ex;
    ex.getMessage() << "No QuotaStats placeholder registered";
    throw ex;
  }

  if ((container->getFlags() & QUOTA_NODE_FLAG) != 0) {
    MDException ex;
    ex.getMessage() << "Already a quota node: " << container->getId();
    throw ex;
  }

  IQuotaNode* node = pQuotaStats->registerNewNode(container->getId());
  container->setFlags(container->getFlags() | QUOTA_NODE_FLAG);
  updateContainerStore(container);
  return node;
}

//------------------------------------------------------------------------------
// Remove the quota node
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::removeQuotaNode(IContainerMD* container)
{
  // Sanity checks
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  if (pQuotaStats == nullptr) {
    MDException ex;
    ex.getMessage() << "No QuotaStats placeholder registered";
    throw ex;
  }

  if ((container->getFlags() & QUOTA_NODE_FLAG) == 0) {
    MDException ex;
    ex.getMessage() << "Not a quota node: " << container->getId();
    throw ex;
  }

  // Get the quota node and meld it with the parent node if present
  IQuotaNode* node = getQuotaNode(container);
  IQuotaNode* parent = nullptr;

  if (container->getId() != 1) {
    parent = getQuotaNode(
               pContainerSvc->getContainerMD(container->getParentId()).get(), true);
  }

  container->setFlags(container->getFlags() & ~QUOTA_NODE_FLAG);
  updateContainerStore(container);

  if (parent != nullptr) {
    try {
      parent->meld(node);
    } catch (const std::runtime_error& e) {
      MDException ex;
      ex.getMessage() << "Failed quota node meld: " << e.what();
      throw ex;
    }
  }

  pQuotaStats->removeNode(container->getId());
}

//------------------------------------------------------------------------------
// Rename container
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::renameContainer(IContainerMD* container,
                                  const std::string& newName)
{
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  if (newName.empty()) {
    MDException ex;
    ex.getMessage() << "Invalid new name (empty)";
    throw ex;
  }

  if (newName.find('/') != std::string::npos) {
    MDException ex;
    ex.getMessage() << "Name cannot contain slashes: " << newName;
    throw ex;
  }

  if (container->getId() == container->getParentId()) {
    MDException ex;
    ex.getMessage() << "Cannot rename /";
    throw ex;
  }

  std::shared_ptr<IContainerMD> parent{
    pContainerSvc->getContainerMD(container->getParentId())};

  if (parent->findContainer(newName) != nullptr) {
    MDException ex;
    ex.getMessage() << "Container exists: " << newName;
    throw ex;
  }

  if (parent->findFile(newName) != nullptr) {
    MDException ex;
    ex.getMessage() << "File exists: " << newName;
    throw ex;
  }

  parent->removeContainer(container->getName());
  container->setName(newName);
  parent->addContainer(container);
  updateContainerStore(container);
}

//------------------------------------------------------------------------------
// Rename file
//------------------------------------------------------------------------------
void
QuarkHierarchicalView::renameFile(IFileMD* file, const std::string& newName)
{
  if (file == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid file (zero pointer)";
    throw ex;
  }

  if (newName.empty()) {
    MDException ex;
    ex.getMessage() << "Invalid new name (empty)";
    throw ex;
  }

  if (newName.find('/') != std::string::npos) {
    MDException ex;
    ex.getMessage() << "Name cannot contain slashes: " << newName;
    throw ex;
  }

  std::shared_ptr<IContainerMD> parent{
    pContainerSvc->getContainerMD(file->getContainerId())};

  if (parent->findContainer(newName) != nullptr) {
    MDException ex;
    ex.getMessage() << "Container exists: " << newName;
    throw ex;
  }

  if (parent->findFile(newName) != nullptr) {
    MDException ex;
    ex.getMessage() << "File exists: " << newName;
    throw ex;
  }

  parent->removeFile(file->getName());
  file->setName(newName);
  parent->addFile(file);
  updateFileStore(file);
}

//------------------------------------------------------------------------------
// Get parent container of a file
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr> QuarkHierarchicalView::getParentContainer(
  IFileMD *file)
{
  IContainerMD::id_t parentId = file->getContainerId();
  return pContainerSvc->getContainerMDFut(parentId);
}

EOSNSNAMESPACE_END
