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

using std::placeholders::_1;

#ifdef __APPLE__
#define EBADFD 77
#endif

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
HierarchicalView::HierarchicalView()
  : pContainerSvc(nullptr), pFileSvc(nullptr),
    pQuotaStats(new QuotaStats()), pRoot(nullptr)
{
  pExecutor.reset(new folly::IOThreadPoolExecutor(8));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
HierarchicalView::~HierarchicalView()
{
  delete pQuotaStats;
}

//------------------------------------------------------------------------------
// Configure the view
//------------------------------------------------------------------------------
void
HierarchicalView::configure(const std::map<std::string, std::string>& config)
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
  pQuotaStats = new QuotaStats();
  pQuotaStats->configure(config);
}

//------------------------------------------------------------------------------
// Initialize the view
//------------------------------------------------------------------------------
void
HierarchicalView::initialize()
{
  initialize1();
  initialize2();
  initialize3();
}

void
HierarchicalView::initialize1()
{
  pContainerSvc->initialize();

  // Get root container
  try {
    pRoot = pContainerSvc->getContainerMD(1);
  } catch (MDException& e) {
    pRoot = pContainerSvc->createContainer();

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
HierarchicalView::initialize2()
{
  pFileSvc->initialize();
}

//------------------------------------------------------------------------------
// Initialize phase 3
//------------------------------------------------------------------------------
void
HierarchicalView::initialize3()
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
HierarchicalView::finalize()
{
  pContainerSvc->finalize();
  pFileSvc->finalize();
  delete pQuotaStats;
  pQuotaStats = nullptr;
}

//------------------------------------------------------------------------------
// Extract FileMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static folly::Future<IFileMDPtr> extractFileMD(FileOrContainerMD ptr) {
  if(!ptr.file) {
    return folly::makeFuture<IFileMDPtr>(make_mdexception(ENOENT, "No such file or directory"));
  }

  return ptr.file;
}

//------------------------------------------------------------------------------
// Extract ContainerMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static folly::Future<IContainerMDPtr> extractContainerMD(FileOrContainerMD ptr) {
  if(!ptr.container) {
    return folly::makeFuture<IContainerMDPtr>(make_mdexception(ENOENT, "No such file or directory"));
  }

  return ptr.container;
}

//------------------------------------------------------------------------------
// Lookup a given path.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
HierarchicalView::getPath(const std::string& uri, bool follow)
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
static FileOrContainerMD toFileOrContainerMD(IContainerMDPtr ptr) {
  return {nullptr, ptr};
}

//------------------------------------------------------------------------------
// Lookup a given path - deferred function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
HierarchicalView::getPathDeferred(folly::Future<FileOrContainerMD> fut, std::deque<std::string> pendingChunks,
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
    .then(std::bind(&HierarchicalView::getPathInternal, this, _1, pendingChunks, follow, expendedEffort));
}

//------------------------------------------------------------------------------
// Lookup a given path - deferred function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
HierarchicalView::getPathDeferred(folly::Future<IContainerMDPtr> fut, std::deque<std::string> pendingChunks,
  bool follow, size_t expendedEffort)
{
  //----------------------------------------------------------------------------
  // Same as getPathDeferred taking FileOrContainerMD.
  //----------------------------------------------------------------------------
  return fut.via(pExecutor.get())
    .then(toFileOrContainerMD)
    .then(std::bind(&HierarchicalView::getPathInternal, this, _1, pendingChunks, follow, expendedEffort));
}

//------------------------------------------------------------------------------
// Lookup a given path - internal function.
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
HierarchicalView::getPathInternal(FileOrContainerMD state, std::deque<std::string> pendingChunks,
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
  while(true) {

    //--------------------------------------------------------------------------
    // Protection against symbolic link loops.
    //--------------------------------------------------------------------------
    expendedEffort++;
    if(expendedEffort > 255) {
      return folly::makeFuture<FileOrContainerMD>(make_mdexception(
        ELOOP, "Too many symbolic links were encountered in translating the pathname"));
    }

    if(!state.container && !state.file) {
      //------------------------------------------------------------------------
      // The previous iteration of the loop resulted in an empty state: Only one
      // way to get here, looking up a non-existent chunk.
      //------------------------------------------------------------------------
      return folly::makeFuture<FileOrContainerMD>(make_mdexception(ENOENT, "No such file or directory"));
    }

    if(pendingChunks.empty()) {
      if( !(follow && state.file && state.file->isLink()) ) {
        //------------------------------------------------------------------------
        // We're done. Our current state contains the desired output.
        //------------------------------------------------------------------------
        return state;
      }
      else {
        //----------------------------------------------------------------------
        // Edge case: State is actually a symlink we must follow, not done yet.
        //----------------------------------------------------------------------
      }
    }

    if(state.container) {
      //------------------------------------------------------------------------
      // Handle special cases, "." and ".."
      //------------------------------------------------------------------------
      if(pendingChunks.front() == ".") {
        pendingChunks.pop_front();
        continue;
      }

      if(pendingChunks.front() == "..") {
        pendingChunks.pop_front();

        folly::Future<IContainerMDPtr> fut = pContainerSvc->getContainerMD(state.container->getParentId());

        if(!fut.isReady()) {
          //--------------------------------------------------------------------
          // We're blocked, "pause" execution, unblock caller.
          //--------------------------------------------------------------------
          return getPathDeferred(std::move(fut), pendingChunks, follow, expendedEffort);
        }

        state.container = fut.get();
        continue;
      }

      //------------------------------------------------------------------------
      // Normal case: Our current state contains a container, and we're simply
      // looking up the next chunk.
      //------------------------------------------------------------------------
      folly::Future<FileOrContainerMD> next = state.container->findItem(pendingChunks.front());
      pendingChunks.pop_front();

      //------------------------------------------------------------------------
      // If we're lucky, the result is ready immediately. Update state, and
      // carry on.
      //------------------------------------------------------------------------
      if(next.isReady()) {
        state = next.get();
        continue;
      }
      else {
        //----------------------------------------------------------------------
        // We're blocked, "pause" execution, unblock caller.
        //----------------------------------------------------------------------
        return getPathDeferred(std::move(next), pendingChunks, follow, expendedEffort);
      }
    }

    if(state.file) {
      //------------------------------------------------------------------------
      // This is unusual.. How come a file came up in the middle of a path
      // lookup?
      //
      // 1. We've hit a symlink.
      // 2. Caller is drunk, and doing "ls /eos/dir1/file1/not/existing".
      //------------------------------------------------------------------------
      if(!state.file->isLink()) {
        return folly::makeFuture<FileOrContainerMD>(make_mdexception(ENOTDIR, "Not a directory"));
      }

      //------------------------------------------------------------------------
      // Ok, this is definitely a symlink. Should we follow it?
      //------------------------------------------------------------------------
      if(pendingChunks.size() == 0u && !follow) {
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

      if(!symlinkTarget.empty() && symlinkTarget[0] == '/') {
        //----------------------------------------------------------------------
        // This is an absolute symlink: Our state becomes pRoot again.
        //----------------------------------------------------------------------
        state = FileOrContainerMD {nullptr, pRoot};
      }
      else {
        //----------------------------------------------------------------------
        // This is a relative symlink: State becomes symlink's parent container.
        //----------------------------------------------------------------------

        folly::Future<IContainerMDPtr> fut = pContainerSvc->getContainerMD(state.file->getContainerId());
        if(!fut.isReady()) {
          //--------------------------------------------------------------------
          // We're blocked, "pause" execution, unblock caller.
          //--------------------------------------------------------------------
          return getPathDeferred(std::move(fut), pendingChunks, follow, expendedEffort);
        }

        state.container = fut.get();
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
HierarchicalView::getFileFut(const std::string& uri, bool follow)
{
  return getPath(uri, follow).then(extractFileMD);
}

//------------------------------------------------------------------------------
// Retrieve a file for given uri
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
HierarchicalView::getFile(const std::string& uri, bool follow,
  size_t* link_depths)
{
  return getFileFut(uri, follow).get();
}

//------------------------------------------------------------------------------
// Create a file for given uri
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
HierarchicalView::createFile(const std::string& uri, uid_t uid, gid_t gid)
{

  if(uri == "/") {
    throw_mdexception(EEXIST, "File exists");
  }

  // Split the path and find the last container
  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  if(chunks.size() == 0u) {
    throw_mdexception(EEXIST, "File exists");
  }

  std::string lastChunk = chunks.back();
  chunks.pop_back();

  FileOrContainerMD item = getPathInternal(FileOrContainerMD {nullptr, pRoot}, chunks, true, 0).get();

  if(item.file) {
    throw_mdexception(ENOTDIR, "Not a directory");
  }

  IContainerMDPtr parent = item.container;

  FileOrContainerMD potentialConflict = parent->findItem(lastChunk).get();
  if(potentialConflict.file || potentialConflict.container) {
    throw_mdexception(EEXIST, "File exists");
  }

  IFileMDPtr file = pFileSvc->createFile();

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
HierarchicalView::createLink(const std::string& uri, const std::string& linkuri,
                             uid_t uid, gid_t gid)
{
  std::shared_ptr<IFileMD> file = createFile(uri, uid, gid);

  if (file) {
    file->setLink(linkuri);
    updateFileStore(file.get());
  }
}

//------------------------------------------------------------------------------
// Remove link
//------------------------------------------------------------------------------
void
HierarchicalView::removeLink(const std::string& uri)
{
  return unlinkFile(uri);
}

//------------------------------------------------------------------------------
// Unlink the file for given uri
//------------------------------------------------------------------------------
void
HierarchicalView::unlinkFile(const std::string& uri)
{
  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  if(chunks.size() == 0) {
    MDException e(ENOENT);
    e.getMessage() << "Not a file";
    throw e;
  }

  std::string lastChunk = chunks[chunks.size()-1];
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
HierarchicalView::unlinkFile(eos::IFileMD* file)
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
HierarchicalView::removeFile(IFileMD* file)
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
HierarchicalView::getContainerFut(const std::string& uri, bool follow)
{
  if (uri == "/") {
    return std::shared_ptr<IContainerMD> {pContainerSvc->getContainerMD(1)};
  }

  return getPath(uri, follow).then(extractContainerMD);
}

//------------------------------------------------------------------------------
// Get a container (directory)
//------------------------------------------------------------------------------
IContainerMDPtr
HierarchicalView::getContainer(const std::string& uri, bool follow,
                               size_t* link_depth)
{
  return getContainerFut(uri, follow).get();
}

//------------------------------------------------------------------------------
// Create container - method eventually consistent
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
HierarchicalView::createContainer(const std::string& uri, bool createParents)
{
  // Split the path
  if (uri == "/") {
    MDException e(EEXIST);
    e.getMessage() << uri << ": Container exist" << std::endl;
    throw e;
  }

  char uriBuffer[uri.length() + 1];
  strcpy(static_cast<char*>(uriBuffer), uri.c_str());
  std::vector<char*> elements;
  eos::PathProcessor::splitPath(elements, static_cast<char*>(uriBuffer));

  if (elements.empty()) {
    MDException e(EEXIST);
    e.getMessage() << uri << ": File exist" << std::endl;
    throw e;
  }

  // Look for the last existing container
  size_t position;
  std::shared_ptr<IContainerMD> lastContainer =
    findLastContainer(elements, elements.size(), position);

  if (position == elements.size()) {
    MDException e(EEXIST);
    e.getMessage() << uri << ": Container exist" << std::endl;
    throw e;
  }

  // One of the parent containers does not exist
  if ((!createParents) && (position < elements.size() - 1)) {
    MDException e(ENOENT);
    e.getMessage() << uri << ": Parent does not exist" << std::endl;
    throw e;
  }

  if (lastContainer->findFile(elements[position])) {
    MDException e(EEXIST);
    e.getMessage() << "File exists" << std::endl;
    throw e;
  }

  // Create the container with all missing parents if required. If a crash
  // happens during the addContainer call and the updateContainerStore then
  // we curate the list of subcontainers in the ContainerMD::findContainer
  // method.
  for (size_t i = position; i < elements.size(); ++i) {
    std::shared_ptr<IContainerMD> newContainer{
      pContainerSvc->createContainer()};
    newContainer->setName(elements[i]);
    newContainer->setCTimeNow();
    lastContainer->addContainer(newContainer.get());
    lastContainer.swap(newContainer);
    updateContainerStore(lastContainer.get());
  }

  return lastContainer;
}

//------------------------------------------------------------------------------
// Lookup a given path, expect a container there.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
HierarchicalView::getPathExpectContainer(const std::deque<std::string> &chunks)
{
  if(chunks.size() == 0u) {
    return pRoot;
  }

  return getPathInternal(FileOrContainerMD {nullptr, pRoot}, chunks, true, 0)
    .then(extractContainerMD);
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
HierarchicalView::removeContainer(const std::string& uri)
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
  std::string lastChunk = chunks[chunks.size()-1];
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
// Find the last existing container in the path
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
HierarchicalView::findLastContainer(std::vector<char*>& elements, size_t end,
                                    size_t& index, size_t* link_depths)
{
  std::shared_ptr<IContainerMD> current = pRoot;
  std::shared_ptr<IContainerMD> found;
  size_t position = 0;

  while (position < end) {
    found = current->findContainer(elements[position]);

    if (!found) {
      // Check if link
      std::shared_ptr<IFileMD> flink = current->findFile(elements[position]);

      if (flink) {
        if (flink->isLink()) {
          if (link_depths != nullptr) {
            (*link_depths)++;

            if ((*link_depths) > 255) {
              MDException e(ELOOP);
              e.getMessage() << "Too many symbolic links were encountered "
                             "in translating the pathname";
              throw e;
            }
          }

          std::string link = flink->getLink();

          if (link[0] != '/') {
            link.insert(0, getUri(current.get()));
            eos::PathProcessor::absPath(link);
          }

          found = getContainer(link, false, link_depths);

          if (!found) {
            index = position;
            return current;
          }
        }
      }

      if (!found) {
        index = position;
        return current;
      }
    }

    current = found;
    ++position;
  }

  index = position;
  return current;
}

//------------------------------------------------------------------------------
// Get uri for the container
//------------------------------------------------------------------------------
std::string
HierarchicalView::getUri(const IContainerMD* container) const
{
  // Check the input
  if (container == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid container (zero pointer)";
    throw ex;
  }

  return getUri(container->getId());
}

//------------------------------------------------------------------------------
// Get uri for the container - asynchronous version
//------------------------------------------------------------------------------
folly::Future<std::string>
HierarchicalView::getUriFut(const IContainerMD* container) const
{
  return folly::via(pExecutor.get()).then([this, container]() {
    return this->getUri(container);
  } );
}

//------------------------------------------------------------------------------
// Get uri for container id
//------------------------------------------------------------------------------
std::string
HierarchicalView::getUri(const IContainerMD::id_t cid) const
{
  // Gather the uri elements
  std::vector<std::string> elements;
  elements.reserve(10);
  std::shared_ptr<IContainerMD> cursor = pContainerSvc->getContainerMD(cid);

  while (cursor->getId() != 1) {
    elements.push_back(cursor->getName());
    cursor = pContainerSvc->getContainerMD(cursor->getParentId());
  }

  // Assemble the uri
  std::string path = "/";
  std::vector<std::string>::reverse_iterator rit;

  for (rit = elements.rbegin(); rit != elements.rend(); ++rit) {
    path += *rit;
    path += "/";
  }

  return path;
}

//------------------------------------------------------------------------------
// Get uri for the container - asynchronous version
//------------------------------------------------------------------------------
folly::Future<std::string>
HierarchicalView::getUriFut(const IFileMD* file) const
{
  return folly::via(pExecutor.get()).then([this, file]() {
    return this->getUri(file);
  } );
}

//------------------------------------------------------------------------------
// Get uri for the file
//------------------------------------------------------------------------------
std::string
HierarchicalView::getUri(const IFileMD* file) const
{
  // Check the input
  if (file == nullptr) {
    MDException ex;
    ex.getMessage() << "Invalid file (zero pointer)";
    throw ex;
  }

  // Get the uri
  std::shared_ptr<IContainerMD> cont =
    pContainerSvc->getContainerMD(file->getContainerId());
  std::string path = getUri(cont.get());
  return path + file->getName();
}

//------------------------------------------------------------------------
// Get real path translating existing symlink
//------------------------------------------------------------------------
std::string HierarchicalView::getRealPath(const std::string& uri)
{
  if (uri == "/") {
    MDException e(ENOENT);
    e.getMessage() << " is not a file";
    throw e;
  }

  std::deque<std::string> chunks;
  eos::PathProcessor::insertChunksIntoDeque(chunks, uri);

  eos_assert(chunks.size() != 0);
  if(chunks.size() == 1) return chunks[0];

  //----------------------------------------------------------------------------
  // Remove last chunk
  //----------------------------------------------------------------------------
  std::string lastChunk = chunks[chunks.size()-1];
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
HierarchicalView::getQuotaNode(const IContainerMD* container, bool search)
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

  // Search for the node
  std::shared_ptr<IContainerMD> current =
    pContainerSvc->getContainerMD(container->getId());

  if (search) {
    while (current->getName() != pRoot->getName() &&
           (current->getFlags() & QUOTA_NODE_FLAG) == 0) {
      current = pContainerSvc->getContainerMD(current->getParentId());
    }
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
HierarchicalView::registerQuotaNode(IContainerMD* container)
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
HierarchicalView::removeQuotaNode(IContainerMD* container)
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
HierarchicalView::renameContainer(IContainerMD* container,
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
HierarchicalView::renameFile(IFileMD* file, const std::string& newName)
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

EOSNSNAMESPACE_END
