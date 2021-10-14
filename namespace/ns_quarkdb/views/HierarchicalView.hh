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
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Hierarchical namespace implementation
//------------------------------------------------------------------------------

#ifndef __EOS_NS_REDIS_HIERARHICAL_VIEW_HH__
#define __EOS_NS_REDIS_HIERARHICAL_VIEW_HH__

#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif

EOSNSNAMESPACE_BEGIN

class MetadataFlusher;

//------------------------------------------------------------------------------
//! Implementation of the hierarchical namespace
//------------------------------------------------------------------------------
class QuarkHierarchicalView : public IView
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkHierarchicalView(qclient::QClient *qcl, MetadataFlusher *quotaFlusher);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkHierarchicalView();

  //----------------------------------------------------------------------------
  //! Specify a pointer to the underlying container service
  //----------------------------------------------------------------------------
  virtual void
  setContainerMDSvc(IContainerMDSvc* containerSvc) override
  {
    pContainerSvc = containerSvc;
  }

  //----------------------------------------------------------------------------
  //! Get the container svc pointer
  //----------------------------------------------------------------------------
  virtual IContainerMDSvc*
  getContainerMDSvc() override
  {
    return pContainerSvc;
  }

  //----------------------------------------------------------------------------
  //! Specify a pointer to the underlying file service that alocates the
  //! actual files
  //----------------------------------------------------------------------------
  virtual void
  setFileMDSvc(IFileMDSvc* fileMDSvc) override
  {
    pFileSvc = fileMDSvc;
  }

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc
  //----------------------------------------------------------------------------
  virtual IFileMDSvc*
  getFileMDSvc() override
  {
    return pFileSvc;
  }

  //----------------------------------------------------------------------------
  //! Configure the view
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config) override;

  //----------------------------------------------------------------------------
  //! Initialize the view
  //----------------------------------------------------------------------------
  virtual void initialize()  override;
  virtual void initialize1() override; // phase 1 - load & setup container
  virtual void initialize2() override; // phase 2 - load files
  virtual void initialize3() override; // phase 3 - register files in container

  //----------------------------------------------------------------------------
  //! Finalize the view
  //----------------------------------------------------------------------------
  virtual void finalize() override;

  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri, asynchronously
  //----------------------------------------------------------------------------
  virtual folly::Future<IFileMDPtr>
  getFileFut(const std::string& uri, bool follow = true) override;

  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD>
  getFile(const std::string& uri, bool follow = true, size_t* link_depths = 0) override;

  //----------------------------------------------------------------------------
  //! Retrieve an item for given path. Could be either file or container, we
  //! don't know.
  //----------------------------------------------------------------------------
  virtual folly::Future<FileOrContainerMD>
  getItem(const std::string& uri, bool follow = true) override;

  //----------------------------------------------------------------------------
  //! Create a file for given uri
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile(const std::string& uri,
					      uid_t uid = 0, gid_t gid = 0, IFileMD::id_t id = 0) override;

  //----------------------------------------------------------------------------
  //! Create a link for given uri
  //----------------------------------------------------------------------------
  virtual void createLink(const std::string& uri, const std::string& linkuri,
                          uid_t uid = 0, gid_t gid = 0) override;

  //----------------------------------------------------------------------------
  //! Update file store
  //----------------------------------------------------------------------------
  virtual void
  updateFileStore(IFileMD* file) override
  {
    pFileSvc->updateStore(file);
  }

  //----------------------------------------------------------------------------
  //! Remove a link
  //----------------------------------------------------------------------------
  virtual void removeLink(const std::string& uri) override;

  //----------------------------------------------------------------------------
  //! Unlink the file
  //!
  //! @param uri full path to file to be unlinked
  //----------------------------------------------------------------------------
  virtual void unlinkFile(const std::string& uri) override;

  //----------------------------------------------------------------------------
  //! Unlink the file
  //!
  //! @param file IFileMD object
  //----------------------------------------------------------------------------
  virtual void unlinkFile(eos::IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Remove the file
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Get a container (directory) asynchronously
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr> getContainerFut(const std::string& uri,
      bool follow = true) override;

  //----------------------------------------------------------------------------
  //! Get a container (directory)
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> getContainer(const std::string& uri,
      bool follow = true,
      size_t* link_depth = 0) override;

  //----------------------------------------------------------------------------
  //! Create a container (directory)
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  createContainer(const std::string& uri, bool createParents = false, uint64_t cid = 0) override;

  //----------------------------------------------------------------------------
  //! Update container store
  //----------------------------------------------------------------------------
  virtual void
  updateContainerStore(IContainerMD* container) override
  {
    pContainerSvc->updateStore(container);
  }

  //----------------------------------------------------------------------------
  //! Remove a container (directory)
  //----------------------------------------------------------------------------
  virtual void removeContainer(const std::string& uri) override;

  //----------------------------------------------------------------------------
  //! Get uri for the container
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD* container) const override;

  //----------------------------------------------------------------------------
  //! Get uri for the container - asynchronous version
  //----------------------------------------------------------------------------
  virtual folly::Future<std::string> getUriFut(ContainerIdentifier id) const override;

  //------------------------------------------------------------------------
  //! Get uri for container id
  //------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD::id_t cid) const override;

  //----------------------------------------------------------------------------
  //! Get uri for the file
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IFileMD* file) const override;

  //----------------------------------------------------------------------------
  //! Get uri for the file - asynchronous version
  //----------------------------------------------------------------------------
  virtual folly::Future<std::string> getUriFut(FileIdentifier id) const override;

  //------------------------------------------------------------------------
  //! Get real path translating existing symlink
  //------------------------------------------------------------------------
  virtual std::string getRealPath(const std::string& path) override;

  //----------------------------------------------------------------------------
  //! Get quota node id concerning given container
  //----------------------------------------------------------------------------
  virtual IQuotaNode* getQuotaNode(const IContainerMD* container,
                                   bool search = true) override;

  //----------------------------------------------------------------------------
  //! Register the container to be a quota node
  //----------------------------------------------------------------------------
  virtual IQuotaNode* registerQuotaNode(IContainerMD* container) override;

  //----------------------------------------------------------------------------
  //! Remove the quota node
  //----------------------------------------------------------------------------
  virtual void removeQuotaNode(IContainerMD* container) override;

  //----------------------------------------------------------------------------
  //! Get the quota stats placeholder
  //----------------------------------------------------------------------------
  virtual IQuotaStats*
  getQuotaStats() override
  {
    return pQuotaStats;
  }

  //----------------------------------------------------------------------------
  //! Set the quota stats placeholder, currently associated object (if any)
  //! won't beX deleted.
  //----------------------------------------------------------------------------
  virtual void
  setQuotaStats(IQuotaStats* quotaStats) override
  {
    if (pQuotaStats) {
      delete pQuotaStats;
    }

    pQuotaStats = quotaStats;
  }

  //----------------------------------------------------------------------------
  //! Rename container
  //----------------------------------------------------------------------------
  virtual void renameContainer(IContainerMD* container,
                               const std::string& newName) override;

  //----------------------------------------------------------------------------
  //! Rename file
  //----------------------------------------------------------------------------
  virtual void renameFile(IFileMD* file, const std::string& newName) override;

  //----------------------------------------------------------------------------
  //! Return whether this is an in-memory namespace.
  //----------------------------------------------------------------------------
  virtual bool inMemory() override {
    return false;
  }

  //----------------------------------------------------------------------------
  //! Get parent container of a file
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr> getParentContainer(
    IFileMD *file) override;

private:
  //----------------------------------------------------------------------------
  //! Lookup a given path - internal function.
  //----------------------------------------------------------------------------
  folly::Future<FileOrContainerMD>
  getPathInternal(FileOrContainerMD state, std::deque<std::string> pendingChunks,
    bool follow, size_t expendedEffort);

  //----------------------------------------------------------------------------
  //! Lookup a given path - deferred function.
  //----------------------------------------------------------------------------
  folly::Future<FileOrContainerMD>
  getPathDeferred(folly::Future<FileOrContainerMD> fut, std::deque<std::string> pendingChunks,
    bool follow, size_t expendedEffort);

  //----------------------------------------------------------------------------
  //! Lookup a given path - deferred function.
  //----------------------------------------------------------------------------
  folly::Future<FileOrContainerMD>
  getPathDeferred(folly::Future<IContainerMDPtr> fut, std::deque<std::string> pendingChunks,
    bool follow, size_t expendedEffort);

  //----------------------------------------------------------------------------
  //! Lookup a given path, expect a container there.
  //----------------------------------------------------------------------------
  folly::Future<IContainerMDPtr>
  getPathExpectContainer(const std::deque<std::string> &chunks);

  //----------------------------------------------------------------------------
  //! Build the URL of the given container, as a deque of chunks. Primary
  //! "resumable" function.
  //----------------------------------------------------------------------------
  folly::Future<std::deque<std::string>> getUriInternal(std::deque<std::string>
    currentChunks, IContainerMDPtr nextToLookup) const;

  //----------------------------------------------------------------------------
  //! Build the URL of the given file, as a deque of chunks.
  //----------------------------------------------------------------------------
  folly::Future<std::deque<std::string>> getUriInternalFmdPtr(IFileMDPtr fmd) const;
  folly::Future<std::deque<std::string>> getUriInternalFmd(const IFileMD *fmd) const;

  //----------------------------------------------------------------------------
  //! Build the URL of the given container ID.
  //----------------------------------------------------------------------------
  folly::Future<std::deque<std::string>> getUriInternalCid(
    std::deque<std::string> currentChunks, ContainerIdentifier cid) const;

  //----------------------------------------------------------------------------
  //! Build the URL of the given fid
  //----------------------------------------------------------------------------
  folly::Future<std::deque<std::string>> getUriInternalFid(FileIdentifier fid) const;

  //----------------------------------------------------------------------------
  //! Clean up contents of container
  //!
  //! @param cont container object
  //----------------------------------------------------------------------------
  void cleanUpContainer(IContainerMD* cont);

  //----------------------------------------------------------------------------
  // Data members
  //----------------------------------------------------------------------------
  qclient::QClient *pQcl;
  MetadataFlusher *pQuotaFlusher;
  IContainerMDSvc* pContainerSvc;
  IFileMDSvc* pFileSvc;
  IQuotaStats* pQuotaStats;
  std::shared_ptr<IContainerMD> pRoot;
  std::unique_ptr<folly::Executor> pExecutor;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_REDIS_HIERARCHICAL_VIEW_HH__
