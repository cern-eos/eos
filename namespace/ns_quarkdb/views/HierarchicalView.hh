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
#include "namespace/ns_quarkdb/views/PathLookupState.hh"

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Implementation of the hierarchical namespace
//------------------------------------------------------------------------------
class HierarchicalView : public IView
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  HierarchicalView();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~HierarchicalView();

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
  //! Create a file for given uri
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile(const std::string& uri,
      uid_t uid = 0, gid_t gid = 0) override;

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
  createContainer(const std::string& uri, bool createParents = false) override;

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
  virtual void removeContainer(const std::string& uri, bool recursive = false) override;

  //----------------------------------------------------------------------------
  //! Get uri for the container
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD* container) const override;

  //------------------------------------------------------------------------
  //! Get uri for container id
  //------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD::id_t cid) const override;

  //----------------------------------------------------------------------------
  //! Get uri for the file
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IFileMD* file) const override;

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

private:
  //----------------------------------------------------------------------------
  //! Get last existing container in the provided path
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> findLastContainer(std::vector<char*>& elements,
      size_t end, size_t& index,
      size_t* link_depths = 0);

  //----------------------------------------------------------------------------
  //! Lookup file, using the URL.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupFileURL(const std::string &uri, size_t symlinkDepth, bool follow);

  //----------------------------------------------------------------------------
  //! Lookup file inside a directory.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupFile(PathLookupState parent, std::string name, bool follow);

  //----------------------------------------------------------------------------
  //! Lookup symlink, expect to find a directory there.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupContainerSymlink(IFileMDPtr symlink, IContainerMDPtr parent, size_t symlinkDepth);

  //----------------------------------------------------------------------------
  //! Lookup a subdirectory asynchronously, while following symlinks.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupContainer(
    IContainerMDPtr root,
    const std::vector<std::string> &chunks,
    size_t symlinkDepth, bool follow);

  //----------------------------------------------------------------------------
  //! Lookup a subdirectory asynchronously, while following symlinks.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupContainer(IContainerMDPtr root,
    const std::string &url,
    size_t symlinkDepth, bool follow);

  //----------------------------------------------------------------------------
  //! Lookup a subdirectory asynchronously, while following symlinks.
  //----------------------------------------------------------------------------
  folly::Future<PathLookupState> lookupSubcontainer(
    PathLookupState parent, std::string name, bool follow);

  //----------------------------------------------------------------------------
  //! Clean up contents of container
  //!
  //! @param cont container object
  //----------------------------------------------------------------------------
  void cleanUpContainer(IContainerMD* cont);

  //----------------------------------------------------------------------------
  // Data members
  //----------------------------------------------------------------------------
  IContainerMDSvc* pContainerSvc;
  IFileMDSvc* pFileSvc;
  IQuotaStats* pQuotaStats;
  std::shared_ptr<IContainerMD> pRoot;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_REDIS_HIERARCHICAL_VIEW_HH__
