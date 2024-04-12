/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   View service interface
//------------------------------------------------------------------------------

#ifndef EOS_NS_I_VIEW_HH
#define EOS_NS_I_VIEW_HH

#include <map>
#include <string>
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/MDLocking.hh"

namespace eos
{
class IQuotaNode;
class IQuotaStats;

//------------------------------------------------------------------------------
//! Interface for the component responsible for the namespace.
//! A concrete implementation could handle a hierarchical namespace,
//! lists of files in the fileservers, lists of files belonging
//! to users, container based store etc.
//------------------------------------------------------------------------------
class IView
{
public:

  //----------------------------------------------------------------------------
  //! Specify a pointer to the underlying container service
  //----------------------------------------------------------------------------
  virtual void setContainerMDSvc(IContainerMDSvc* containerSvc) = 0;

  //----------------------------------------------------------------------------
  //! Get the container svc pointer
  //----------------------------------------------------------------------------
  virtual IContainerMDSvc* getContainerMDSvc() = 0;

  //----------------------------------------------------------------------------
  //! Specify a pointer to the underlying file service, that alocates the
  //! actual files
  //----------------------------------------------------------------------------
  virtual void setFileMDSvc(IFileMDSvc* fileMDSvc) = 0;

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileMDSvc() = 0;

  //----------------------------------------------------------------------------
  //! Configure the view
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config) = 0;

  //----------------------------------------------------------------------------
  //! Initialize the view
  //----------------------------------------------------------------------------
  virtual void initialize() = 0;

  virtual void initialize1() = 0;
  virtual void initialize2() = 0;
  virtual void initialize3() = 0;

  //----------------------------------------------------------------------------
  //! Finalizelize the view
  //----------------------------------------------------------------------------
  virtual void finalize() = 0;

  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri, asynchronously
  //----------------------------------------------------------------------------
  virtual folly::Future<IFileMDPtr> getFileFut(const std::string& uri, bool follow = true) = 0;

  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFile(const std::string& uri,
      bool follow = true,
      size_t* link_depths = 0) = 0;


  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri and read-lock it
  //----------------------------------------------------------------------------
  virtual MDLocking::FileReadLockPtr
  getFileReadLocked(const std::string & uri,
                                                                        bool follow = true,
                                                                        size_t * link_depths = 0) = 0;

  //----------------------------------------------------------------------------
  //! Retrieve a file for given uri and write-lock it
  //----------------------------------------------------------------------------
  virtual MDLocking::FileWriteLockPtr
  getFileWriteLocked(const std::string & uri,
                                                                        bool follow = true,
                                                                        size_t * link_depths = 0) = 0;


  //----------------------------------------------------------------------------
  //! Retrieve an item for given path. Could be either file or container, we
  //! don't know.
  //----------------------------------------------------------------------------
  virtual folly::Future<FileOrContainerMD>
  getItem(const std::string& uri, bool follow = true) = 0;

  //----------------------------------------------------------------------------
  //! Update file store
  //----------------------------------------------------------------------------
  virtual void updateFileStore(IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Create a file for given uri
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile(const std::string& uri,
					      uid_t uid = 0,
					      gid_t gid = 0,
					      uint64_t fid = 0 ) = 0;

  //----------------------------------------------------------------------------
  //! Create a link for given uri
  //----------------------------------------------------------------------------
  virtual void createLink(const std::string& uri,
                          const std::string& linkuri,
                          uid_t uid = 0, gid_t gid = 0) = 0;

  //----------------------------------------------------------------------------
  //! Remove the file - the pointer is not valid any more once the call
  //! returns
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Remove a link
  //----------------------------------------------------------------------------
  virtual void removeLink(const std::string& uri) = 0;

  //----------------------------------------------------------------------------
  //! Remove the file from the hierarchy so that it won't be accessible
  //! by path anymore and unlink all of it's replicas. The file needs
  //! to be manually removed (ie. using removeFile method) once it has
  //! no valid replicas.
  //!
  //! @param uri full path to file to be unlinked
  //----------------------------------------------------------------------------
  virtual void unlinkFile(const std::string& uri) = 0;

  //----------------------------------------------------------------------------
  //! Remove the file from the hierarchy so that it won't be accessible
  //! by path anymore and unlink all of it's replicas. The file needs
  //! to be manually removed (ie. using removeFile method) once it has
  //! no valid replicas.
  //!
  //! @param file IFileMD object to be removed
  //----------------------------------------------------------------------------
  virtual void unlinkFile(eos::IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Get a container (directory) asynchronously
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr> getContainerFut(const std::string& uri,
      bool follow = true) = 0;

  //----------------------------------------------------------------------------
  //! Get a container (directory)
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> getContainer(const std::string& uri,
      bool follow = true,
      size_t* link_depths = 0) = 0;

  //----------------------------------------------------------------------------
  //! Get a container (directory) and read lock it
  //----------------------------------------------------------------------------
  virtual MDLocking::ContainerReadLockPtr
  getContainerReadLocked(const std::string & uri,
                                                                         bool follow = true,
                                                                         size_t * link_depths = 0) = 0;
  //----------------------------------------------------------------------------
  //! Get a container (directory) and write lock it
  //----------------------------------------------------------------------------
  virtual MDLocking::ContainerWriteLockPtr
  getContainerWriteLocked(const std::string & uri,
                                                                           bool follow = true,
                                                                           size_t * link_depths = 0) = 0;

  //----------------------------------------------------------------------------
  //! Get parent container of a file
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr> getParentContainer(
    IFileMD *file) = 0;

  //----------------------------------------------------------------------------
  //! Create a container (directory)
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer(const std::string& uri,
							bool createParents = false, 
							uint64_t cid = 0) = 0;

  //----------------------------------------------------------------------------
  //! Update container store
  //----------------------------------------------------------------------------
  virtual void updateContainerStore(IContainerMD* container) = 0;

  //----------------------------------------------------------------------------
  //! Remove a container (directory)
  //----------------------------------------------------------------------------
  virtual void removeContainer(const std::string& uri) = 0;

  //----------------------------------------------------------------------------
  //! Get uri for the container
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD* container) const = 0;

  //----------------------------------------------------------------------------
  //! Get uri for the container - asynchronous version
  //----------------------------------------------------------------------------
  virtual folly::Future<std::string> getUriFut(ContainerIdentifier id) const = 0;

  //----------------------------------------------------------------------------
  //! Get uri for container id
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IContainerMD::id_t cid) const = 0;

  //----------------------------------------------------------------------------
  //! Get uri for the file
  //----------------------------------------------------------------------------
  virtual std::string getUri(const IFileMD* file) const = 0;

  //----------------------------------------------------------------------------
  //! Get uri for the file - asynchronous version
  //----------------------------------------------------------------------------
  virtual folly::Future<std::string> getUriFut(FileIdentifier id) const = 0;

  //----------------------------------------------------------------------------
  //! Get real path translating existing symlink
  //----------------------------------------------------------------------------
  virtual std::string getRealPath(const std::string& path) = 0;

  //----------------------------------------------------------------------------
  //! Get quota node id concerning given container
  //----------------------------------------------------------------------------
  virtual IQuotaNode* getQuotaNode(const IContainerMD* container,
                                   bool search = true) = 0;

  //----------------------------------------------------------------------------
  //! Register the container to be a quota node
  //----------------------------------------------------------------------------
  virtual IQuotaNode* registerQuotaNode(IContainerMD* container) = 0;

  //----------------------------------------------------------------------------
  //! Remove the quota node
  //----------------------------------------------------------------------------
  virtual void removeQuotaNode(IContainerMD* container) = 0;

  //----------------------------------------------------------------------------
  //! Get the quota stats placeholder
  //----------------------------------------------------------------------------
  virtual IQuotaStats* getQuotaStats() = 0;

  //----------------------------------------------------------------------------
  //! Set the quota stats placeholder, currently associated object (if any)
  //! won't beX deleted.
  //----------------------------------------------------------------------------
  virtual void setQuotaStats(IQuotaStats* quotaStats) = 0;

  //----------------------------------------------------------------------------
  //! Rename container
  //----------------------------------------------------------------------------
  virtual void renameContainer(IContainerMD* container,
                               const std::string& newName) = 0;

  //----------------------------------------------------------------------------
  //! Rename file
  //----------------------------------------------------------------------------
  virtual void renameFile(IFileMD* file, const std::string& newName) = 0;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IView() {}

  //----------------------------------------------------------------------------
  //! Return whether this is an in-memory namespace.
  //----------------------------------------------------------------------------
  virtual bool inMemory() = 0;
};
};

#endif // EOS_NS_I_VIEW_HH
