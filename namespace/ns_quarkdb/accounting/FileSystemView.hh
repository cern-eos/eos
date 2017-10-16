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
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @brief The filesystem view stored in Redis
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FILESYSTEM_VIEW_HH__
#define __EOS_NS_FILESYSTEM_VIEW_HH__

#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include <utility>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! File System iterator implementation on top of Redis.
//! The proper solution would be that the object itself contacts redis running
//! SCAN, but this should be fine for now.
//------------------------------------------------------------------------------
class FilesystemIterator : public IFsIterator
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FilesystemIterator(std::set<IFileMD::location_t> &&filesystems)
  {
    pFilesystems = std::move(filesystems);
    iterator = pFilesystems.begin();
  }

  //----------------------------------------------------------------------------
  //! Get current fsid
  //----------------------------------------------------------------------------
  IFileMD::location_t getFilesystemID() override
  {
    return *iterator;
  }

  //----------------------------------------------------------------------------
  //! Check if iterator is valid
  //----------------------------------------------------------------------------
  bool valid() override
  {
    return iterator != pFilesystems.end();
  }

  //----------------------------------------------------------------------------
  //! Progress iterator by 1 - only has any effect if iterator is valid
  //----------------------------------------------------------------------------
  void next() override
  {
    if(valid()) {
      iterator++;
    }
  }

private:
  std::set<IFileMD::location_t> pFilesystems;
  std::set<IFileMD::location_t>::iterator iterator;
};

//------------------------------------------------------------------------------
//! FileSystemView implementation on top of Redis
//!
//! This class keeps a mapping between filesystem ids and the actual file ids
//! that reside on that particular filesystem. For each fs id we keep a set
//! structure in Redis i.e. fs_id:fsview_files that holds the file ids. E.g.:
//!
//! 1:fsview_files -->  fid4, fid87, fid1002 etc.
//! 2:fsview_files ...
//! ...
//! n:fsview_files ...
//!
//! Besides these data structures we also have:
//!
//! fsview_set_fsid   - set with all the file system ids used
//! fsview_noreplicas - file ids that don't have any replicas on any fs
//! x:fsview_unlinked - set of file ids that are unlinked on file system "x"
//------------------------------------------------------------------------------
class FileSystemView : public IFsView
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemView();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileSystemView() = default;

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e) override;

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog - not used
  //----------------------------------------------------------------------------
  virtual void fileMDRead(IFileMD* obj) override {};

  //----------------------------------------------------------------------------
  //! Recheck the current file object and make any modifications necessary so
  //! that the information is consistent in the back-end KV store.
  //!
  //! @param file file object to be checked
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool fileMDCheck(IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Return set of files on filesystem
  //! BEWARE: any replica change may invalidate iterators
  //! @param location filesystem identifier
  //!
  //! @return set of files on filesystem
  //----------------------------------------------------------------------------
  IFsView::FileList getFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Return set of unlinked files
  //! BEWARE: any replica change may invalidate iterators
  //! @param location filesystem identifier
  //!
  //! @return set of unlinked files
  //----------------------------------------------------------------------------
  IFsView::FileList getUnlinkedFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Get set of files without replicas
  //! BEWARE: any replica change may invalidate iterators
  //!
  //! @return set of files with no replicas
  //----------------------------------------------------------------------------
  IFsView::FileList getNoReplicasFileList() override;

  //----------------------------------------------------------------------------
  //! Clear unlinked files for filesystem
  //!
  //! @param location filssystem id
  //!
  //! @return true if cleanup done successfully, otherwise false
  //----------------------------------------------------------------------------
  bool clearUnlinkedFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Get number of file systems
  //!
  //! @return number of file systems
  //----------------------------------------------------------------------------
  size_t getNumFileSystems() override;

  //----------------------------------------------------------------------------
  //! Get iterator object to run through all currently active filesystem IDs
  //----------------------------------------------------------------------------
  std::shared_ptr<IFsIterator> getFilesystemIterator() override;

  //----------------------------------------------------------------------------
  //! Configure
  //!
  //! @param config map of configuration parameters
  //----------------------------------------------------------------------------
  void configure(const std::map<std::string, std::string>& config) override;

  //----------------------------------------------------------------------------
  //! Finalize
  //----------------------------------------------------------------------------
  void finalize() override {};

  //----------------------------------------------------------------------------
  //! Shrink maps
  //----------------------------------------------------------------------------
  void shrink() override {};

  //----------------------------------------------------------------------------
  //! Add tree - no-op for this type of view
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD* obj, int64_t dsize) override {};

  //----------------------------------------------------------------------------
  //! Remove tree - no-op for this type of view
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD* obj, int64_t dsize) override {};

private:
  MetadataFlusher* pFlusher; ///< Metadata flusher object
  qclient::QClient* pQcl;    ///< QClient object
  qclient::QSet pNoReplicasSet; ///< Set of file ids without replicas
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILESYSTEM_VIEW_HH__
