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
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @brief The filesystem view stored in Redis
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FILESYSTEM_VIEW_HH__
#define __EOS_NS_FILESYSTEM_VIEW_HH__

#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include <utility>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! FileSystemView implementation on top of Redis
//!
//! This class keeps a mapping between filesystem ids and the actual file ids
//! that reside on that particular filesystem. For each fs id we keep a set
//! structure in Redis i.e. fsview_files:fs_ids that holds the file ids. E.g.:
//!
//! fsview_files:1 -->  fid4, fid87, fid1002 etc.
//! fsview_files:2 ...
//! ...
//! fsview_files:n ...
//!
//! Besides these data structures we alos have:
//!
//! fsview_set_fsid   - set with all the file system ids used
//! fsview_noreplicas - file ids that don't have any replicas on any fs
//! fsview_unlinked:x - set of file ids that are unlinked on file system "x"
//------------------------------------------------------------------------------
class FileSystemView: public IFsView
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemView();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileSystemView() {};


  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e);

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  virtual void fileMDRead(IFileMD* obj);

  //----------------------------------------------------------------------------
  //! Return set of files on filesystem
  //! BEWARE: any replica change may invalidate iterators
  //! @param location filesystem identifier
  //!
  //! @return set of files on filesystem
  //----------------------------------------------------------------------------
  IFsView::FileList getFileList(IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Return set of unlinked files
  //! BEWARE: any replica change may invalidate iterators
  //! @param location filesystem identifier
  //!
  //! @return set of unlinked files
  //----------------------------------------------------------------------------
  IFsView::FileList getUnlinkedFileList(IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Get set of files without replicas
  //! BEWARE: any replica change may invalidate iterators
  //!
  //! @return set of files with no replicas
  //----------------------------------------------------------------------------
  IFsView::FileList getNoReplicasFileList();

  //----------------------------------------------------------------------------
  //! Clear unlinked files for filesystem
  //!
  //! @param location filssystem id
  //!
  //! @return true if cleanup done successfully, otherwise false
  //----------------------------------------------------------------------------
  bool clearUnlinkedFileList(IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Get number of file systems
  //!
  //! @return number of file systems
  //----------------------------------------------------------------------------
  size_t getNumFileSystems();

  //----------------------------------------------------------------------------
  //! Initizalie
  //----------------------------------------------------------------------------
  void initialize();

  //----------------------------------------------------------------------------
  //! Initizalie for testing purposes
  //----------------------------------------------------------------------------
  void initialize(const std::map<std::string, std::string>& config);


  //----------------------------------------------------------------------------
  //! Finalize
  //----------------------------------------------------------------------------
  void finalize();

 private:

  // Redis related variables
  redox::Redox* pRedox; ///< Redix C++ client
  //! Set prefix for file ids on a fs
  static const std::string sFilesPrefix;
  //! Set prefix for unlinked file ids on a fs
  static const std::string sUnlinkedPrefix;
  //! Set prefix for file ids with no replicas
  static const std::string sNoReplicaPrefix;
  static const std::string sSetFsIds; ///< Set of FS ids
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILESYSTEM_VIEW_HH__
