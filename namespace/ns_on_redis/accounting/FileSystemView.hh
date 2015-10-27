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
//! @brief The filesystem view which is stored in Redis
//------------------------------------------------------------------------------

#ifndef EOS_NS_FILESYSTEM_VIEW_HH
#define EOS_NS_FILESYSTEM_VIEW_HH

#include "redox.hpp"
#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IFsView.hh"
#include <utility>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// File System view implementation of a in-memeory namespace
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
  virtual ~FileSystemView();


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
  //! Finalize
  //----------------------------------------------------------------------------
  void finalize();

 private:

  // Redis related variables
  redox::Redox pRedox; ///< Redix C++ client
  //! Set prefix for file ids on a fs
  static const std::string sFilesPrefix;
  //! Set prefix for unlinked file ids on a fs
  static const std::string sUnlinkedPrefix;
  //! Set prefix for file ids with no replicas
  static const std::string sNoReplicaPrefix;
  static const std::string sSetFsIds; ///< Set of FS ids
};

EOSNSNAMESPACE_END

#endif // EOS_NS_FILESYSTEM_VIEW_HH
