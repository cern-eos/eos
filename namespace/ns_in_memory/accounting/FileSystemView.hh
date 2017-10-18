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
// desc:   The filesystem view over the stored files
//------------------------------------------------------------------------------

#ifndef EOS_NS_FILESYSTEM_VIEW_HH
#define EOS_NS_FILESYSTEM_VIEW_HH

#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IFsView.hh"
#include <utility>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// File System iterator implementation of a in-memory namespace
// Trivial implementation, using the same logic to iterate over filesystems
// as we did with "getNumFileSystems" before.
//------------------------------------------------------------------------------
class FilesystemIterator : public IFsIterator
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FilesystemIterator(IFileMD::location_t maxfs) : pCurrentFS(0), pMaxFS(maxfs)
  {
  }

  //----------------------------------------------------------------------------
  //! Get current fsid
  //----------------------------------------------------------------------------
  IFileMD::location_t getFilesystemID() override
  {
    return pCurrentFS;
  }

  //----------------------------------------------------------------------------
  //! Check if iterator is valid
  //----------------------------------------------------------------------------
  bool valid() override
  {
    return pCurrentFS < pMaxFS;
  }

  //----------------------------------------------------------------------------
  //! Retrieve next fsid - returns false when no more filesystems exist
  //----------------------------------------------------------------------------
  void next() override
  {
    if(valid()) {
      pCurrentFS++;
    }
  }

private:
  IFileMD::location_t pCurrentFS;
  IFileMD::location_t pMaxFS;
};

//------------------------------------------------------------------------------
// File System view implementation of a in-memory namespace
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
  virtual ~FileSystemView() {}


  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e) override;

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  virtual void fileMDRead(IFileMD* obj) override;

  //----------------------------------------------------------------------------
  //! Recheck the current file object and make any modifications necessary so
  //! that the information is consistent - NOT used for this NS implementation
  //!
  //! @param obj file object to be checked
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool fileMDCheck(IFileMD* obj) override
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! Return reference to a list of files
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  FileList getFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Return reference to a list of unlinked files
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  FileList getUnlinkedFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Clear unlinked files for filesystem
  //!
  //! @param location filssystem id
  //!
  //! @return True if cleanup done successfully, otherwise false.
  //----------------------------------------------------------------------------
  bool clearUnlinkedFileList(IFileMD::location_t location) override;

  //----------------------------------------------------------------------------
  //! Get iterator object to run through all currently active filesystem IDs
  //----------------------------------------------------------------------------
  std::shared_ptr<IFsIterator> getFilesystemIterator() override
  {
    return std::shared_ptr<IFsIterator>(new FilesystemIterator(pFiles.size()));
  }

  //----------------------------------------------------------------------------
  //! Get list of files without replicas
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  FileList getNoReplicasFileList() override
  {
    return pNoReplicas;
  }

  //----------------------------------------------------------------------------
  //! Configure
  //!
  //! @param config map of configuration parameters
  //----------------------------------------------------------------------------
  void configure(const std::map<std::string, std::string>& config) override {}

  //----------------------------------------------------------------------------
  //! Finalize
  //----------------------------------------------------------------------------
  void finalize() override;

  //----------------------------------------------------------------------------
  //! Shrink maps
  //----------------------------------------------------------------------------
  void shrink() override;

  //----------------------------------------------------------------------------
  //! Add tree
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD* obj, int64_t dsize) override {};

  //----------------------------------------------------------------------------
  //! Remove tree
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD* obj, int64_t dsize) override {};

private:
  std::vector<FileList> pFiles;
  std::vector<FileList> pUnlinkedFiles;
  FileList              pNoReplicas;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_FILESYSTEM_VIEW_HH
