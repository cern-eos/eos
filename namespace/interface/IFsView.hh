//------------------------------------------------------------------------------
//! @file IFsView.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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

#ifndef __EOS_NS_IFSVIEW_HH__
#define __EOS_NS_IFSVIEW_HH__

#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <google/dense_hash_set>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! File System view abtract class
//------------------------------------------------------------------------------
class IFsView: public IFileMDChangeListener
{
 public:

  //------------------------------------------------------------------------
  // Google sparse table is used for much lower memory overhead per item
  // than a list and it's fragmented structure speeding up deletions.
  // The filelists we keep are quite big - a list would be faster
  // but more memory consuming, a vector would be slower but less
  // memory consuming. We changed to dense hash set since it is much faster
  // and the memory overhead is not visible in a million file namespace.
  //------------------------------------------------------------------------
  typedef google::dense_hash_set<IFileMD::id_t> FileList;
  typedef FileList::iterator                    FileIterator;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IFsView() {};

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //! IFileeMDChangeListener interface
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e) = 0;

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //! IFileeMDChangeListener interface
  //----------------------------------------------------------------------------
  virtual void fileMDRead(IFileMD* obj) = 0;

  //----------------------------------------------------------------------------
  //! Initizalie
  //----------------------------------------------------------------------------
  virtual void initialize() = 0;

  //----------------------------------------------------------------------------
  //! Finalize
  //----------------------------------------------------------------------------
  virtual void finalize() = 0;

  //----------------------------------------------------------------------------
  //! Return reference to a list of files
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  virtual const FileList& getFileList(IFileMD::location_t location) = 0;

  //----------------------------------------------------------------------------
  //! Return reference to a list of unlinked files
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  virtual  const FileList& getUnlinkedFileList(IFileMD::location_t location) = 0;

  //----------------------------------------------------------------------------
  //! Get number of file systems
  //----------------------------------------------------------------------------
  virtual size_t getNumFileSystems() const = 0;

  //----------------------------------------------------------------------------
  //! Get list of files without replicas
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  virtual FileList& getNoReplicasFileList() = 0;

  //----------------------------------------------------------------------------
  //! Get list of files without replicas
  //! BEWARE: any replica change may invalidate iterators
  //----------------------------------------------------------------------------
  virtual const FileList& getNoReplicasFileList() const = 0;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_IFSVIEW_HH__
