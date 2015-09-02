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

//------------------------------------------------------------------------------
//! @author Elvin Sindrialru <esindril@cern.ch>
//! @brief  Filesystem-based file metadata service
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FS_FILE_MD_SVC_HH__
#define __EOS_NS_FS_FILE_MD_SVC_HH__

#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <list>

EOSNSNAMESPACE_BEGIN

class LockHandler;
class FsContainerMDSvc;

//------------------------------------------------------------------------------
//! Filesystem-based file metadata service
//------------------------------------------------------------------------------
class FsFileMDSvc: public IFileMDSvc
{
  friend class FileMDFollower;
 public:
  
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsFileMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsFileMDSvc();

  //----------------------------------------------------------------------------
  //! Initizlize the file service
  //----------------------------------------------------------------------------
  virtual void initialize();

  //----------------------------------------------------------------------------
  //! Configure the file service
  //----------------------------------------------------------------------------
  virtual void configure(std::map<std::string, std::string>& config)
  {
    return;
  }

  //----------------------------------------------------------------------------
  //! Finalize the file service
  //----------------------------------------------------------------------------
  virtual void finalize();

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //----------------------------------------------------------------------------
  virtual IFileMD* getFileMD(IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Create new file metadata object with an assigned id
  //----------------------------------------------------------------------------
  virtual IFileMD* createFile();

  //----------------------------------------------------------------------------
  //! Update the file metadata in the backing store after the FileMD object
  //! has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IFileMD* obj);

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD* obj);

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD::id_t fileId);

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  virtual uint64_t getNumFiles() const
  {
    return pIdMap.size();
  }

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IFileMDChangeListener* listener);

  //----------------------------------------------------------------------------
  //! Visit all the files
  //----------------------------------------------------------------------------
  virtual void visit(IFileVisitor* visitor);

  //----------------------------------------------------------------------------
  //! Notify the listeners about the change
  //----------------------------------------------------------------------------
  virtual void notifyListeners(IFileMDChangeListener::Event* event)
  {
    ListenerList::iterator it;

    for (it = pListeners.begin(); it != pListeners.end(); ++it)
      (*it)->fileMDChanged(event);
  }

  //----------------------------------------------------------------------------
  //! Set container service
  //!
  //! @param cont_svc container service
  //----------------------------------------------------------------------------
  void setContainerService(IChLogContainerMDSvc* cont_svc);

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //!
  //! @param quota_stats object implementing the IQuotaStats interface
  //----------------------------------------------------------------------------
  void setQuotaStats(IQuotaStats* quota_stats);

 private:

  typedef google::dense_hash_map<IFileMD::id_t, DataInfo> IdMap;
  typedef std::list<IFileMDChangeListener*> ListenerList;

  //----------------------------------------------------------------------------
  //! Data
  //----------------------------------------------------------------------------
  IFileMD::id_t      pFirstFreeId;
  IdMap              pIdMap;
  ListenerList       pListeners;
  FsContainerMDSvc*  pContSvc;
  IQuotaStats*       pQuotaStats;
  bool               pAutoRepair;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FS_FILE_MD_SVC_HH__
