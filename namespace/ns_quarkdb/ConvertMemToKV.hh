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
//! @brief Executable used to convert an in-memory namespace representation to
//!        to a KV one
//------------------------------------------------------------------------------

#ifndef __EOS_NS_CONVERTMEMTOKV_HH__
#define __EOS_NS_CONVERTMEMTOKV_HH__

#include "namespace/Namespace.hh"
#include "namespace/ns_in_memory/ContainerMD.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/ns_quarkdb/accounting/SyncTimeAccounting.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "ContainerMd.pb.h"
#include "common/RWMutex.hh"
#include <cstdint>

EOSNSNAMESPACE_BEGIN

using QuotaNodeMapT = std::map<std::string, eos::IQuotaNode::UsageInfo>;

//------------------------------------------------------------------------------
//! Class ConvertQuotaView
//------------------------------------------------------------------------------
class ConvertQuotaView
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConvertQuotaView(qclient::QClient* qcl, eos::IContainerMDSvc* csvc,
                   eos::IFileMDSvc* fsvc):
    mQcl(qcl), mContSvc(csvc), mFileSvc(fsvc) {}

  //----------------------------------------------------------------------------
  //!Destructor
  //----------------------------------------------------------------------------
  ~ConvertQuotaView() {};

  //----------------------------------------------------------------------------
  //! Add quota node for a specific container
  //!
  //! @param id id of the container which is also a quota node
  //----------------------------------------------------------------------------
  void addQuotaNode(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Add quota info for a specific file object
  //!
  //! @param file file object
  //----------------------------------------------------------------------------
  void addQuotaInfo(IFileMD* file);

  //----------------------------------------------------------------------------
  //! Commit all of the quota view information to the backend
  //----------------------------------------------------------------------------
  void commitToBackend();

private:
  qclient::QClient* mQcl; ///< Qclient object
  eos::IContainerMDSvc* mContSvc; ///< Container metadata service
  eos::IFileMDSvc* mFileSvc; ///< File metadata service
  //! Map beween quota node id and uid and gid maps holding info about the
  //! quota accounting
  std::set<std::string> mSetQuotaIds; ///< Set of quota ids
  std::map< std::string, std::pair<QuotaNodeMapT, QuotaNodeMapT> > mQuotaMap;
  eos::common::RWMutex mRWMutex; ///< Mutex protecting access to the map
};


//------------------------------------------------------------------------------
//! Class ConvertFsView
//------------------------------------------------------------------------------
class ConvertFsView
{
public:
  //----------------------------------------------------------------------------
  //! Add file info to the file system view
  //!
  //! @param fsid file system id where the file can be found
  //----------------------------------------------------------------------------
  void addFileInfo(IFileMD* file);

  //----------------------------------------------------------------------------
  //! Commit all of the fs view information to the backend
  //----------------------------------------------------------------------------
  void commitToBackend();

private:
  std::set<std::string> mFileNoReplica; ///< Set of files with no replica
  //! Map of file system ids to set of file replicas and set of unlinked file ids
  std::map<std::string, std::pair<std::set<std::string>,
      std::set<std::string> > > mFsView;
  std::mutex mMutex; ///< Mutex protecting access tot he map and set
};


//------------------------------------------------------------------------------
//! Class ConvertContainerMD
//------------------------------------------------------------------------------
class ConvertContainerMD : public eos::ContainerMD
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConvertContainerMD(id_t id, IFileMDSvc* file_svc, IContainerMDSvc* cont_svc);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ConvertContainerMD() {};

  //----------------------------------------------------------------------------
  //! Add container
  //----------------------------------------------------------------------------
  void addContainer(IContainerMD* container) override;

  //----------------------------------------------------------------------------
  //! Add file
  //----------------------------------------------------------------------------
  void addFile(IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Find file
  //----------------------------------------------------------------------------
  std::shared_ptr<IFileMD> findFile(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Update the name of the directories and files hmap based on the id of the
  //! container. This should be called after a deserialize.
  //----------------------------------------------------------------------------
  void updateInternal();

  //------------------------------------------------------------------------------
  //! Serialize the object to a std::string buffer
  //!
  //! @param buffer output of the serialized object
  //------------------------------------------------------------------------------
  void serialize(std::string& buffer);

private:
  eos::ns::ContainerMdProto mCont; ///< Protobuf container representation
  std::string pFilesKey; ///< Key of hmap holding info about files
  std::string pDirsKey;  ///< Key of hmap holding info about subcontainers
  qclient::QHash pFilesMap; ///< Map of files
  qclient::QHash pDirsMap; ///< Map of dirs
  std::mutex mMutexFiles; ///< Mutex protecting access to the files map
};


//------------------------------------------------------------------------------
//! Class for converting in-memory containers to KV-store representation
//------------------------------------------------------------------------------
class ConvertContainerMDSvc : public eos::ChangeLogContainerMDSvc
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ConvertContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ConvertContainerMDSvc() {};

  //----------------------------------------------------------------------------
  //! Recreate the container in the KV store
  //----------------------------------------------------------------------------
  void recreateContainer(IdMap::iterator& it, ContainerList& orphans,
                         ContainerList& nameConflicts);

  //----------------------------------------------------------------------------
  //! Load container object
  //----------------------------------------------------------------------------
  void loadContainer(IdMap::iterator& it);

  //----------------------------------------------------------------------------
  //! Get first free container id
  //----------------------------------------------------------------------------
  IContainerMD::id_t getFirstFreeId();

  //----------------------------------------------------------------------------
  //! Set quota view object reference
  //!
  //! @param qview quota view object
  //----------------------------------------------------------------------------
  void setQuotaView(ConvertQuotaView* qview);

  //----------------------------------------------------------------------------
  //! Commit all the container info to the backend.
  //----------------------------------------------------------------------------
  void CommitToBackend();

private:
  static std::uint64_t sNumContBuckets; ///< Numnber of buckets power of 2

  //----------------------------------------------------------------------------
  //! Get container bucket
  //!
  //! @param id container id
  //!
  //! @return string representation of the bucket id
  //----------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  IContainerMD::id_t mFirstFreeId; ///< First free container id
  ConvertQuotaView* mConvQView; ///< Quota view object
};


//------------------------------------------------------------------------------
//! Class for converting in-memory files to KV-store representation
//------------------------------------------------------------------------------
class ConvertFileMDSvc : public eos::ChangeLogFileMDSvc
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ConvertFileMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ConvertFileMDSvc() {};

  //----------------------------------------------------------------------------
  //! Initizlize the file service
  //----------------------------------------------------------------------------
  virtual void initialize();

  //----------------------------------------------------------------------------
  //! Get first free file id
  //----------------------------------------------------------------------------
  IFileMD::id_t getFirstFreeId();

  //----------------------------------------------------------------------------
  //! Set quota and file system view object references
  //!
  //! @param qview quota view object
  //! @param fsview filesystem view object
  //----------------------------------------------------------------------------
  void setViews(ConvertQuotaView* qview, ConvertFsView* fsview);

  //----------------------------------------------------------------------------
  //! Set sync time accounting view
  //!
  //! @param synctime sync time object
  //----------------------------------------------------------------------------
  inline void setSyncTimeAcc(IContainerMDChangeListener* synctime)
  {
    mSyncTimeAcc = dynamic_cast<eos::SyncTimeAccounting*>(synctime);
  };

  //----------------------------------------------------------------------------
  //! Set container accounting view
  //!
  //! @param contacc container accounting view
  //----------------------------------------------------------------------------
  inline void setContainerAcc(IFileMDChangeListener* contacc)
  {
    mContAcc = dynamic_cast<eos::ContainerAccounting*>(contacc);
  };

private:
  static std::uint64_t sNumFileBuckets; ///< Number of buckets power of 2

  //------------------------------------------------------------------------------
  //! Get file bucket
  //!
  //! @param id container id
  //!
  //! @return string representation of the bucket id
  //------------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  std::mutex mMutexFreeId; ///< Mutex protecting access to first free id value
  IFileMD::id_t mFirstFreeId; ///< First free file id
  ConvertQuotaView* mConvQView; ///< Quota view object
  ConvertFsView* mConvFsView; ///< Filesystem view object
  std::atomic<std::uint64_t> mCount; ///< Number of files proccessed
  eos::SyncTimeAccounting* mSyncTimeAcc; ///< Sync time accounting view
  eos::ContainerAccounting* mContAcc; ///< Subtree size accounting
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_CONVERTMEMTOKV_HH__
