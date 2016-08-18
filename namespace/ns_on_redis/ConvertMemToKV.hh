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
#include "namespace/ns_on_redis/RedisClient.hh"
#include <cstdint>

EOSNSNAMESPACE_BEGIN

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
  //! Update the name of the directories and files hmap based on the id of the
  //! container. This should be called after a deserialize.
  //----------------------------------------------------------------------------
  void updateInternal();

private:
  std::string pFilesKey; ///< Key of hmap holding info about files
  std::string pDirsKey;  ///< Key of hmap holding info about subcontainers
  redox::RedoxHash pFilesMap; ///< Map of files
  redox::RedoxHash pDirsMap; ///< Map of dirs
};


//------------------------------------------------------------------------------
//! Class for converting in-memory containers to KV-store representation
//------------------------------------------------------------------------------
class ConvertContainerMDSvc : public eos::ChangeLogContainerMDSvc
{
public:
  //----------------------------------------------------------------------------
  //! Recreate the container in the KV store
  //----------------------------------------------------------------------------
  void recreateContainer(IdMap::iterator& it, ContainerList& orphans,
                         ContainerList& nameConflicts);

private:
  static std::uint64_t sNumContBuckets; ///< Numnber of buckets power of 2

  //------------------------------------------------------------------------------
  //! Get container bucket
  //!
  //! @param id container id
  //!
  //! @return string representation of the bucket id
  //------------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  //------------------------------------------------------------------------------
  //! Export container info to the quota view
  //!
  //! @parma cont container object
  //------------------------------------------------------------------------------
  void exportToQuotaView(IContainerMD* cont);
};


//------------------------------------------------------------------------------
//! Class for converting in-memory files to KV-store representation
//------------------------------------------------------------------------------
class ConvertFileMDSvc : public eos::ChangeLogFileMDSvc
{
public:
  //----------------------------------------------------------------------------
  //! Initizlize the file service
  //----------------------------------------------------------------------------
  virtual void initialize();

private:
  static std::uint64_t sNumFileBuckets; ///< Numnber of buckets power of 2

  //------------------------------------------------------------------------------
  //! Get file bucket
  //!
  //! @param id container id
  //!
  //! @return string representation of the bucket id
  //------------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  //------------------------------------------------------------------------------
  //! Export file info to the file-system view
  //!
  //! @parma file file object
  //------------------------------------------------------------------------------
  void exportToFsView(IFileMD* file);

  //------------------------------------------------------------------------------
  //! Export file info to the quota view
  //!
  //! @param file file object
  //------------------------------------------------------------------------------
  void exportToQuotaView(IFileMD* file);
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_CONVERTMEMTOKV_HH__
