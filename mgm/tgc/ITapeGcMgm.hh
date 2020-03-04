// ----------------------------------------------------------------------
// File: ITapeGcMgm.hh
// Author: Steven Murray - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSMGMTGC_ITAPEGCMGM_HH__
#define __EOSMGMTGC_ITAPEGCMGM_HH__

#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
#include "mgm/tgc/SpaceStats.hh"
#include "mgm/tgc/SpaceConfig.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

#include <atomic>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>

/*----------------------------------------------------------------------------*/
/**
 * @file ITapeGcMgm.hh
 *
 * @brief Specifies the tape-aware garbage collector's interface to the EOS MGM
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Specifies the tape-aware garbage collector's interface to the EOS MGM
//------------------------------------------------------------------------------
class ITapeGcMgm {
public:
  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  ITapeGcMgm() = default;

  //----------------------------------------------------------------------------
  //! Virtual destructor
  //----------------------------------------------------------------------------
  virtual ~ITapeGcMgm() = 0;

  //----------------------------------------------------------------------------
  //! @return The configuration of a tape-aware garbage collector for the
  //! specified space.
  //! @param spaceName The name of the space
  //----------------------------------------------------------------------------
  virtual SpaceConfig getTapeGcSpaceConfig(const std::string &spaceName) = 0;

  //----------------------------------------------------------------------------
  //! @return Statistics about the specified space
  //! @param space The name of the EOS space to be queried
  //! @throw TapeAwareGcSpaceNotFound when the EOS space named m_spaceName
  //! cannot be found
  //----------------------------------------------------------------------------
  [[nodiscard]] virtual SpaceStats getSpaceStats(const std::string &spaceName) const = 0;

  //----------------------------------------------------------------------------
  //! Thrown when there is a failure to get the size of a file
  //----------------------------------------------------------------------------
  struct FailedToGetFileSize: public std::runtime_error {
    FailedToGetFileSize(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! @param fid The file identifier
  //! @return The size of the specified file in bytes.
  //! @throw FailedToGetFileSize When there is a failure to get the size of the
  //! file
  //----------------------------------------------------------------------------
  virtual std::uint64_t getFileSizeBytes(IFileMD::id_t fid) = 0;

  //----------------------------------------------------------------------------
  //! Determine if the specified file exists and is not scheduled for deletion
  //!
  //! @param fid The file identifier
  //! @return True if the file exists in the EOS namespace and is not scheduled
  //! for deletion
  //----------------------------------------------------------------------------
  virtual bool fileInNamespaceAndNotScheduledForDeletion(IFileMD::id_t fid) = 0;

  //----------------------------------------------------------------------------
  //! Execute stagerrm as user root
  //!
  //! @param fid The file identifier
  //----------------------------------------------------------------------------
  virtual void stagerrmAsRoot(const IFileMD::id_t fid) = 0;

  //----------------------------------------------------------------------------
  //! @return Map from file system ID to EOS space name
  //----------------------------------------------------------------------------
  virtual std::map<common::FileSystem::fsid_t, std::string> getFsIdToSpaceMap() = 0;

  //------------------------------------------------------------------------------
  //! Structure containing the identifier and ctime of an EOS file which can
  //! be ordered by ctime within an std container.
  //------------------------------------------------------------------------------
  struct FileIdAndCtime
  {
    //----------------------------------------------------------------------------
    //! The EOS identifier
    //----------------------------------------------------------------------------
    IFileMD::id_t id;

    //----------------------------------------------------------------------------
    //! The ctime
    //----------------------------------------------------------------------------
    timespec ctime;

    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    FileIdAndCtime(): id(0) {
      ctime.tv_sec = 0;
      ctime.tv_nsec = 0;
    }

    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    FileIdAndCtime(const IFileMD::id_t i, const timespec c): id(i), ctime(c) {}

    //----------------------------------------------------------------------------
    //! Less than operator
    //----------------------------------------------------------------------------
    bool operator<(const FileIdAndCtime &rhs) const {
      if (ctime.tv_sec == rhs.ctime.tv_sec) {
        return ctime.tv_nsec < rhs.ctime.tv_nsec;
      } else {
        return ctime.tv_sec < rhs.ctime.tv_sec;
      }
    }
  };

  //----------------------------------------------------------------------------
  //! @return map from EOS space name to disk replicas within that space - the
  //! disk replicas are ordered from oldest first to youngest last
  //! @param spaces names of the EOS spaces to be mapped
  //! @param stop reference to a shared atomic boolean that if set to true will
  //! cause this method to stop and return
  //! @param nbFilesScanned reference to a counter which this method will set to
  //! the total number of files scanned
  //----------------------------------------------------------------------------
  virtual std::map<std::string, std::set<FileIdAndCtime> > getSpaceToDiskReplicasMap(
    const std::set<std::string> &spacesToMap, std::atomic<bool> &stop, uint64_t &nbFilesScanned) = 0;
};

EOSTGCNAMESPACE_END

#endif
