// ----------------------------------------------------------------------
// File: DummyTapeGcMgm.hh
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

#ifndef __EOSMGMTGC_DUMMYTAPEGCMGM_HH__
#define __EOSMGMTGC_DUMMYTAPEGCMGM_HH__

#include "mgm/tgc/ITapeGcMgm.hh"

#include <map>
#include <mutex>

/*----------------------------------------------------------------------------*/
/**
 * @file DummyTapeGcMgm.hh
 *
 * @brief A dummy implementation of access to the EOS MGM.  The main purpose of
 * this class is to facilitate unit testing.
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A dummy implementation of access to the EOS MGM.  The main purpose of this
//! class is to facilitate unit testing.
//------------------------------------------------------------------------------
class DummyTapeGcMgm: public ITapeGcMgm {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DummyTapeGcMgm();

  //----------------------------------------------------------------------------
  //! Delete copy constructor
  //----------------------------------------------------------------------------
  DummyTapeGcMgm(const DummyTapeGcMgm &) = delete;

  //----------------------------------------------------------------------------
  //! Delete move constructor
  //----------------------------------------------------------------------------
  DummyTapeGcMgm(const DummyTapeGcMgm &&) = delete;

  //----------------------------------------------------------------------------
  //! Delete assignment operator
  //----------------------------------------------------------------------------
  DummyTapeGcMgm &operator=(const DummyTapeGcMgm &) = delete;

  //----------------------------------------------------------------------------
  //! @return The configuration of a tape-aware garbage collector for the
  //! specified space.
  //! @param spaceName The name of the space
  //----------------------------------------------------------------------------
  SpaceConfig getTapeGcSpaceConfig(const std::string &spaceName) override;

  //----------------------------------------------------------------------------
  //! @return Statistics about the specified space
  //! @param space The name of the EOS space to be queried
  //! @throw TapeAwareGcSpaceNotFound when the EOS space named m_spaceName
  //! cannot be found
  //----------------------------------------------------------------------------
  SpaceStats getSpaceStats(const std::string &space) const override;

  //----------------------------------------------------------------------------
  //! @param fid The file identifier
  //! @return The size of the specified file in bytes.  If the file cannot be
  //! found in the EOS namespace then a file size of 0 is returned.
  //----------------------------------------------------------------------------
  std::uint64_t getFileSizeBytes(IFileMD::id_t fid) override;

  //----------------------------------------------------------------------------
  //! Determine if the specified file exists and is not scheduled for deletion
  //!
  //! @param fid The file identifier
  //! @return True if the file exists in the EOS namespace and is not scheduled
  //! for deletion
  //----------------------------------------------------------------------------
  bool fileInNamespaceAndNotScheduledForDeletion(IFileMD::id_t fid) override;

  //----------------------------------------------------------------------------
  //! Execute evict as user root
  //!
  //! @param fid The file identifier
  //----------------------------------------------------------------------------
  void evictAsRoot(const IFileMD::id_t fid) override;

  //----------------------------------------------------------------------------
  //! @return Map from file system ID to EOS space name
  //----------------------------------------------------------------------------
  std::map<common::FileSystem::fsid_t, std::string> getFsIdToSpaceMap() override;

  //----------------------------------------------------------------------------
  //! @return map from EOS space name to disk replicas within that space - the
  //! disk replicas are ordered from oldest first to youngest last
  //! @param spaces names of the EOS spaces to be mapped
  //! @param stop reference to a shared atomic boolean that if set to true will
  //! cause this method to stop and return
  //! @param nbFilesScanned reference to a counter which this method will set to
  //! the total number of files scanned
  //----------------------------------------------------------------------------
  std::map<std::string, std::set<FileIdAndCtime> > getSpaceToDiskReplicasMap(
    const std::set<std::string> &spacesToMap, std::atomic<bool> &stop, uint64_t &nbFilesScanned) override;

  //----------------------------------------------------------------------------
  //! @return The stdout of the specified shell cmd as a string
  //! @param cmdStr The shell command string to be executed
  //! @param maxLen The maximum length of the result
  //----------------------------------------------------------------------------
  std::string getStdoutFromShellCmd(const std::string &cmdStr, const ssize_t maxLen) const override;

  //----------------------------------------------------------------------------
  //! Set the tape-aware garbage collector configuration for the specified EOS
  //! space
  //!
  //! @param space Name of the space.
  //! @param config The configuration
  //----------------------------------------------------------------------------
  void setTapeGcSpaceConfig(const std::string &space, const SpaceConfig &config);

  //----------------------------------------------------------------------------
  //! Set the statistics of the specified EOS space.
  //!
  //! @param space Name of the space.
  //! @param spaceStats The statistics.
  //----------------------------------------------------------------------------
  void setSpaceStats(const std::string &space, const SpaceStats &spaceStats);

  //----------------------------------------------------------------------------
  //! @return number of times getTapeGcSpaceConfig() has been called
  //----------------------------------------------------------------------------
  std::uint64_t getNbCallsToGetTapeGcSpaceConfig() const;

  //----------------------------------------------------------------------------
  //! @return number of times getSpaceStats() has been called
  //----------------------------------------------------------------------------
  std::uint64_t getNbCallsToGetSpaceStats() const;

  //----------------------------------------------------------------------------
  //! @return number of times fileInNamespaceAndNotScheduledForDeletion() has
  //! been called
  //----------------------------------------------------------------------------
  std::uint64_t getNbCallsToFileInNamespaceAndNotScheduledForDeletion() const;

  //----------------------------------------------------------------------------
  //! @return number of times getFileSizeBytes() has been called
  //----------------------------------------------------------------------------
  std::uint64_t getNbCallsToGetFileSizeBytes() const;

  //------------------------------------------------------------------------------
  //! @return number of times evictAsRoot() has been called
  //------------------------------------------------------------------------------
  std::uint64_t getNbCallsToEvictAsRoot() const;

  //----------------------------------------------------------------------------
  //! Set the standard out from the shell command
  //! @param stdoutFromShellCmd Standard out from the shell command as a string
  //----------------------------------------------------------------------------
  void setStdoutFromShellCmd(const std::string &stdoutFromShellCmd);

private:

  //----------------------------------------------------------------------------
  //! Mutex protecting this dummy object representing access to the MGM
  //----------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //----------------------------------------------------------------------------
  //! Map from EOS space name to the tape-aware garbage collector configuration
  //----------------------------------------------------------------------------
  std::map<std::string, SpaceConfig> m_spaceToTapeGcConfig;

  //----------------------------------------------------------------------------
  //! Map from the name of an EOS space to its statistics
  //----------------------------------------------------------------------------
  std::map<std::string, SpaceStats> m_spaceToStats;

  //----------------------------------------------------------------------------
  //! Number of times getTapeGcSpaceConfig() has been called
  //----------------------------------------------------------------------------
  std::uint64_t m_nbCallsToGetTapeGcSpaceConfig;

  //----------------------------------------------------------------------------
  //! Number of times getSpaceStats() has been called
  //----------------------------------------------------------------------------
  mutable std::uint64_t m_nbCallsToGetSpaceStats;

  //----------------------------------------------------------------------------
  //! Number of times fileInNamespaceAndNotScheduledForDeletion() has been
  //! called
  //----------------------------------------------------------------------------
  std::uint64_t m_nbCallsToFileInNamespaceAndNotScheduledForDeletion;

  //----------------------------------------------------------------------------
  //! Number of times getFileSizeBytes() has been called
  //----------------------------------------------------------------------------
  std::uint64_t m_nbCallsToGetFileSizeBytes;

  //----------------------------------------------------------------------------
  //! Number of times evictAsRoot() has been called
  //----------------------------------------------------------------------------
  std::uint64_t m_nbCallsToEvictAsRoot;

  //----------------------------------------------------------------------------
  //! Standard out from the shel command as a string.
  //----------------------------------------------------------------------------
  std::string m_stdoutFromShellCmd;
};

EOSTGCNAMESPACE_END

#endif
