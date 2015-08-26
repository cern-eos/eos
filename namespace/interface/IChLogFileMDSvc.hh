//------------------------------------------------------------------------------
//! @file IChLogFileMDSvc.hh
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

#ifndef __EOS_NS_ICHLOGFILEMDSVC_HH__
#define __EOS_NS_ICHLOGFILEMDSVC_HH__

#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Change log container metadata service interface
//------------------------------------------------------------------------------

//! Forward declaration
class LockHandler;
class IChLogContainerMDSvc;

class IChLogFileMDSvc
{
 public:

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IChLogFileMDSvc() {}

  //----------------------------------------------------------------------------
  //! Start slave
  //----------------------------------------------------------------------------
  virtual void startSlave() = 0;

  //----------------------------------------------------------------------------
  //! Stop slave
  //----------------------------------------------------------------------------
  virtual void stopSlave() = 0;

  //----------------------------------------------------------------------------
  //! Do the compacting.
  //!
  //! This does not access any of the in-memory structures so any external
  //! metadata operations (including mutations) may happen while it is
  //! running.
  //!
  //! @param  compactingData state information returned by compactPrepare
  //----------------------------------------------------------------------------
  virtual void compact(void*& compactingData) = 0;

  //----------------------------------------------------------------------------
  //! Prepare for online compacting.
  //!
  //! No external file metadata mutation may occur while the method is
  //! running.
  //!
  //! @param  newLogFileName name for the compacted log file
  //! @return                compacting information that needs to be passed
  //!                        to other functions
  //----------------------------------------------------------------------------
  virtual void* compactPrepare(const std::string& ocdir) const = 0;

  //----------------------------------------------------------------------------
  //! Commit the compacting information.
  //!
  //! Updates the metadata structures. Needs an exclusive lock on the
  //! namespace. After successfull completion the new compacted
  //! log will be used for all the new data
  //!
  //! @param compactingData state information obtained from CompactPrepare
  //!                       and modified by Compact
  //! @param autorepair     indicates to skip broken records
  //----------------------------------------------------------------------------
  virtual void compactCommit(void* comp_data, bool autorepair = false) = 0;

  //----------------------------------------------------------------------------
  //! Make transition from slave to master
  //!
  //! @param conf_settings map of configuration settings
  //----------------------------------------------------------------------------
  virtual void slave2Master(std::map<std::string, std::string>&
                            conf_settings) = 0;

  //----------------------------------------------------------------------------
  //! Switch the namespace to read-only mode
  //----------------------------------------------------------------------------
  virtual void makeReadOnly() = 0;

  //----------------------------------------------------------------------------
  //! Register slave lock
  //!
  //! @param slave_lock slave lock object
  //----------------------------------------------------------------------------
  virtual void setSlaveLock(LockHandler* slave_lock) = 0;

  //----------------------------------------------------------------------------
  //! Get changelog warning messages
  //!
  //! @return vector of warning messages
  //----------------------------------------------------------------------------
  virtual std::vector<std::string> getWarningMessages() = 0;

  //----------------------------------------------------------------------------
  //! Clear changelog warning messages
  //----------------------------------------------------------------------------
  virtual void clearWarningMessages() = 0;

  //----------------------------------------------------------------------------
  //! Get the following offset
  //!
  //! @return offset value
  //----------------------------------------------------------------------------
  virtual uint64_t getFollowOffset() = 0;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_ICHLOGFILEMDSVC_HH__
