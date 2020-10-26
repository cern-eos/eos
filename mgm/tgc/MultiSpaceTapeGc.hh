// ----------------------------------------------------------------------
// File: MultiSpaceTapeGc.hh
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

#ifndef __EOSMGM_MULTISPACETAPEGC_HH__
#define __EOSMGM_MULTISPACETAPEGC_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/Lru.hh"
#include "mgm/tgc/SpaceToTapeGcMap.hh"
#include "mgm/tgc/TapeGcStats.hh"

#include <atomic>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <XrdSfs/XrdSfsInterface.hh>

/*----------------------------------------------------------------------------*/
/**
 * @file MultiSpaceTapeGc.hh
 *
 * @brief Class implementing a tape aware garbage collector that can work over
 * multiple EOS spaces
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A tape aware garbage collector that can work over multiple EOS spaces
//------------------------------------------------------------------------------
class MultiSpaceTapeGc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mgm the interface to the EOS MGM
  //----------------------------------------------------------------------------
  explicit MultiSpaceTapeGc(ITapeGcMgm &mgm);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~MultiSpaceTapeGc();

  //----------------------------------------------------------------------------
  //! Delete copy constructor
  //----------------------------------------------------------------------------
  MultiSpaceTapeGc(const MultiSpaceTapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Delete assignment operator
  //----------------------------------------------------------------------------
  MultiSpaceTapeGc &operator=(const MultiSpaceTapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Thrown if garbage collection has already started
  //----------------------------------------------------------------------------
  struct GcAlreadyStarted: public std::runtime_error {using std::runtime_error::runtime_error;};

  //----------------------------------------------------------------------------
  //! Start garbage collection for the specified EOS spaces
  //!
  //! Please note that calling this method tells this object that support for
  //! tape is enabled
  //!
  //! @param spaces names of the EOS spaces that are to be garbage collected
  //! @throw GCAlreadyStarted if garbage collection has already been started
  //----------------------------------------------------------------------------
  void start(const std::set<std::string> spaces);

  //----------------------------------------------------------------------------
  //! Notify GC the specified file has been opened for write
  //! @note This method does nothing and returns immediately if the GC has not
  //! been enabled
  //!
  //! @param space where the file will be written to
  //! @param fid file identifier
  //----------------------------------------------------------------------------
  void fileOpenedForWrite(const std::string &space, const eos::IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! Notify GC the specified file has been opened for read
  //! @note This method does nothing and returns immediately if the GC has not
  //! been enabled
  //!
  //! @param space where the file resides
  //! @param fid file identifier
  //----------------------------------------------------------------------------
  void fileOpenedForRead(const std::string &space, const eos::IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! Notify GC the specified file has been converted
  //! @note This method does nothing and returns immediately if the GC has not
  //! been enabled
  //!
  //! @param space where the destination converted file resides
  //! @param fid file identifier
  //----------------------------------------------------------------------------
  void fileConverted(const std::string &space, const eos::IFileMD::id_t fid);

  //----------------------------------------------------------------------------
  //! @return map from EOS space name to tape-aware GC statistics
  //----------------------------------------------------------------------------
  std::map<std::string, TapeGcStats> getStats() const;

  //----------------------------------------------------------------------------
  //! Handles a cmd=SFS_FSCTL_PLUGIO arg1=tgc request
  //----------------------------------------------------------------------------
  int handleFSCTL_PLUGIO_tgc(XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const XrdSecEntity* client);

private:

  //----------------------------------------------------------------------------
  //! True if tape support is enabled
  //----------------------------------------------------------------------------
  std::atomic<bool> m_tapeEnabled;

  //----------------------------------------------------------------------------
  //! Ensures start() only starts garbage collection once
  //----------------------------------------------------------------------------
  std::atomic_flag m_startMethodCalled = ATOMIC_FLAG_INIT;

  //----------------------------------------------------------------------------
  //! The interface to the EOS MGM
  //----------------------------------------------------------------------------
  ITapeGcMgm &m_mgm;

  //----------------------------------------------------------------------------
  //! Thread safe map from EOS space name to tape aware garbage collector
  //----------------------------------------------------------------------------
  SpaceToTapeGcMap m_gcs;

  //----------------------------------------------------------------------------
  //! True if the worker thread of this object should stop
  //----------------------------------------------------------------------------
  std::atomic<bool> m_stop = false;

  //----------------------------------------------------------------------------
  //! Mutex dedicated to protecting the m_worker member variable
  //----------------------------------------------------------------------------
  std::mutex m_workerMutex;

  //----------------------------------------------------------------------------
  //! The worker thread for this object (each TapeGc also has its own separate
  //! worker thread)
  //----------------------------------------------------------------------------
  std::unique_ptr<std::thread> m_worker;

  //----------------------------------------------------------------------------
  //! Becomes true when the metadata of the tape-aware GCs has been fully
  //! populated using Quark DB
  //----------------------------------------------------------------------------
  std::atomic<bool> m_gcsPopulatedUsingQdb = false;

  //----------------------------------------------------------------------------
  //! Entry point for the worker thread of this object
  //----------------------------------------------------------------------------
  void workerThreadEntryPoint() noexcept;

  //----------------------------------------------------------------------------
  //! Populate the in-memory LRU data structures of the tape aware garbage
  //! collectors using Quark DB
  //----------------------------------------------------------------------------
  void populateGcsUsingQdb();

  //----------------------------------------------------------------------------
  //! Thrown if an EOS file system cannot determined
  //----------------------------------------------------------------------------
  struct FileSystemNotFound: public std::runtime_error {using std::runtime_error::runtime_error;};

  //----------------------------------------------------------------------------
  //! Dispach file accessed event to the space specific tape garbage collector
  // 
  //! @param event human readable string describing the event
  //! @param space the name of the EOS space where the file resides
  //! @param fileId ID of the file
  //----------------------------------------------------------------------------
  void dispatchFileAccessedToGc(const std::string &event, const std::string &space, const IFileMD::id_t fileId);
};

EOSTGCNAMESPACE_END

#endif
