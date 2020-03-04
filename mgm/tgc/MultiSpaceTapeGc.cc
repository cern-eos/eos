// ----------------------------------------------------------------------
// File: MultiSpaceTapeGc.cc
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

#include "common/Logging.hh"
#include "mgm/tgc/MaxLenExceeded.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "mgm/tgc/Utils.hh"

#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <time.h>

/*----------------------------------------------------------------------------*/
/**
 * @file MultiSpaceTapeGc.cc
 *
 * @brief Class implementing a tape aware garbage collector that can work over
 * multiple EOS spaces
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MultiSpaceTapeGc::MultiSpaceTapeGc(ITapeGcMgm &mgm):
m_tapeEnabled(false), m_mgm(mgm), m_gcs(mgm)
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
MultiSpaceTapeGc::~MultiSpaceTapeGc()
{
  try {
    std::lock_guard<std::mutex> workerLock(m_workerMutex);
    if(m_worker) {
      m_stop = true;
      m_worker->join();
    }
  } catch(std::exception &ex) {
    eos_static_err("msg=\"%s\"", ex.what());
  } catch(...) {
    eos_static_err("msg=\"Caught an unknown exception\"");
  }
}

//------------------------------------------------------------------------------
// Notify GC the specified file has been opened
//------------------------------------------------------------------------------
void
MultiSpaceTapeGc::fileOpened(const std::string &space, const IFileMD::id_t fid)
{
  if (!m_tapeEnabled || !m_gcsPopulatedUsingQdb) return;

  const char *const msgFormat =
    "space=\"%s\" fxid=%08llx msg=\"Error handling 'file opened' event: %s\"";

  try {
    auto &gc = m_gcs.getGc(space);
    gc.fileOpened(fid);
  } catch (SpaceToTapeGcMap::UnknownEOSSpace&) {
    // Ignore events for EOS spaces that do not have a tape-aware GC
  } catch (std::exception &ex) {
    eos_static_err(msgFormat, space.c_str(), fid, ex.what());
  } catch (...) {
    eos_static_err(msgFormat, space.c_str(), fid, "Caught an unknown exception");
  }
}

//------------------------------------------------------------------------------
// Return map from EOS space name to tape-aware GC statistics
//------------------------------------------------------------------------------
std::map<std::string, TapeGcStats>
MultiSpaceTapeGc::getStats() const
{
  const char *const msgFormat =
    "msg=\"Unable to get statistics about tape-aware garbage collectors: %s\"";

  try {
    if (!m_tapeEnabled) {
      return std::map<std::string, TapeGcStats>();
    }

    return m_gcs.getStats();
  } catch (std::exception &ex) {
    eos_static_err(msgFormat, ex.what());
  } catch (...) {
    eos_static_err(msgFormat, "Caught an unknown exception");
  }

  return std::map<std::string, TapeGcStats>();
}

//----------------------------------------------------------------------------
// Handles a cmd=SFS_FSCTL_PLUGIO arg1=tgc request
//----------------------------------------------------------------------------
int
MultiSpaceTapeGc::handleFSCTL_PLUGIO_tgc(XrdOucErrInfo& error,
                                         eos::common::VirtualIdentity& vid,
                                         const XrdSecEntity* client)
{
  try {
    if (vid.host != "localhost" && vid.host != "localhost.localdomain") {
      std::ostringstream replyMsg, logMsg;
      replyMsg << __FUNCTION__ << ": System access restricted - unauthorized identity used";
      logMsg << "msg=\"" << replyMsg.str() << "\"";
      eos_static_err(logMsg.str().c_str());
      error.setErrInfo(EACCES, replyMsg.str().c_str());
      return SFS_ERROR;
    }

    if (!m_tapeEnabled) {
      std::ostringstream replyMsg, logMsg;
      replyMsg << __FUNCTION__ << ": Support for tape is not enabled";
      logMsg << "msg=\"" << replyMsg.str() << "\"";
      eos_static_err(logMsg.str().c_str());
      error.setErrInfo(ENOTSUP, replyMsg.str().c_str());
      return SFS_ERROR;
    }

    const uint64_t replySize = 1048576; // 1 MiB
    char * const reply = static_cast<char*>(malloc(replySize));
    if (!reply) {
      std::ostringstream replyMsg, logMsg;
      replyMsg << __FUNCTION__ << ": Failed to allocate memory for reply: replySize=" << replySize;
      logMsg << "msg=\"" << replyMsg.str() << "\"";
      eos_static_err(logMsg.str().c_str());
      error.setErrInfo(ENOMEM, replyMsg.str().c_str());
      return SFS_ERROR;
    }

    std::ostringstream json;
    try {
      m_gcs.toJson(json, replySize - 1);
    } catch(MaxLenExceeded &ml) {
      std::ostringstream msg;
      msg << "msg=\"" << ml.what() << "\"";
      eos_static_err(msg.str().c_str());
      error.setErrInfo(ERANGE, ml.what());
      return SFS_ERROR;
    }
    std::strncpy(reply, json.str().c_str(), replySize);
    reply[replySize - 1] = '\0';

    // Ownership of reply is taken by the xrd_buff object.
    // Error then takes ownership of the xrd_buff object
    XrdOucBuffer * const xrd_buff = new XrdOucBuffer(reply, replySize);
    xrd_buff->SetLen(strlen(reply + 1));
    error.setErrInfo(xrd_buff->BuffSize(), xrd_buff);
    return SFS_DATA;
  } catch (std::exception &ex) {
    eos_static_err("msg=\"handleFSCTL_PLUGIO_tgc failed: %s\"", ex.what());
  } catch (...) {
    eos_static_err("msg=\"handleFSCTL_PLUGIO_tgc failed: Caught an unknown exception\"");
  }

  error.setErrInfo(ECANCELED, "handleFSCTL_PLUGIO_tgc failed");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Start garbage collection for the specified EOS spaces
//------------------------------------------------------------------------------
void
MultiSpaceTapeGc::start(const std::set<std::string> spaces) {
  // Starting garbage collecton implies that support for tape is enabled
  m_tapeEnabled = true;

  if (m_startMethodCalled.test_and_set()) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: Garbage collection has already been started";
    throw GcAlreadyStarted(msg.str());
  }

  for (const auto &space: spaces) {
    m_gcs.createGc(space);
  }

  std::function<void()> entryPoint = std::bind(&MultiSpaceTapeGc::workerThreadEntryPoint, this);
  {
    std::lock_guard<std::mutex> workerLock(m_workerMutex);
    m_worker = std::make_unique<std::thread>(entryPoint);
  }
}

//------------------------------------------------------------------------------
// Entry point for the worker thread of this object
//------------------------------------------------------------------------------
void
MultiSpaceTapeGc::workerThreadEntryPoint() noexcept
{
  try {
    populateGcsUsingQdb();
    m_gcsPopulatedUsingQdb = true;
    m_gcs.startGcWorkerThreads();
  } catch (std::exception &ex) {
    eos_static_crit("msg=\"Worker thread of the multi-space tape-aware garbage collector failed: %s\"", ex.what());
  } catch (...) {
    eos_static_crit("msg=\"Worker thread of the multi-space tape-aware garbage collector failed:"
      " Caught an unknown exception\"");
  }
}

//----------------------------------------------------------------------------
// Populate the in-memory LRUs of the tape garbage collectors using Quark DB
//----------------------------------------------------------------------------
void
MultiSpaceTapeGc::populateGcsUsingQdb() {
  eos_static_info("msg=\"Starting to populate the meta-data of the tape-aware garbage collectors\"");
  const auto startTgcPopulation = time(nullptr);

  const auto gcSpaces = m_gcs.getSpaces();
  uint64_t nbFilesScanned = 0;
  auto gcSpaceToFiles = m_mgm.getSpaceToDiskReplicasMap(gcSpaces, m_stop, nbFilesScanned);

  // Build up space GC LRU structures whilst reducing space file lists
  for (auto &spaceAndFiles : gcSpaceToFiles) {
    const auto &space = spaceAndFiles.first;
    auto &files = spaceAndFiles.second;
    auto &gc = m_gcs.getGc(space);
    {
      std::ostringstream msg;
      msg << "msg=\"About to populate the tape-aware GC meta-data for an EOS space\" space=\"" << space << "\" nbFiles="
        << files.size();
      eos_static_info(msg.str().c_str());
    }
    for (auto fileItor = files.begin(); fileItor != files.end();) {
      if (m_stop) {
        eos_static_info("msg=\"Requested to stop populating the meta-data of the tape-aware garbage collectors\"");
        return;
      }

      gc.fileOpened(fileItor->id);
      fileItor = files.erase(fileItor);
    }
  }

  {
    const auto populationDurationSecs = time(nullptr) - startTgcPopulation;
    std::ostringstream msg;
    msg << "msg=\"Finished populating the meta-data of the tape-aware garbage collectors\" nbFilesScanned=" << nbFilesScanned << " durationSecs=" <<
    populationDurationSecs;
    eos_static_info(msg.str().c_str());
  }
}

EOSTGCNAMESPACE_END
