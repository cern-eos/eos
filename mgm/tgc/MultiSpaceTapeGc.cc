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
m_tapeEnabled(false), m_gcs(mgm)
{
}

//------------------------------------------------------------------------------
// Enable garbage collection for the specified EOS space
//------------------------------------------------------------------------------
void
MultiSpaceTapeGc::enable(const std::string &space) noexcept
{
  // Any attempt to enable tape support for an EOS space means tape support in
  // general is enabled
  m_tapeEnabled = true;

  const char *const msgFormat =
    "Unable to enable tape-aware garbage collection space=%s: %s";

  try {
    auto &gc = m_gcs.createGc(space);
    gc.enable();
  } catch (std::exception &ex) {
    eos_static_err(msgFormat, space.c_str(), ex.what());
  } catch (...) {
    eos_static_err(msgFormat, space.c_str(), "Caught an unknown exception");
  }
}

//------------------------------------------------------------------------------
// Notify GC the specified file has been opened
//------------------------------------------------------------------------------
void
MultiSpaceTapeGc::fileOpened(const std::string &space, const std::string &path,
  const IFileMD::id_t fid) noexcept
{
  if (!m_tapeEnabled) return;

  const char *const msgFormat =
    "Error handling 'file opened' event space=%s fxid=%08llx path=%s: %s";

  try {
    auto &gc = m_gcs.getGc(space);
    gc.fileOpened(path, fid);
  } catch (SpaceToTapeGcMap::UnknownEOSSpace&) {
    // Ignore events for EOS spaces that do not have an enabled tape-aware GC
  } catch (std::exception &ex) {
    eos_static_err(msgFormat, space.c_str(), fid, path.c_str(), ex.what());
  } catch (...) {
    eos_static_err(msgFormat, space.c_str(), path.c_str(),
      "Caught an unknown exception");
  }
}

//------------------------------------------------------------------------------
// Return map from EOS space name to tape-aware GC statistics
//------------------------------------------------------------------------------
std::map<std::string, TapeGcStats>
MultiSpaceTapeGc::getStats() const
{
  if (!m_tapeEnabled) std::map<std::string, TapeGcStats>();

  return m_gcs.getStats();
}

//----------------------------------------------------------------------------
// Handles a cmd=SFS_FSCTL_PLUGIO arg1=tgc request
//----------------------------------------------------------------------------
int
MultiSpaceTapeGc::handleFSCTL_PLUGIO_tgc(XrdOucErrInfo& error,
                                         eos::common::VirtualIdentity& vid,
                                         const XrdSecEntity* client)
{
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
}

EOSTGCNAMESPACE_END
