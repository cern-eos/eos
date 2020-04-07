//------------------------------------------------------------------------------
//! @file XrdMgmOfsDirectory.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief XRootD OFS plugin class implementing directory handling of EOS
//------------------------------------------------------------------------------

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

#pragma once
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "common/LRU.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include <dirent.h>
#include <string>
#include <set>
#include <mutex>

//! Forward declaration
namespace eos
{
class IContainerMD;
};

//------------------------------------------------------------------------------
//! Class implementing directories and operations
//------------------------------------------------------------------------------
class XrdMgmOfsDirectory : public XrdSfsDirectory, public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMgmOfsDirectory(char* user = 0, int MonID = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdMgmOfsDirectory() = default;

  //----------------------------------------------------------------------------
  //! Open a directory object with bouncing/mapping & namespace mapping
  //!
  //! @param inpath directory path to open
  //! @param client XRootD authentication object
  //! @param ininfo CGI
  //!
  //! @return SFS_OK otherwise SFS_ERROR
  //!
  //! @note We create during the open the full directory listing which then is
  //! retrieved via nextEntry() and cleaned up with close().
  //----------------------------------------------------------------------------
  int open(const char* dirName, const XrdSecClientName* client = 0,
           const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Open a directory by vid
  //----------------------------------------------------------------------------
  int open(const char* dirName, eos::common::VirtualIdentity& vid,
           const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Open a directory by vid
  //----------------------------------------------------------------------------
  int _open(const char* dirName,
            eos::common::VirtualIdentity& vid,
            const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! @brief Read the next directory entry
  //!
  //! @return name of the next directory entry
  //!
  //! Upon success, returns the contents of the next directory entry as
  //! a null terminated string. Returns a null pointer upon EOF or an
  //! error. To differentiate the two cases, getErrorInfo will return
  //! 0 upon EOF and an actual error code (i.e., not 0) on error.
  // ---------------------------------------------------------------------------
  const char* nextEntry();

  //----------------------------------------------------------------------------
  //! Create an error message
  //!
  //! @param pfx message prefix value
  //! @param einfo error text/code object
  //! @param ecode error code
  //! @param op name of the operation performed
  //! @param target target of the operation e.g. file name etc.
  //!
  //! @return SFS_ERROR in all cases
  //!
  //!This routines prints also an error message into the EOS log if it was not
  //! due to a stat call or the error codes EIDRM or ENODATA
  //----------------------------------------------------------------------------
  int Emsg(const char* pfx,
           XrdOucErrInfo& einfo,
           int ecode,
           const char* op,
           const char* target = "");

  //----------------------------------------------------------------------------
  //! @brief Close a directory object
  //!
  //! return SFS_OK
  //!---------------------------------------------------------------------------
  int close();

  // ---------------------------------------------------------------------------
  //! return name of an open directory
  // ---------------------------------------------------------------------------
  const char*
  FName()
  {
    return dirName.c_str();
  }


  typedef std::set<std::string> listing_t;

  static eos::common::LRU::Cache<std::string, shared_ptr<listing_t>> dirCache;

private:

  std::string getCacheName(uint64_t id, uint64_t mtime_sec, uint64_t mtime_nsec, bool nofiles, bool nodirs);

  std::string dirName;
  eos::common::VirtualIdentity vid;
  shared_ptr<listing_t> dh_list;
  listing_t::const_iterator dh_it;
  std::mutex mDirLsMutex; ///< Mutex protecting access to dh_list
};
