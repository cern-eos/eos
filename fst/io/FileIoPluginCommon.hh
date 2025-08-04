//------------------------------------------------------------------------------
//! @file FileIoPluginHelper.hh
//! @author Geoffray Adde - CERN
//! @brief Class generating an IO plugin object
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

#ifndef __EOS_FST_FILEIOPLUGINHELPER_HH__
#define __EOS_FST_FILEIOPLUGINHELPER_HH__

#include "fst/io/FileIo.hh"
#include "fst/io/local/FsIo.hh"
#include "fst/io/xrd/XrdIo.hh"
#include "fst/io/davix/DavixIo.hh"
#include "fst/io/nfs/NfsIo.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

//! Forward declaration
class XrdFstOfsFile;

//------------------------------------------------------------------------------
//! Class used to obtain a IO plugin object
//------------------------------------------------------------------------------
class FileIoPluginHelper
{
public:
  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  FileIoPluginHelper() {}

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~FileIoPluginHelper() {}

  //--------------------------------------------------------------------------
  //! Get IO object
  //!
  //! @param file file handler
  //! @param layoutId layout id type
  //! @param error error information
  //!
  //! @return requested layout type object
  //--------------------------------------------------------------------------
  static FileIo*
  GetIoObject(std::string path, XrdFstOfsFile* file = 0,
              const XrdSecEntity* client = 0)
  {
    auto ioType = eos::common::LayoutId::GetIoType(path.c_str());

    if (ioType == LayoutId::kLocal) {
      return static_cast<FileIo*>(new FsIo(path));
    } else if (ioType == LayoutId::kXrdCl) {
      return static_cast<FileIo*>(new XrdIo(path));
    } else if (ioType == LayoutId::kDavix) {
#ifdef HAVE_DAVIX
      return static_cast<FileIo*>(new DavixIo(path));
#endif // HAVE_DAVIX
      eos_static_warning("%s", "msg=\"EOS has been compiled without DAVIX support\"");
      return nullptr;
    } else if (ioType == LayoutId::kNfs) {
#ifdef HAVE_NFS
      return static_cast<FileIo*>(new NfsIo(path, file, client));
#endif // HAVE_NFS
      eos_static_warning("%s", "msg=\"EOS has been compiled without NFS support\"");
      return nullptr;
    }

    return nullptr;
  }
};

EOSFSTNAMESPACE_END

#endif // __ EOS_FST_FILEIOPLUGINHELPER_HH__
