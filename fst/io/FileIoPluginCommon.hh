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

#ifndef __EOSFST_FILEIOPLUGINHELPER_HH__
#define __EOSFST_FILEIOPLUGINHELPER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include "fst/io/FsIo.hh"
#include "fst/io/XrdIo.hh"
#ifdef KINETICIO_FOUND
#include "fst/io/KineticIo.hh"
#endif
#ifdef DAVIX_FOUND
#include "fst/io/DavixIo.hh"
#endif
#include "common/LayoutId.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

//! Forward declaration
class XrdFstOfsFile;

//------------------------------------------------------------------------------
//! Class used to obtain a IO plugin object
//------------------------------------------------------------------------------

class FileIoPluginHelper {
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------

  FileIoPluginHelper ()
  {
    //empty
  }


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------

  ~FileIoPluginHelper ()
  {
    //empty
  }



  //--------------------------------------------------------------------------
  //! Get IO object
  //!
  //! @param file file handler
  //! @param layoutId layout id type
  //! @param error error information
  //!
  //! @return requested layout type object
  //!
  //--------------------------------------------------------------------------

  static FileIo*
  GetIoObject (int ioType,
               XrdFstOfsFile* file = 0,
               const XrdSecEntity* client = 0)
  {
    if (ioType == LayoutId::kLocal)
    {
      return static_cast<FileIo*> (new FsIo());
    }
    else
      if (ioType == LayoutId::kXrdCl)
    {
      return static_cast<FileIo*> (new XrdIo());
    }
    else
      if (ioType == LayoutId::kKinetic)
    {
#ifdef KINETICIO_FOUND
      return static_cast<FileIo*> (new KineticIo());
#endif
      eos_static_warning("EOS has been compiled without Kinetic support.");
      return NULL;
    }
    else
      if (ioType == LayoutId::kRados)
    {
      return static_cast<FileIo*> (new RadosIo(file, client));
    }
    else
      if (ioType == LayoutId::kDavix)
    {
#ifdef DAVIX_FOUND
      return static_cast<FileIo*> (new DavixIo());
#endif
      eos_static_warning("EOS has been compiled without DAVIX support.");
      return NULL;
    }
    if (ioType == LayoutId::kXrdCl)
    {
      return static_cast<FileIo*> (new XrdIo());
    }

    return 0;
  }

  static eos::common::Attr*
  GetIoAttr (const char* url)
  {
    int ioType = eos::common::LayoutId::GetIoType(url);

    if (ioType == LayoutId::kLocal)
    {
      return static_cast<eos::common::Attr*> (eos::common::Attr::OpenAttr(url));
    }
    else
      if (ioType == LayoutId::kXrdCl)
    {
      return static_cast<eos::common::Attr*> (eos::fst::XrdIo::Attr::OpenAttr(url));
    }
    else
      if (ioType == LayoutId::kKinetic)
    {
#ifdef KINETICIO_FOUND
      return static_cast<eos::common::Attr*> (eos::fst::KineticIo::Attr::OpenAttr(url));
#endif
      eos_static_warning("EOS has been compiled without Kinetic support.");
      return NULL;
    }
    else
      if (ioType == LayoutId::kRados)
    {
      return static_cast<eos::common::Attr*> (eos::fst::RadosIo::Attr::OpenAttr(url));
    }
    else
      if (ioType == LayoutId::kDavix)
    {
#ifdef DAVIX_FOUND
      return static_cast<eos::common::Attr*> (eos::fst::DavixIo::Attr::OpenAttr(url));
#endif
      eos_static_warning("EOS has been compiled without DAVIX support.");
      return NULL;
    }
    return NULL;
  }
};

EOSFSTNAMESPACE_END

#endif // __ EOSFST_FILEIOPLUGINHELPER_HH__

