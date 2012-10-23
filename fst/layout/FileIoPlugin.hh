//------------------------------------------------------------------------------
// File: FileIoPlugin.hh
// Author: Elvin-Alin Sindrilaru - CERN
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

//------------------------------------------------------------------------------
//! @file FileIoPlugin.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class generating an IO plugin object
//------------------------------------------------------------------------------

#ifndef __EOSFST_FILEIOPLUGIN_HH__
#define __EOSFST_FILEIOPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "fst/layout/LocalFileIo.hh"
#include "fst/layout/XrdFileIo.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

//------------------------------------------------------------------------------
//! Class used to obtain a IO plugin object
//------------------------------------------------------------------------------
class FileIoPlugin
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    FileIoPlugin() {
      //empty
    };

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~FileIoPlugin() {
      //empty
    };

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
    static FileIo* GetIoObject( XrdFstOfsFile*      file,
                                int                 ioType,
                                const XrdSecEntity* client,
                                XrdOucErrInfo*      error ) {
      
      if ( ioType == LayoutId::kLocal ) {
        return dynamic_cast<FileIo*>( new LocalFileIo( file, client, error ) );
      }

      if ( ioType == LayoutId::kXrdCl ) {
        return dynamic_cast<FileIo*>( new XrdFileIo( file, client, error ) );
      }
 
      return 0;
    }
};

EOSFSTNAMESPACE_END

#endif
