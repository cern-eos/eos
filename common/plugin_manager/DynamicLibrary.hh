//------------------------------------------------------------------------------
//! @file DynamicLibrary.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef __EOS_PF_DYNAMIC_LIBRARY_HH__
#define __EOS_PF_DYNAMIC_LIBRARY_HH__

/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class DyanmicLibrary
//------------------------------------------------------------------------------
class DynamicLibrary
{
 public:

  //----------------------------------------------------------------------------
  //! Load dynamic library
  //!
  //! @param path path to dynamic library
  //! @param error error message
  //!
  //! @return handle to DynamicLibrary object
  //----------------------------------------------------------------------------
  static DynamicLibrary* Load(const std::string& path,
                              std::string& error);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DynamicLibrary();

  //----------------------------------------------------------------------------
  //! Get hadle to symbol from current dynamic library
  //!
  //! @param name symbol name
  //!
  //! @return handle to requestes object if successful, otherwise NULL
  //----------------------------------------------------------------------------
  void* GetSymbol(const std::string& name);

 private:

  void* mHandle;  ///< handle to dynamic library

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DynamicLibrary();

  //----------------------------------------------------------------------------
  //! Constructor with parameter
  //----------------------------------------------------------------------------
  DynamicLibrary(void* handle);

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  DynamicLibrary(const DynamicLibrary&) = delete;
};

EOSCOMMONNAMESPACE_END

#endif // __EOS_PF_DYNAMIC_LIBRARY_HH__
