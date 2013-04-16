//------------------------------------------------------------------------------
// File: DirEos.hh
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

#ifndef __EOSBMK_DIREOS_HH__
#define __EOSBMK_DIREOS_HH__

/*-----------------------------------------------------------------------------*/
#include <cstring>
#include "Namespace.hh"
#include "common/Logging.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*-----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

//! Forward declaration of low level config object
class ConfigProto;

//------------------------------------------------------------------------------
//! Class DirEos - used for doing operations on EOS directories
//------------------------------------------------------------------------------
class DirEos: public eos::common::LogId
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param dirPath path to the directory
  //! @param eosInstance EOS instance to which to connect
  //!
  //----------------------------------------------------------------------------
  DirEos(const std::string& dirPath, const std::string& eosInstance);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DirEos();

  
  //----------------------------------------------------------------------------
  //! Stat directory
  //!
  //! @return true if file exists, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool Exist();


  //----------------------------------------------------------------------------
  //! Create directory
  //!
  //! @return true if creation successful, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool Create();

  
  //----------------------------------------------------------------------------
  //! Set extended attribute
  //!
  //! @param attrName extended attribute name
  //! @param attrValue extended attribute value
  //!
  //! @return true if attribute set successfully, otherwise false
  //! 
  //----------------------------------------------------------------------------
  bool SetXattr(const std::string& attrName, const std::string& attrValue);


  //----------------------------------------------------------------------------
  //! Get files form benchmark directory having the requried file size 
  //!
  //! @param fileSize requried file size
  //!
  //! @return vector of files in directory matchin the requirements
  //! 
  //----------------------------------------------------------------------------
  std::vector<std::string> GetMatchingFiles(const uint64_t fileSize);

  
  //----------------------------------------------------------------------------
  //! Check extended attribute
  //!
  //! @param attrName extended attribute name
  //! @param refValue reference value to which we compare
  //!
  //! @return true if attribute value matches the reference one, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool CheckXattr(const std::string& attrName, const std::string& refValue);


  //----------------------------------------------------------------------------
  //! Check if directory matches with the supplied configuration
  //!
  //! @param llconfig low level configuration object
  //!
  //! @return true directory matches with configuration, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool MatchConfig(const ConfigProto& llconfig);


  //----------------------------------------------------------------------------
  //! Set the extended attributes of the directory so that they match the config
  //!
  //! @param llconfig low level configuration object
  //!
  //! @return true if ext. attr. were successfully set, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool SetConfig(const ConfigProto& llconfig);
 
  
  //----------------------------------------------------------------------------
  //! Remove directory
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Remove();

  
 private:
  
  std::string mDirPath;    ///< path to the directory
  XrdCl::FileSystem* mFs;  ///< XrdCl file system instance 
};

EOSBMKNAMESPACE_END

#endif // __EOSBMK_DIREOS_HH__
