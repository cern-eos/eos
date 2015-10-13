// ----------------------------------------------------------------------
// File: FileMap.hh
// Author: Andreas-Joachim Peters - CERN
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

/**
 * @file   FileMap.hh
 *
 * @brief  Class storing a key value map in a append blob
 *
 *
 */

#ifndef __EOSCOMMON_FILEMAP_HH__
#define __EOSCOMMON_FILEMAP_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class storing a key-value map in a append log
//!
//! The format of the blob is
//! when setting a key:
//! '+ base64(key) base64(value)\n'
//! when deleting a key:
//! '- base64(key) base64(":")\n'
/*----------------------------------------------------------------------------*/
class FileMap {
private:
  std::map<std::string, std::string> mMap;
  std::string mBlob;
  XrdSysMutex mMutex;

public:

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FileMap ()
  {
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~FileMap ()
  {
  }

  // ---------------------------------------------------------------------------
  //! Set a Key-Val pair, returns append string
  // ---------------------------------------------------------------------------

  std::string Set (std::string key, std::string val)
  {
    XrdSysMutexHelper mLock(mMutex);
    mMap[key] = val;
    XrdOucString key64;
    XrdOucString val64;
    eos::common::SymKey::Base64Encode((char*) key.c_str(), key.length(), key64);
    eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);
    std::string append_string = "+ ";
    append_string += key64.c_str();
    append_string += " ";
    append_string += val64.c_str();
    append_string += "\n";
    return append_string;
  }

  // ---------------------------------------------------------------------------
  //! Get a Key
  // ---------------------------------------------------------------------------

  std::string Get (std::string key)
  {
    XrdSysMutexHelper mLock(mMutex);
    return mMap[key];
  }

  // ---------------------------------------------------------------------------
  //! Delete a Key, returns append string
  // ---------------------------------------------------------------------------

  std::string Delete (std::string key)
  {
    XrdSysMutexHelper mLock(mMutex);
    mMap.erase(key);
    XrdOucString key64;
    XrdOucString val64;
    eos::common::SymKey::Base64Encode((char*) key.c_str(), key.length(), key64);
    eos::common::SymKey::Base64Encode((char*) ":", 1, val64);
    std::string append_string = "- ";
    append_string += key64.c_str();
    append_string += " ";
    append_string += val64.c_str();
    append_string += "\n";
    return append_string;
  }

  // ---------------------------------------------------------------------------
  //! Fill Map from file blob
  // ---------------------------------------------------------------------------

  bool Load (std::string blob)
  {
    XrdSysMutexHelper mLock(mMutex);
    mBlob = blob;
    mMap.clear();
    std::istringstream mapStream(mBlob);
    if (!blob.length())
      return true;
    while (!mapStream.eof())
    {
      std::string line;
      getline(mapStream, line);
      std::vector<std::string> tokens;
      if (!line.length())
        continue;
      eos::common::StringConversion::Tokenize(line,
                                              tokens);

      if (tokens.size() != 3)
      {
        return false;
      }
      XrdOucString key64 = tokens[1].c_str();
      XrdOucString val64 = tokens[2].c_str();
      char* keyout = 0;
      char* valout = 0;
      unsigned int keyout_len = 0;
      unsigned int valout_len = 0;
      eos::common::SymKey::Base64Decode(key64, keyout, keyout_len);
      eos::common::SymKey::Base64Decode(val64, valout, valout_len);
      if (keyout && valout)
      {
        std::string key;
        std::string val;
        key.assign(keyout, keyout_len);
        val.assign(valout, valout_len);
        mMap[key] = val;
      }
      if (keyout)
        free(keyout);
      if (valout)
        free(valout);
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  //! Return a trimmed blob
  // ---------------------------------------------------------------------------

  std::string Trim ()
  {
    XrdSysMutexHelper mLock(mMutex);
    mBlob = "";
    for (auto it = mMap.begin(); it != mMap.end(); ++it)
    {
      XrdOucString key64;
      XrdOucString val64;
      eos::common::SymKey::Base64Encode((char*) it->first.c_str(), it->first.length(), key64);
      eos::common::SymKey::Base64Encode((char*) it->second.c_str(), it->second.length(), val64);
      std::string append_string = "+ ";
      append_string += key64.c_str();
      append_string += " ";
      append_string += val64.c_str();
      append_string += "\n";
      mBlob += append_string;
    }
    return mBlob;
  }
};
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
