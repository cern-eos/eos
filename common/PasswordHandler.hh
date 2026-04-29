// ----------------------------------------------------------------------
// File: PasswordFileReader.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef EOS_COMMON_PASSWORD_HANDLER_HH
#define EOS_COMMON_PASSWORD_HANDLER_HH

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "common/Namespace.hh"
#include "common/Logging.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper class to perform common operations related to passwords.
//------------------------------------------------------------------------------
class PasswordHandler {
public:
  //----------------------------------------------------------------------------
  // Check if file permissions are secure: 0400, or 0440 if ending in '.grp'
  //----------------------------------------------------------------------------
  static bool
  areFilePermissionsSecure(mode_t mode, const char* path)
  {
    mode_t perms = mode & 0777;
    size_t len = strlen(path);
    // this matches upstream xrootd logic in XrdSecsssKT::fileMode
    bool is_grp = (len >= 4 && memcmp(path + len - 4, ".grp", 4) == 0);

    if (is_grp) {
      if (perms == 0440) {
        return true;
      } else {
        eos_static_crit("Refusing to read %s: permissions must be 0440.", path);
      }
    } else {
      if (perms == 0400) {
        return true;
      } else {
        eos_static_crit("Refusing to read %s: permissions must be 0400.", path);
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  // Right trim password, remove whitespace
  //----------------------------------------------------------------------------
  static void rightTrimWhitespace(std::string &src)
  {
    src.erase(src.find_last_not_of(" \t\n\r\f\v") + 1);
  }

  //----------------------------------------------------------------------------
  // Read a password file, while taking the following into account:
  // - Permissions must be secure - refuse to do anything otherwise.
  // - Ending newlines are discarded.
  //----------------------------------------------------------------------------
  static bool readPasswordFile(const std::string &path, std::string &contents) {
    FILE *in = fopen(path.c_str(), "rb");
    if(!in) {
      eos_static_crit("Could not read pasword file: %s", path.c_str());
      return false;
    }

    // Ensure file permissions are secure.
    struct stat sb;
    if(fstat(fileno(in), &sb) != 0) {
      fclose(in);
      eos_static_crit("Could not fstat %s after opening (should never happen?!)", path.c_str());
      return false;
    }

    if (!areFilePermissionsSecure(sb.st_mode, path.c_str())) {
      fclose(in);
      return false;
    }

    // Do actual read...
    std::ostringstream ss;

    const int BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];

    bool retvalue = true;
    while(true) {
      size_t bytesRead = fread(buffer, 1, BUFFER_SIZE, in);

      if(bytesRead > 0) {
        ss.write(buffer, bytesRead);
      }

      // end of file
      if(bytesRead != BUFFER_SIZE) {
        retvalue = feof(in);
        break;
      }
    }

    fclose(in);
    contents = ss.str();

    rightTrimWhitespace(contents);
    return retvalue;
  }

};

EOSCOMMONNAMESPACE_END

#endif
