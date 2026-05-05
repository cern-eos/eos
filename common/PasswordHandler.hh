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

#pragma once
#include "common/Logging.hh"
#include "common/Namespace.hh"
#include "common/StringUtils.hh"
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper class to perform common operations related to passwords.
//------------------------------------------------------------------------------
class PasswordHandler {
public:

  //----------------------------------------------------------------------------
  // Check if file permissions are secure.
  //----------------------------------------------------------------------------
  static bool
  areFilePermissionsSecure(const std::string& path, mode_t mode)
  {
    // Disallow access to others
    if ((mode & 0007) != 0) {
      return false;
    }
    // By default files need to be readable only by the user
    mode_t expected_mode = 0400;
    mode_t expected_mask = 0770;
    // ".grp" files should be readable by the user and the group
    if (eos::common::endsWith(path, ".grp")) {
      expected_mode = 0440;
    }

    if ((mode & expected_mask) != expected_mode) {
      eos_static_crit("msg=\"unsecure permissions\" path=\"%s\" mode=%o "
                      "expected_mode=%o",
                      path.c_str(), mode, expected_mode);
      return false;
    }

    return true;
  }

  //----------------------------------------------------------------------------
  //! Read a password file, while taking the following into account:
  //! - Permissions must be secure - refuse to do anything otherwise
  //! - If file path ends in ".grp" the permissions can be 440
  //! - Otherwise the permissions need to be 400
  //! - Ending newlines are discarded.
  //----------------------------------------------------------------------------
  static bool readPasswordFile(const std::string &path, std::string &contents) {
    FILE *in = fopen(path.c_str(), "rb");
    if(!in) {
      eos_static_crit("Could not read pasword file: %s", path.c_str());
      return false;
    }

    // Ensure file permissions are 400.
    struct stat sb;
    if(fstat(fileno(in), &sb) != 0) {
      fclose(in);
      eos_static_crit("msg=\"failed fstat after open\" path=\"%s\"", path.c_str());
      return false;
    }

    if (!areFilePermissionsSecure(path, sb.st_mode)) {
      eos_static_crit("msg=\"file permissions are not secure\" path=\"%s\"",
                      path.c_str());
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
    eos::common::rtrim(contents);
    return retvalue;
  }
};

EOSCOMMONNAMESPACE_END
