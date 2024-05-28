 //------------------------------------------------------------------------------
//! @file cfsutil.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the quota en-/disabling
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include <string>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

class cfsutil
{
public:
  static bool checkAndCreateDirectory(const std::string& path) {
    struct stat info;
    
    // Check if the directory exists
    if (stat(path.c_str(), &info) != 0) {
      if (errno == ENOENT) {
	std::cerr << "info: Directory does not exist. Creating directory: " << path << std::endl;
	// Create the directory
	if (mkdir(path.c_str(), 0500) != 0) {
	  std::cerr << "error: could not create directory: " << strerror(errno) << std::endl;
	  return false;
	}
	std::cerr << "info: directory created and permissions set to 500." << std::endl;
      } else {
	std::cerr << "error: could not check for directory ' " << path << "' : " << strerror(errno) << std::endl;
	return false;
      }
    } else {
      if (S_ISDIR(info.st_mode)) {
	// Check the permissions
	if ((info.st_mode & 0777) == 0500) {
	} else {
	  std::cerr << "info: directory exists but permissions are not 500. Setting permissions to 500 on directory '" << path << "'" << std::endl;
	  // Set the permissions to 500
	  if (chmod(path.c_str(), 0500) != 0) {
	    std::cerr << "error: failed to set permissions: " << strerror(errno) << std::endl;
	    return false;
	  }
	  std::cerr << "info: permissions set to 500 on directory '" << path << "'" << std::endl;
	}
      } else {
	std::cerr << "error: path exists but is not a directory '" << path << "'"<< std::endl;
	return false;
      }
    }
    std::cerr << std::flush;
    return true;
  }
private:
};
