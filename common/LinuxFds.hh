// ----------------------------------------------------------------------
// File: LinuxFds.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/ASwitzerland                                 *
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
 * @file   LinuxFds.hh
 * 
 * @brief  Class counting the current filedescriptor usage
 *  
 */

#ifndef __EOSCOMMON__LINUXFDS__HH
#define __EOSCOMMON__LINUXFDS__HH

#include "common/Namespace.hh" 
#include <sys/types.h>
#include <dirent.h>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static Class to inspect file descriptor usage
//! 
//! Example: linux_fds_t fds; GetFdUsage(fds);
//! 
/*----------------------------------------------------------------------------*/
class LinuxFds {
public:
  typedef struct {
    unsigned long long devices,filesystem,sockets,pipes,anon_inode,other,all;
  } linux_fds_t;

  static bool GetFdUsage(linux_fds_t& result)
  {
    std::string link_base = "/proc/self/fd/";

    const char* fd_path = "/proc/self/fd";
    result.devices = result.filesystem = result.sockets = result.pipes = result. anon_inode, result.other = result.all = 0;

    DIR *d = opendir(fd_path);
    if(!d){
      perror(fd_path);
      return false;
    }
    
    struct dirent *dent;
    char linkbuffer[4096];
    
    while((dent=readdir(d))!=NULL) {
      std::string link_name = link_base;
      link_name += dent->d_name;
      ssize_t lsize=0;
      if ( (lsize = ::readlink(link_name.c_str(),linkbuffer, sizeof(linkbuffer))) > 0)
      {
	result.all++;
	std::string target(linkbuffer,lsize);
	if (target.substr(0,7) == "socket:") {
	  result.sockets++;
	} else if (target.substr(0,4) == "/dev/") {
	  result.devices++;
	} else if (target.substr(0,1) == "/") {
	  result.filesystem++;
	} else if (target.substr(0,5) == "pipe:") {
	  result.pipes++;
	} else if (target.substr(0,11) == "anon_inode:") {
	  result.anon_inode++;
	} else {
	  result.other++;
	}
      } else {
	// if we have a forked setuid program, we don't have the permission to resolve symlinks,
	// we just count all fds
	result.all++;
      }
    }

    closedir(d);
    return true;
  }
};

EOSCOMMONNAMESPACE_END
 
#endif
