// ----------------------------------------------------------------------
// File: eosdropboxd.hh
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

/*----------------------------------------------------------------------------*/
#include "console/dropbox/eosdropboxd.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "unistd.h"
/*----------------------------------------------------------------------------*/

#define PROGNAME "eosdropboxd"

void shutdown(int sig) 
{
  exit(0);
}


int main() 
{
  // stop all other running drop box commands under our account
  signal(SIGTERM, shutdown);

  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  vid.uid = getuid();
  vid.gid = getgid();

  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eosdropboxd@localhost");

  if(getenv("EOSDEBUG")) {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  } else {
    eos::common::Logging::SetLogPriority(LOG_INFO);
  }

  XrdOucString syskill = "kill -15 `pgrep -f dropbox -U ";
  syskill += (int) getuid();
  syskill += " | grep -v grep | grep -v "; syskill += (int) getpid(); syskill += "`";
  eos_static_debug("system: %s", syskill.c_str());
  system(syskill.c_str());
  
  // go into background mode
  pid_t m_pid=fork();
  if(m_pid<0) {
    fprintf(stderr,"ERROR: Failed to fork daemon process\n");
    exit(-1);
  } 
  
  // kill the parent
  if(m_pid>0) {
    exit(0);
  }
  
  umask(0); 
  
  pid_t sid;
  if((sid=setsid()) < 0) {
    fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
    exit(-1);
  }

  if ((chdir("/")) < 0) {
    /* Log any failure here */
    exit(-1);
  }
  
  XrdOucString logfile = "/var/tmp/eosdropbox."; logfile += (int)getuid(); logfile += ".log";

  close(STDERR_FILENO);
  freopen(logfile.c_str(),"w+",stderr);
  freopen(logfile.c_str(),"w+",stdout);

  eos_static_info("started %s ...", PROGNAME);

  // configurations are stored here
  XrdOucString configdirectory = homedirectory + "/.eosdropboxd";

  do {
    eos_static_info("checking dropbox configuration ...");
    
    A
    sleep(60);
  } while (1);

}

