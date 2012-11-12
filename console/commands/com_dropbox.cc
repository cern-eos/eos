// ----------------------------------------------------------------------
// File: com_dropbox.cc
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
#include "console/ConsoleMain.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/

/* Dropbox interface */
int
com_dropbox (char *arg) {
  XrdOucTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();

  XrdOucString homedirectory=getenv("HOME");
  XrdOucString configdirectory = homedirectory + "/.eosdropboxd";
      
  if (!getenv("HOME")) {
    fprintf(stderr,"error: your HOME environment variable is not defined - I need that!\n");
    global_retc = -1;
    return (0);
  }
  if ( (subcommand.find("--help")!=STR_NPOS) || (subcommand.find("-h")!=STR_NPOS))
    goto com_dropbox_usage;
  
  if ( (subcommand != "add") && (subcommand != "rm") && (subcommand != "start") && (subcommand != "stop") && (subcommand != "ls") ) {
    goto com_dropbox_usage;
  }
  
  if (subcommand == "add") {
    XrdOucString remotedir = subtokenizer.GetToken();
    XrdOucString localdir  = subtokenizer.GetToken();

    if (localdir.beginswith("/eos")) {
      fprintf(stderr,"error: the local directory can not start with /eos!\n");
      global_retc = -1;
      return (0);      
    }

    if (!remotedir.beginswith("/eos")) {
      fprintf(stderr,"error: the remote directory has to start with /eos!\n");
      global_retc = -1;
      return (0);      
    }

    if ((!remotedir.endswith("/dropbox/") && (!remotedir.endswith("/dropbox")))) {
      fprintf(stderr,"error: your remote directory has to be named '/dropbox'\n");
      global_retc = -1;
      return (0);
    }

    XrdOucString configdummy = configdirectory + "/dummy";
    eos::common::Path cPath(configdummy.c_str());
    if (!cPath.MakeParentPath(S_IRUSR | S_IWUSR)) {
      fprintf(stderr,"error: cannot create %s\n", configdirectory.c_str());
      global_retc = -EPERM;
      return (0);
    }
    
    XrdOucString localdirdummy = localdir + "/dummy";
    eos::common::Path lPath(localdirdummy.c_str());
    if ( (!lPath.MakeParentPath(S_IRUSR | S_IWUSR)) || (access(localdir.c_str(),W_OK|X_OK))) {
      fprintf(stderr,"error: cannot access %s\n", localdirdummy.c_str());
      global_retc = -EPERM;
      return (0);
    }

    XrdOucString localdircontracted = localdir;
    while (localdircontracted.replace("/","::")) {}

    XrdOucString newconfigentry = configdirectory + "/";
    newconfigentry += localdircontracted;

    struct stat buf;
    if (!stat(newconfigentry.c_str(), &buf)) {
      fprintf(stderr,"error: there is already a configuration for the local directory %s\n", localdir.c_str());
      global_retc = EEXIST;
      return (0);
    }

    // we store the remote dir configuration in the target of a symlink!
    if (symlink(remotedir.c_str(),newconfigentry.c_str())) {
      fprintf(stderr,"error: failed to symlink the new configuration entry %s\n", localdir.c_str());
      global_retc = errno;
      return (0);
    }
    
    fprintf(stderr,"success: created dropbox configuration from %s |==> %s\n", remotedir.c_str(), localdir.c_str());
    global_retc = 0;
    return (0);
  }

  if (subcommand == "start") {
    XrdOucString resync = subtokenizer.GetToken();    
    if (resync.length()) {
      int rc = system("eosdropboxd --resync");
      if (WEXITSTATUS(rc)) {
	fprintf(stderr,"error: failed to run eosdropboxd --resync\n");
      }
    } else {
      int rc = system("eosdropboxd");
      if (WEXITSTATUS(rc)) {
	fprintf(stderr,"error: failed to run eosdropboxd\n");
      }
    }
    global_retc = 0;
    return (0);
  }

  if (subcommand == "rm") {

    global_retc = 0;
    return (0);
  }

  if (subcommand == "stop") {
    int rc = system("pkill -15 eosdropboxd >& /dev/null");
    if (WEXITSTATUS(rc)) {
      fprintf(stderr,"warning: didn't kill any esodropboxd");
    }
    global_retc = 0;
    return (0);
  }

  if (subcommand == "ls") {
    DIR* dir = opendir(configdirectory.c_str());
    if (!dir) {
      fprintf(stderr,"error: cannot opendir %s\n", configdirectory.c_str());
      global_retc = errno;
      return (0);
    }
    struct dirent* entry=0;
    while ( (entry = readdir(dir))) {
      XrdOucString sentry = entry->d_name;
      XrdOucString configentry= configdirectory; configentry += "/";
      if ( (sentry == ".")  || (sentry == "..") ) {
	continue;
      }
      configentry += sentry;
      char buffer[4096];
      ssize_t nr = readlink(configentry.c_str(), buffer, sizeof(buffer));
      if (nr < 0) {
	fprintf(stderr,"error: unable to read link %s errno=%d\n", configentry.c_str(), errno);
      } else {
	buffer[nr] = 0;
	while (sentry.replace("::","/")) {}
	fprintf(stdout,"[sync] %32s |==> %-32s\n", buffer, sentry.c_str());
      }
    }
    closedir(dir);
    global_retc = 0;
    return (0);
  }

 com_dropbox_usage:
  fprintf(stdout,"Usage: dropbox add|rm|start|stop|add|rm|ls ...\n");
  fprintf(stdout,"'[eos] dropbox ...' provides dropbox functionality for eos.\n");

  fprintf(stdout,"Options:\n");
  fprintf(stdout,"dropbox add <eos-dir> <local-dir>   :\n");
  fprintf(stdout,"                                                  add drop box configuration to synchronize from <eos-dir> to <local-dir>!\n");
  fprintf(stdout,"dropbox rm <eos-dir>                :\n");
  fprintf(stdout,"                                                  remove drop box configuration to synchronize from <eos-dir>!\n");
  fprintf(stdout,"dropbox start [--resync]             :\n");
  fprintf(stdout,"                                                  start the drop box daemon for all configured dropbox directories! If the --resync flag is given, the local directory is resynced from scratch from the remote directory!\n");
  fprintf(stdout,"dropbox stop                        :\n");
  fprintf(stdout,"                                                  stop the drop box daemon for all configured dropbox directories!\n");
  fprintf(stdout,"dropbox ls                          :\n");
  fprintf(stdout,"                                                  list configured drop box daemons and their status\n");
  return (0);
}
