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
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include "unistd.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

#define PROGNAME "eosdropboxd"

void shutdown(int sig) 
{
  exit(0);
}

bool changed(const char* path) 
{
  static std::map<std::string, std::string> changemap;

  XrdOucString findstring = "find "; findstring += path; findstring += " | sha1sum";
  eos_static_debug(findstring.c_str());
  FILE* shafp = popen(findstring.c_str(),"r");
  bool retc = false;
  if (shafp) {
    char sha1sum[4096];
    int item = fscanf(shafp,"%s -\n", sha1sum);
    if (item == 1) {
      std::string tmpsha1sum = sha1sum;
      if (changemap[path].length()) {
	if (tmpsha1sum != changemap[path]) {
	  retc =  true;
	}
      }
      changemap[path] = tmpsha1sum;
    }
    pclose(shafp);
  }
  return retc;
}


int touch(const char* path, unsigned long long mtime) {
  // touches a file and set's mtime to access & modification time
  int fd = open(path, O_RDWR | O_CREAT, S_IRWXU);
  if (fd < 0) 
    return errno;

  struct timeval tv[2];
  tv[0].tv_sec = mtime;
  tv[0].tv_usec = 0;
  tv[1].tv_sec = mtime;
  tv[1].tv_usec = 0;

  if (futimes(fd, tv)) {
    int rc = errno;
    close(fd);
    return rc;
  }
  close(fd);
  eos_static_debug("path=%s utime=%llu", path, mtime);
  return  0;
}

int main(int argc, char* argv[]) 
{
  // stop all other running drop box commands under our account
  signal(SIGTERM, shutdown);
  
  bool resync = false;

  if ( (argc ==2) ) {
    if (!strcmp(argv[1], "--resync")) {
      resync = true;
    }
  }

  XrdOucString logfile = "/var/tmp/eosdropbox."; logfile += (int)getuid(); logfile += ".log";
  XrdOucString homedirectory=getenv("HOME");

  fprintf(stderr,"# Starting %s : logfile %s\n", PROGNAME, logfile.c_str());
  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  vid.uid = getuid();
  vid.gid = getgid();

  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eosdropboxd@localhost");

  if(getenv("EOS_DEBUG")) {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  } else {
    eos::common::Logging::SetLogPriority(LOG_INFO);
  }

  XrdOucString syskill = "kill -15 `pgrep -f eosdropboxd -U ";
  syskill += (int) getuid();
  syskill += " | grep -v grep | grep -v "; syskill += (int) getpid(); syskill += " | awk '{printf(\"%s \",$1)}' `";
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

  if ((chdir("/var/tmp/")) < 0) {
    /* Log any failure here */
    exit(-1);
  }
  

  close(STDERR_FILENO);
  freopen(logfile.c_str(),"w+",stderr);
  freopen(logfile.c_str(),"w+",stdout);

  eos_static_info("started %s ...", PROGNAME);

  // configurations are stored here
  XrdOucString configdirectory = homedirectory + "/.eosdropboxd";

  size_t counter=0;
  do {
    eos_static_debug("checking dropbox configuration ...");
    std::map<std::string, std::string> syncmap;
    std::map<std::string, unsigned long long> remotesizemap;
    std::map<std::string, struct timespec>    remotemtimemap;
    std::map<std::string, unsigned long long> localsizemap;
    std::map<std::string, struct timespec>    localmtimemap;

    // -----------------------------------------------------------------------------------
    // read the configured synchronizations
    // -----------------------------------------------------------------------------------
    
    DIR* dir = opendir(configdirectory.c_str());
    if (!dir) {
      eos_static_err("cannot opendir %s\n", configdirectory.c_str());
      sleep(60);
      continue;
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
        eos_static_err("unable to read link %s errno=%d\n", configentry.c_str(), errno);
      } else {
        buffer[nr] = 0;
        while (sentry.replace("::","/")) {}
        eos_static_debug("[sync] %32s |==> %-32s\n", buffer, sentry.c_str());
	syncmap[buffer] = sentry.c_str();
      }
    }
    
    closedir(dir);
    
    // -----------------------------------------------------------------------------------
    // do the synchronisation
    // -----------------------------------------------------------------------------------

    std::map<std::string, std::string>::const_iterator it;
    
    for (it=syncmap.begin(); it != syncmap.end(); it++) {
      if (resync) {
	// -----------------------------------------------------------------------------------
	// in resync mode we wipe out all state from the local drop box and force a full resynchronization
	// -----------------------------------------------------------------------------------
	XrdOucString statedir = it->second.c_str();
	statedir += "/.dropbox/";
	XrdOucString rmstatedir = "rm -rf "; rmstatedir += statedir;
	int rc = system(rmstatedir.c_str());
	if (WEXITSTATUS(rc)) {
	  eos_static_err("[resync] could not wipe %s", statedir.c_str());
	} else {
	  eos_static_debug("[resync] wiped %s", statedir.c_str());
	}
      }

      if (!resync && ((counter%6))) {
	// check if there was a change in the local directory since last time
	if (!changed(it->second.c_str())) {
	  eos_static_debug("skipping check ... no local changes");
	  continue;
	} else {
	  eos_static_info("local modifications found ...");
	}
      }
      // -----------------------------------------------------------------------------------
      // do the remote scan
      // -----------------------------------------------------------------------------------
      XrdOucString eosfind="eos -b find -f --mtime --size "; eosfind += it->first.c_str();
      eos_static_debug("[find] %s", eosfind.c_str());

      FILE* pf = popen(eosfind.c_str(),"r");
      if (!pf) {
	eos_static_err("[find] cannot execute eos find command");
      } else {
	int item=0;
	char path[4096];
	unsigned long long mtime, mtime_nsec;
	unsigned long long size;

	while ( (item = fscanf(pf,"path=%s size=%llu mtime=%llu.%llu\n", path, &size, &mtime, &mtime_nsec) ) == 4) {
	  XrdOucString endpath = path;
	  endpath.replace(it->first.c_str(),"");

	  // check if we have this file
	  XrdOucString localpath = it->second.c_str(); 
	  while(localpath.replace("::","/")) {}
	  localpath += "/"; localpath += endpath;
	  while(localpath.replace("//","/")){}
	  eos_static_debug("[find] path=%s mtime=%llu.%llu size=%llu syncpath=%s localpath=%s", path, mtime, mtime_nsec, size, endpath.c_str(), localpath.c_str());
	  // store the meta data identifying the files
	  remotesizemap[endpath.c_str()] = size;
	  remotemtimemap[endpath.c_str()].tv_sec = mtime;
	  remotemtimemap[endpath.c_str()].tv_nsec = mtime_nsec;
	}
	pclose(pf);
      }

      // -----------------------------------------------------------------------------------
      // do the local scan
      // -----------------------------------------------------------------------------------
      XrdOucString localfind="find "; localfind += it->second.c_str(); localfind += " -type f -printf \"path=/%P size=%s mtime=%C@\\n\"";
      eos_static_debug("[find] %s", localfind.c_str());

      pf = popen(localfind.c_str(),"r");
      if (!pf) {
	eos_static_err("[find] cannot execute eos find command");
      } else {
	int item=0;
	char path[4096];
	unsigned long long mtime;
	unsigned long long size;

	while ( (item = fscanf(pf,"path=%s size=%llu mtime=%llu\n", path, &size, &mtime) ) == 3) {
	  XrdOucString spath=path;
	  if (spath.beginswith("/.")) {
	    // this are tag files which we skip for synchronization
	    continue;
	  }
	  eos_static_debug("[find] path=%s mtime=%llu size=%llu\n", path, mtime, size);
	  // store the meta data identifying the files
	  localsizemap[path] = size;
	  localmtimemap[path].tv_sec = mtime;
	  localmtimemap[path].tv_nsec = 0;
	}
	pclose(pf);
      }

      // -----------------------------------------------------------------------------------
      // upload,download,delete local files
      // -----------------------------------------------------------------------------------
      
      std::map<std::string, unsigned long long>::const_iterator pathit;    
      
      eos_static_info("[local] %d files", localsizemap.size());
      
      for (pathit = localsizemap.begin(); pathit != localsizemap.end(); pathit++) {
	XrdOucString localpath=it->second.c_str(); localpath += pathit->first.c_str(); 
	XrdOucString remotepath=it->first.c_str(); remotepath += pathit->first.c_str();

	
	eos_static_debug("[local] checking %s\n", pathit->first.c_str());
	struct stat buf;
	struct stat filebuf;
	
	XrdOucString dropboxtagfile="";
	dropboxtagfile += "."; 
	dropboxtagfile += pathit->first.c_str(); 
	while (dropboxtagfile.replace("/","::")) {}
	
	dropboxtagfile.insert("/.dropbox/",0);
	dropboxtagfile.insert(it->second.c_str(),0);
	
	eos::common::Path cPath(dropboxtagfile.c_str());
	if (!cPath.MakeParentPath(S_IRWXU)) {
	  eos_static_err("unable to make parent path of %s", dropboxtagfile.c_str());
	  continue;
	}
	
	eos_static_debug("[local] stat %s", dropboxtagfile.c_str());
	// check if the file is not coming via drop box synchronisation
	
	XrdOucString action = "noaction";

	if (stat(localpath.c_str(), &filebuf) ) {
	  // file disappeared in the meanwhil
	  eos_static_err("[local] file %s cannot be stat'ed errno=%d", localpath.c_str(), errno);
	  continue;
	}

	if ( (stat(dropboxtagfile.c_str(), &buf) ) ) {
	  // no tag file - new file
	  if (!remotesizemap.count(pathit->first)) {
	    // file does not exist on target => clear upload case
	    action = "upload";
	  } else {
	    if ( (remotemtimemap[pathit->first].tv_sec < localmtimemap[pathit->first].tv_sec ) ) {
	      // the file exists in the remote box but is older
	      action = "upload";
	    } 
	    if ( (remotemtimemap[pathit->first].tv_sec > localmtimemap[pathit->first].tv_sec ) ) {
	      // the file exists in the remote box but is newer
	      action = "download";
	    }
	    if ( (remotemtimemap[pathit->first].tv_sec == localmtimemap[pathit->first].tv_sec ) ) {
	      // this is resynchronization
	      action = "download";
	    }
	  }
	} else {
	  // the file has been placed into the drop box already
	  if ( (remotesizemap.count(pathit->first)) ) {
	    if ((remotemtimemap[pathit->first].tv_sec < localmtimemap[pathit->first].tv_sec )) {
	      // the file exists in the remote box but is older
	      action = "upload";
	    } 
	    if ((remotemtimemap[pathit->first].tv_sec > localmtimemap[pathit->first].tv_sec )) {
	      // the file exists in the remote box but is newer
	      action = "download";
	    }
	  } else {
	    // check if we have to delete the file
	    if (buf.st_mtime == filebuf.st_mtime) {
	      // the file has not been touched since the download from the drop box
	      action = "delete";
	    } else {
	      // the file should be uploaded
	      action = "upload";
	    }
	  }
	}
	
	if (action == "upload") {
	  XrdOucString uploadline= "eos cp -n -s "; uploadline += localpath; uploadline += " ";
	  uploadline += remotepath;
	  eos_static_info("[upload] %s", uploadline.c_str());
	  int rc = system(uploadline.c_str());
	  if (WEXITSTATUS(rc)) {
	    eos_static_err("[upload] upload %s=>%s failed!", localpath.c_str(), remotepath.c_str());
	  }

	  // set the tag file to the mtime of the remote dropboxfile
	  XrdOucString getmtime = "eos -b file info "; getmtime += remotepath; getmtime += " -m";
	  FILE* mfile = popen(getmtime.c_str(),"r");
	  if (!mfile) {
	    eos_static_err("unable to execute %s", getmtime.c_str());
	    continue;
	  }
	  unsigned long long newmtime, newmtime_ms;

	  int item = fscanf(mfile, "file=%*s size=%*s mtime=%llu.%llu",&newmtime, &newmtime_ms);
	  fprintf(stderr,"item=%d\n",item);
	  if (item != 2) {
	    eos_static_err("unable to read modification time of %s", remotepath.c_str());
	    continue;
	  }

	  if (touch(dropboxtagfile.c_str(),newmtime)) {
	    eos_static_err("[touch] failed to update tag file %s", dropboxtagfile.c_str());
	  }

	  if (touch(localpath.c_str(),newmtime)) {
	    eos_static_err("[touch] failed to update local file %s", localpath.c_str());
	  }
	  pclose(mfile);
	}
	
	if (action == "download") {
	  XrdOucString downloadline= "eos cp -n -s "; downloadline += remotepath; downloadline += " ";
	  downloadline += localpath;
	  eos_static_info("[download] %s", downloadline.c_str());
	  int rc = system(downloadline.c_str());
	  if (WEXITSTATUS(rc)) {
	    eos_static_err("[download] download %s=>%s failed!", remotepath.c_str(),localpath.c_str());
	  }
	  // set the tag file to the mtime of the remote dropboxfile
	  if (touch(dropboxtagfile.c_str(),remotemtimemap[pathit->first].tv_sec)) {
	    eos_static_err("[touch] failed to update tag file %s", dropboxtagfile.c_str());
	  }
	  if (touch(localpath.c_str(), remotemtimemap[pathit->first].tv_sec)) {
	    eos_static_err("[touch] failed to update mtime of %s",localpath.c_str());
	  }
	}
	
	if (action == "delete") {
	  if (unlink(localpath.c_str())) {
	    eos_static_err("[unlink] cannot unlink %s!", localpath.c_str());
	  }
	  if (touch(dropboxtagfile.c_str(),filebuf.st_mtime)) {
	    eos_static_err("[touch] failed to update tag file %s", dropboxtagfile.c_str());
	  }
	  eos_static_info("[delete] removed file %s", localpath.c_str());
	}

	if (action == "noaction") {
	  eos_static_debug("[noaction] file=%s", localpath.c_str());
	}
      }

      // -----------------------------------------------------------------------------------
      // upload,download,delete remote files
      // -----------------------------------------------------------------------------------

      eos_static_info("[remote] %d files", remotesizemap.size());
      
      for (pathit = remotesizemap.begin(); pathit != remotesizemap.end(); pathit++) {
	XrdOucString localpath=it->second.c_str(); localpath += pathit->first.c_str(); 
	XrdOucString remotepath=it->first.c_str(); remotepath += pathit->first.c_str();

	eos_static_debug("[remote] checking %s\n", pathit->first.c_str());
	struct stat buf;
	struct stat filebuf;

	XrdOucString dropboxtagfile="";
	dropboxtagfile += "."; 
	dropboxtagfile += pathit->first.c_str(); 
	while (dropboxtagfile.replace("/","::")) {}
	
	dropboxtagfile.insert("/.dropbox/",0);
	dropboxtagfile.insert(it->second.c_str(),0);
	
	eos::common::Path cPath(dropboxtagfile.c_str());
	if (!cPath.MakeParentPath(S_IRWXU)) {
	  eos_static_err("unable to make parent path of %s", dropboxtagfile.c_str());
	  continue;
	}

	eos_static_debug("[local] stat %s", dropboxtagfile.c_str());
	// check if the file is not coming via drop box synchronisation
	
	XrdOucString action = "noaction";


	if ( (stat(dropboxtagfile.c_str(), &buf) ) ) {
	  // no tag file - new file
	  if (!localsizemap.count(pathit->first)) {
	    // file does not exist locally => clear download case
	    action = "download";
	  } else {
	    // this case is handled already in the loop before, we can skip that
	  }
	} else {
	  // most cases are handled already in the loop before, we can skip them, just care if a file disappears remotely
	  if (!localsizemap.count(pathit->first)) {
	    
	    if (stat(localpath.c_str(), &filebuf) ) {
	      // file disappeared in the meanwhile
	      eos_static_err("[delete] file %s cannot be stat'ed errno=%d", localpath.c_str(), errno);
	      continue;
	    }
	    // check if we have to delete the file
	    if (buf.st_mtime == filebuf.st_mtime) {
	      // the file has not been touched since the download from the drop box and is now gone remotely
	      action = "delete";
	    } 
	  }
	}
	
	if (action == "download") {
	  XrdOucString downloadline= "eos cp -n -s "; downloadline += remotepath; downloadline += " ";
	  downloadline += localpath;
	  eos_static_info("[download] %s", downloadline.c_str());
	  int rc = system(downloadline.c_str());
	  if (WEXITSTATUS(rc)) {
	    eos_static_err("[download] download %s=>%s failed!", remotepath.c_str(),localpath.c_str());
	  }
	  // set the tag file to the mtime of the remote dropboxfile
	  if (touch(dropboxtagfile.c_str(),remotemtimemap[pathit->first].tv_sec)) {
	    eos_static_err("[touch] failed to update tag file %s", dropboxtagfile.c_str());
	  }
	  if (touch(localpath.c_str(), remotemtimemap[pathit->first].tv_sec)) {
	    eos_static_err("[touch] failed to update mtime of %s",localpath.c_str());
	  }
	}
	
	if (action == "delete") {
	  if (unlink(localpath.c_str())) {
	    eos_static_err("[unlink] cannot unlink %s!", localpath.c_str());
	  }
	  if (touch(dropboxtagfile.c_str(),filebuf.st_mtime)) {
	    eos_static_err("[touch] failed to update tag file %s", dropboxtagfile.c_str());
	  }
	  eos_static_info("[delete] removed file %s", localpath.c_str());
	}

	if (action == "noaction") {
	  eos_static_debug("[noaction] file=%s", localpath.c_str());
	}
      }
    }
    counter++;
    sleep(10);

    resync = false;
  } while (1);

}

