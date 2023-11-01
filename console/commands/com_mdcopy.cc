// ----------------------------------------------------------------------
// File: com_mdcopy.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright(C) 2023 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 *(at your option) any later version.                                  *
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
#include "console/commands/helpers/NewfindHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/

#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClPropertyList.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <optional>

extern XrdOucString serveruri;

struct fs_entry {
  struct timespec mtime;
  struct stat mstat;
  size_t size;
  std::string type;
  std::string target;
  
  bool newer(struct timespec& cmptime) {
    if (mtime.tv_sec < cmptime.tv_sec) {
      return true;
    } else if (mtime.tv_sec > cmptime.tv_sec) {
      return false;
    } else if (mtime.tv_nsec < cmptime.tv_nsec) {
      return true;
    } else {
      return false;
    }
  }
}; 
  
struct fs_result {
  std::map<std::string, fs_entry> directories;
  std::map<std::string, fs_entry> files;
  std::map<std::string, fs_entry> links;
}; 

bool md_dryrun = false;
bool md_noreplace = false;
bool md_nodelete = false;
bool md_verbose = false;
bool md_is_silent = false;
bool md_filter_versions = false;
bool md_filter_atomic = false;
bool md_filter_hidden = false;

fs_result local_find(const char* path)
{
  fs_result result;
  std::stringstream s;
  eos::common::Path cPath(path);
  
  namespace fs = std::filesystem;
  fs::path path_to_traverse = path;
  struct stat buf;
  try {
    for (const auto& entry : fs::recursive_directory_iterator(path_to_traverse, std::filesystem::directory_options::skip_permission_denied)) {
      std::string p = entry.path().string();
      
      // filter functions
      eos::common::Path iPath(p.c_str());
      if (md_filter_versions) {
	if (iPath.isVersionPath()) {
	  continue;
	}
      }
      if (md_filter_atomic) {
	if (iPath.isAtomicFile()) {
	  continue;
	}
      }
      if (md_filter_hidden && iPath.GetFullPath().find("/.")!=STR_NPOS) {
	if (!iPath.isVersionPath() && !iPath.isAtomicFile()) {
	  continue;
	}
      }

      std::string t = p;
      if (!::lstat(p.c_str(), &buf)) {

	p.erase(0,cPath.GetFullPath().length());
	switch ( (buf.st_mode & S_IFMT) ) {
	case S_IFDIR :
	  p+= "/";
	  result.directories[p].mtime = buf.st_mtim;
	  result.directories[p].size  = buf.st_size;
	  memcpy(&result.directories[p].mstat, &buf, sizeof(struct stat));
	  //	s << "path=\"" << p << "/\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
	  break;
	case S_IFREG :
	  result.files[p].mtime = buf.st_mtim;
	  result.files[p].size  = buf.st_size;
	  memcpy(&result.files[p].mstat, &buf, sizeof(struct stat));
	  //s << "path=\"" << p << "\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
	  break;
	case S_IFLNK :
	  result.links[p].size = 0;
	  result.links[p].mtime = buf.st_mtim;
	  memcpy(&result.links[p].mstat, &buf, sizeof(struct stat));
	char link[4096];
	ssize_t target = readlink(t.c_str(), link, sizeof(link));
	if (target>=0) {
	  result.links[p].target = std::string(link,target);
	}
	break;
	}
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    std::cerr
      << "error:  " << ex.what() << '\n'
      << "#      path  : " << ex.path1() << '\n'
      << "#      errc  :    " << ex.code().value() << '\n'
      << "#      msg   :  " << ex.code().message() << '\n'
      << "#      class : " << ex.code().category().name() << '\n';
    exit(-1);
  }
  //  std::cout << s.str();
  return result;
}

void mdcopy_usage() {
  fprintf(stderr,"usage: mdcopy <local-src> <local-dst>\n");
  fprintf(stderr,"                         : copies files sparse from source to destionation - no data is copied!\n");
  exit(-1);
}  


std::string local_parent(const std::string& path) {
  std::filesystem::path p(path);
  return p.parent_path();
}
  
int
com_mdcopy(char* arg1)
{
  XrdOucString mountpoint = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();

  XrdOucString src = subtokenizer.GetToken();
  XrdOucString dst = subtokenizer.GetToken();

  XrdOucString ssize = subtokenizer.GetToken();

  size_t min_size = 0;
  if (ssize.length()) {
    min_size = strtoul(ssize.c_str(),0,10);
  }

	      
  eos::common::Path srcPath(src.c_str());
  eos::common::Path dstPath(dst.c_str());

  src = srcPath.GetFullPath();
  dst = dstPath.GetFullPath();
  
  if (!src.length() || !dst.length()) {
    fprintf(stderr,"error 1\n");
    mdcopy_usage();
  }
  
  md_nodelete = true;
  md_noreplace = false;
  md_dryrun = false;
  
  XrdOucString option;
  do {
    option = subtokenizer.GetToken();
    if (!option.length()) {
      break;
    }
    if (option == "--delete") {
      md_nodelete = false;
    } else if (option == "--noreplace") {
      md_noreplace = true;
    } else if (option == "--dryrun") {
      md_dryrun = true;
    } else if (option == "-v" || option == "--verbose") {
      md_verbose = true;
    } else if (option == "-s" || option == "--silent") {
      md_is_silent = true;
    } else {
      mdcopy_usage();
    }
  } while (1);
  
  fs_result srcmap;
  fs_result dstmap;

  srcmap = local_find(src.c_str());

  for ( auto afile = srcmap.files.begin(); afile != srcmap.files.end(); ++afile ) {
    if (afile->second.size <= min_size) {
      continue;
    }
    std::string target = dst.c_str();
    target += afile->first;
    int fd = ::open (target.c_str(), O_RDWR|O_CREAT);
    if (fd>=0) {
      ::fchown(fd, afile->second.mstat.st_uid, afile->second.mstat.st_gid);
      ::ftruncate(fd, afile->second.size);
      ::fchmod(fd, afile->second.mstat.st_mode);
      struct timespec tv[2];
      tv[0].tv_sec = afile->second.mstat.st_atim.tv_sec;
      tv[0].tv_nsec = afile->second.mstat.st_atim.tv_nsec;
      tv[1].tv_sec = afile->second.mstat.st_mtim.tv_sec;
      tv[1].tv_nsec = afile->second.mstat.st_mtim.tv_nsec;
      ::futimens(fd, tv);
      close(fd);
    }
    fprintf(stderr, "name='%s' target='%s' uid=%u\n", afile->first.c_str(), target.c_str(), afile->second.mstat.st_uid);
  }
  exit(0);
}
