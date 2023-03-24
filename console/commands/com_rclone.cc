// ----------------------------------------------------------------------
// File: com_rclone.cc
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

extern XrdOucString serveruri;

struct fs_entry {
  struct timespec mtime;
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

fs_result fs_find(const char* path)
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
      std::string t = p;
      if (!::lstat(p.c_str(), &buf)) {
	p.erase(0,cPath.GetFullPath().length());
	switch ( (buf.st_mode & S_IFMT) ) {
	case S_IFDIR :
	  p+= "/";
	  result.directories[p].mtime = buf.st_mtim;
	  result.directories[p].size  = buf.st_size;
	  //	s << "path=\"" << p << "/\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
	  break;
	case S_IFREG :
	  result.files[p].mtime = buf.st_mtim;
	  result.files[p].size  = buf.st_size;
	  //s << "path=\"" << p << "\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
	  break;
	case S_IFLNK :
	  result.links[p].size = 0;
	  result.links[p].mtime = buf.st_mtim;
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

fs_result eos_find(const char* path) {
  fs_result result;
  eos::common::Path cPath(path);
  
  NewfindHelper find(gGlobalOpts);
  std::string args = "--format type,mtime,size,link ";
  args += path;
  
  if (!find.ParseCommand(args.c_str())) {
    std::cerr << "error: illegal subcommand '" << args << "'" << std::endl;
  }
  find.Silent();
  int rc = find.Execute();
  if (!rc) {
    std::string findresult = find.GetResult();
    std::vector<std::string> lines;

    eos::common::StringConversion::Tokenize(findresult, lines, "\n");
    for (auto l:lines) {
      std::vector<std::string> kvs;
      eos::common::StringConversion::Tokenize(l, kvs, " ");
      struct timespec ts{0,0};
      size_t size{0};
      string path;
      string type;
      for (auto k:kvs) {
	std::string tag, value;
	eos::common::StringConversion::SplitKeyValue(k, tag, value, "=");
	if (tag == "mtime") {
	  eos::common::Timing::Timespec_from_TimespecStr(value, ts);
	  if (type == "directory") {
	    result.directories[path].mtime = ts;
	  } else if (type == "file") {
	    result.files[path].mtime = ts;
	  } else if (type == "symlink") {
	    result.links[path].mtime = ts;
	  }
	}
	if (tag == "size") {
	  size = std::stoull(value.c_str(),0,10);
	  if (type == "directory") {
	    result.directories[path].size = size;
	  } else if (type == "file") {
	    result.files[path].size = size;
	  } else if (type == "symlink") {
	    result.links[path].size = 0;
	  }
	}	  
	if (tag == "path") {
	  // remove quotes
	  value.erase(0,1);
	  value.erase(value.length()-1);
	  value.erase(0,cPath.GetFullPath().length());
	  path = value;
	}
	if (tag == "type") {
	  type = value;
	}
	if ( tag == "target" && type == "symlink") {
	  value.erase(0,1);
	  value.erase(value.length()-1);
	  result.links[path].target = value;
	}
	//	std::cout << " ... " << tag << " => " << value << std::endl;
      }
    }
  }
  return result;
}

int createDir(const std::string& i, eos::common::Path& prefix) {
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    std::string mkpath = std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
    int rc = ::mkdir(mkpath.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    std::cerr << "[ mkdir                 ] : path:" << "[mkdir] path: " << mkpath.c_str() << " retc: " << rc << std::endl;
    return rc;
  } else {     
    XrdCl::URL url(serveruri.c_str());
    url.SetPath( std::string(prefix.GetFullPath().c_str()) + std::string("/") + i );
    
    if (!url.IsValid()) {
      fprintf(stderr,"invalid url %s\n", i.c_str());
      return 0;
    } else {
      fprintf(stderr,"valid url %s\n", i.c_str());
    }
    XrdCl::FileSystem fs(url);
    mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP;
    XrdCl::Access::Mode mode_xrdcl = eos::common::LayoutId::MapModeSfs2XrdCl(mode);
    XrdCl::XRootDStatus status = fs.MkDir(url.GetPath(),
					  XrdCl::MkDirFlags::MakePath,
					  mode_xrdcl);
    std::cerr << "[ mkdir                 ] : url:" << url.GetURL() << " : " << status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int removeDir(const std::string& i, eos::common::Path& prefix) {
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    std::string rmpath = std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
    int rc = ::rmdir(rmpath.c_str());
    std::cerr << "[ rmdir                 ] : path:" << rmpath.c_str() << " retc: " << rc << std::endl;
    return rc;
  } else { 
    XrdCl::URL url(serveruri.c_str());
    url.SetPath( std::string(prefix.GetFullPath().c_str()) + std::string("/") + i );
    
    if (!url.IsValid()) {
      fprintf(stderr,"invalid url %s\n", i.c_str());
      return 0;
    } else {
      fprintf(stderr,"valid url %s\n", i.c_str());
    }
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.RmDir(url.GetPath());
    std::cerr << "[ rmdir                 ] : url:" << url.GetURL() << " : " << status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int removeFile(const std::string& i, eos::common::Path& prefix) {
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    std::string rmpath = std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
    int rc = ::unlink(rmpath.c_str());
    std::cerr << "[ unlink                ] : path:" << rmpath.c_str() << " retc: " << rc << std::endl;
    return rc;
  } else { 
    XrdCl::URL url(serveruri.c_str());
    url.SetPath( std::string(prefix.GetFullPath().c_str()) + std::string("/") + i );
    
    if (!url.IsValid()) {
      fprintf(stderr,"invalid url %s\n", i.c_str());
      return 0;
    } else {
      fprintf(stderr,"valid url %s\n", i.c_str());
    }
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Rm(url.GetPath());
    std::cerr << "[ unlink                ] : url:" << url.GetURL() << " : " << status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int createLink(const std::string& i, eos::common::Path& prefix, const std::string& target, eos::common::Path& targetprefix, struct timespec& mtime) {
  std::string targetpath = target;
  if (targetpath.find(prefix.GetFullPath().c_str()) == 0) {
    // might need to rewrite the link target with a new prefix!
    targetpath.erase(0, prefix.GetFullPath().length());
    targetpath.insert(0,targetprefix.GetFullPath().c_str());
  }
  
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    std::string linkpath = std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
    fprintf(stderr,"linking %s => %s %lu.%lu\n", linkpath.c_str(), target.c_str(), mtime.tv_sec, mtime.tv_nsec);
    int rc = ::symlink(target.c_str(),linkpath.c_str());
    fprintf(stderr,"rc=%d errno=%d\n", rc, errno);
    struct timespec times[2];
    times[0] = mtime;
    times[1] = mtime;
    rc |= utimensat(0, linkpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    fprintf(stderr,"rc=%d errno=%d\n", rc, errno);
    std::cerr << "[ symlink               ] : path:" << linkpath.c_str() << " retc: " << rc << std::endl;
    return rc; 
  } else { 
    XrdCl::URL url(serveruri.c_str());
    url.SetPath( std::string(prefix.GetFullPath().c_str()) + std::string("/") + i );
    
    if (!url.IsValid()) {
      fprintf(stderr,"invalid url %s\n", i.c_str());
      return 0;
    } else {
      fprintf(stderr,"valid url %s\n", i.c_str());
    }

    int retc=0;
    std::string request;
    {
      // create link
      XrdCl::Buffer arg;
      XrdCl::Buffer* response=nullptr;
      
      request = eos::common::StringConversion::curl_escaped(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=symlink&target=";
      request += eos::common::StringConversion::curl_escaped(targetpath);
      request += "&eos.encodepath=1";
      
      arg.FromString(request);
      
      XrdCl::FileSystem fs(url);
      XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
      
      if (response) {
	delete response;
      }
      retc = !status.IsOK();
    }

    {
      // fix mtime
      XrdCl::Buffer arg;
      XrdCl::Buffer* response=nullptr;
      request = eos::common::StringConversion::curl_escaped(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=utimes";
      request += "&tv1_sec=0";  //ignored
      request += "&tv1_nsec=0"; // ignored
      request += "&tv2_sec=";
      request += std::to_string(mtime.tv_sec);
      request += "&tv2_nsec=";
      std::stringstream oss; 
      oss << std::setfill('0') << std::setw(9) << mtime.tv_nsec;
      request += oss.str();
      request += "&eos.encodepath=1";
      
      arg.FromString(request);
      
      XrdCl::FileSystem fs(url);
      XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
      
      if (response) {
	delete response;
      }
      retc |= !status.IsOK();
    }
    
    std::cerr << "[ symlink               ] : url:" << url.GetURL() << " : " << retc << std::endl;
    return retc;
  }
}


int setDirMtime(const std::string& i, eos::common::Path& prefix, struct timespec mtime) {
  std::string mtpath = std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    // apply local mtime;
    struct timespec times[2];
    times[0] = mtime;
    times[1] = mtime;
    int rc = utimensat(0, mtpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    std::cerr << "[ mtime                 ] : path:" << "[utime] path: " << mtpath.c_str() << " retc: " << rc <<  " " << mtime.tv_sec << ":" << mtime.tv_nsec <<std::endl;
    return rc;
  } else {     
    XrdCl::URL url(serveruri.c_str());
        url.SetPath( std::string(prefix.GetFullPath().c_str()) + std::string("/") + i );
    
    if (!url.IsValid()) {
      fprintf(stderr,"invalid url %s\n", i.c_str());
      return 0;
    } else {
      fprintf(stderr,"valid url %s\n", i.c_str());
    }

    std::string request;
    XrdCl::Buffer arg;
    XrdCl::Buffer* response=nullptr;
    
    request = eos::common::StringConversion::curl_escaped(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
    request += "?";
    request += "mgm.pcmd=utimes";
    request += "&tv1_sec=0";  //ignored
    request += "&tv1_nsec=0"; // ignored
    request += "&tv2_sec=";
    request += std::to_string(mtime.tv_sec);
    request += "&tv2_nsec=";
    std::stringstream oss; 
    oss << std::setfill('0') << std::setw(9) << mtime.tv_nsec;
    request += oss.str();
    request += "&eos.encodepath=1";

    arg.FromString(request);
        
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

    if (response) {
      delete response;
    }
    int rc = !status.IsOK();
    
    std::cerr << "[ mtime                 ] : path:" << "[utime] path: " << mtpath.c_str() << " retc: " << rc << std::endl;
    return rc;
  }
}

XrdCl::CopyProcess copyProcess;
std::vector<XrdCl::PropertyList*> tprops;

XrdCl::PropertyList* copyFile(const std::string& i, eos::common::Path& src, eos::common::Path& dst, struct timespec mtime) {
  XrdCl::PropertyList props;
  XrdCl::PropertyList* result = new XrdCl::PropertyList();

  std::string srcurl = std::string(src.GetFullPath().c_str()) + i;
  std::string dsturl = std::string(dst.GetFullPath().c_str()) + i;

  if (srcurl.substr(0,5) == "/eos/") {
    XrdCl::URL surl(serveruri.c_str());
    surl.SetPath(srcurl);
    srcurl = surl.GetURL();
  }

  if (dsturl.substr(0,5) == "/eos/") {
    XrdCl::URL durl(serveruri.c_str());
    durl.SetPath(dsturl);
    XrdCl::URL::ParamsMap params;
    params["eos.mtime"] = eos::common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  } else {
    XrdCl::URL durl(dsturl);
    XrdCl::URL::ParamsMap params;
    params["local.mtime"] = eos::common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  }
  
  props.Set("source", srcurl);
  props.Set("target", dsturl);
  props.Set("force", true); // allows overwrite
  
  result->Set("source", srcurl);
  result->Set("target", dsturl);

  //  props.Set("parallel", 10);
  std::cerr << "[ copy file             ] : srcurl: " << srcurl << " dsturl: " << dsturl << std::endl;
  copyProcess.AddJob(props,result);
  return result;
}

int
com_rclone(char* arg1)
{
  if (interactive) {
    fprintf(stderr,
            "error: don't call <rclone> from an interactive shell - run 'eos -b rclone ...'!\n");
    global_retc = -1;
    return 0;
  }

// split subcommands
  XrdOucString mountpoint = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();

  std::set<std::string> target_create_dirs;
  std::set<std::string> target_delete_dirs;
  std::set<std::string> target_mtime_dirs;
  std::set<std::string> target_create_files;
  std::set<std::string> target_delete_files;
  std::set<std::string> target_updated_files;
  std::set<std::string> target_mismatch_files;
  std::set<std::string> target_create_links;
  std::set<std::string> target_delete_links;
  std::set<std::string> target_updated_links;
  std::set<std::string> target_mismatch_links;

  std::set<std::string> source_create_dirs;
  std::set<std::string> source_delete_dirs;
  std::set<std::string> source_mtime_dirs;
  std::set<std::string> source_create_files;
  std::set<std::string> source_delete_files;
  std::set<std::string> source_updated_files;
  std::set<std::string> source_mismatch_files;
  std::set<std::string> source_create_links;
  std::set<std::string> source_delete_links;
  std::set<std::string> source_updated_links;
  std::set<std::string> source_mismatch_links;
  
  std::set<std::string> cp_target_files;
  std::set<std::string> cp_source_files;
  
  enum eActions {
    kTargetDirCreate, kSourceDirCreate,
    kTargetDirDelete, kSourceDirDelete,
    kTargetFileCreate, kSourceFileCreate,
    kTargetFileUpdate, kSourceFileUpdate,
    kTargetFileDelete, kSourceFileDelete,
    kTargetFileMismatch, kSourceFileMismatch,
    kTargetLinkCreate, kSourceLinkCreate,
    kTargetLinkUpdate, kSourceLinkUpdate,
    kTargetLinkDelete, kSourceLinkDelete,
    kTargetLinkMismatch, kSourceLinkMismatch,
    kTargetDirMtime, kSourceDirMtime
  };
  
  std::vector<eActions> actions;
  
  
  if (cmd == "copy") {
    actions.push_back(kTargetDirCreate);
    actions.push_back(kTargetFileCreate);
    actions.push_back(kTargetFileUpdate);
    actions.push_back(kTargetFileMismatch);
    actions.push_back(kTargetLinkCreate);
    actions.push_back(kTargetLinkUpdate);
    actions.push_back(kTargetLinkMismatch);
    actions.push_back(kTargetLinkDelete);
    actions.push_back(kTargetFileDelete);
    actions.push_back(kTargetDirDelete);
    actions.push_back(kTargetDirMtime);
  }
  
  if (cmd == "sync") {
    actions.push_back(kTargetDirCreate);
    actions.push_back(kTargetFileCreate);
    actions.push_back(kTargetFileUpdate);
    actions.push_back(kTargetFileMismatch);
    actions.push_back(kTargetLinkCreate);
    actions.push_back(kTargetLinkUpdate);
    actions.push_back(kTargetLinkMismatch);
    actions.push_back(kTargetLinkDelete);
    actions.push_back(kTargetFileDelete);
    actions.push_back(kTargetDirDelete);
    actions.push_back(kTargetDirMtime);
    actions.push_back(kSourceDirCreate);
    actions.push_back(kSourceFileCreate);
    actions.push_back(kSourceFileUpdate);    
    //    actions.push_back(kSourceFileMismatch);
    actions.push_back(kSourceLinkCreate);
    actions.push_back(kSourceLinkUpdate);
    //    actions.push_back(kSourceLinkMismatch);
    actions.push_back(kSourceLinkDelete);   
    actions.push_back(kSourceFileDelete);
    actions.push_back(kSourceDirDelete);
    actions.push_back(kSourceDirMtime);
  }
    
  XrdOucString src = subtokenizer.GetToken();
  XrdOucString dst = subtokenizer.GetToken();
  
  eos::common::Path srcPath(src.c_str());
  eos::common::Path dstPath(dst.c_str());
  
  
  fs_result srcmap;
  fs_result dstmap;

  bool ignore_errors = false;
  
  if (src.beginswith("/eos/")) {
    // get the sync informtion using newfind
    srcmap = eos_find(src.c_str());
  } else {
    // travers using UNIX find
    srcmap = fs_find(src.c_str());
  }
  if (dst.beginswith("/eos/")) {
    // get the sync information using newfind
    dstmap = eos_find(dst.c_str());
  } else {
    // travers using UNIX find
    dstmap = fs_find(dst.c_str());
  }

  srcmap.directories.erase("/");
  dstmap.directories.erase("/");
  
  // forward comparison
  for ( auto d:srcmap.directories ) {
    if (!dstmap.directories.count(d.first)) {
      std::cout << "[ target folder missing ] : " << d.first << std::endl;
      target_create_dirs.insert(d.first);
      target_mtime_dirs.insert(d.first);
    } else {
      if (dstmap.directories[d.first].newer(srcmap.directories[d.first].mtime)) {
	target_mtime_dirs.insert(d.first);
      }
    }
  }
  /// backward
  for ( auto d:dstmap.directories ) {
    if (!srcmap.directories.count(d.first)) {
      std::cout << "[ source folder missing ] : " << d.first << std::endl;
      source_create_dirs.insert(d.first);
      source_mtime_dirs.insert(d.first);
    } else {
      if (dstmap.directories[d.first].newer(dstmap.directories[d.first].mtime)) {
	source_mtime_dirs.insert(d.first);
      }
    }
  }
  
  // forward comparison
  for ( auto d:srcmap.files ) {
    if (!dstmap.files.count(d.first)) {
      std::cout << "[ target file   missing ] : " << d.first << std::endl;
      target_create_files.insert(d.first);
    } else {
      if (dstmap.files[d.first].newer(srcmap.files[d.first].mtime)) {
	std::cout << "[ target file   older   ] : " << d.first << std::endl;
	target_updated_files.insert(d.first);
      } else {
	if (dstmap.files[d.first].size != srcmap.files[d.first].size) {
	  std::cout << "[ target file diff size ] : " << d.first << std::endl;
	  target_mismatch_files.insert(d.first);
	}
      }
    }
  }
  
  // backward comparison
  for ( auto d:dstmap.files ) {
    if (!srcmap.files.count(d.first)) {
      std::cout << "[ source file   missing ] : " << d.first << std::endl;
      source_create_files.insert(d.first);
    } else {
      if (srcmap.files[d.first].newer(dstmap.files[d.first].mtime)) {
	std::cout << "[ source file   older   ] : " << d.first << std::endl;
	source_updated_files.insert(d.first);
      } else {
	if (dstmap.files[d.first].size != srcmap.files[d.first].size) {
	  std::cout << "[ source file diff size ] : " << d.first << std::endl;
	  source_mismatch_files.insert(d.first);
	}
      }
    }
  }

    // forward comparison
  for ( auto d:srcmap.links ) {
    if (!dstmap.links.count(d.first)) {
      std::cout << "[ target link   missing ] : " << d.first << std::endl;
      target_create_links.insert(d.first);
    } else {
      if (dstmap.links[d.first].newer(srcmap.links[d.first].mtime)) {
	std::cout << "[ target link   older   ] : " << d.first << std::endl;
	target_updated_links.insert(d.first);
      } else {
	if (dstmap.links[d.first].target != srcmap.links[d.first].target) {
	  std::cout << "[ target link diff size ] : " << d.first << std::endl;
	  target_mismatch_links.insert(d.first);
	}
      }
    }
  }
  
  // backward comparison
  for ( auto d:dstmap.links ) {
    if (!srcmap.links.count(d.first)) {
      std::cout << "[ source link   missing ] : " << d.first << std::endl;
      source_create_links.insert(d.first);
    } else {
      if (srcmap.links[d.first].newer(dstmap.links[d.first].mtime)) {
	std::cout << "[ source link   older   ] : " << d.first << std::endl;
	source_updated_links.insert(d.first);
      } else {
	if (dstmap.links[d.first].target != srcmap.links[d.first].target) {
	  std::cout << "[ source link diff size ] : " << d.first << std::endl;
	  source_mismatch_links.insert(d.first);
	}
      }
    }
  }

  std::cout << "[ target                ]" << std::endl;
  std::cout << "[ # dir,files,links to create ] : " << target_create_dirs.size() << "," << target_create_files.size() << "," << target_create_links.size() << std::endl;
  std::cout << "[ # dir,files,links to delete ] : " << target_delete_dirs.size() << "," << target_delete_files.size() << "," << target_delete_links.size() << std::endl;
  std::cout << "[ # files,links to update     ] : " << target_updated_files.size() << "," << target_updated_links.size() << std::endl;
  std::cout << "[ # files,links mismatch      ] : " << target_mismatch_files.size() << "," << target_mismatch_links.size() << std::endl;

  std::cout << "[ source                ]" << std::endl;
  std::cout << "[ # dir,files,links to create ] : " << source_create_dirs.size() << "," << source_create_files.size() << "," << source_create_links.size() << std::endl;
  std::cout << "[ # dir,files,links to delete ] : " << source_delete_dirs.size() << "," << source_delete_files.size() << "," << source_delete_links.size() << std::endl;
  std::cout << "[ # files,links to update     ] : " << source_updated_files.size() << "," << source_updated_links.size() << std::endl;
  std::cout << "[ # files,links mismatch      ] : " << source_mismatch_files.size() << "," << source_mismatch_links.size() << std::endl;
  
  for ( auto a:actions ) {
    if (a == kTargetDirCreate) {
      for ( auto i:target_create_dirs ) {
	int rc = createDir(i, dstPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to create directory '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kSourceDirCreate) {
      for ( auto i:source_create_dirs ) {
	int rc = createDir(i, srcPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to create directory '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
    
    if (a == kTargetDirDelete) {
      for ( auto i:target_delete_dirs ) {
	int rc = removeDir(i, dstPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove directory '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kTargetFileDelete) {
      for ( auto i:target_delete_files ) {
	int rc = removeFile(i, dstPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove file '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kTargetLinkDelete) {
      for ( auto i:target_delete_links ) {
	int rc = removeFile(i, dstPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove link '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kSourceDirDelete) {
      for ( auto i:source_delete_dirs ) {
	int rc = removeDir(i, srcPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove directory '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
    
    if (a == kSourceFileDelete) {
      for ( auto i:source_delete_files ) {
	int rc = removeFile(i, srcPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove file '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kSourceLinkDelete) {
      for ( auto i:source_delete_links ) {
	int rc = removeFile(i, srcPath);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to remove link '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kTargetFileCreate) {
      for ( auto i:target_create_files ) {
	cp_target_files.insert(i);
      }
    }
    
    if (a == kTargetFileUpdate) {
      for ( auto i:target_updated_files ) {
	cp_target_files.insert(i);
      }
    }

    if (a == kTargetFileMismatch) {
      for ( auto i:target_mismatch_files ) {
	cp_target_files.insert(i);
      }
    }

    if(a == kTargetLinkCreate) {
      for ( auto i:target_create_links ) {
	fprintf(stderr,"create link %s => %s\n", i.c_str(), srcmap.links[i].target.c_str());
	int rc = createLink(i, dstPath, srcmap.links[i].target, srcPath, srcmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to create link '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if(a == kTargetLinkUpdate) {
      for ( auto i:target_updated_links ) {
	int rc = removeFile(i, dstPath);	
	rc |= createLink(i, dstPath, srcmap.links[i].target, srcPath, srcmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update link '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
    
    if(a == kTargetLinkMismatch) {
      for ( auto i:target_mismatch_links ) {
	int rc = removeFile(i, dstPath);	
	rc |= createLink(i, dstPath, srcmap.links[i].target, srcPath, srcmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update mismatching link '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if (a == kSourceFileCreate) {
      for ( auto i:source_create_files ) {
	cp_source_files.insert(i);
      }
    }
    
    if (a == kSourceFileUpdate) {
      for ( auto i:source_updated_files ) {
	cp_source_files.insert(i);
      }
    }

    if (a == kSourceFileMismatch) {
      for ( auto i:source_mismatch_files ) {
	cp_source_files.insert(i);
      }
    }

    if(a == kSourceLinkCreate) {
      for ( auto i:source_create_links ) {
	int rc = createLink(i, srcPath, dstmap.links[i].target, dstPath, dstmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to create link '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if(a == kSourceLinkUpdate) {
      for ( auto i:source_updated_links ) {
	int rc = removeFile(i, srcPath);	
	rc |= createLink(i, srcPath, dstmap.links[i].target, dstPath, dstmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update link '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }

    if(a == kSourceLinkMismatch) {
      for ( auto i:source_mismatch_links ) {
	int rc = removeFile(i, srcPath);	
	rc |= createLink(i, srcPath, dstmap.links[i].target, dstPath, dstmap.links[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update mismatching link '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
  }
  
  for ( auto i:cp_target_files ) {
    tprops.push_back(copyFile(i, srcPath, dstPath, srcmap.files[i].mtime));
  }
  
  for ( auto i:cp_source_files ) {
    tprops.push_back(copyFile(i, dstPath, srcPath, dstmap.files[i].mtime));
  }

  class RCloneProgressHandler : public XrdCl::CopyProgressHandler {
  public:
    virtual void BeginJob( uint16_t   jobNum,
			   uint16_t   jobTotal,
			   const URL *source,
			   const URL *destination )
    {
      n = jobNum;
      tot = jobTotal;
    }
    
    virtual void EndJob( uint16_t            jobNum,
			 const PropertyList *result )
    {
      
      (void)jobNum; (void)result;
      std::string src;
      std::string dst;
      result->Get("source",src);
      result->Get("target",dst);
      XrdCl::URL durl(dst.c_str());
      auto param = durl.GetParams();
      if (param.count("local.mtime")) {
	// apply mtime changes when done to local files
	struct timespec ts;
	std::string tss = param["local.mtime"];
	if (!eos::common::Timing::Timespec_from_TimespecStr(tss, ts)) {
	  // apply local mtime;
	  struct timespec times[2];
	  times[0] = ts;
	  times[1] = ts;
	  if (utimensat(0, durl.GetPath().c_str(), times, AT_SYMLINK_NOFOLLOW)) {
	    std::cerr << "error: failed to update modification time of '" << durl.GetPath() << "'" << std::endl;
	  }
	}
      }
      //      fprintf(stderr,"info: finished src:%s=>dst:%s\n", src.c_str(), dst.c_str());
    };
    
    virtual void JobProgress( uint16_t jobNum,
			      uint64_t bytesProcessed,
			      uint64_t bytesTotal )
    {
      bp = bytesProcessed;
      bt = bytesTotal;
      n  = jobNum;
      std::cerr << "[ " << jobNum << "/" << tot << " ]" << std::endl;
    }
    
    virtual bool ShouldCancel( uint16_t jobNum )
    {
      (void)jobNum;
      return false;
    }
    
    std::atomic<uint64_t> bp;
    std::atomic<uint64_t> bt;
    std::atomic<uint16_t> n;
    std::atomic<uint16_t> tot;
  };

  RCloneProgressHandler copyProgress;
  std::cerr << "... preparing" << std::endl;
  copyProcess.Prepare();
  std::cerr << "... running" << std::endl;
  copyProcess.Run(&copyProgress);


  // last step is to adjust directory mtimes
  for ( auto a:actions ) {
    if ( a == kTargetDirMtime ) {
      for ( auto i:target_mtime_dirs ) {
	int rc = setDirMtime(i, dstPath, srcmap.directories[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update directory mtime  '" << dstPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
    
    if ( a == kSourceDirMtime ) {
      for ( auto i:source_mtime_dirs ) {
	int rc = setDirMtime(i, srcPath, dstmap.directories[i].mtime);
	if (rc && !ignore_errors) {
	  std::cerr << "error: failed to update directory mtime  '" << srcPath.GetFullPath() << i << "'" << std::endl;
	  exit(-1);
	}
      }
    }
  }
  
  exit(0);
  
 com_rclone_usage:
  fprintf(stdout,
          "usage: rclone ... \n");
  fprintf(stdout,
          "       rclone ... \n");
  exit(-1);
}
