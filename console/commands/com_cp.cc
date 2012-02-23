// ----------------------------------------------------------------------
// File: com_cp.cc
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
#include <vector>
/*----------------------------------------------------------------------------*/

extern XrdOucString serveruri;
extern char* com_fileinfo (char* arg1);

int com_cp_usage() {
  fprintf(stdout,"Usage: cp [--rate=<rate>] [--streams=<n>] [--recursive|-R|-r] [-a] [-n] [-S] [-s|--silent] [-d] [--checksum] <src> <dst>");
  fprintf(stdout,"'[eos] cp ..' provides copy functionality to EOS.\n");
  fprintf(stdout,"Options:\n");
  fprintf(stdout,"                                                             <src>|<dst> can be root://<host>/<path>, a local path /tmp/../ or an eos path /eos/ in the connected instanace...\n");
  fprintf(stdout,"       --rate          : limit the cp rate to <rate>\n");
  fprintf(stdout,"       --streams       : use <#> parallel streams\n");
  fprintf(stdout,"       --checksum      : output the checksums\n");
  fprintf(stdout,"       -a              : append to the target, don't truncate\n");
  fprintf(stdout,"       -n              : hide progress bar\n");
  fprintf(stdout,"       -S              : print summary\n");
  fprintf(stdout,"       -s --silent     : no output just return code\n");
  fprintf(stdout,"       -d              : enable debug information\n");
  fprintf(stdout,"   -k | --no-overwrite : disable overwriting of files\n");
  fprintf(stdout,"\n");
  fprintf(stdout,"Remark: \n");
  fprintf(stdout,"       If you deal with directories always add a '/' in the end of source or target paths e.g. if the target should be a directory and not a file put a '/' in the end. To copy a directory hierarchy use '-r' and source and target directories terminated with '/' !\n");
  fprintf(stdout,"\n");
  fprintf(stdout,"Examples: \n");
  fprintf(stdout,"       eos cp /var/data/myfile /eos/foo/user/data/                   : copy 'myfile' to /eos/foo/user/data/myfile\n");
  fprintf(stdout,"       eos cp /var/data/ /eos/foo/user/data/                         : copy all plain files in /var/data to /eos/foo/user/data/\n");
  fprintf(stdout,"       eos cp -r /var/data/ /eos/foo/user/data/                      : copy the full hierarchy from /var/data/ to /var/data to /eos/foo/user/data/ => empty directories won't show up on the target!\n");
  fprintf(stdout,"       eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  : copy the full hierarchy and just printout the checksum information for each file copied!\n");
  return (0);
}


/* Cp Interface */
int
com_cp (char* argin) {
  // split subcommands
  XrdOucTokenizer subtokenizer(argin);
  subtokenizer.GetLine();
  XrdOucString rate ="0";
  XrdOucString streams="0";
  XrdOucString option="";
  XrdOucString arg1="";
  XrdOucString arg2="";
  XrdOucString cmdline;
  std::vector<XrdOucString> source_list;
  std::vector<unsigned long long> source_size;
  std::vector<XrdOucString> source_base_list;
  std::vector<XrdOucString> source_find_list;

  XrdOucString target;
  XrdOucString nextarg="";
  XrdOucString lastarg="";

  bool recursive = false;
  bool summary = false;
  bool noprogress = false;
  bool append = false;
  bool debug = false;
  bool checksums = false;
  bool silent = false;
  bool nooverwrite = false;

  unsigned long long copysize=0;
  int retc=0;
  int copiedok=0;
  unsigned long long copiedsize=0;
  struct timeval tv1,tv2;
  struct timezone tz;

  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option.beginswith("--rate=")) {
      rate = option;
      rate.replace("--rate=","");
    } else {
      if (option.beginswith("--streams=")) {
	streams = option;
	streams.replace("--streams=","");
      } else {
	if ( (option== "--recursive") ||
	     (option =="-R") ||
	     (option =="-r")) {
	  recursive = true;
	} else {
	  if (option == "-n") {
	    noprogress = true;
	  } else {
	    if (option == "-a") {
	      append = true;
	    } else {
	      if ( option == "-S") {
		summary = true;
	      } else {
		if ( (option == "-s") || (option == "--silent")) {
		  silent = true;
		} else {
		  if ( (option == "-k") || (option == "--no-overwrite")) {
		    nooverwrite = true;
		  } else {
		    if ( option == "--checksum" ) {
		      checksums=true;
		    } else {
		      if ( option == "-d") {
			debug =true; 
		      } else {
			if (option.beginswith("-")) {
			  return com_cp_usage();
			} else {
			  source_list.push_back(option.c_str());
			  break;
			}
		      }
		    }
		  }
		}
	      }
	    }
	  }
	}
      }
    }
  } while(1);

  if (silent) 
    noprogress = true;

  nextarg=subtokenizer.GetToken();
  lastarg=subtokenizer.GetToken();
  do {
    if (lastarg.length()) {
      source_list.push_back(nextarg.c_str());
      nextarg = lastarg;
      lastarg = subtokenizer.GetToken();
    } else {
      target = nextarg;
      if (debug) 
	fprintf(stderr,"[eos-cp] Setting target %s\n", target.c_str());
      break;
    }
  } while(1);

  if (debug) {
    for (size_t l=0; l< source_list.size(); l++) {
      fprintf(stderr,"[eos-cp] Copylist: %s\n",source_list[l].c_str());
    }
  }

  if ( (!target.length() )) 
    return com_cp_usage();

  if ((source_list.size()>1) && (!target.endswith("/")))
    return com_cp_usage();


  if (!recursive) {
    source_find_list = source_list;
    source_list.clear();
    for (size_t l =0; l< source_find_list.size(); l++) {
      // one wildcard file or directory
      if ( (source_find_list[l].endswith("*") || (source_find_list[0].endswith("/")))) {
	arg1 = source_find_list[l];
	// translate the wildcard
	if ( (arg1.endswith("*") != STR_NPOS ) || (arg1.endswith("/")) ) {
	  if ( arg1.beginswith("/eos/")) {
	    if (arg1.endswith("*")) {
	      arg1.erase(arg1.length()-1);
	    }
	    eos::common::Path cPath(arg1.c_str());
	    std::string dname = "";
	    XrdOucString l = "eos -b ls -l "; 
	    if (!arg1.endswith("/")) {
	      dname = cPath.GetParentPath() ;
	    } else {
	      dname = arg1.c_str();
	    }
	    l += dname.c_str();
	    
	    l += " | grep -v ^d | awk '{print $9}'" ; 
	    if (!arg1.endswith("/")) {
	      std::string match=cPath.GetName();
	      if (match.length()) {
		l += " | grep -w "; l+= match.c_str();
	      }
	    }
	    if (debug) fprintf(stderr,"[eos-cp] running %s\n", l.c_str());
	    FILE* fp = popen(l.c_str(),"r");
	    if (!fp) {
	      fprintf(stderr, "error: unable to run 'eos' - I need it in the path");
	      exit(-1);
	    }
	    int item;
	    char f2c[4096];
	    while ( (item = fscanf(fp,"%s", f2c) == 1)) {
	      std::string fullpath = dname;
	      fullpath += f2c;
	      if (debug) fprintf(stdout,"[eos-cp] add file %s\n",fullpath.c_str());
	      source_list.push_back(fullpath.c_str());
	    }
	    pclose(fp);
	  } else {
	    if (debug) fprintf(stderr,"[eos-cp] %s\n", arg1.c_str());
	    if (arg1.endswith("*")) {
	      arg1.erase(arg1.length()-1);
	    }
	    eos::common::Path cPath(arg1.c_str());
	    std::string dname = "";
	    XrdOucString l = "ls -l "; 
	    if (!arg1.endswith("/")) {
	      dname = cPath.GetParentPath() ;
	    } else {
	      dname = arg1.c_str();
	    }
	    l += dname.c_str();
	    
	    l += " | grep -v ^d | awk '{print $9}'" ; 
	    if (!arg1.endswith("/")) {
	      std::string match=cPath.GetName();
	      if (match.length()) {
		l += " | grep -w "; l+= match.c_str();
	      }
	    }
	    if (debug) fprintf(stderr,"[eos-cp] running %s\n", l.c_str());
	    FILE* fp = popen(l.c_str(),"r");
	    if (!fp) {
	      fprintf(stderr, "error: unable to run 'ls' - I need it in the path");
	      exit(-1);
	    }
	    int item;
	    char f2c[4096];
	    while ( (item = fscanf(fp,"%s", f2c) == 1)) {
	      std::string fullpath = dname;
	      fullpath += f2c;
	      if (debug) fprintf(stdout,"[eos-cp] add file %s\n",fullpath.c_str());
	      source_list.push_back(fullpath.c_str());
	    }
	    pclose(fp);
	  }
	} else {
	  source_list.push_back(arg1.c_str());
	}
      } else {
	source_list.push_back(source_find_list[l].c_str());
      }
    }
  } else {
    // use find to get a file list
    source_find_list = source_list;
    source_list.clear();
    for (size_t nfile = 0 ; nfile < source_find_list.size(); nfile++) {
      if (!source_find_list[nfile].endswith("/")) {
	fprintf(stderr,"error: for recursive copy you have to give a directory name ending with '/'\n");
	return com_cp_usage();
      }

      if (source_find_list[nfile].beginswith("/eos")) {

      } else {
	XrdOucString l="";
	l+= "find "; l += source_find_list[nfile]; l+= " -type f";
	if (debug) fprintf(stderr,"[eoscp] running %s\n", l.c_str());
	FILE* fp = popen(l.c_str(),"r");
	if (!fp) {
	  fprintf(stderr, "error: unable to run 'eos' - I need it in the path");
	  exit(-1);
	}
	int item;
	char f2c[4096];
	while ( (item = fscanf(fp,"%s", f2c) == 1)) {
	  if (debug) fprintf(stdout,"[eos-cp] add file %s\n",f2c);
	  source_list.push_back(f2c);
	  source_base_list.push_back(source_find_list[nfile]);
	}
	fclose(fp);
      }

    }
  }

  // check if there is any file in the list
  if (!source_list.size()) {
    fprintf(stderr,"error: your source seems not to exist or match any file!\n");
    return com_cp_usage();
  }

  // create the target directory if it is a local one
  if ((!target.beginswith("/eos"))) {
    if (target.endswith("/")) {
      XrdOucString mktarget = "mkdir --mode 755 -p "; mktarget += target.c_str();
      system(mktarget.c_str());
      if (access(target.c_str(), R_OK|W_OK)) {
	fprintf(stderr,"error: cannot create/access your target directory!\n");
	exit(0);
      }
    } else {
      eos::common::Path cTarget(target.c_str());
      XrdOucString mktarget = "mkdir --mode 755 -p "; mktarget += cTarget.GetParentPath();
      system(mktarget.c_str());
      if (access(cTarget.GetParentPath(), R_OK|W_OK)) {
	fprintf(stderr,"error: cannot create/access your target directory!\n");
	exit(0);
      }
    }
  }
  

  // compute the size to copy
  std::vector<std::string> file_info;
  for (size_t nfile = 0 ; nfile < source_list.size(); nfile++) {
    if (source_list[nfile].beginswith("/eos/")) {
      struct stat buf;
      XrdOucString url=serveruri.c_str();
      url+="/";
      url+= source_list[nfile];
      if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
	if (S_ISDIR(buf.st_mode)) {
	  fprintf(stderr,"error: %s is a directory - use '-r' to copy directories!\n", source_list[nfile].c_str());
	  return com_cp_usage();
	}
	if (debug)fprintf(stderr,"[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(), (unsigned long long)buf.st_size);
	copysize += buf.st_size;
	source_size.push_back((unsigned long long)buf.st_size);
      }
    } else {
      struct stat buf;
      if (!stat(source_list[nfile].c_str(), &buf)) {
	if (S_ISDIR(buf.st_mode)) {
	  fprintf(stderr,"error: %s is a directory - use '-r' to copy directories\n", source_list[nfile].c_str());
	  return com_cp_usage();
	}
	if (debug)fprintf(stderr,"[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(),(unsigned long long)buf.st_size);
	copysize += buf.st_size;
	source_size.push_back((unsigned long long)buf.st_size);
      }
    }
  }

  XrdOucString sizestring1;
  if (!silent) fprintf(stderr,"[eos-cp] going to copy %d files and %s\n", (int)source_list.size(), eos::common::StringConversion::GetReadableSizeString(sizestring1, copysize, "B"));
      
  gettimeofday(&tv1, &tz);
  // process the file list for wildcards
  for (size_t nfile = 0 ; nfile < source_list.size(); nfile++) {
    XrdOucString targetfile="";
    cmdline="";
    eos::common::Path cPath(source_list[nfile].c_str());
    arg1 = source_list[nfile];
    
    if (arg1.beginswith("./")) {
      arg1.erase(0,2);
    }
    
    arg2 = target;
    
    if (arg2.beginswith(".")) {
      arg2.erase(0,2);
    }
    
    if (arg1.beginswith("/eos")) {
      arg1.insert("/",0);
      arg1.insert(serveruri.c_str(),0);
    }

    if (arg2.endswith("/")) {
      if (recursive) {
	// append the source directory structure
	XrdOucString targetname = source_list[nfile];
	targetname.replace(source_base_list[nfile],"");
	arg2.append(targetname.c_str());
      } else {
	arg2.append(cPath.GetName());
      }
    }

    targetfile = arg2;

    if (arg2.beginswith("/eos")) {
      arg2.insert("/",0);
      arg2.insert(serveruri.c_str(),0);
      char targetadd[1024];
      snprintf(targetadd,sizeof(targetadd)-1,"\\?eos.targetsize=%llu\\&eos.bookingsize=%llu", source_size[nfile], source_size[nfile]);
      arg2.append(targetadd);
    }

    if (nooverwrite) {
      struct stat buf;
      // check if target exists
      if (targetfile.beginswith("/eos/")) {
	XrdOucString url=serveruri.c_str();
	url+="/";
	url+= targetfile;
	if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
	  fprintf(stderr,"error: target file %s exists and you specified no overwrite!\n", targetfile.c_str());
	  continue;
	}
      } else {
	if (!stat(targetfile.c_str(), &buf)) {
	  fprintf(stderr,"error: target file %s exists and you specified no overwrite!\n", targetfile.c_str());
	  continue;
	}
      }
    }
    
    if (interactive) {
      if (!arg1.beginswith("/")) {
	arg1.insert(pwd.c_str(),0);
      }
      if (!arg2.beginswith("/")) {
	arg2.insert(pwd.c_str(),0);
      }
    }
    
    cmdline += "eosfstcp ";
    if (append) cmdline += "-a ";
    if (!summary) cmdline += "-s ";
    if (noprogress) cmdline += "-n ";
    cmdline += "-N "; cmdline += cPath.GetName();cmdline += " ";

    cmdline += arg1; cmdline += " ";
    cmdline += arg2;
    
    if(debug)fprintf(stderr,"[eos-cp] running: %s\n", cmdline.c_str());
    int lrc=system(cmdline.c_str());
    // check the target size
    if (targetfile.beginswith("/eos/")) {
      struct stat buf;
      buf.st_size=0;
      XrdOucString url=serveruri.c_str();
      url+="/";
      url+= targetfile;
      if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
	if (buf.st_size != (int)source_size[nfile]) {
	  fprintf(stderr,"error: filesize differ between source and target file!\n");
	  lrc = 0xffff00;
	} 
      } else {
	fprintf(stderr,"error: target file was not created!\n");
	lrc = 0xffff00;
      }
    } else {
      struct stat buf;
      buf.st_size=0;
      if (!stat(targetfile.c_str(), &buf)) {
	if (buf.st_size != (int)source_size[nfile]) {
	  fprintf(stderr,"error: filesize differ between source and target file!\n");
	  lrc = 0xffff00;
	} 
      } else {
	fprintf(stderr,"error: target file was not created!\n");
	lrc = 0xffff00;
      }
    }      
    if (!WEXITSTATUS(lrc)) {
      if (target.beginswith("/eos")) {
	if (checksums) {
	  XrdOucString adminurl=serveruri.c_str(); adminurl += "//dummy";
	  XrdClientAdmin admin(adminurl.c_str());
	  admin.Connect();
	  kXR_char* answer=0;
	  admin.GetChecksum((kXR_char*)targetfile.c_str(), &answer);
	  if (answer) {
	    XrdOucString sanswer=(char*)answer;
	    sanswer.replace("eos ","");
	    fprintf(stdout,"path=%s size=%llu checksum=%s\n", source_list[nfile].c_str(), source_size[nfile], sanswer.c_str());
	    free(answer);
	  }
	}
      }
      copiedok++;
      copiedsize += source_size[nfile];
    }
    retc |= lrc;

  }    

  gettimeofday(&tv2, &tz);
  
  float passed = (float)(((tv2.tv_sec - tv1.tv_sec) *1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0);
  float crate   = (copiedsize *1.0 / passed);
  XrdOucString sizestring="";
  XrdOucString sizestring2="";
  XrdOucString warningtag="";
  if (retc) warningtag="#WARNING ";
  if (!silent) fprintf(stderr,"%s[eos-cp] copied %d/%d files and %s in %.02f seconds with %s\n", warningtag.c_str(),copiedok, (int)source_list.size(), eos::common::StringConversion::GetReadableSizeString(sizestring, copiedsize, "B"), passed, eos::common::StringConversion::GetReadableSizeString(sizestring2, (unsigned long long)crate,"B/s"));
  exit(WEXITSTATUS(retc));
}
