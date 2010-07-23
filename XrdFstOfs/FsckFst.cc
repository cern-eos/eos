#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonPath.hh"
#include "XrdCommon/XrdCommonFileId.hh"

void usage(const char* name) {
  fprintf(stderr,"usage: %s <changelogfile> [-f] [-dump] [-trim] [-inplace]\n", name);
  fprintf(stderr,"       -f         : force the reading even if the version does not match\n");
  fprintf(stderr,"    -dump         : dump out the meta data blocks\n");
  fprintf(stderr,"    -trim         : trim this file (erases faulty records)\n");
  fprintf(stderr,"    -inplace      : replace the original file with the trimmed copy\n");
  fprintf(stderr,"    --data=<path> : compare with files in path\n");


  exit(-1);
}

int main(int argc, char* argv[] ) {
  XrdCommonLogging::Init();
  XrdCommonLogging::SetUnit("eosfstfsck");
  XrdCommonLogging::SetLogPriority(LOG_NOTICE);
  XrdOucString passoption="c";
  XrdOucString changelogdir="";
  XrdOucString searchpath="";

  int rc = 0;

  bool trim = false;
  bool inplace = false;

  if (argc<=1) {
    usage(argv[0]);
  }

  for (int i=1; i< argc; i++) {
    XrdOucString option = argv[i];
    if (option == "-f") {
      fprintf(stdout,"=> setting force option ... \n");
      passoption+= "f";
    }
    if (option == "-dump") {
      fprintf(stdout,"=> setting dump option ...\n");
      passoption+= "d";
    }
    if (option == "-trim") {
      fprintf(stdout,"=> setting trim option ...\n");
      passoption+= "t";
      trim = true;
    }
    if (option == "-inplace") {
      fprintf(stdout,"=> activated in-place for trim option ...\n");
      inplace = true;
    }
    if (option.beginswith("--data=")) {
      searchpath = option;
      searchpath.erase(0,7);
      fprintf(stdout,"=> searching for files under path %s ...\n", searchpath.c_str());
    }
  }


  XrdOucString changelogfile = argv[1];
  XrdOucString sfsid = changelogfile;

  int fsid=0;
  
  XrdCommonFmdHandler gFmd;

  int idpos = changelogfile.rfind(".mdlog");
  changelogdir = changelogfile;
  int bpos = changelogdir.rfind("/");
  if (bpos == STR_NPOS) {
    changelogdir = ".";
  } else {
    changelogdir.erase(bpos);
  }

  gFmd.ChangeLogDir=changelogdir;

  if ((idpos == STR_NPOS) || (changelogfile.find("/fmd.")==STR_NPOS)) {
    fprintf(stderr,"error: this is not a valid changelog filename!\n");
    exit(-1);
  }

  int fsidpos = changelogfile.rfind(".",idpos-1);

  sfsid.erase(idpos);
  sfsid.erase(0,fsidpos+1);

  changelogfile.erase(fsidpos);
  //  printf("%s %s %d\n", changelogfile.c_str(),sfsid.c_str(), fsid);
  fsid = atoi(sfsid.c_str());

  if (!gFmd.SetChangeLogFile(changelogfile.c_str(),fsid,passoption)) {
    fprintf(stderr,"%s: error: check has failed\n", argv[0]);
    rc = -1;
  }

  if (trim) {
    XrdCommonLogging::SetLogPriority(LOG_NOTICE);
    if (!gFmd.TrimLogFile(fsid,passoption)) {
      fprintf(stderr,"%s: error: trimming has failed\n",argv[0]);
      rc = -2;
    } else {

      if (inplace) {
	if (!rename(gFmd.ChangeLogFileName.c_str(),argv[1])) {
	  fprintf(stdout,"=> trimmed in place := renaming  %s => %s\n", gFmd.ChangeLogFileName.c_str(),argv[1]);
	} else {
	  fprintf(stderr,"%s: error: cannot rename new trim logfile to be in place!\n",argv[0]);
	  rc = -3;
	}
      }
    }
  }

  if (searchpath.length()) {
    XrdCommonLogging::SetLogPriority(LOG_NOTICE);
    XrdOucString findstring = "find "; findstring += searchpath; findstring += " -type f -name \"????????\" ";
    FILE* fp = popen(findstring.c_str(),"r");
    google::dense_hash_map<long long, bool> DiskFid;
    DiskFid.set_empty_key(0);

    if (!fp) {
      fprintf(stderr,"%s: error: cannot search in path %s !\n",argv[0], searchpath.c_str());
      rc = -4;
    } else {
      char filename[16384];
      while ( (fscanf(fp,"%s\n",filename)) == 1) {
	XrdCommonPath cPath(filename);
	XrdOucString bname = cPath.GetName();
	long long fid = XrdCommonFileId::Hex2Fid(bname.c_str());
	//	fprintf(stdout,"%llu\t", fid);
	DiskFid.insert(make_pair(fid,1));
      }
      fprintf(stdout,"=> loaded %ld FID's from local path %s \n", DiskFid.size(), searchpath.c_str());
      fclose(fp);
    }

    unsigned long long error_missing_changelog=0;
    // compare disk vs changelog
    {
      google::dense_hash_map<long long, bool>::iterator it;
      for (it = DiskFid.begin(); it != DiskFid.end(); ++it) {
	// check if this exists in the meta data log
	if (gFmd.FmdSize.count(it->first)==1) {
	  // ok, that's good
	} else {
	  // bad, this is missing
	  eos_static_info("fid %06llu on disk      : missing in changelog file !\n", it->first);
	  error_missing_changelog++;
	}
      }
    }
    unsigned long long error_missing_disk=0;

    // compare changelog vs disk
    {
      google::dense_hash_map<long long, unsigned long long>::iterator it;
      for (it = gFmd.FmdSize.begin(); it != gFmd.FmdSize.end(); ++it) {
	if ((DiskFid.count(it->first))==1) {
	  // ok, that's good
	} else {
	  // bad, this is missing
	  eos_static_info("fid %06llu on changelog : missing on disk !\n", it->first);
	  error_missing_disk++;
	}
      }
    }
    fprintf(stdout,"=> files missing in change log : %llu\n", error_missing_changelog);
    fprintf(stdout,"=> files missing in data dir   : %llu\n", error_missing_disk);

  }
  return rc;
}
