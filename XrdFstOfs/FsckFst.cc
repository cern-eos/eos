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
    XrdOucString findstring = "find "; findstring += searchpath; findstring += " -type f -name \"????????\" ";
    FILE* fp = popen(findstring.c_str(),"r");
    if (!fp) {
      fprintf(stderr,"%s: error: cannot search in path %s !\n",argv[0], searchpath.c_str());
      rc = -4;
    } else {
      char filename[16384];
      std::vector<unsigned long long> DiskFid;
      while ( (fscanf(fp,"%s\n",filename)) == 1) {
	XrdCommonPath cPath(filename);
	XrdOucString bname = cPath.GetName();
	unsigned long long fid = XrdCommonFileId::Hex2Fid(bname.c_str());
	//	fprintf(stdout,"%llu\t", fid);
	DiskFid.push_back(fid);
      }
      fprintf(stdout,"=> loaded %ld FID's from local path %s \n", DiskFid.size(), searchpath.c_str());
      fclose(fp);
    }
  }
  return rc;
}
