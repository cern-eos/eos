#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonPath.hh"
#include "XrdCommon/XrdCommonFileId.hh"
#include "XrdClient/XrdClient.hh"

void usage(const char* name) {
  fprintf(stderr,"usage: %s <changelogfile> [-f] [-dump] [-trim] [-inplace] [--data=<path>] [-show] [--mgm=<url>] [-h] [--help] [-quiet]\n", name);
  fprintf(stderr,"       -f         : force the reading even if the version does not match\n");
  fprintf(stderr,"    -dump         : dump out the meta data blocks\n");
  fprintf(stderr,"    -trim         : trim this file (erases faulty records)\n");
  fprintf(stderr,"    -inplace      : replace the original file with the trimmed copy\n");
  fprintf(stderr,"    --data=<path> : compare with files in path\n");
  fprintf(stderr,"    -show         : show all inconsistencies\n");
  fprintf(stderr,"    --mgm=<url>   : URL of the management server to do comparison of cached meta data\n");
  fprintf(stderr,"    -h | --help   : show usage information\n");
  fprintf(stderr,"    -quiet        : don't print error or info messages\n");

  exit(-1);
}

int main(int argc, char* argv[] ) {
  // change to daemon account
  setuid(2);
  XrdCommonLogging::Init();
  XrdCommonLogging::SetUnit("eosfstfsck");
  XrdCommonLogging::SetLogPriority(LOG_NOTICE);
  XrdOucString passoption="c";
  XrdOucString changelogdir="";
  XrdOucString searchpath="";
  XrdOucString mgmurl="";
  bool quiet=false;
  int rc = 0;

  bool trim = false;
  bool inplace = false;
  bool show = false;

  if (argc<=1) {
    usage(argv[0]);
  }

  for (int i=1; i< argc; i++) {
    XrdOucString option = argv[i];
    if (option == "-h" || option == "--help") {
      usage(argv[0]);
    }

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
    if (option.beginswith("--mgm=")) {
      mgmurl = option;
      mgmurl.erase(0,6);
      fprintf(stdout,"=> querying management server %s ...\n", mgmurl.c_str());
    }
    if (option == "-show") {
      fprintf(stdout,"=> activated show option ...\n");
      show = true;
    }
    if (option == "-quiet") {
      quiet = true;
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
    rc = 1;
  }

  if (trim) {
    if (quiet)
      XrdCommonLogging::SetLogPriority(LOG_CRIT);
    else 
      XrdCommonLogging::SetLogPriority(LOG_NOTICE);
    if (!gFmd.TrimLogFile(fsid,passoption)) {
      fprintf(stderr,"%s: error: trimming has failed\n",argv[0]);
      rc = 2;
    } else {

      if (inplace) {
	if (!rename(gFmd.ChangeLogFileName.c_str(),argv[1])) {
	  fprintf(stdout,"=> trimmed in place := renaming  %s => %s\n", gFmd.ChangeLogFileName.c_str(),argv[1]);
	} else {
	  fprintf(stderr,"%s: error: cannot rename new trim logfile to be in place!\n",argv[0]);
	  rc = 3;
	}
      }
    }
  }

  if (searchpath.length()) {
    fprintf(stdout,"---------------------------------------\n")
;    if (quiet)
       XrdCommonLogging::SetLogPriority(LOG_CRIT);
    else 
      if (!show) 
	XrdCommonLogging::SetLogPriority(LOG_NOTICE);
      else
	XrdCommonLogging::SetLogPriority(LOG_INFO);

    XrdOucString findstring = "find "; findstring += searchpath; findstring += " -type f -name \"????????\" ";
    FILE* fp = popen(findstring.c_str(),"r");
    google::dense_hash_map<long long, std::string> DiskFid;
    DiskFid.set_empty_key(0);

    if (!fp) {
      fprintf(stderr,"%s: error: cannot search in path %s !\n",argv[0], searchpath.c_str());
      rc = 4;
    } else {
      char filename[16384];
      while ( (fscanf(fp,"%s\n",filename)) == 1) {
	XrdCommonPath cPath(filename);
	XrdOucString bname = cPath.GetName();
	long long fid = XrdCommonFileId::Hex2Fid(bname.c_str());
	//	fprintf(stdout,"%llu\t", fid);
	DiskFid.insert(make_pair(fid,filename));
      }
      fprintf(stdout,"=> loaded %ld FID's from local path %s \n", DiskFid.size(), searchpath.c_str());
      fclose(fp);
    }
    unsigned long long error_wrong_filesize=0;
    unsigned long long error_missing_changelog=0;
    // compare disk vs changelog
    {
      google::dense_hash_map<long long, std::string>::iterator it;
      for (it = DiskFid.begin(); it != DiskFid.end(); ++it) {
	// check if this exists in the meta data log
	if (gFmd.FmdSize.count(it->first)==1) {
	  // ok, that's good, let's crosscheck the size

	} else {
	  // bad, this is missing
	  eos_static_info("fid %08llx on disk      : missing in changelog file !", it->first);
	  error_missing_changelog++;
	}
      }
    }
    unsigned long long error_missing_disk=0;
    unsigned long long warning_wrong_ctime=0;
    unsigned long long warning_wrong_mtime=0;

    // compare changelog vs disk
    {
      google::dense_hash_map<long long, unsigned long long>::iterator it;
      for (it = gFmd.FmdSize.begin(); it != gFmd.FmdSize.end(); ++it) {
	if ((DiskFid.count(it->first))==1) {
	  // ok, that's good, check the most basic meta data now
	  struct stat buf;
	  if (stat(DiskFid[it->first].c_str(),&buf)) {
	    // uups, cannot stat this file
	    eos_static_err("fid %08llx - cannot do stat on %s !", it->first,DiskFid[it->first].c_str());
	    error_wrong_filesize++;
	  } else {
	    // check size
	    if ((unsigned long long) buf.st_size != gFmd.FmdSize[it->first]) {
	      eos_static_notice("fid %08llx has size=%llu on disk but size=%llu in the changelog!", it->first, (unsigned long long) buf.st_size, gFmd.FmdSize[it->first]);
	      error_wrong_filesize++;
	    } 
	    // check modification time
	    XrdCommonFmd* fmd = gFmd.GetFmd(it->first, fsid, 0,0,0);
	    if (!fmd) {
	      eos_static_err("fid %08llx - cannot retrieve file meta data from changelog",it->first);
	    } else {
	      if ( (unsigned long long) buf.st_mtime != fmd->fMd.mtime) {
		eos_static_notice("fid %08llx has mtime=%llu on disk but mtime=%llu in the changelog!", it->first, (unsigned long long)buf.st_mtime, fmd->fMd.mtime);
		warning_wrong_mtime++;
	      }
	      if ( (unsigned long long) buf.st_ctime != fmd->fMd.ctime) {
		eos_static_notice("fid %08llx has ctime=%llu on disk but ctime=%llu in the changelog!", it->first, (unsigned long long)buf.st_ctime, fmd->fMd.ctime);
		warning_wrong_ctime++;
	      }
	    }
	  }
	} else {
	  // bad, this is missing
	  eos_static_info("fid %08llx on changelog : missing on disk !\n", it->first);
	  error_missing_disk++;
	}
      }
    }
    fprintf(stdout,"---------------------------------------\n");
    fprintf(stdout,"=> files missing in change log : %llu\n", error_missing_changelog);
    fprintf(stdout,"=> files missing in data dir   : %llu\n", error_missing_disk);
    fprintf(stdout,"=> files with wrong filesize   : %llu\n", error_wrong_filesize);
    fprintf(stdout,"=> files with wrong mtime      : %llu\n", warning_wrong_mtime);
    fprintf(stdout,"=> files with wrong ctime      : %llu\n", warning_wrong_ctime);
    fprintf(stdout,"---------------------------------------\n");
    if (error_missing_changelog || error_missing_disk || error_wrong_filesize) 
      rc = 5;
  }

  if (mgmurl.length()) {
    // compare with the central meta data cache
    XrdOucString in = "";
    in += "&eos.ruid=0";
    in += "&eos.rgid=0";
    in += "&mgm.cmd=fs&mgm.subcmd=dumpmd";
    in += "&mgm.fsid="; in += fsid;
    
    XrdOucString out="";
    XrdOucString path = mgmurl;
    path += "//proc/admin/";
    path += "?";
    path += in;
    XrdClient client(path.c_str());
    if (client.Open(kXR_async,0,0)) {
      off_t offset = 0;
      int nbytes=0;
      char buffer[4096+1];
      while ((nbytes = client.Read(buffer,offset, 4096)) >0) {
	buffer[nbytes]=0;
	out += buffer;
	offset += nbytes;
      }
      client.Close();
    }

    XrdOucEnv result(out.c_str());

    unsigned long long error_no_fmd=0;
    unsigned long long error_diff_lid=0;
    unsigned long long error_diff_uid=0;
    unsigned long long error_diff_gid=0;
    unsigned long long error_diff_cid=0;
    unsigned long long error_diff_ctime=0;
    unsigned long long error_diff_ctime_ns=0;
    unsigned long long error_diff_mtime=0;
    unsigned long long error_diff_mtime_ns=0;
    unsigned long long error_diff_checksum=0;
    unsigned long long error_diff_name=0;
    unsigned long long error_diff_container=0;
    unsigned long long error_diff_size=0;

    // this is not the most efficient since we copy twice the whole buffer, but it is more 'clean' to do like that
    if (result.Get("mgm.proc.stdout")) {
      out = result.Get("mgm.proc.stdout");
      // parse the result
      int linebreak=0;
      int lastlinebreak=-1;
      while ( (linebreak = out.find('\n',linebreak)) != STR_NPOS) {
	XrdOucString thisline="";
	thisline.assign(out,lastlinebreak+1,linebreak);
	thisline.replace("#and#","&");
	XrdOucEnv mdEnv(thisline.c_str());

	// parse back the info
	struct XrdCommonFmd::FMD fmd;
	memset((void*)&fmd,0,sizeof(struct XrdCommonFmd::FMD));
	bool parseerror=false;
	if (mdEnv.Get("id")) 
	  fmd.fid = strtoull(mdEnv.Get("id"),0,10);
	else 
	  parseerror=true;

	if (mdEnv.Get("cid"))
	  fmd.cid = strtoull(mdEnv.Get("cid"),0,10);
	else
	  parseerror=true;
	
	if (mdEnv.Get("uid")) 
	  fmd.uid = (uid_t) strtoul(mdEnv.Get("uid"),0,10);
	else 
	  parseerror=true;
	  
	if (mdEnv.Get("gid")) 
	  fmd.gid = (gid_t) strtoul(mdEnv.Get("gid"),0,10);
	else 
	  parseerror=true;

	if (mdEnv.Get("ctime"))
	  fmd.ctime = strtoull(mdEnv.Get("ctime"),0,10);
	else
	  parseerror=true;

	if (mdEnv.Get("ctime_ns"))
	  fmd.ctime_ns = strtoull(mdEnv.Get("ctime_ns"),0,10);
	else
	  parseerror=true;

	if (mdEnv.Get("mtime"))
	  fmd.mtime = strtoull(mdEnv.Get("mtime"),0,10);
	else
	  parseerror=true;

	if (mdEnv.Get("mtime_ns"))
	  fmd.mtime_ns = strtoull(mdEnv.Get("mtime_ns"),0,10);
	else
	  parseerror=true;

	if (mdEnv.Get("size"))
	  fmd.size = strtoull(mdEnv.Get("size"),0,10);
	else
	  parseerror=true;

	if (mdEnv.Get("lid"))
	  fmd.lid = strtoul(mdEnv.Get("lid"),0,10);

	if (mdEnv.Get("location")) {
	  XrdOucString location = mdEnv.Get("location");
	  XrdOucString loctag ="" ; loctag+= fsid; loctag+= ",";
	  if ( (location.find(loctag.c_str())) != STR_NPOS) {
	    fmd.fsid = fsid;
	  } else {
	    fmd.fsid = 0;
	  }
	}
	 
	if (mdEnv.Get("name")) {
	  strncpy(fmd.name, mdEnv.Get("name"),sizeof(fmd.name)-1);
	  fmd.name[sizeof(fmd.name)-1] = 0;
	}

	if (mdEnv.Get("container")) {
	  strncpy(fmd.name, mdEnv.Get("container"),sizeof(fmd.container)-1);
	  fmd.container[sizeof(fmd.container)-1] = 0;
	}

	if (mdEnv.Get("checksum")) {
	  for (int i=0; i< SHA_DIGEST_LENGTH; i++ ) {
	    XrdOucString cs="";
	    cs.assign(mdEnv.Get("checksum"),(i*2),(i*2)+1);
	    fmd.checksum[i] = (char) strtol(cs.c_str(),0,16);
	  }
	}

	// do the comparisons
	XrdCommonFmd* rfmd = gFmd.GetFmd(fmd.fid, fsid, 0,0,0);
	if (!rfmd) {
	  eos_static_err("fid %08llx - cannot retrieve file meta data from changelog",fmd.fid);
	  error_no_fmd++;
	} else {
	  if (rfmd->fMd.lid != fmd.lid)
	    error_diff_lid++;
	  if (rfmd->fMd.uid != fmd.uid)
	    error_diff_uid++;
	  if (rfmd->fMd.gid != fmd.gid)
	    error_diff_gid++;
	  if (rfmd->fMd.cid != fmd.cid) 
	    error_diff_cid++;
	  if (rfmd->fMd.ctime != fmd.ctime)
	    error_diff_ctime++;
	  if (rfmd->fMd.ctime_ns != fmd.ctime_ns)
	    error_diff_ctime_ns++;
	  if (rfmd->fMd.mtime != fmd.mtime)
	    error_diff_mtime_ns++;
	  if (memcmp(rfmd->fMd.checksum, fmd.checksum,SHA_DIGEST_LENGTH)) 
	    error_diff_checksum++;
	  if (strncmp(rfmd->fMd.name,fmd.name,255))
	    error_diff_name++;
	  if (strncmp(rfmd->fMd.container,fmd.container,255))
	    error_diff_container++;
	  if (rfmd->fMd.size != fmd.size)
	    error_diff_size++;
	}
	lastlinebreak=linebreak;
	linebreak++;
      }
      /*      unsigned long long error_no_fmd=0;
	      unsigned long long error_diff_lid=0;
	      unsigned long long error_diff_uid=0;
	      unsigned long long error_diff_gid=0;
	      unsigned long long error_diff_cid=0;
	      unsigned long long error_ctime=0;
	      unsigned long long error_ctime_ns=0;
	      unsigned long long error_mtime=0;
	      unsigned long long error_mtime_ns=0;
	      unsigned long long error_diff_checksum=0;
	      unsigned long long error_diff_name=0;
	      unsigned long long error_diff_container=0;
	      unsigned long long error_diff_size=0;         */

      fprintf(stdout,"---------------------------------------\n");
      fprintf(stdout,"=> files missing cached central: %llu\n", error_no_fmd);
      fprintf(stdout,"=> files layout id differs     : %llu\n", error_diff_lid);
      fprintf(stdout,"=> files uid differs           : %llu\n", error_diff_uid);
      fprintf(stdout,"=> files gid differes          : %llu\n", error_diff_gid);
      fprintf(stdout,"=> files ctime differs         : %llu\n", error_diff_ctime);
      fprintf(stdout,"=> files ctime_ns differs      : %llu\n", error_diff_ctime_ns);
      fprintf(stdout,"=> files mtime differs         : %llu\n", error_diff_mtime);
      fprintf(stdout,"=> files mtime_ns differs      : %llu\n", error_diff_mtime_ns);
      fprintf(stdout,"=> files checksum differs      : %llu\n", error_diff_checksum);
      fprintf(stdout,"=> files name differs          : %llu\n", error_diff_name);
      fprintf(stdout,"=> files container differs     : %llu\n", error_diff_container);
      fprintf(stdout,"=> files size differs          : %llu\n", error_diff_size);
      if (error_no_fmd ||
	  error_diff_lid ||
	  error_diff_uid ||
	  error_diff_gid ||
	  error_diff_ctime ||
	  error_diff_ctime_ns ||
	  error_diff_mtime ||
	  error_diff_mtime_ns ||
	  error_diff_checksum ||
	  error_diff_name ||
	  error_diff_container ||
	  error_diff_size)
	rc = 7;
    } else {
      eos_static_err("cannot get filelist from mgm [%s]",result.Get("mgm.proc.stderr"));
      rc = 6;
    }
  }

  return rc;
}
