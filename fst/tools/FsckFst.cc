#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "XrdOuc/XrdOucString.hh"
#include "common/Fmd.hh"
#include "common/Path.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "common/StringConversion.hh"

#include "XrdClient/XrdClient.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "fst/checksum/ChecksumPlugins.hh"

#include <google/sparse_hash_set>

void usage(const char* name) {
  fprintf(stderr,"usage: %s <changelogfile> [-f] [--dump] [--trim] [--inplace] [--data=<path>] [--delete-missing-changelog] [--delete-missing-disk] [--show] [--mgm=<url>] [--repair-local] [--repair-cache] [-h] [--help] [--checksum] [--quiet] [--test] [--upload-fid=<hex-fid>] [--delete-enoent] [--delete-deleted] [--clean-transactions]\n", name);
  fprintf(stderr,"       -f         : force the reading even if the version does not match\n");
  fprintf(stderr,"    --dump        : dump out the meta data blocks\n");
  fprintf(stderr,"    --trim        : trim this file (erases faulty records)\n");
  fprintf(stderr,"    --inplace     : replace the original file with the trimmed copy\n");
  fprintf(stderr,"    --data=<path> : compare with files in path\n");
  fprintf(stderr,"    --delete-missing-changelog : files which are on disk but not anymore active in the changelog get unlinked - WARNING - this can be VERY dangerous if the data path does not match the changelogfile!\n");
  fprintf(stderr,"    --delete-missing-disk      : files which are not anymore on disk get removed from the changelog!\n");
  fprintf(stderr,"    --show        : show all inconsistencies\n");
  fprintf(stderr,"    --mgm=<url>   : URL of the management server to do comparison of cached meta data\n");
  fprintf(stderr,"    --repair-local: correct the filesize different from disk size to local changelog size\n");
  fprintf(stderr,"    --repair-cache: correct filesize and replica information to the central cache\n");
  fprintf(stderr,"    --checksum    : recalculate a checksum if there is a checksum mismatch\n");
  fprintf(stderr,"    -h | --help   : show usage information\n");
  fprintf(stderr,"    --quiet       : don't print error or info messages\n");
  fprintf(stderr,"    --upload-fid=<hex-fid> : force a commit of meta data of fid <hex-fid> - if * is specified all files missing in the central cache are commited\n");
  fprintf(stderr,"    --delete-enoent : local files get unlinked if the file is not anymore reachable via the cache namespace [combine with --upload-fid=*]\n");
  fprintf(stderr,"    --delete-deleted: local files get unlinked if the file is unlinked and the local file has to be deleted [combine with --upload-fid=*]\n");
  fprintf(stderr,"    --test        : do not touch local files/changelog/meta data cache - just provide the numbers of applied corrections (does not apply for trimming)\n");
  exit(-1);
}

bool yesno(const char* text) {
  fprintf(stdout,"%s ",text);
  int i = getchar();
  if ( (i==121)  || (i==89) ) 
    return true;
  return false;
}

int main(int argc, char* argv[] ) {
  // change to daemon account
  setuid(2);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eosfstfsck");
  eos::common::Logging::SetLogPriority(LOG_NOTICE);
  XrdOucString passoption="c";
  XrdOucString changelogdir="";
  XrdOucString searchpath="";
  XrdOucString uploadfid="";

  XrdOucString mgmurl="";
  bool quiet=false;
  int rc = 0;

  bool trim = false;
  bool inplace = false;
  bool show = false;
  bool repairlocal = false;
  bool repaircache = false;
  bool checksum = false;
  bool deleteenoent = false;
  bool deletedeleted = false;
  bool deletemissingchangelog = false;
  bool deletemissingdisk      = false;
  bool sure = false;
  bool testonly = false;
  bool cleantransactions = false;

  char managerresult[8192]; managerresult[0]=0;
  int  managerresult_size=8192;



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
    if (option == "--dump") {
      fprintf(stdout,"=> setting dump option ...\n");
      passoption+= "d";
    }
    if (option == "--trim") {
      fprintf(stdout,"=> setting trim option ...\n");
      passoption+= "t";
      trim = true;
    }
    if (option == "--inplace") {
      fprintf(stdout,"=> activated in-place for trim option ...\n");
      inplace = true;
    }
    if (option.beginswith("--data=")) {
      searchpath = option;
      searchpath.erase(0,7);
      fprintf(stdout,"=> searching for files under path %s ...\n", searchpath.c_str());
    }
    if (option == "--delete-missing-changelog") {
      deletemissingchangelog = true;
    }
    if (option == "--delete-missing-disk") {
      deletemissingdisk = true;
    }
    if (option == "--test") {
      testonly = true;
    }
    if (option.beginswith("--mgm=")) {
      mgmurl = option;
      mgmurl.erase(0,6);
      fprintf(stdout,"=> querying management server %s ...\n", mgmurl.c_str());
    }
    if (option.beginswith("--upload-fid=")) {
      uploadfid = option;
      uploadfid.erase(0,13);
      fprintf(stdout,"=> uploading meta data of fid=%s ...\n", uploadfid.c_str());
    }

    if (option.beginswith("--repair-local")) {
      repairlocal = true;
    }
    if (option.beginswith("--repair-cache")) {
      repaircache = true;
    }
    if (option.beginswith("--checksum")) {
      checksum = true;
    }
    if (option == "--show") {
      fprintf(stdout,"=> activated show option ...\n");
      show = true;
    }
    if (option == "--quiet") {
      quiet = true;
    }
    if (option == "--delete-enoent") {
      deleteenoent=true;
    }
    if (option == "--delete-deleted") {
      deletedeleted=true;
    }
    if (option == "--clean-transactions") {
      cleantransactions=true;
    }
  }

  if (cleantransactions && !searchpath.length()) {
    fprintf(stderr,"error: you have to give the --data argument to use --clean-transactions\n");
    exit(-1);
  }

  XrdOucString changelogfile = argv[1];
  XrdOucString sfsid = changelogfile;

  // the changelog file has to be there and owned by the daemon account!
  struct stat buf;
  if (stat(changelogfile.c_str(),&buf)) {
    fprintf(stderr,"error: cannot open changelog file\n");
    exit(-1);
  }
  if (buf.st_uid != 2) {
    fprintf(stderr,"error: changelog file has to be owned be uid=2 (daemon\n");
    exit(-1);
  }
  

  int fsid=0;
  
  eos::common::FmdHandler gFmd;
  XrdClientAdmin* gManager;

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
      eos::common::Logging::SetLogPriority(LOG_CRIT);
    else 
      eos::common::Logging::SetLogPriority(LOG_NOTICE);
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
    fprintf(stdout,"---------------------------------------\n");
    if (quiet)
       eos::common::Logging::SetLogPriority(LOG_CRIT);
    else 
      if (!show) 
	eos::common::Logging::SetLogPriority(LOG_NOTICE);
      else
	eos::common::Logging::SetLogPriority(LOG_INFO);

    if (cleantransactions) {
      unsigned long long transaction_cleanup_ok=0;
      unsigned long long transaction_cleanup_failed=0;
      fprintf(stdout,"---------------------------------------\n");
      fprintf(stdout,"Cleaning transactions ...\n");
      XrdOucString tadir=searchpath;
      tadir += "/"; tadir += ".eostransaction";
      DIR* dir = opendir(tadir.c_str());
      if (!dir) {
	fprintf(stderr,"error: cannot open transactiondirectory %s\n", tadir.c_str());
	exit(-1);
      }
      struct dirent* entry;
      while ( (entry=readdir(dir))) {
	if ( (!strcmp(entry->d_name,".")) ||
	     (!strcmp(entry->d_name,".."))) {
	  continue;
	}
	eos_static_info("transactions directory: cleaning %s", entry->d_name);
	XrdOucString txname = tadir; txname += "/"; txname += entry->d_name;
	if ((testonly) || (!unlink(txname.c_str()))) {
	  transaction_cleanup_ok++;
	} else {
	  eos_static_crit("transactions directory: cleanup failed for %s", txname.c_str());
	  transaction_cleanup_failed++;
	}
      }
      closedir(dir);
      fprintf(stdout,"=> transactions cleaned ok     : %llu\n", transaction_cleanup_ok);
      fprintf(stdout,"=> transcations cleaned failed : %llu\n", transaction_cleanup_failed);
    }
    

    XrdOucString findstring = "find "; findstring += searchpath; findstring += "/[0-9]* -type f -name \"????????\" ";
    FILE* fp = popen(findstring.c_str(),"r");
    google::sparse_hash_map<long long, std::string> DiskFid;

    if (!fp) {
      fprintf(stderr,"%s: error: cannot search in path %s !\n",argv[0], searchpath.c_str());
      rc = 4;
    } else {
      char filename[16384];
      while ( (fscanf(fp,"%s\n",filename)) == 1) {
	eos::common::Path cPath(filename);
	XrdOucString bname = cPath.GetName();
	long long fid = eos::common::FileId::Hex2Fid(bname.c_str());
	//	fprintf(stdout,"%llu\t", fid);
	DiskFid.insert(make_pair(fid,filename));
      }
      fprintf(stdout,"=> loaded %lld FID's from local path %s \n", (long long)DiskFid.size(), searchpath.c_str());
      fclose(fp);
    }
    unsigned long long error_wrong_filesize=0;
    unsigned long long error_missing_changelog=0;
    unsigned long long files_unlinked_data=0;
    unsigned long long files_removed_changelog=0;

    // compare disk vs changelog
    {
      google::sparse_hash_map<long long, std::string>::iterator it;
      for (it = DiskFid.begin(); it != DiskFid.end(); ++it) {
	// check if this exists in the meta data log
	if (gFmd.FmdSize.count(it->first)==1) {
	  // ok, that's good, let's crosscheck the size

	} else {
	  // bad, this is missing
	  eos_static_info("fid %08llx on disk      : missing in changelog file !", it->first);
	  error_missing_changelog++;
	  if (deletemissingchangelog) {
	    if (!sure) {
	      if (yesno("You asked to unlink files from the data disk .... are you really sure? [y/n + ENTER]")) {
		sure = true;
	      } else {
		exit(0);
	      }
	    }
	    if (sure) {
	      eos_static_crit("unlinking file %s", it->second.c_str());
	      // uncomment that to make the tool 'sharp'
	      if (!testonly) {
		if (unlink(it->second.c_str())) {eos_static_err("failed to unlink file %s", it->second.c_str());}
	      }
	      files_unlinked_data++;
	    }
	  }
	}
      }
    }
    unsigned long long error_missing_disk=0;
    unsigned long long warning_wrong_ctime=0;
    unsigned long long warning_wrong_mtime=0;
    unsigned long long repaired_files=0;

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
	    eos::common::Fmd* fmd = gFmd.GetFmd(it->first, fsid, 0,0,0);

	    // check size
	    if ((unsigned long long) buf.st_size != gFmd.FmdSize[it->first]) {
	      eos_static_notice("fid %08llx has size=%llu on disk but size=%llu in the changelog!", it->first, (unsigned long long) buf.st_size, gFmd.FmdSize[it->first]);
	      error_wrong_filesize++;
	      if (repairlocal) {
		// repair the size
		fmd->fMd.size = buf.st_size;
		if (!testonly)
		  gFmd.FmdSize[it->first] = buf.st_size;
		if ((!testonly) && (!gFmd.Commit(fmd))) {
		  eos_static_err("unable to repair file size in changelog file fo fid %08llx size=%llu", it->first, (unsigned long long) buf.st_size);
		} else {
		  repaired_files++;
		}
	      }
	    } 
	    // check modification time
	    if (!fmd) {
	      eos_static_err("fid %08llx - cannot retrieve file meta data from changelog",it->first);
	    } else {
	      if ( labs((unsigned long long) buf.st_mtime - fmd->fMd.mtime)>1 ) {
		//eos_static_notice("fid %08llx has mtime=%llu on disk but mtime=%llu in the changelog!", it->first, (unsigned long long)buf.st_mtime, fmd->fMd.mtime);
		warning_wrong_mtime++;
	      }
	      if ( (unsigned long long) buf.st_ctime != fmd->fMd.ctime) {
		//eos_static_notice("fid %08llx has ctime=%llu on disk but ctime=%llu in the changelog!", it->first, (unsigned long long)buf.st_ctime, fmd->fMd.ctime);
		warning_wrong_ctime++;
	      }
	    }
	  }
	} else {
	  // bad, this is missing
	  eos_static_info("fid %08llx on changelog : missing on disk !\n", it->first);
	  error_missing_disk++;
	  if (deletemissingdisk) {
	    if (!testonly)
	      gFmd.DeleteFmd(it->first,fsid);
	    files_removed_changelog++;
	  }
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

    if (repairlocal) {
      fprintf(stdout,"=> files repaired              : %llu\n",repaired_files);
      fprintf(stdout,"---------------------------------------\n");
    }
    if (deletemissingchangelog) {
      fprintf(stdout,"=> files unlinked from disk    : %llu\n",files_unlinked_data);
      fprintf(stdout,"---------------------------------------\n");
    }
    if (deletemissingdisk) {
      fprintf(stdout,"=> files removed from changelog: %llu\n",files_removed_changelog);
      fprintf(stdout,"---------------------------------------\n");
    }
    if (error_missing_changelog || error_missing_disk || error_wrong_filesize) 
      rc = 5;
  }

  if (mgmurl.length()) {
    if (quiet)
      eos::common::Logging::SetLogPriority(LOG_CRIT);
    else 
      if (!show) 
	eos::common::Logging::SetLogPriority(LOG_NOTICE);
      else
	eos::common::Logging::SetLogPriority(LOG_INFO);

    google::sparse_hash_set<unsigned long long> fidsInCache;

    XrdOucString mgmdummy=mgmurl;
    mgmdummy += "/dummy";
    fprintf(stderr,"Connecting to %s\n", mgmdummy.c_str());
    gManager = new XrdClientAdmin(mgmdummy.c_str());
    gManager->Connect();

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
    } else {
      fprintf(stderr,"error: unable to dump meta data from the MGM!\n");
      exit(-1);
    }

    XrdOucEnv result(out.c_str());

    unsigned long long error_no_fmd=0;
    unsigned long long error_parsing=0;
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
    unsigned long long error_xsum_failed=0;
    unsigned long long error_xsum_illegaltype=0;
    unsigned long long repaired_local_checksum=0;
    unsigned long long repaired_cache_checksum=0;
    unsigned long long failed_update_local=0;
    unsigned long long failed_update_central=0;

    if (result.Get("mgm.proc.stderr") && strlen(result.Get("mgm.proc.stderr"))) {
      fprintf(stderr,"error: couldn't get a meta data dump from the MGM - %s\n", result.Get("mgm.proc.stderr"));
      exit(-1);
    }
    // this is not the most efficient since we copy twice the whole buffer, but it is more 'clean' to do like that
    if (result.Get("mgm.proc.stdout")) {
      out = result.Get("mgm.proc.stdout");
      // parse the result
      int linebreak=0;
      int lastlinebreak=-1;
      unsigned long long nfiles=0;
      while ( (linebreak = out.find('\n',linebreak)) != STR_NPOS) {
	nfiles++;
	XrdOucString thisline="";
	thisline.assign(out,lastlinebreak+1,linebreak);
	thisline.replace("#and#","&");
	thisline.replace("\n","");
	XrdOucEnv mdEnv(thisline.c_str());

	// parse back the info
	struct eos::common::Fmd::FMD fmd;
	memset((void*)&fmd,0,sizeof(struct eos::common::Fmd::FMD));
	bool parseerror=false;
	if (mdEnv.Get("id")) {
	  fmd.fid = strtoull(mdEnv.Get("id"),0,10);
	  fidsInCache.insert(fmd.fid);
	} else {
	  parseerror=true;
	}

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


	if (!parseerror) {
	  XrdOucString localChecksum="";
	  
	  // do the comparisons
	  eos::common::Fmd* rfmd = gFmd.GetFmd(fmd.fid, fsid, 0,0,0);
	  if (!rfmd) {
	    eos_static_err("fid %08llx - cannot retrieve file meta data from changelog",fmd.fid);
	    error_no_fmd++;
	  } else {
	    for (int i=0; i< SHA_DIGEST_LENGTH; i++ ) {
	      char c[3];
	      // caution, this checksum have swapped byte order, because they come from reading an int bytewise!
	      if ( ((eos::common::LayoutId::GetChecksum(rfmd->fMd.lid) == eos::common::LayoutId::kAdler) ||    
		    (eos::common::LayoutId::GetChecksum(rfmd->fMd.lid) == eos::common::LayoutId::kCRC32) ||
		    (eos::common::LayoutId::GetChecksum(rfmd->fMd.lid) == eos::common::LayoutId::kCRC32C))  && (i<4) ) {
		sprintf(c,"%02x", (unsigned char) rfmd->fMd.checksum[3-i]);
	      } else {
		sprintf(c,"%02x", (unsigned char) rfmd->fMd.checksum[i]);
	      }
	      localChecksum+=c;
	    }
	    
	    XrdOucString centralChecksum = mdEnv.Get("checksum");
	    if (centralChecksum != localChecksum) {
	      eos_static_notice("fid %08llx has CX=%s LX=%s TYPE=%s",rfmd->fMd.fid, centralChecksum.c_str(),localChecksum.c_str(),  eos::common::LayoutId::GetChecksumString(rfmd->fMd.lid));
	      error_diff_checksum++;
	      if (checksum) {
		// recalculate a checksum
		eos::fst::CheckSum* checksummer = eos::fst::ChecksumPlugins::GetChecksumObject(rfmd->fMd.lid);
		if (!checksummer) {
		  eos_static_crit("cannot load any checksum plugin");
		  error_xsum_illegaltype++;
		} else {
		  // build the local filename
		  XrdOucString hexstring="";
		  XrdOucString fullpath="";
		  eos::common::FileId::Fid2Hex(rfmd->fMd.fid, hexstring);
		  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), searchpath.c_str(), fullpath);
		  // scan the checksum of that file
		  eos_static_notice("Scanning checksum of file %s ...", fullpath.c_str());
		  unsigned long long scansize=0;
		  float scantime = 0;
		  if (!checksummer->ScanFile(fullpath.c_str(), scansize, scantime)) {
		    eos_static_crit("cannot scan the checksum of fid %08llx under path %s", rfmd->fMd.fid, fullpath.c_str());
		    error_xsum_failed++;
		  } else {
		    XrdOucString sizestring;
		    eos_static_notice("name=%s path=%s fid=%08llx CX=%s size=%s time=%.02fms rate=%.02f MB/s", rfmd->fMd.name, fullpath.c_str(), rfmd->fMd.fid,checksummer->GetHexChecksum() , eos::common::StringConversion::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0*scansize/1000/(scantime?scantime:99999999999999LL));
		    int checksumlen;
		    checksummer->GetBinChecksum(checksumlen);
		    // copy checksum into meta data entry
		    memset(rfmd->fMd.checksum, 0, sizeof(SHA_DIGEST_LENGTH));
		    memcpy(rfmd->fMd.checksum, checksummer->GetBinChecksum(checksumlen),checksumlen);
		    
		    if (repairlocal) {
		      if ((!testonly) && (!gFmd.Commit(rfmd))) {
			eos_static_err("unable to commit checksum update in changelog file fo fid %08llx", rfmd->fMd.fid);
			failed_update_local++;
		      } else {
			repaired_local_checksum++;
		      }
		    }
		    
		    
		    if (repaircache) {
		      if (!testonly) {
			XrdOucString capOpaqueFile="";
			XrdOucString mTimeString="";
			capOpaqueFile += "/?";
			capOpaqueFile += "&mgm.pcmd=commit";
			capOpaqueFile += "&mgm.size=";
			char filesize[1024]; sprintf(filesize,"%llu", rfmd->fMd.size);
			capOpaqueFile += filesize;
			capOpaqueFile += "&mgm.checksum=";
			capOpaqueFile += checksummer->GetHexChecksum();
			
			capOpaqueFile += "&mgm.mtime=";
			capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)rfmd->fMd.mtime);
			capOpaqueFile += "&mgm.mtime_ns=";
			capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)rfmd->fMd.mtime_ns);
			
			capOpaqueFile += "&mgm.add.fsid=";
			capOpaqueFile += (int)rfmd->fMd.fsid;
			
			capOpaqueFile += "&mgm.path=<UNDEF>";
			capOpaqueFile += "&mgm.fid=";
			XrdOucString hexfid;
			eos::common::FileId::Fid2Hex(rfmd->fMd.fid,hexfid);
			capOpaqueFile += hexfid;
			
			gManager->GetClientConn()->ClearLastServerError();
			gManager->GetClientConn()->SetOpTimeLimit(10);
			gManager->Query(kXR_Qopaquf,
					(kXR_char *) capOpaqueFile.c_str(),
					(kXR_char *) managerresult, managerresult_size);
			
			bool ok=false;
			
			if (!gManager->LastServerResp()) {
			  eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - manager is unavailable", rfmd->fMd.fid);
			} else {
			  switch (gManager->LastServerResp()->status) {
			  case kXR_ok:
			    eos_static_notice("commited meta data in central cache for fid=%08llx", rfmd->fMd.fid);
			    ok = true;
			    break;
			  case kXR_error:
			    eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - update failed", rfmd->fMd.fid);
			    ok = false;
			    break;
			    
			  default:
			    ok = true;
			    break;
			  }
			}
			if (!ok) {
			  failed_update_central++;		      
			}
			if (ok) {
			  repaired_cache_checksum++;
			}
		      } else {
			repaired_cache_checksum++;
		      }
		    }
		    delete checksummer;
		  }
		}
	      }
	    }
	    
	    if (rfmd->fMd.lid != fmd.lid) {
		error_diff_lid++;
	    }

	    if (rfmd->fMd.uid != fmd.uid) {
	      error_diff_uid++;
	    }

	    if (rfmd->fMd.gid != fmd.gid) {
	      error_diff_gid++;
	    }

	    if (rfmd->fMd.cid != fmd.cid) {
	      error_diff_cid++;
	    }

	    if ( labs(rfmd->fMd.ctime - fmd.ctime) > 1) {
	      error_diff_ctime++;
	    }

	    if (rfmd->fMd.ctime_ns != fmd.ctime_ns) {
	      error_diff_ctime_ns++;
	    }

	    if ( (rfmd->fMd.ctime != fmd.ctime) || (rfmd->fMd.ctime_ns != fmd.ctime_ns) ) {
	      //eos_static_notice("fid %08llx has ctime=%llu.%llu in cache but ctime=%llu.%llu in the changelog!", fmd.fid, fmd.ctime, fmd.ctime_ns,rfmd->fMd.ctime, rfmd->fMd.ctime_ns);
	    }

	    if (rfmd->fMd.mtime != fmd.mtime) {
	      error_diff_mtime_ns++;
	    }

	    if (strncmp(rfmd->fMd.name,fmd.name,255)) {
	      eos_static_info("fid %08llx has name=%s in cache but name=%s in the changelog!", fmd.fid, fmd.name, rfmd->fMd.name);
	      error_diff_name++;
	    }

	    if ( (rfmd->fMd.mtime != fmd.mtime) || (rfmd->fMd.mtime_ns != fmd.mtime_ns) ) {
	      //	      eos_static_notice("fid %08llx has mtime=%llu.%llu in cache but mtime=%llu.%llu in the changelog!", fmd.fid, fmd.mtime, fmd.mtime_ns,rfmd->fMd.mtime, rfmd->fMd.mtime_ns);
	    }

	    if (strncmp(rfmd->fMd.container,fmd.container,255)) {
	      eos_static_info("fid %08llx has contanier id cid=%llu in cache but cid=%llu in the changelog!", fmd.fid, fmd.cid, rfmd->fMd.cid);
	      error_diff_container++;
	    }

	    if (rfmd->fMd.size != fmd.size) {
	      eos_static_notice("fid %08llx has file size size=%llu in cache but size=%llu in the changelog!", fmd.fid, fmd.size, rfmd->fMd.size);
	      error_diff_size++;
	    }
	  }
	  lastlinebreak=linebreak;
	  linebreak++;
	} else {
	  error_parsing++;
	}
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
      fprintf(stdout,"=> files in central cache      : %llu\n", nfiles);
      fprintf(stdout,"---------------------------------------\n");
      fprintf(stdout,"=> parse error                 : %llu\n", error_parsing);
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
      fprintf(stdout,"=> files checksumming failed   : %llu\n", error_xsum_failed);
      fprintf(stdout,"=> files checksum type illegal : %llu\n", error_xsum_illegaltype);
      fprintf(stdout,"---------------------------------------\n");
      fprintf(stdout,"=> repaired local checksum     : %llu\n", repaired_local_checksum);
      fprintf(stdout,"=> repaired cache checksum     : %llu\n", repaired_cache_checksum);
      fprintf(stdout,"=> failed to update local MD   : %llu\n", failed_update_local);
      fprintf(stdout,"=> failed to update central MD : %llu\n", failed_update_central);

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
    {
      unsigned long long error_diff_cachemiss=0;
      unsigned long long files_not_uploaded_deleted=0;
      unsigned long long files_upload_ok=0;
      unsigned long long files_upload_failed=0;
      unsigned long long files_enoent=0;
      unsigned long long files_delete_local=0;
      unsigned long long files_drop_ok=0;
      unsigned long long files_drop_failed=0;

      // now dow the comparison in the opposite direction - compare local changelog to cache
      google::dense_hash_map<long long, unsigned long long>::iterator it;
      for (it = gFmd.FmdSize.begin(); it != gFmd.FmdSize.end(); ++it) {
	if (!fidsInCache.count(it->first)) {
	  error_diff_cachemiss++;
	  eos_static_notice("fid %08x is in the changelog but missing in central cache",it->first);

	  if (uploadfid == "*") {
	    eos::common::Fmd* fmd = gFmd.GetFmd(it->first, fsid, 0,0,0);
	    if (fmd) {
	      if (!testonly) {
		XrdOucString capOpaqueFile="";
		XrdOucString mTimeString="";
		capOpaqueFile += "/?";
		capOpaqueFile += "&mgm.pcmd=commit";
		capOpaqueFile += "&mgm.size=";
		char filesize[1024]; sprintf(filesize,"%llu", fmd->fMd.size);
		capOpaqueFile += filesize;
		//	capOpaqueFile += "&mgm.checksum=";
		//	capOpaqueFile += checksummer->GetHexChecksum();
		
		capOpaqueFile += "&mgm.mtime=";
		capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fmd->fMd.mtime);
		capOpaqueFile += "&mgm.mtime_ns=";
		capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fmd->fMd.mtime_ns);
		
		capOpaqueFile += "&mgm.add.fsid=";
		capOpaqueFile += (int)fmd->fMd.fsid;
		
		capOpaqueFile += "&mgm.path=<UNDEF>";
		capOpaqueFile += "&mgm.fid=";
		XrdOucString hexfid;
		eos::common::FileId::Fid2Hex(fmd->fMd.fid,hexfid);
		capOpaqueFile += hexfid;
		
		gManager->GetClientConn()->ClearLastServerError();
		gManager->GetClientConn()->SetOpTimeLimit(10);
		gManager->Query(kXR_Qopaquf,
				(kXR_char *) capOpaqueFile.c_str(),
				(kXR_char *) managerresult, managerresult_size);
		
		bool ok=false;
		bool isdeleted=false;
		bool nosuchfile=false;
		
		XrdOucString errtext = "";
		if (!gManager->LastServerResp()) {
		  eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - manager is unavailable", fmd->fMd.fid);
		} else {
		  switch (gManager->LastServerResp()->status) {
		  case kXR_ok:
		    eos_static_notice("commited meta data in central cache for fid=%08llx", fmd->fMd.fid);
		    ok = true;
		    break;
		  case kXR_error:
		    errtext = gManager->LastServerError()->errmsg;
		  if (errtext.find("file is already removed")!=STR_NPOS) {
		    isdeleted = true;
		  } else {
		    eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - update failed - %d", fmd->fMd.fid, gManager->LastServerError()->errnum);
		  }
		  if (errtext.find("No such file or directory")!=STR_NPOS) {
		    nosuchfile=true;
		  }
		  ok = false;
		  break;
		  
		  default:
		    ok = true;
		    break;
		  }
		}
	      
		if (!ok) {
		  if (isdeleted) {
		    eos_static_err("fid=%08llx is already deleted", fmd->fMd.fid);
		    files_not_uploaded_deleted++;
		    if (deletedeleted) {
		      // remove from changelog
		      gFmd.DeleteFmd(fmd->fMd.fid, fsid);
		      // unlink on disk
		      XrdOucString hexstring="";
		      XrdOucString fullpath="";
		      eos::common::FileId::Fid2Hex(fmd->fMd.fid, hexstring);
		      eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), searchpath.c_str(), fullpath);
		      eos_static_crit("unlinking %s", fullpath.c_str());
		      if ((!testonly) && (unlink(fullpath.c_str()))) {eos_static_err("failed to unlink file %s", fullpath.c_str());}
		      
		      // drop this replica in the cache
		      XrdOucString capOpaqueFile="";
		      capOpaqueFile += "/?";
		      capOpaqueFile += "&mgm.pcmd=drop";
		      capOpaqueFile += "&mgm.fsid=";
		      capOpaqueFile += (int)fmd->fMd.fsid;
		      capOpaqueFile += "&mgm.fid=";
		      XrdOucString hexfid;
		      eos::common::FileId::Fid2Hex(fmd->fMd.fid,hexfid);
		      capOpaqueFile += hexfid;
		      
		      gManager->GetClientConn()->ClearLastServerError();
		      gManager->GetClientConn()->SetOpTimeLimit(10);
		      gManager->Query(kXR_Qopaquf,
				      (kXR_char *) capOpaqueFile.c_str(),
				      (kXR_char *) managerresult, managerresult_size);
		      
		      bool ok=false;
		      
		      if (!gManager->LastServerResp()) {
			eos_static_err("unable to drop replica fid=%08llx - manager is unavailable", fmd->fMd.fid);
		      } else {
			switch (gManager->LastServerResp()->status) {
			case kXR_ok:
			  eos_static_notice("dropped replica in central cache for fid=%08llx fsid=%d", fmd->fMd.fid,fsid);
			  ok = true;
			  break;
			case kXR_error:
			  eos_static_err("unable to drop replica in meta data cache for fid=%08llx - drop failed - %d", fmd->fMd.fid, gManager->LastServerError()->errnum);
			  ok = false;
			  break;
			  
			default:
			  ok = true;
			  break;
			}
		      }
		      if (!ok) {
			eos_static_err("unable to drop replica for fid=%08llx", fmd->fMd.fid);
			rc = 8;
			files_drop_failed++;
		      }
		      if (ok) {
			rc = 0;
			eos_static_info("dropped replica of fid=%08llx fsid=%d", fmd->fMd.fid, fsid);
			files_drop_ok++;
		      }
		    }
		  } else {
		    if (nosuchfile) {
		      files_enoent++;
		      
		      if (deleteenoent) {
			// remove from changelog
			gFmd.DeleteFmd(fmd->fMd.fid, fsid);
			// unlink on disk
			XrdOucString hexstring="";
			XrdOucString fullpath="";
			eos::common::FileId::Fid2Hex(fmd->fMd.fid, hexstring);
			eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), searchpath.c_str(), fullpath);
			eos_static_crit("unlinking %s", fullpath.c_str());
			if ((!testonly) && (unlink(fullpath.c_str()))) {eos_static_err("failed to unlink file %s", fullpath.c_str());}
			files_delete_local++;
		      }
		    } else {
		      eos_static_err("unable to update file meta data of fid=%08llx", fmd->fMd.fid);
		      files_upload_failed++;
		    }
		  }
		  rc = 8;
		}
		if (ok) {
		  rc = 0;
		  eos_static_info("updated file meta data of fid=%08llx", fmd->fMd.fid);
		  files_upload_ok++;
		}
	      }
	    } else {
	      fprintf(stdout,"error: fid %s is not known !", uploadfid.c_str());
	      rc = 8;
	    }
	  }
	}
      }
      fprintf(stdout,"---------------------------------------\n");
      fprintf(stdout,"=> files missing in cache        %llu\n", error_diff_cachemiss);
      fprintf(stdout,"=> files MD upload ok            %llu\n", files_upload_ok);
      fprintf(stdout,"=> files MD upload failed        %llu\n", files_upload_failed);
      fprintf(stdout,"=> files already unlinked        %llu\n", files_not_uploaded_deleted);
      fprintf(stdout,"=> files already removed         %llu\n", files_enoent);
      fprintf(stdout,"=> files locally deleted         %llu\n", files_delete_local);
      fprintf(stdout,"=> replica drop ok               %llu\n", files_drop_ok);
      fprintf(stdout,"=> replica drop failed           %llu\n", files_drop_failed);
      fprintf(stdout,"---------------------------------------\n");   
    }

    // this piece of code uploads the meta data of an explicit given hex fid
    if (uploadfid.length() && (uploadfid!="*")) {
      unsigned long long ufid = strtoull(uploadfid.c_str(),0,16);
      eos::common::Fmd* fmd = gFmd.GetFmd(ufid, fsid, 0,0,0);
      if (fmd) {
	if (!testonly) {
	  XrdOucString capOpaqueFile="";
	  XrdOucString mTimeString="";
	  capOpaqueFile += "/?";
	  capOpaqueFile += "&mgm.pcmd=commit";
	  capOpaqueFile += "&mgm.size=";
	  char filesize[1024]; sprintf(filesize,"%llu", fmd->fMd.size);
	  capOpaqueFile += filesize;
	  //	capOpaqueFile += "&mgm.checksum=";
	  //	capOpaqueFile += checksummer->GetHexChecksum();
	  
	  capOpaqueFile += "&mgm.mtime=";
	  capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fmd->fMd.mtime);
	  capOpaqueFile += "&mgm.mtime_ns=";
	  capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fmd->fMd.mtime_ns);
	  
	  capOpaqueFile += "&mgm.add.fsid=";
	  capOpaqueFile += (int)fmd->fMd.fsid;
	  
	  capOpaqueFile += "&mgm.path=<UNDEF>";
	  capOpaqueFile += "&mgm.fid=";
	  XrdOucString hexfid;
	  eos::common::FileId::Fid2Hex(fmd->fMd.fid,hexfid);
	  capOpaqueFile += hexfid;
	  
	  gManager->GetClientConn()->ClearLastServerError();
	  gManager->GetClientConn()->SetOpTimeLimit(10);
	  gManager->Query(kXR_Qopaquf,
			  (kXR_char *) capOpaqueFile.c_str(),
			  (kXR_char *) managerresult, managerresult_size);
	  
	  bool ok=false;
	  
	  if (!gManager->LastServerResp()) {
	    eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - manager is unavailable", fmd->fMd.fid);
	  } else {
	    switch (gManager->LastServerResp()->status) {
	    case kXR_ok:
	      eos_static_notice("commited meta data in central cache for fid=%08llx", fmd->fMd.fid);
	      ok = true;
	      break;
	    case kXR_error:
	      eos_static_err("unable to commit meta data update to meta data cache for fid=%08llx - update failed - %d", fmd->fMd.fid, gManager->LastServerError()->errnum);
	      ok = false;
	      break;
	      
	    default:
	      ok = true;
	      break;
	    }
	  }
	  if (!ok) {
	    eos_static_err("unable to update file meta data of fid=%08llx", fmd->fMd.fid);
	    rc = 8;
	  }
	  if (ok) {
	    rc = 0;
	    eos_static_info("updated file meta data of fid=%08llx", fmd->fMd.fid);
	  }
	}
      } else {
	fprintf(stdout,"error: fid %s is not known !", uploadfid.c_str());
	rc = 8;
      }
    }
  } 
  
  if (testonly) {
    fprintf(stdout,"=> TESTMODE ( no modifications done )\n");
  }
  return rc;
}
