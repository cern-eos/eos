/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
struct fidpair {
  unsigned long long fid;
  unsigned long long size;
  unsigned int  nrep;
};

struct fsinfo {
  unsigned long long usedbytes;
  unsigned long long freebytes;
  std::multimap <unsigned long long, struct fidpair*> files;
  std::string space;
  unsigned long group;
};

/*----------------------------------------------------------------------------*/

/* this are objects used by the 'flatten' function                     */
/* space => subgroup => fsid => fsinfo                                        */
std::map< std::string, google::sparse_hash_map <unsigned long , google::sparse_hash_map < unsigned long , struct fsinfo* > > > fshash;
/* space => subgroup := free bytes */
std::map< std::string, google::sparse_hash_map <unsigned long , unsigned long long > >  groupfree;


/* fsid => fsinfo                                                             */
google::sparse_hash_map< unsigned long, struct fsinfo*> fsptr;

/* fsid => space */
google::sparse_hash_map< unsigned long, std::string> spaceptr;

/* fsid => group */
google::sparse_hash_map< unsigned long, unsigned long> groupptr;


google::sparse_hash_set<unsigned long long> movelist;

/* fid => fsid */
std::multimap<unsigned long long , unsigned long> fidptr;


bool load_fs_ls () {
  // load the present fs ls configuration into a hash
  XrdOucString subcmd="ls -s ";
  com_fs((char*)subcmd.c_str());
  
  std::vector<std::string> files_found;
  files_found.clear();
  command_result_stdout_to_vector(files_found);
  std::vector<std::string>::const_iterator it;
  
  fshash.clear();
  fsptr.clear();
  spaceptr.clear();
  groupptr.clear();
  
  if (!files_found.size()) {
    output_result(CommandEnv);
  } else {    
    if (CommandEnv) {
      delete CommandEnv; CommandEnv = 0;
    }
    
    for (unsigned int i=0; i< files_found.size(); i++) {
      if (!files_found[i].length())
	continue;
      XrdOucString line = files_found[i].c_str();
      if (line.beginswith("/eos/") && 
	  ( (line.find("offline")==STR_NPOS) ) &&
	  ( (line.find("online")== STR_NPOS) )) {
	XrdOucTokenizer subtokenizer((char*)line.c_str());
	subtokenizer.GetLine();
	XrdOucString queue = subtokenizer.GetToken();
	XrdOucString sfsid  = subtokenizer.GetToken();
	XrdOucString path  = subtokenizer.GetToken();
	XrdOucString schedgroup = subtokenizer.GetToken();
	int sep = schedgroup.find(".");
	XrdOucString space = schedgroup; space.erase(sep);
	XrdOucString subgroup = schedgroup; subgroup.erase(0,sep+1);
	XrdOucString bootstat = subtokenizer.GetToken();
	XrdOucString boottime = subtokenizer.GetToken();
	XrdOucString configstat = subtokenizer.GetToken();
	XrdOucString blocks =subtokenizer.GetToken();
	XrdOucString blocksunit = subtokenizer.GetToken();
	XrdOucString freeblocks = subtokenizer.GetToken();
	XrdOucString freeblocksunit = subtokenizer.GetToken();

	unsigned long fsid   = strtoul(sfsid.c_str(),0,10);
	unsigned long sgroup = strtoul(subgroup.c_str(),0,10);
	float fblocks = strtof(freeblocks.c_str(),NULL);
	if (blocksunit == "KB") fblocks *= 1000;
	if (blocksunit == "MB") fblocks *= 1000000;
	if (blocksunit == "GB") fblocks *= 1000000000;
	if (blocksunit == "TB") fblocks *= 1000000000000;

	//struct fsinfo {
	//  unsigned long long usedbytes;
	//  unsigned long long freebytes;
	//  std::list <unsigned long long> fids;
	// };
	
	// google::sparse_hash_map< std::string, google::sparse_hash_map <unsigned long long, struct fsinfo > > fshash;
	std::string sspace = space.c_str();

	fshash[sspace][sgroup][fsid] = new struct fsinfo;
	fshash[sspace][sgroup][fsid]->usedbytes = 0;
	fshash[sspace][sgroup][fsid]->freebytes = (unsigned long long) fblocks;
	fshash[sspace][sgroup][fsid]->space = sspace;
	fshash[sspace][sgroup][fsid]->group = sgroup;
	// set a pointer
	fsptr[fsid] = fshash[sspace][sgroup][fsid];

	spaceptr[fsid] = sspace;
	groupptr[fsid] = sgroup;
	groupfree[sspace][sgroup] += (unsigned long long) fblocks;

	//	printf("%s %s %s %lu %lu free=%f\n", sfsid.c_str(), space.c_str(), subgroup.c_str(), fsid, sgroup, fblocks);
	//	printf("%lu %llu free %llu %llu\n", fsid, (unsigned long long) &fsptr[fsid],(unsigned long long) &fsptr[6], (unsigned long long) fsptr[fsid]->freebytes);
      }
    }
  }
  return true;
}


/* Filesystem listing, configuration, manipulation */
int
com_fs (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=ls";
    XrdOucString silent = subtokenizer.GetToken();

    XrdOucEnv* result = client_admin_command(in);
    if (silent != "-s") {
      global_retc = output_result(result);
    } else {
      if (result) {
	global_retc = 0;
      } else {
	global_retc = EINVAL;
      }
    }
    return (0);
  }

  if ( subcommand == "set" ) {
    XrdOucString fsname = subtokenizer.GetToken();
    XrdOucString fsid   = subtokenizer.GetToken();
    if (fsname.length() && fsid.length()) {
      XrdOucString in = "mgm.cmd=fs&mgm.subcmd=set&mgm.fsid=";
      in += fsid;
      in += "&mgm.fsname=";
      in += fsname;
      XrdOucString arg = subtokenizer.GetToken();
      
      do {
	if (arg == "-sched") {
	  XrdOucString sched = subtokenizer.GetToken();
	  if (!sched.length()) 
	    goto com_fs_usage;
	  
	  in += "&mgm.fsschedgroup=";
	  in += sched;
	  arg = subtokenizer.GetToken();
	} else {
	  if (arg == "-force") {
	    in += "mgm.fsforce=1";
	  }
	  arg = subtokenizer.GetToken();
	} 
      } while (arg.length());

      global_retc = output_result(client_admin_command(in));
      // boot by fsid
      return (0);
    }
  }

  if ( subcommand == "rm" ) {
    XrdOucString arg = subtokenizer.GetToken();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=rm";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fsid=";
    } else {
      if (arg.endswith("/fst"))
	in += "&mgm.nodename=";
      else 
	in += "&mgm.fsname=";
    }

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
    return (0);
  }

  if ( subcommand == "boot" ) {
    XrdOucString arg = subtokenizer.GetToken();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=boot";
    if (!arg.length()) 
      goto com_fs_usage;
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fsid=";
    } else {
      in += "&mgm.nodename=";
    }

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "config" ) {
    XrdOucString arg;
    arg = subtokenizer.GetToken();
    XrdOucString sched;
    sched ="";
    if (!arg.length())
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=config";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // config by fsid
      in += "&mgm.fsid=";
    } else {
      if (arg.endswith("/fst"))
	in += "&mgm.nodename=";
      else 
	in += "&mgm.fsname=";
    }
    
    in += arg;

    arg = subtokenizer.GetToken();

    if (arg == "-sched") {
      sched = subtokenizer.GetToken();
      arg                = subtokenizer.GetToken();
      if (!sched.length() || !arg.length()) 
	goto com_fs_usage;
    }
    
    sched = subtokenizer.GetToken();
    if (sched == "-sched") {
      sched = subtokenizer.GetToken();
      if (!sched.length())
	goto com_fs_usage;
    }
    
    if (sched.length()) {
      in += "&mgm.fsschedgroup=";
      in += sched;
    }

    if (!arg.length())     
      goto com_fs_usage;

    in += "&mgm.fsconfig=";
    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "clone" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if ( (!sourceid.length()) || (!targetid.length()))
      goto com_fs_usage;

    XrdOucString subcmd="dumpmd -s "; subcmd += sourceid; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
	delete CommandEnv; CommandEnv = 0;
      }

      for (unsigned int i=0; i< files_found.size(); i++) {
	if (!files_found[i].length())
	  continue;
	XrdOucString line = files_found[i].c_str();
	if (line.beginswith("path=")) {
	  line.replace("path=","");
	  fprintf(stdout,"%06d: %s\n", i, line.c_str());
	  // call the replication command here
	  subcmd = "replicate "; subcmd += line; subcmd += " ";subcmd += sourceid; subcmd += " "; subcmd += targetid;
	  com_file( (char*) subcmd.c_str());
	}
      }
    }
    
    return (0);
  }

  if ( subcommand == "compare" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if (!sourceid.length() || !targetid.length()) 
      goto com_fs_usage;

    XrdOucString subcmd1="dumpmd -s "; subcmd1 += sourceid; subcmd1 += " -path";

    com_fs((char*)subcmd1.c_str());

    std::vector<std::string> files_found1;
    std::vector<std::string> files_found2;
    std::vector<std::string> files_miss1;

    files_found1.clear();
    files_found2.clear();
    files_miss1.clear();

    command_result_stdout_to_vector(files_found1);

    if (CommandEnv) {
      delete CommandEnv; CommandEnv = 0;
    }
    

    XrdOucString subcmd2="dumpmd -s "; subcmd2 += targetid; subcmd2 += " -path";

    com_fs((char*)subcmd2.c_str());

    command_result_stdout_to_vector(files_found2);

    if ( (!files_found1.size()) && (!files_found2.size())) {
      output_result(CommandEnv);
    }

    if (CommandEnv) {
      delete CommandEnv; CommandEnv = 0;
    }

    for (unsigned int i = 0 ; i < files_found1.size(); i++) {
      bool found=false;
      std::vector<std::string>::iterator it;
      for (it = files_found2.begin(); it != files_found2.end(); ++it) {
	if (files_found1[i] == *it) {
	  files_found2.erase(it);
	  found = true;
	  break;
	}
      }
      if (!found) {
	files_miss1.push_back(files_found1[i]);
      }
    }
    // files_miss1 contains the missing files in 2
    // files_found2 contains the missing files in 1
      
    for (unsigned int i=0; i< files_miss1.size(); i++) {
      if (files_miss1[i].length())
	fprintf(stderr,"error: %s => found in %s - missing in %s\n", files_miss1[i].c_str(), sourceid.c_str(), targetid.c_str());
    }
    
    for (unsigned int i=0; i< files_found2.size(); i++) {
      if (files_found2[i].length())
	fprintf(stderr,"error: %s => found in %s - missing in %s\n", files_found2[i].c_str(), targetid.c_str(), sourceid.c_str());
    }
    
    return (0);
  }

  if ( subcommand == "dropfiles" ) {
    XrdOucString id;
    XrdOucString option;
    id = subtokenizer.GetToken();    
    option = subtokenizer.GetToken();

    if (!id.length()) 
      goto com_fs_usage;

    if (option.length() && (option != "-f")) {
      goto com_fs_usage;
    }

    XrdOucString subcmd="dumpmd -s "; subcmd += id; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
	delete CommandEnv; CommandEnv = 0;
      }

      string s;
      printf("Do you really want to delete ALL %u replica's from filesystem %s ?\n" , (int)files_found.size(), id.c_str());
      printf("Confirm the deletion by typing => ");
      XrdOucString confirmation="";
      for (int i=0; i<10; i++) {
	confirmation += (int) (9.0 * rand()/RAND_MAX);
      }
      printf("%s\n", confirmation.c_str());
      printf("                               => ");
      getline( std::cin, s );
      std::string sconfirmation = confirmation.c_str();
      if ( s == sconfirmation) {
	printf("\nDeletion confirmed\n");
	for (unsigned int i=0; i< files_found.size(); i++) {
	  if (!files_found[i].length())
	    continue;
	  XrdOucString line = files_found[i].c_str();
	  if (line.beginswith("path=")) {
	    line.replace("path=","");
	    fprintf(stdout,"%06d: %s\n", i, line.c_str());
	    // call the replication command here
	    subcmd = "drop "; subcmd += line; subcmd += " ";subcmd += id; 
	    if (option.length()) { 
	      subcmd += " "; subcmd += option; 
	    }
	    com_file( (char*) subcmd.c_str());
	  }
	}
	printf("=> Deleted %u replicas from filesystem %s\n", (unsigned int) files_found.size(), id.c_str());
      } else {
	printf("\nDeletion aborted!\n");
      }
    }

   

    return (0);
  }
   
  if ( subcommand == "verify" ) {
    XrdOucString id;
    XrdOucString option="";
    id = subtokenizer.GetToken();
    XrdOucString options[5];
    
    options[0] = subtokenizer.GetToken();
    options[1] = subtokenizer.GetToken();
    options[2] = subtokenizer.GetToken();
    options[3] = subtokenizer.GetToken();
    options[4] = subtokenizer.GetToken();

    if (!id.length()) 
      goto com_fs_usage;

    for (int i=0; i< 5; i++) {
      if (options[i].length() && 
	  ( options[i] != "-checksum") && ( options[i] != "-commitchecksum") && (options[i] != "-commitsize") && (options[i] != "-rate")) {
	goto com_fs_usage;
      }
      option += options[i]; option += " ";
      if (options[i] == "-rate") {
	option += options[i+1]; option += " ";
	i++;
      }
    }

    XrdOucString subcmd="dumpmd -s "; subcmd += id; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
	delete CommandEnv; CommandEnv = 0;
      }

      for (unsigned int i=0; i< files_found.size(); i++) {
	if (!files_found[i].length())
	  continue;
	XrdOucString line = files_found[i].c_str();
	if (line.beginswith("path=")) {
	  line.replace("path=","");
	  fprintf(stdout,"%06d: %s\n", i, line.c_str());
	  // call the replication command here
	  subcmd = "verify "; subcmd += line; subcmd += " "; subcmd += id; subcmd += " ";
	  if (option.length()) { 
	    subcmd += option; 
	  }
	  com_file( (char*) subcmd.c_str());
	}
      }
    }
    return (0);
  }

  if ( subcommand == "heal" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetspace;
    targetspace = subtokenizer.GetToken();

    if (!sourceid.length())
      goto com_fs_usage;
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();

    // check for heal by path
    if (sourceid.beginswith("/")) {
      // heal by path
      XrdOucString subcmd="-s -f "; subcmd += sourceid;
      com_find((char*)subcmd.c_str());
    } else {
      // heal by fsid
      XrdOucString subcmd="dumpmd -s "; subcmd += sourceid; subcmd += " -path";

      com_fs((char*)subcmd.c_str());
    }

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
	delete CommandEnv; CommandEnv = 0;
      }

      for (unsigned int i=0; i< files_found.size(); i++) {
	if (!files_found[i].length())
	  continue;
	XrdOucString line = files_found[i].c_str();
	if (line.beginswith("path=")) {
	  line.replace("path=","");
	}
	
	fprintf(stdout,"%06d: %s\n", i, line.c_str());
	// call the heal command here
	XrdOucString subcmd = "adjustreplica "; subcmd += line; subcmd += " "; subcmd += " "; 
	if (targetspace.length())  { subcmd += targetspace; }
	if (targetid.length())     { subcmd += " "; subcmd += targetid; }
	com_file( (char*) subcmd.c_str());
      }
    }
    return (0);
  } 


  if ( subcommand == "flatten" ) {
    XrdOucString querypath = subtokenizer.GetToken();
    XrdOucString space     = subtokenizer.GetToken();
    XrdOucString subgroup  = subtokenizer.GetToken();

    /* space => total bytes */
    std::map < std::string, unsigned long long > spaceusage;

    /* sched group => total bytes */
    std::map < std::string, google::sparse_hash_map <unsigned long, unsigned long long> > groupusage;
    std::map < std::string, google::sparse_hash_map <unsigned long, unsigned long long> > groupfree;
    

    // create an inmemory image of the current fs states.
    load_fs_ls ();

    
    // fids already moved in flatten
    google::sparse_hash_set<unsigned long long> fidmoved;
    

    if (!querypath.length())
      goto com_fs_usage;

    fprintf(stdout,"==> getting file information under path %s ...\n", querypath.c_str());
    XrdOucString subfind = " -s -f --fid --fs --ctime --size ";
    subfind+=querypath;
    com_find((char*)subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;

    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {    
      if (CommandEnv) {
	delete CommandEnv; CommandEnv = 0;
      }
    }

    fprintf(stdout,"==> found %u files under path %s ...\n",(unsigned int) files_found.size(), querypath.c_str());

    unsigned long long zeroentries=0;
    unsigned long long unassignedfsentries=0;

    for (unsigned int i=0; i< files_found.size(); i++) {
      if (files_found[i].length()) {
	char parser[4096];
	sprintf(parser,"%s",files_found[i].c_str());
	XrdOucTokenizer subtokenizer(parser);
	subtokenizer.GetLine();
	const char* val=0;
	unsigned long long size=0;
	unsigned long long ctime=0;
	unsigned long long fid=0;
	std::vector<int> locations;
	
	if (!(i%10000)) {
	  fprintf(stdout,"..  file md %u/%u ...\n", i, (unsigned int)files_found.size());
	}

	while ( ( val = subtokenizer.GetToken()) ) {
	  XrdOucString sval = val;
	  if (sval.beginswith("size=")) {
	    sval.erase(0,5);
	    size = strtoull(sval.c_str(),0,10);
	  }

	  if (sval.beginswith("fid=")) {
	    sval.erase(0,4);
	    fid = strtoull(sval.c_str(),0,10);
	  }


	  if (sval.beginswith("fsid=")) {
	    sval.erase(0,5);
	    XrdOucString fslist = sval;
	    int cpos=0;
	    int cposstart=0;
	    do {
	      XrdOucString fsl;
	      cpos = sval.find(",",cpos);
	      if (cpos != STR_NPOS) {
		fsl.assign(fslist,cposstart,cpos-1);
		int ifs = atoi(fsl.c_str());
		locations.push_back(ifs);
		cpos++;
		cposstart = cpos;
	      } else {
		fsl.assign(fslist,cposstart);
		int ifs = atoi(fsl.c_str());
		locations.push_back(ifs);
		break;
	      }
	    } while (1);
	  }

	  if (sval.beginswith("ctime=")) {
	    sval.erase(0,6);
	    ctime = strtoull(sval.c_str(),0,10);
	  }

	}
	// space => subgroup => fsid => fsinfo
	google::sparse_hash_map< std::string, google::sparse_hash_map <unsigned long , google::sparse_hash_map < unsigned long , struct fsinfo > > > fshash;

	std::vector<int>::const_iterator it;

	if (locations.size()) {
	  for ( it = locations.begin(); it != locations.end(); it++) {
	    if (*it) {
	      if (fsptr.count(*it)) {
		fsptr[*it]->usedbytes  += size;
		struct fidpair* fp = new struct fidpair;
		fp->fid  = fid;
		fp->size = size;
		fp->nrep = locations.size();
		fsptr[*it]->files.insert(pair<unsigned long long, struct fidpair*>(ctime, fp));
		spaceptr[*it];
		groupptr[*it];
		groupusage[(spaceptr[*it])][(groupptr[*it])] += size;
		spaceusage[spaceptr[*it]] += size;
		fidptr.insert(pair<unsigned long long, unsigned long> (fid,*it));
	      }
	    } else {
	      unassignedfsentries++;
	    }
	  } 
	} else {
	  zeroentries++;
	}
      }
    }
    fprintf(stdout,"==> loaded %u entries - (zero-location=%llu , unassigned-fs=%llu)\n", (unsigned int) files_found.size(), zeroentries, unassignedfsentries);
    
    XrdOucString sizestring1;
    XrdOucString sizestring2;
    XrdOucString sizestring3;

    std::map < std::string, unsigned long long>::const_iterator sit;

    std::map < std::string, unsigned long> fsperspace;

    // compute average usage and standard deviation over groups
    std::map <std::string, unsigned long long> groupavg;
    std::map <std::string, unsigned long long> groupstddev;
    
    // compute average usage and standard deviation over fs in a one group
    std::map <std::string, google::sparse_hash_map <unsigned long , unsigned long long> > fsavg;
    std::map <std::string, google::sparse_hash_map <unsigned long , unsigned long long> > fsstddev;

    for (sit = spaceusage.begin(); sit != spaceusage.end(); sit++) {
      google::sparse_hash_map < unsigned long, unsigned long long>::const_iterator git;
      for (git = groupusage[sit->first].begin(); git != groupusage[sit->first].end(); git++) {
	groupavg[sit->first] += git->second;

	google::sparse_hash_map < unsigned long, struct fsinfo*> ::const_iterator fit;

	for (fit = fshash[sit->first][git->first].begin(); fit != fshash[sit->first][git->first].end(); fit++) {
	  fsavg[sit->first][git->first] += fit->second->usedbytes;
	  groupusage[sit->first][git->first] += fit->second->freebytes;
	}
	fsavg[sit->first][git->first] /= fshash[sit->first][git->first].size();
      }
      groupavg[sit->first] /= groupusage[sit->first].size();
    }

    
    for (sit = spaceusage.begin(); sit != spaceusage.end(); sit++) {
      google::sparse_hash_map < unsigned long, unsigned long long>::const_iterator git;
      for (git = groupusage[sit->first].begin(); git != groupusage[sit->first].end(); git++) {
	groupstddev[sit->first] += (git->second - groupavg[sit->first] ) * (git->second - groupavg[sit->first]);

	google::sparse_hash_map < unsigned long, struct fsinfo*> ::const_iterator fit;

	fsperspace[sit->first] += fshash[sit->first][git->first].size();

	for (fit = fshash[sit->first][git->first].begin(); fit != fshash[sit->first][git->first].end(); fit++) {
	  fsstddev[sit->first][git->first] += (fit->second->usedbytes - fsavg[sit->first][git->first]) * (fit->second->usedbytes - fsavg[sit->first][git->first]);
	}
	fsstddev[sit->first][git->first] /= fshash[sit->first][git->first].size();
	fsstddev[sit->first][git->first] = (unsigned long long) sqrt((double) 	fsstddev[sit->first][git->first]);
      }
      groupstddev[sit->first] /= groupusage[sit->first].size();
      groupstddev[sit->first] = (unsigned long long)sqrt((double)groupstddev[sit->first]);
    }

    
    
    for (sit = spaceusage.begin(); sit != spaceusage.end(); sit++) {
      fprintf(stdout,"::> space=%16s \t         \t bytes=%llu \t volume=%10s \t avg-grp-volume=%10s +- %10s\n", sit->first.c_str(), sit->second, XrdCommonFileSystem::GetReadableSizeString(sizestring1,sit->second,"B"), XrdCommonFileSystem::GetReadableSizeString(sizestring2,groupavg[sit->first],"B"), XrdCommonFileSystem::GetReadableSizeString(sizestring3,groupstddev[sit->first],"B"));
      fprintf(stdout,"# --------------------------------------------------------------------------------------------------------------------------------------\n");
      google::sparse_hash_map < unsigned long, unsigned long long>::const_iterator git;
      for (git = groupusage[sit->first].begin(); git != groupusage[sit->first].end(); git++) {
	fprintf(stdout,"::> space=%16s \t group=%lu \t bytes=%llu \t volume=%10s \t  avg-fs-volume=%10s +- %10s\n", sit->first.c_str(), git->first, git->second, XrdCommonFileSystem::GetReadableSizeString(sizestring1,git->second,"B"), XrdCommonFileSystem::GetReadableSizeString(sizestring2, fsavg[sit->first][git->first],"B"), XrdCommonFileSystem::GetReadableSizeString(sizestring3, fsstddev[sit->first][git->first],"B"));
      }
      fprintf(stdout,"# --------------------------------------------------------------------------------------------------------------------------------------\n");
    }

    if (space.length()) {
      if (subgroup.length()) {
	fprintf(stdout,"==> restricting balancing to space %s group %s\n", space.c_str(), subgroup.c_str());
      } else {
	fprintf(stdout,"==> restricting balancing to space %s\n", space.c_str());
      }
    } else {
      fprintf(stdout,"==> balancing all spaces\n");
    }

    unsigned long isubgroup=0;
    if (subgroup.c_str()) isubgroup = strtoul(subgroup.c_str(),0,10);

    for (sit = spaceusage.begin(); sit != spaceusage.end(); sit++) {
      XrdOucString sspace = sit->first.c_str();
      if ( (!space.length()) || (space == sspace) ) {
	unsigned long long desiredfsusage = sit->second/fsperspace[sit->first];
	if (subgroup.length()) {
	  desiredfsusage = groupusage[sit->first][isubgroup] / fshash[sit->first][isubgroup].size() ;
	}

	fprintf(stdout,"==> space=%16s := optimizing towards %s per file system\n",sit->first.c_str(), XrdCommonFileSystem::GetReadableSizeString(sizestring1, desiredfsusage,"B"));
	

	// create a list of target groups
	std::vector<unsigned long> targetgroup;

	google::sparse_hash_map < unsigned long, unsigned long long>::const_iterator git;
	// loop over all filesystems in a space
	for (git = groupusage[sit->first].begin(); git != groupusage[sit->first].end(); git++) {
	  targetgroup.push_back(git->first);
	}	  

	unsigned long currentgroup=0;
	unsigned long currentfs=0;

	if (!subgroup.length()) {
	  google::sparse_hash_map < unsigned long, unsigned long long>::const_iterator git;
	  // loop over all filesystems in a space
	  for (git = groupusage[sit->first].begin(); git != groupusage[sit->first].end(); git++) {
	    google::sparse_hash_map < unsigned long, struct fsinfo*> ::const_iterator fit;
	    // classify group as a receiver or donator group
	    bool issource;
	    if (git->second > groupavg[sit->first]) {
	      issource = true;
	    } else {
	      issource = false;
	    }

	    for (fit = fshash[sit->first][git->first].begin(); fit != fshash[sit->first][git->first].end(); fit++) {
	      // fit->first is the fsid
	      long long bytediff = desiredfsusage - fit->second->usedbytes;
	      XrdOucString sign ="+";
	      if (bytediff <0)
		sign ="-";

	      fprintf(stdout,"==> fs %lu needs correction of %s%s\n",  fit->first,  sign.c_str(), XrdCommonFileSystem::GetReadableSizeString(sizestring1, llabs(bytediff),"B"));

	      // move files away
	      std::multimap <unsigned long long, struct fidpair*>::const_iterator fileit;
	      
	      if (issource) {
		// loop over all files
		for (fileit = fit->second->files.begin(); fileit != fit->second->files.end(); fileit++) {
		  // check if this fid was already moved
		  if (movelist.count(fileit->second->fid)) {
		    fprintf(stdout,"file %llu already moved\n", fileit->second->fid);
		    continue;
		  }
		  
		  // take one  file ID and count all the copies

		  pair<std::multimap<unsigned long long, unsigned long>::iterator, std::multimap<unsigned long long, unsigned long>::iterator> findit = fidptr.equal_range(fileit->second->fid);

		  std::multimap<unsigned long long, unsigned long>::const_iterator findresult;

		  int nrep=0;
		  
		  unsigned long long transfersize = fileit->second->nrep*fileit->second->size;

		  
		  while ( (groupusage[sit->first][targetgroup[currentgroup]] > groupavg[sit->first] ) || ( ((long long)groupfree[sit->first][targetgroup[currentgroup]]) - (100ll*1024*1024*1024) ) < (long long)(transfersize)) {
		    fprintf(stderr,"Condition: %ld %llu %llu %llu %llu\n", currentgroup, (groupusage[sit->first][targetgroup[currentgroup]]), groupavg[sit->first],((long long)groupfree[sit->first][targetgroup[currentgroup]]),  (long long)(transfersize));
		    currentgroup++;
		    if (currentgroup > targetgroup.size()) {
		      fprintf(stderr,"*** Fatal: there is no space left in any group to place this file");
		      exit(-1);
		    }
		  }

		  for (findresult = findit.first; findresult != findit.second; findresult++) {
		    nrep++;
		    fprintf(stderr,"replica for %llu on %lu\n", fileit->second->fid, findresult->second);
		    // substract from the source here
		    //		    fsptr[findresult->second]
		  }
		  
		  
		  currentfs = currentfs;
		  fprintf(stdout,"==> moving fsid=%lu fid=%llu size=%llu into group %lu\n", fit->first, fileit->second->fid, fileit->second->size,  targetgroup[currentgroup]);
		  
		  groupfree[sit->first][targetgroup[currentgroup]]  -= transfersize;
		  groupusage[sit->first][targetgroup[currentgroup]] += transfersize;
		  fprintf(stdout," %llu / %llu => %llu %lu\n",  groupfree[sit->first][targetgroup[currentgroup]], groupusage[sit->first][targetgroup[currentgroup]] ,groupavg[sit->first], currentgroup);
		  movelist.insert(fileit->second->fid);
		  
		  // add the loop termination condition once we have migrated enough away from this group
		}
	      } else {
		fprintf(stdout,"==> skipping fsid=%lu (target) \n", fit->first);
	      }
	    }
	  }
	}
	
      }
    }
    
    return (0);
  } 

  if ( subcommand == "dumpmd" ) {
    bool silentcommand=false;

    XrdOucString arg = subtokenizer.GetToken();
    if (arg == "-s") {
      silentcommand=true;
      arg = subtokenizer.GetToken();
    }


    XrdOucString option1 = subtokenizer.GetToken();
    XrdOucString option2 = subtokenizer.GetToken();
    XrdOucString option3 = subtokenizer.GetToken();

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dumpmd";
    if (!arg.length()) 
      goto com_fs_usage;
    
    int fsid = atoi(arg.c_str());
    in += "&mgm.fsid=";
    in += (int) fsid;
    
    if ( (option1 == "-path") || (option2 == "-path") || (option3 == "-path") ) {
      in += "&mgm.dumpmd.path=1";
    } 
      
    if ( (option1 == "-fid") || (option2 == "-fid") || (option3 == "-fid") ) {
      in += "&mgm.dumpmd.fid=1";
    } 

    if ( (option1 == "-size") || (option2 == "-size") || (option3 == "-size") ) {
      in += "&mgm.dumpmd.size=1";
    } 
    
    if ( option1.length() && (option1 != "-path") && (option1 != "-fid") && (option1 != "-size")) 
      goto com_fs_usage;
    
    if ( option2.length() && (option2 != "-path") && (option2 != "-fid") && (option2 != "-size"))
      goto com_fs_usage;

    if ( option3.length() && (option3 != "-path") && (option3 != "-fid") && (option3 != "-size"))
      goto com_fs_usage;

    XrdOucEnv* result = client_admin_command(in);
    if (!silentcommand) {
      global_retc = output_result(result);
    } else {
      if (result) {
	global_retc = 0;
      } else {
	global_retc = EINVAL;
      }
    }

    return (0);
  }


  com_fs_usage:

  printf("usage: fs ls                                                    : list configured filesystems (or by name or id match\n");
  printf("       fs set   <fs-name> <fs-id> [-sched <group> ] [-force]    : configure filesystem with name and id\n");
  printf("       fs rm    <fs-name>|<fs-id>                               : remove filesystem configuration by name or id\n");
  printf("       fs boot  <fs-id>|<node-queue>                            : boot filesystem/node ['fs boot *' to boot all]  \n");
  printf("       fs config <fs-id>|<node-queue> <status> [-sched <group>] : set filesystem configuration status\n");
  printf("                    <status> can be := rw                       : filesystem is in read write mode\n");
  printf("                                    := wo                       : filesystem is in write-once mode\n");
  printf("                                    := ro                       : filesystem is in read-only mode\n");
  printf("                                    := drain                    : filesystem is in drain mode\n");
  printf("                                    := off                      : filesystem is disabled\n"); 
  printf("                    -sched <group>                              : allows to change the scheduling group\n");
  printf("       fs clone <fs-id-src> <fs-id-dst>                         : allows to clone the contents of <fs-id-src> to <fs-id-dst>\n");
  printf("       fs compare <fs-id-src> <fs-id-dst>|<space>               : does a comparison of <fs-id-src> with <fs-id-dst>|<space>\n");
  printf("       fs dropfiles <fs-id> [-f]                                : allows to drop all files on <fs-id> - force (-f) unlinks/removes files at the time from the NS (you have to cleanup or remove the files from disk) \n");
  printf("       fs heal <fs-id-src>|<path> [<space-dst> [<subgroup>]]    : heals replica's of filesystem <fs-id> or path <path> placing/keeping in <space-dst> (+<subgroup>)\n");
  printf("       fs flatten <fs-id>|<path> <space> [<subgroup>]           : allows to flatten the file distribution of files in <fs-id> or under <path> in <space> [and <subgroup>]\n");
  printf("       fs dumpmd [-s] <fs-id> [-fid] [-path]                    : dump all file meta data on this filesystem in query format\n");
  printf("                                                                  -s    : don't printout keep an internal reference\n");
  printf("                                                                  -fid  : dump only a list of file id's stored on this filesystem\n");
  printf("                                                                  -path : dump only a list of file names stored on this filesystem\n");
  printf("       fs verify <fs-name>|<fs-id> [-checksum] [-commitchecksum] [-commitsize] [-rate <rate>]\n");
  printf("                                                                : schedule asynchronous replication [with checksumming] on a filesystem\n");
  printf("                                                      -checksum : trigger the checksum calculation during the verification process\n");
  printf("                                                -commitchecksum : commit the computed checksum to the MGM\n");
  printf("                                                -commitsize     : commit the file size to the MGM\n");
  printf("                                                -rate <rate>    : restrict the verification speed to <rate> per node\n");

  return (0);
}
