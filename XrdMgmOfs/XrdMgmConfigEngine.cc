/*----------------------------------------------------------------------------*/
#include "XrdMgmOfs/XrdMgmConfigEngine.hh"
/*----------------------------------------------------------------------------*/
#include <iostream>
#include <fstream>
#include <sstream>

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
bool 
XrdMgmConfigEngine::LoadConfig(XrdOucEnv &env, XrdOucString &err) 
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);

  if (!name) {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  XrdOucString fullpath = configDir;
  fullpath += name;
  fullpath += XRDMGMCONFIGENGINE_EOS_SUFFIX;

  if (::access(fullpath.c_str(),R_OK)) {
    err = "error: unable to open config file ";
    err += name;
    return false;
  }
  
  ifstream infile(fullpath.c_str());
  std::string s;
  XrdOucString allconfig="";
  if (infile.is_open()) {
    XrdOucString config="";XrdOucEnv env("");
    while (!infile.eof()) {          
      getline(infile, s);
      if (s.length()) {
	allconfig += s.c_str();
	allconfig += "\n";
      }
      eos_notice("IN ==> %s", s.c_str());
    }
    infile.close();
    return ParseConfig(allconfig, err);
  } else {
    err = "error: failed to open configuration file with name \""; err += name; err += "\"!";
    return false;
  }
  eos_debug("shouldn't get here");
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmConfigEngine::SaveConfig(XrdOucEnv &env, XrdOucString &err)
{
  const char* name = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");
  const char* comment = env.Get("mgm.config.comment");

  eos_notice("saving config name=%s comment=%s force=%d", name, comment, force);

  if (!name) {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  XrdOucString sname = name;

  if ( (sname.find("..")) != STR_NPOS) {
    err = "error: the config name cannot contain ..";
    errno = EINVAL;
    return false;
  }

  if ( (sname.find("/")) != STR_NPOS) {
    err = "error: the config name cannot contain /";
    errno = EINVAL;
    return false;
  }

  XrdOucString fullpath = configDir;
  XrdOucString halfpath = configDir;
  fullpath += name;
  halfpath += name;

  fullpath += XRDMGMCONFIGENGINE_EOS_SUFFIX;

  if ( !::access(fullpath.c_str(),R_OK)) {
    if (!force) {
      errno = EEXIST;
      err = "error: a configuration file with name \""; err += name; err += "\" exists already!";
      return false;
    }  else {
      char backupfile[4096];
      sprintf(backupfile,"%s.backup.%lu%s",halfpath.c_str(),time(0),XRDMGMCONFIGENGINE_EOS_SUFFIX);
      if (rename(fullpath.c_str(),backupfile)) {
	err = "error: unable to move existing config file to backup version!";
	return false;
      }
    }
  }

  std::ofstream outfile(fullpath.c_str());
  if (outfile.is_open()) {
    XrdOucString config="";XrdOucEnv env("");
    DumpConfig(config, env);
    if (comment) {
      config+= "comment: => "; config += comment;
    }
			     
    outfile << config.c_str();
    outfile.close();
  } else {
    err = "error: failed to save configuration file with name \""; err += name; err += "\"!";
    return false;
  }
  
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmConfigEngine::ListConfigs(XrdOucString &configlist, bool showbackup)
{
  struct filestat {
    struct stat buf;
    char filename[1024];
  };

  configlist="Existing Configurations\n";
  configlist+="=======================\n";

  XrdOucString FileName="";

  DIR* dir = opendir(configDir.c_str());
  if (!dir) {
    eos_err("unable to open config directory %s", configDir.c_str());
    return false;
  }
  
  long tdp=0;
  struct filestat* allstat = 0;
   
  struct dirent *dp;
  int nobjects=0;

  tdp = telldir(dir);
  
  while ((dp = readdir (dir)) != 0) {
    FileName = dp->d_name;
    if ( (!strcmp(dp->d_name,".")) || (!strcmp(dp->d_name,"..")) || (!FileName.endswith(XRDMGMCONFIGENGINE_EOS_SUFFIX)))
      continue;
    
    nobjects++;
  }
  
  allstat = (struct filestat*) malloc(sizeof(struct filestat) * nobjects);
  
  if (!allstat) {
      eos_err("cannot allocate sorting array");
      return false;
  }
  
  seekdir(dir,tdp);
  
  int i=0;
  while ((dp = readdir (dir)) != 0) {
    FileName = dp->d_name;
    if ( (!strcmp(dp->d_name,".")) || (!strcmp(dp->d_name,"..")) || (!FileName.endswith(XRDMGMCONFIGENGINE_EOS_SUFFIX)))
      continue;
    
    char fullpath[8192];
    sprintf(fullpath,"%s/%s",configDir.c_str(),dp->d_name);

    sprintf(allstat[i].filename,"%s",dp->d_name);
    eos_debug("stat on %s\n", dp->d_name);
    if (stat(fullpath, &(allstat[i].buf))) {
      eos_err("cannot stat after readdir file %s", fullpath);
    }
    i++;
  }
  closedir(dir);
  // do the sorting
  qsort(allstat,nobjects,sizeof(struct filestat),XrdMgmConfigEngine::CompareCtime);

  if (allstat && (nobjects >0)) {
    for (int j=0; j< i; j++) {
      char outline[1024];
      time_t modified = allstat[j].buf.st_mtime;

      sprintf(outline,"created: %s name: %s", ctime(&modified), allstat[j].filename);
      XrdOucString removelinefeed = outline;
      while(removelinefeed.replace('\n',"")) {}
      // remove  suffix
      removelinefeed.replace(XRDMGMCONFIGENGINE_EOS_SUFFIX,"");
      if ( (!showbackup) &&  (removelinefeed.find(".backup.") != STR_NPOS)) {
	// don't show this ones
      } else {
	configlist += removelinefeed;
	configlist += "\n";
      }
    }
    free(allstat);
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmConfigEngine::BroadCastConfig() 
{

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmConfigEngine::ParseConfig(XrdOucString &inconfig, XrdOucString &err)
{
  err = "";
  Mutex.Lock();
  configDefinitions.Purge();

  std::istringstream streamconfig(inconfig.c_str());

  int linenumber = 0;
  std::string s;

  while ( ( getline(streamconfig, s, '\n') ) ) {
    linenumber++;

    if (s.length()) {
      XrdOucString key = s.c_str();
      XrdOucString value;
      int seppos;
      seppos = key.find(" => ");
      if (seppos == STR_NPOS) {
	Mutex.UnLock();
	err = "parsing error in configuration file line "; err += (int) linenumber; err += " : "; err += s.c_str();
	errno = EINVAL;
	return false;
      }
      value.assign(key, seppos + 4);
      eos_notice("setting config key=%s value=%s", key.c_str(),value.c_str());
    }
  }

  Mutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmConfigEngine::DeleteConfigByMatch(const char* key, XrdOucString* def, void* Arg)
{
  XrdOucString* matchstring = (XrdOucString*) matchstring;
  XrdOucString skey = key;

  if (skey.beginswith(matchstring->c_str())) {
    return -1;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmConfigEngine::PrintEachConfig(const char* key, XrdOucString* def, void* Arg)
{
  if (Arg == NULL)
    eos_static_info("%s => %s", key, def->c_str());
  else {
    eos_static_debug("%s => %s", key, def->c_str());
    
    XrdOucString* outstring = ((struct PrintInfo*) Arg)->out;
    XrdOucString option    = ((struct PrintInfo*) Arg)->option;
    XrdOucString skey = key;
    bool filter = false;
    if (option.find("v")!=STR_NPOS) {
      if (skey.beginswith("vid:"))
	filter = true;
    }
    if (option.find("f")!=STR_NPOS) {
      if (skey.beginswith("fs:"))
	filter = true;
    }
    if (option.find("q")!=STR_NPOS) {
      if (skey.beginswith("quota:"))
	filter = true;
    }
    if (option.find("c")!=STR_NPOS) {
      if (skey.beginswith("comment:"))
	filter = true;
    }
    
    if (filter) {
      (*outstring) += key; (*outstring) += " => "; (*outstring) += def->c_str(); (*outstring) += "\n";
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmConfigEngine::DumpConfig(XrdOucString &out, XrdOucEnv &filter)
{
  struct PrintInfo pinfo;

  const char* name = filter.Get("mgm.config.file");

  pinfo.out = &out;
  pinfo.option = "vfqc";
  
  if (filter.Get("mgm.config.vid") || (filter.Get("mgm.config.fs")) || (filter.Get("mgm.config.quota")) || (filter.Get("mgm.config.comment"))) 
    pinfo.option = "";
  
  if (filter.Get("mgm.config.vid")) {
    pinfo.option += "v";
    }
  if (filter.Get("mgm.config.fs")) {
    pinfo.option += "f";
  }
  if (filter.Get("mgm.config.quota")) {
    pinfo.option += "q";
  }
  if (filter.Get("mgm.config.comment")) {
    pinfo.option += "c";
  }
  
  if (name == 0) {
    Mutex.Lock();
    configDefinitions.Apply(PrintEachConfig,&pinfo);
    Mutex.UnLock();
    while (out.replace("&"," ")) {}
  } else {
    // dump from stored config file
    XrdOucString fullpath = configDir;
    fullpath += name;
    fullpath += XRDMGMCONFIGENGINE_EOS_SUFFIX;

    std::ifstream infile(fullpath.c_str());
    std::string inputline;
    while (getline(infile, inputline)) {
      XrdOucString sinputline = inputline.c_str();
      // filter according to user specification
      bool filter = false;
      if ( (pinfo.option.find("v")!=STR_NPOS) && (sinputline.beginswith("vid:")))
	filter = true;
      if ( (pinfo.option.find("f")!=STR_NPOS) && (sinputline.beginswith("fs:")))
	filter = true;
      if ( (pinfo.option.find("q")!=STR_NPOS) && (sinputline.beginswith("quota:")))
	filter = true;
      if ( (pinfo.option.find("c")!=STR_NPOS) && (sinputline.beginswith("comment:")))
	filter = true;
      if (filter) {
	out += sinputline;
	out += "\n";
      }
    }
  }
  return true;
}
