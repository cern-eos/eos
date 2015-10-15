/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#ifdef KINETICIO_FOUND
/*----------------------------------------------------------------------------*/
#include <kio/KineticIoFactory.hh>
/*----------------------------------------------------------------------------*/

extern int com_space (char*);

typedef kio::AdminClusterInterface::OperationTarget OperationTarget;

enum class Operation{
  STATUS, COUNT, SCAN, REPAIR, RESET, INVALID, CONFIG_SHOW, CONFIG_PUBLISH
};

struct Configuration{
    Operation op;
    OperationTarget target; 
    std::string id;
    std::string space;
    int numthreads;
    int verbosity; 
    bool monitoring; 
};

int kinetic_help(){
  fprintf(stdout, "usage: kinetic --id <clusterid> [--op] status|count|scan|repair|reset --target all|file|attribute|indicator [--threads <numthreads>] [-v debug|notice|warning] [-m]\n"); 
  fprintf(stdout, "                                                                  -m : monitoring key=value output format\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... --id <clusterid> ...                              : specify the cluster identifier where to run an operation\n");
  fprintf(stdout, "       kinetic ... [--threads <numthreads>] ...                      : (optional) specify the number of background io threads \n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... [-op] <operation> ...                             : run <operation> - the --op flag can be omitted\n");
  fprintf(stdout, "       kinetic status ...                                            : print status of connections of the cluster\n");
  fprintf(stdout, "       kinetic count  ...                                            : count number of keys existing in the cluster\n");
  fprintf(stdout, "       kinetic scan   ...                                            : check all keys existing in the cluster and display their status information (Warning: long run=-time)\n");
  fprintf(stdout, "       kinetic repair ...                                            : check all keys existing in the cluster, repair as required, display their status information. (Warning: Long Runtime)\n");
  fprintf(stdout, "       kinetic reset  ...                                            : force remove all keys on all drives associated with the cluster, you will loose ALL data!\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... --target all ...                                  : perform operation on all keys of the cluster\n");
  fprintf(stdout, "       kinetic ... --target file ...                                 : perform operation on keys associated with files\n");
  fprintf(stdout, "       kinetic ... --target attribute ...                            : perform operation on attribute keys only \n");
  fprintf(stdout, "       kinetic ... --target indicator ...                            : perform operation only on keys with indicators (written automatically when encountering partial failures during a get/put/remove in normal operation)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... [-v debug|notice|warning ] ...                    : (optional) specify verbosity level \n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic config [--space <space> ]                             : shows the currently deployed kinetic configuration - by default 'default' space\n");
  fprintf(stdout, "       kinetic config --publish                                      : publishes the default MGM configuration files under <mgm>:/var/eos/kinetic to all currently existing FSTs in default or referenced space\n");
  fprintf(stdout, "       kinetic config --publish [--space <name>]                     : identifier <name> refers to the MGM files like /var/eos/kinetic/kinetic-cluster-<name>.json /var/eos/kinetic/kinetic-drives-<name>.json /var/eos/kinetic/kinetic-security-<name>.json\n");

  return EXIT_SUCCESS;
}

void printKeyCount(const kio::AdminClusterInterface::KeyCounts& kc, Configuration& config)
{
  if (config.monitoring) 
  {
    fprintf(stdout, "kinetic.stat.keys.n=%d kinetic.stat.drives.inaccessible.n=%d kinetic.stat.require.action.n=%d kinetic.stat.repaired.n=%d kinetic.stat.removed.n=%d Kinetic.stat.notrepairable.n=%d\n",
	    kc.total,
	    kc.incomplete,
	    kc.need_action,
	    kc.repaired,
	    kc.removed,
	    kc.unrepairable);
  }
  else
  {
    fprintf(stdout, "# ------------------------------------------------------------------------\n");
    fprintf(stdout, "# Completed Operation - scanned a total of %d keys\n", kc.total);
    fprintf(stdout, "# ------------------------------------------------------------------------\n");
    fprintf(stdout, "# Keys with inaccessible drives:        %d\n", kc.incomplete);
    fprintf(stdout, "# Keys requiring action:                %d\n", kc.need_action);
    fprintf(stdout, "# Keys Repaired:                        %d\n", kc.repaired);
    fprintf(stdout, "# Keys Removed:                         %d\n", kc.removed);
    fprintf(stdout, "# Not repairable:                       %d\n", kc.unrepairable);
    fprintf(stdout, "# ------------------------------------------------------------------------\n");
  }
}

bool mshouldLog(const char* func, int level, int target_level){
  return level <= target_level;
}

void mlog(const char* func, const char* file, int line, int level, const char* msg){
  switch(level){
    case LOG_DEBUG:
      fprintf(stdout, "DEBUG:");
      break;
    case LOG_NOTICE:
      fprintf(stdout, "NOTICE:");
      break;
    case LOG_WARNING:
      fprintf(stdout, "WARNING:");
      break;
  }
  fprintf(stdout, " %s\n", msg);
}


bool parseArguments(char* arg, Configuration& config) {
  config.op = Operation::INVALID;
  config.target = OperationTarget::INVALID;
  config.numthreads = 1;
  config.verbosity = LOG_WARNING;
  config.monitoring = false;
  config.space = "default";
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  XrdOucString str = subtokenizer.GetToken();
  
  while(str.length()){
    if(str == "--id"){
      str = subtokenizer.GetToken(); 
      if(str.length()) 
        config.id = str.c_str();
    }
    else if(str == "--space"){
      str = subtokenizer.GetToken(); 
      if(str.length()) 
        config.space = str.c_str();
    }
    else if(str == "-m") {
      config.monitoring=true;
    }
    else if(str == "--threads"){
      str = subtokenizer.GetToken(); 
      if(str.length())
       config.numthreads = atoi(str.c_str());
    }
    else if( (str == "--op") || (!str.beginswith("--"))) {
      if (str.beginswith("--"))
	str = subtokenizer.GetToken();
      if(str.length()){
         if(str == "scan")
           config.op = Operation::SCAN;
         else if(str == "count")
           config.op = Operation::COUNT;
         else if(str == "repair")
           config.op = Operation::REPAIR;
         else if(str == "status")
           config.op = Operation::STATUS;
         else if(str == "reset")
	   config.op = Operation::RESET;
	 else if(str == "config")
	   {
	     config.op = Operation::CONFIG_SHOW;
	     config.id = "default";
	   }
      }
    }
    else if(str == "--target"){
      str = subtokenizer.GetToken();
      if(str.length()){
        if(str == "all")
          config.target = OperationTarget::ALL;
        else if(str == "indicator")
          config.target = OperationTarget::INDICATOR;
        else if(str == "file")
          config.target = OperationTarget::FILE;
        else if(str == "attribute")
          config.target = OperationTarget::ATTRIBUTE;
      }
    }
    else if(str == "-v"){
      str = subtokenizer.GetToken();
      if(str.length()){
        if(str == "debug")
          config.verbosity = LOG_DEBUG;
        else if(str == "notice")
          config.verbosity = LOG_NOTICE;
        else if(str == "warning")
          config.verbosity = LOG_WARNING;
      }
    }
    else if (str == "--publish") {
      if (config.op == Operation::CONFIG_SHOW)
	config.op = Operation::CONFIG_PUBLISH;
      else
	config.op = Operation::INVALID;
    }

    str = subtokenizer.GetToken();
  }
  
  return config.id.length() && config.op != Operation::INVALID &&
    ( (config.op == Operation::STATUS) | (config.target != OperationTarget::INVALID) | (config.op == Operation::CONFIG_SHOW) | (config.op == Operation::CONFIG_PUBLISH) );

}

int countkeys(std::unique_ptr<kio::AdminClusterInterface>& ac)
{
  fprintf(stdout, "Counting number of keys on cluster: \n");
  auto total = 0;
  while(true){
    auto c = ac->count(5000, total == 0);
    if(!c) break;
    total += c;
    fprintf(stdout, "\r\t %d",total);
    fflush(stdout);
  }
  fprintf(stdout, "\r\t %d\n",total);;
  return total;
}

void printStatus(std::unique_ptr<kio::AdminClusterInterface>& ac, Configuration& config)
{
  /* Wait a second so that connections register as failed correctly */
  if (!config.monitoring)
  {
    fprintf(stdout, "# ------------------------------------------------------------------------\n");
    fprintf(stdout, "# Cluster Status: \n");
    fprintf(stdout, "# ------------------------------------------------------------------------\n");
  }
  sleep(1);
  auto v = ac->status();
  for(size_t i=0; i<v.size(); i++) 
  {
    if (config.monitoring) 
      fprintf(stdout, "kinetic.drive.index=%lu kinetic.drive.status=%s\n", i, v[i] ? "OK" : "FAILED");
    else
    {
      XrdOucString sdrive;
      sdrive += (int)i;
      fprintf(stdout, "# drive %5s : %s\n", sdrive.c_str(), v[i] ? "OK" : "FAILED");
    }
  }
}

void doConfig( Configuration& config )
{
  if (config.op == Operation::CONFIG_SHOW) 
  {
    XrdOucString cmd1 = "node-get ";
    cmd1 += config.space.c_str();
    cmd1+= " kinetic.cluster.";  
    cmd1 += config.space.c_str();
    XrdOucString cmd2 = "node-get ";
    cmd2 += config.space.c_str(); 
    cmd2 += " kinetic.location.";
    cmd2 += config.space.c_str(); 
    XrdOucString cmd3 = "node-get ";
    cmd3 += config.space.c_str();
    cmd3 += " kinetic.security.";
    cmd3 += config.space.c_str(); 

    com_space( (char*)cmd1.c_str());
    com_space( (char*)cmd2.c_str());
    com_space( (char*)cmd3.c_str());
  }
  if (config.op == Operation::CONFIG_PUBLISH)
  {
    XrdOucString cmd1 = "node-set ";
    XrdOucString cmd2 = "node-set ";
    XrdOucString cmd3 = "node-set ";
    cmd1 += config.space.c_str();
    cmd1 += " kinetic.cluster.";
    cmd1 += config.space.c_str(); 
    cmd1 += " file:/var/eos/kinetic/kinetic-cluster-"; 
    cmd1 += config.space.c_str();
    cmd1 += ".json";

    cmd2 += config.space.c_str();
    cmd2 += " kinetic.location.";
    cmd2 += config.space.c_str(); 
    cmd2 += " file:/var/eos/kinetic/kinetic-location-"; 
    cmd2 += config.space.c_str();
    cmd2 += ".json";

    cmd3 += config.space.c_str();
    cmd3 += " kinetic.security.";
    cmd3 += config.space.c_str();
    cmd3 += " file:/var/eos/kinetic/kinetic-security-"; 
    cmd3 += config.space.c_str();
    cmd3 += ".json";
    com_space( (char*)cmd1.c_str());
    com_space( (char*)cmd2.c_str());
    com_space( (char*)cmd3.c_str());
  }
  return;
}


void doOperation(
    std::unique_ptr<kio::AdminClusterInterface>& ac,
    Configuration& config
)
{
  const auto totalkeys = countkeys(ac);
  auto numsteps = 50;
  const auto perstep = (totalkeys+numsteps-1) / numsteps;

  int step = perstep; 
  
  for(int i=0; step; i++){
    switch(config.op){
      case Operation::SCAN:
        step = ac->scan(perstep, i==0);
        break;
      case Operation::REPAIR:
        step = ac->repair(perstep, i==0);
        break;
      case Operation::RESET:
        step = ac->reset(perstep, i==0);
        break;
      default: 
        break;
    }
    fprintf(stdout, "\r[");
    for(int j=0; j<=i; j++)
      fprintf(stdout, "*");
    for(int j=i+1; j<numsteps; j++)
      fprintf(stdout, "-");
    fprintf(stdout, "]");
    fflush(stdout);
  }
  fprintf(stdout, "\n");
  printKeyCount(ac->getCounts(), config);
}


int com_kinetic (char *arg)
{
  if(wants_help(arg))
    return kinetic_help();

  Configuration config;
  if(!parseArguments(arg, config)){
    fprintf(stdout, "Incorrect arguments\n");
    kinetic_help();
    return EXIT_FAILURE;
  }

  switch(config.op){
  case Operation::CONFIG_SHOW:
  case Operation::CONFIG_PUBLISH:
    doConfig(config);
    return EXIT_SUCCESS;
  default: break;
  }


  try{
    kio::Factory::registerLogFunction(mlog, 
            std::bind(mshouldLog, std::placeholders::_1, std::placeholders::_2, config.verbosity)
    );
    
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str(), config.target, config.numthreads);

    switch(config.op){
      case Operation::STATUS: 
        printStatus(ac, config);
        break;
      case Operation::COUNT:
        countkeys(ac);
        break;
      default:
        doOperation(ac, config);
        break;
    }
  }catch(std::exception& e){
    fprintf(stdout, "Encountered Exception: %s\n", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
#else

int com_kinetic (char *arg){
  fprintf(stdout, "EOS has not been compiled with Kinetic support.\n");
  return EXIT_FAILURE;
}
#endif
