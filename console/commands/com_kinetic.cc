/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#ifdef KINETICIO_FOUND
/*----------------------------------------------------------------------------*/
#include <kio/KineticIoFactory.hh>
/*----------------------------------------------------------------------------*/

typedef kio::AdminClusterInterface::OperationTarget OperationTarget;

enum class Operation{
  STATUS, COUNT, SCAN, REPAIR, RESET, INVALID
};

struct Configuration{
    Operation op;
    OperationTarget target; 
    std::string id;
    int numthreads;
    int verbosity; 
};

int kinetic_help(){
  fprintf(stdout, " Usage: -id clusterid -op status|count|scan|repair|reset -target all|file|attribute|indicator [-threads numthreads] [-v debug|notice|warning]\n"); 
  fprintf(stdout, " -id: specify the cluster identifier \n");
  fprintf(stdout, " -op: specify one of the following operations to execute\n");
  fprintf(stdout, "    status: print status of connections of the cluster. \n");
  fprintf(stdout, "    count: number of keys existing in the cluster. \n");
  fprintf(stdout, "    scan: check all keys existing in the cluster and display their status information (Warning: Long Runtime) \n");
  fprintf(stdout, "    repair: check all keys existing in the cluster, repair as required, display their status information. (Warning: Long Runtime) \n");
  fprintf(stdout, "    reset: force remove all keys on all drives associated with the cluster, you will loose ALL data! \n");
  fprintf(stdout, " -target: specify one of the following target ranges\n");
  fprintf(stdout, "    all: perform operation on all keys of the cluster\n");
  fprintf(stdout, "    file: perform operation on keys associated with files\n");
  fprintf(stdout, "    attribute: perform operation on attribute keys only \n");
  fprintf(stdout, "    indicator: perform operation only on keys with indicators (written automatically when encountering partial failures during a get/put/remove in normal operation)\n");
  fprintf(stdout, " -threads: (optional) specify the number of background io threads \n");
  fprintf(stdout, " -v: (optional) specify verbosity level \n");
  return EXIT_SUCCESS;
}

void printKeyCount(const kio::AdminClusterInterface::KeyCounts& kc)
{
  fprintf(stdout, "Completed Operation. Scanned a total of %d keys\n\n", kc.total);
  fprintf(stdout, "Keys with inaccessible drives: %d\n", kc.incomplete);
  fprintf(stdout, "Keys requiring action: %d\n", kc.need_action);
  fprintf(stdout, "Keys Repaired: %d\n", kc.repaired);
  fprintf(stdout, "Keys Removed: %d\n", kc.removed);
  fprintf(stdout, "Not repairable: %d\n", kc.unrepairable);
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
    
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  XrdOucString str = subtokenizer.GetToken();
  
  while(str.length()){
    if(str == "-id"){
      str = subtokenizer.GetToken(); 
      if(str.length()) 
        config.id = str.c_str();
    }
    else if(str == "-threads"){
      str = subtokenizer.GetToken(); 
      if(str.length())
       config.numthreads = atoi(str.c_str());
    }
    else if(str == "-op"){
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
      }
    }
    else if(str == "-target"){
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
    str = subtokenizer.GetToken();
  }

  return config.id.length() && config.op != Operation::INVALID &&
         (config.op == Operation::STATUS || config.target != OperationTarget::INVALID);
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

void printStatus(std::unique_ptr<kio::AdminClusterInterface>& ac)
{
  /* Wait a second so that connections register as failed correctly */
  fprintf(stdout, "Cluster Status: \n");
  sleep(1);
  auto v = ac->status();
  for(size_t i=0; i<v.size(); i++) 
    fprintf(stdout, "drive %lu: %s\n", i, v[i] ? "OK" : "FAILED");
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
  printKeyCount(ac->getCounts());
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

  try{
    kio::Factory::registerLogFunction(mlog, 
            std::bind(mshouldLog, std::placeholders::_1, std::placeholders::_2, config.verbosity)
    );
    
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str(), config.target, config.numthreads);

    switch(config.op){
      case Operation::STATUS: 
        printStatus(ac);
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