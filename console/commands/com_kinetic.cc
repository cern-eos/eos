/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#ifdef KINETICIO_FOUND
/*----------------------------------------------------------------------------*/
#include <kio/KineticIoFactory.hh>
/*----------------------------------------------------------------------------*/

extern int com_space (char*);

typedef kio::AdminClusterInterface::OperationTarget OperationTarget;

enum class Operation
{
  STATUS, COUNT, SCAN, REPAIR, RESET, INVALID, CONFIG_SHOW, CONFIG_PUBLISH
};

struct Configuration
{
  Operation op;
  OperationTarget target;
  std::string id;
  std::string space;
  int numthreads;
  bool monitoring;
};

int
kinetic_help ()
{
  fprintf(stdout, "usage: kinetic config [--publish] [--space <space> ]\n");
  fprintf(stdout, "       kinetic config [--space <space> ]                     : shows the currently deployed kinetic configuration - by default 'default' space\n");
  fprintf(stdout, "       kinetic config --publish                              : publishes the default MGM configuration files under <mgm>:/var/eos/kinetic to all currently existing FSTs in default or referenced space\n");
  fprintf(stdout, "       kinetic config --publish [--space <name>]             : identifier <name> refers to the MGM files like /var/eos/kinetic/kinetic-cluster-<name>.json /var/eos/kinetic/kinetic-drives-<name>.json /var/eos/kinetic/kinetic-security-<name>.json\n");
  fprintf(stdout, "\n\n");
  fprintf(stdout, "usage: kinetic --id <clusterid> <operation> <target> [--threads <numthreads>] [-m]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... --id <clusterid> ...                      : specify cluster, <clusterid> refers to the name of the cluster set in the cluster configuration\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... <operation> <target> ...                  : run <operation> on keys of type <target>\n");
  fprintf(stdout, "         <operation>\n");
  fprintf(stdout, "             status                                          : show connection status, does not require a <target> type\n");
  fprintf(stdout, "             count                                           : count number of keys existing in the cluster\n");
  fprintf(stdout, "             scan                                            : check keys and display their status information (Warning: Long Runtime)\n");
  fprintf(stdout, "             repair                                          : check keys, repair as required, display key status information (Warning: Long Runtime)\n");
  fprintf(stdout, "             reset                                           : force remove keys (Warning: Data will be lost!)\n");
  fprintf(stdout, "         <target>\n");
  fprintf(stdout, "             data                                            : data keys\n");
  fprintf(stdout, "             metadata                                        : metadata keys\n");
  fprintf(stdout, "             attribute                                       : attribute keys\n");
  fprintf(stdout, "             indicator                                       : keys with indicators (written automatically when encountering partial failures during normal operation)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... [--threads <numthreads>] ...              : (optional) specify the number of background io threads \n");
  fprintf(stdout, "       kinetic ... [-m] ...                                  : (optional) monitoring key=value output format\n");

  return EXIT_SUCCESS;
}

void
printKeyCount (const kio::AdminClusterInterface::KeyCounts& kc, Configuration& config)
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

bool
parseArguments (char* arg, Configuration& config)
{
  config.op = Operation::INVALID;
  config.target = OperationTarget::INVALID;
  config.numthreads = 1;
  config.monitoring = false;
  config.space = "default";
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  XrdOucString str = subtokenizer.GetToken();

  while (str.length())
  {
    if (str == "--id")
    {
      str = subtokenizer.GetToken();
      if (str.length())
        config.id = str.c_str();
    }
    else if (str == "--space")
    {
      str = subtokenizer.GetToken();
      if (str.length())
        config.space = str.c_str();
    }
    else if (str == "-m")
    {
      config.monitoring = true;
    }
    else if (str == "--threads")
    {
      str = subtokenizer.GetToken();
      if (str.length())
        config.numthreads = atoi(str.c_str());
    }
    else if (str == "scan")
      config.op = Operation::SCAN;
    else if (str == "count")
      config.op = Operation::COUNT;
    else if (str == "repair")
      config.op = Operation::REPAIR;
    else if (str == "status")
      config.op = Operation::STATUS;
    else if (str == "reset")
      config.op = Operation::RESET;
    else if (str == "config")
    {
      config.op = Operation::CONFIG_SHOW;
      config.id = "default";
    }
    else if (str == "indicator")
      config.target = OperationTarget::INDICATOR;
    else if (str == "data")
      config.target = OperationTarget::DATA;
    else if (str == "metadata")
      config.target = OperationTarget::METADATA;
    else if (str == "attribute")
      config.target = OperationTarget::ATTRIBUTE;
    else if (str == "--publish")
    {
      if (config.op == Operation::CONFIG_SHOW)
        config.op = Operation::CONFIG_PUBLISH;
      else
        config.op = Operation::INVALID;
    }
    str = subtokenizer.GetToken();
  }

  if(!config.id.length())
    return false;
  if(config.op == Operation::INVALID)
    return false;
  if(config.op == Operation::STATUS || config.op == Operation::CONFIG_SHOW || config.op == Operation::CONFIG_PUBLISH)
    return true;
  return config.target != OperationTarget::INVALID;
}

void printinc(int value){
  fprintf(stdout, "\r\t %d", value);
  fflush(stdout);
}

void
doConfig (Configuration& config)
{
  if (config.op == Operation::CONFIG_SHOW)
  {
    XrdOucString cmd1 = "node-get ";
    cmd1 += config.space.c_str();
    cmd1 += " kinetic.cluster.";
    cmd1 += config.space.c_str();
    XrdOucString cmd2 = "node-get ";
    cmd2 += config.space.c_str();
    cmd2 += " kinetic.location.";
    cmd2 += config.space.c_str();
    XrdOucString cmd3 = "node-get ";
    cmd3 += config.space.c_str();
    cmd3 += " kinetic.security.";
    cmd3 += config.space.c_str();

    com_space((char*) cmd1.c_str());
    com_space((char*) cmd2.c_str());
    com_space((char*) cmd3.c_str());
  }
  if (config.op == Operation::CONFIG_PUBLISH)
  {
    XrdOucString cmd1 = "node-set ";
    XrdOucString cmd2 = "node-set ";
    XrdOucString cmd3 = "node-set ";
    XrdOucString cmd4 = "node-set ";

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

    // to trigger the configuration reload
    cmd4 += config.space.c_str();
    cmd4 += " kinetic.reload ";
    cmd4 += config.space.c_str();

    com_space((char*) cmd1.c_str());
    com_space((char*) cmd2.c_str());
    com_space((char*) cmd3.c_str());
    com_space((char*) cmd4.c_str());
  }
  return;
}

int
com_kinetic (char *arg)
{
  if (wants_help(arg))
    return kinetic_help();

  Configuration config;
  if (!parseArguments(arg, config))
  {
    fprintf(stdout, "Incorrect arguments\n");
    kinetic_help();
    return EXIT_FAILURE;
  }

  switch (config.op)
  {
    case Operation::CONFIG_SHOW:
    case Operation::CONFIG_PUBLISH:
      doConfig(config);
      return EXIT_SUCCESS;
    default: break;
  }

  try{
    auto ac = kio::KineticIoFactory::makeAdminCluster(
        config.id.c_str(),
        config.target == OperationTarget::DATA ? kio::RedundancyType::ERASURE_CODING : kio::RedundancyType::REPLICATION
    );

    switch(config.op){
      case Operation::STATUS:{
        if (!config.monitoring)
        {
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
          fprintf(stdout, "# Cluster Status: \n");
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
        }
        auto v = ac->status();
        for (size_t i = 0; i < v.size(); i++) {
          if (config.monitoring)
            fprintf(stdout, "kinetic.drive.index=%lu kinetic.drive.status=%s\n", i, v[i] ? "OK" : "FAILED");
          else {
            XrdOucString sdrive;
            sdrive += (int) i;
            fprintf(stdout, "# drive %5s : %s\n", sdrive.c_str(), v[i] ? "OK" : "FAILED");
          }
        }
        break;
      }
      case Operation::COUNT:
        fprintf(stdout, "Counting number of keys on cluster: \n");
        ac->count(config.target, printinc);
        fprintf(stdout, "\n");
        break;
      case Operation::SCAN:
        fprintf(stdout, "Scanning keys on cluster: \n");
        printKeyCount( ac->scan(config.target, printinc, config.numthreads), config);
        break;
      case Operation::REPAIR:
        fprintf(stdout, "Scan & repair of keys on cluster: \n");
        printKeyCount( ac->repair(config.target, printinc, config.numthreads), config);
        break;
      case Operation::RESET:
        fprintf(stdout, "Removing keys from cluster: \n");
        printKeyCount( ac->reset(config.target, printinc, config.numthreads), config);
        break;
      case Operation::INVALID:
      default:
        fprintf(stdout, "Invalid Operation");
    }
  }catch(std::exception& e){
    fprintf(stdout, "Encountered Exception: %s\n", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
#else

int
com_kinetic (char *arg)
{
  fprintf(stdout, "EOS has not been compiled with Kinetic support.\n");
  return EXIT_FAILURE;
}
#endif
