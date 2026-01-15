// ----------------------------------------------------------------------
// File: space-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <memory>
#include <sstream>

namespace {

class SpaceHelper : public ICmdHelper {
public:
  SpaceHelper(const GlobalOptions& opts) : ICmdHelper(opts) { mIsAdmin = true; }
  ~SpaceHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::SpaceProto* space = mReq.mutable_space();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token))
      return false;
    if (token == "ls") {
      eos::console::SpaceProto_LsProto* ls = space->mutable_ls();
      while (tokenizer.NextToken(token)) {
        if (token == "-s") {
          mIsSilent = true;
        } else if (token == "-g") {
          if (!tokenizer.NextToken(token) ||
              !eos::common::StringTokenizer::IsUnsignedNumber(token))
            return false;
          try {
            ls->set_outdepth(std::stoi(token));
          } catch (...) {
            return false;
          }
        } else if (token == "-m") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::MONITORING);
        } else if (token == "-l") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::LISTING);
        } else if (token == "--io") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::IO);
        } else if (token == "--fsck") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::FSCK);
        } else if (token.find('-') != 0) {
          ls->set_selection(token);
        } else {
          return false;
        }
      }
    } else if (token == "config") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_ConfigProto* config = space->mutable_config();
      bool removing = false;
      if (token == "rm") {
        removing = true;
        if (!tokenizer.NextToken(token))
          return false;
      }
      config->set_mgmspace_name(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (removing) {
        config->set_remove(true);
        config->set_mgmspace_key(token);
      } else {
        std::string::size_type pos = token.find('=');
        if (pos != std::string::npos &&
            std::count(token.begin(), token.end(), '=') == 1) {
          config->set_mgmspace_key(token.substr(0, pos));
          config->set_mgmspace_value(token.substr(pos + 1));
        } else {
          return false;
        }
      }
    } else if (token == "set") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_SetProto* set = space->mutable_set();
      set->set_mgmspace(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "on")
        set->set_state_switch(true);
      else if (token == "off")
        set->set_state_switch(false);
      else
        return false;
    } else if (token == "rm") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_RmProto* rm = space->mutable_rm();
      rm->set_mgmspace(token);
    } else if (token == "status") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_StatusProto* status = space->mutable_status();
      status->set_mgmspace(token);
      if (tokenizer.NextToken(token)) {
        if (token == "-m")
          status->set_outformat_m(true);
        else
          return false;
      }
    } else if (token == "define") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_DefineProto* define = space->mutable_define();
      define->set_mgmspace(token);
      // optional groupsize
      if (!tokenizer.NextToken(token)) {
        define->set_groupsize(0);
        define->set_groupmod(24);
      } else {
        try {
          define->set_groupsize(std::stoi(token));
        } catch (...) {
          return false;
        }
        if (!tokenizer.NextToken(token)) {
          define->set_groupmod(24);
        } else {
          try {
            define->set_groupmod(std::stoi(token));
          } catch (...) {
            return false;
          }
        }
      }
    } else if (token == "quota") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_QuotaProto* quota = space->mutable_quota();
      quota->set_mgmspace(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "on")
        quota->set_quota_switch(true);
      else if (token == "off")
        quota->set_quota_switch(false);
      else
        return false;
    } else {
      return false;
    }
    return true;
  }
};

class SpaceProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "space";
  }
  const char*
  description() const override
  {
    return "Space configuration";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    SpaceHelper helper(gGlobalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stdout,
        " Usage:\n"
        "space ls [-s|-g <depth>] [-m|-l|--io|--fsck] [<space>] : list in all "
        "spaces or select only <space>. <space> is a substring match and can "
        "be a comma separated list\n"
        "\t      -s : silent mode\n"
        "\t      -m : monitoring key=value output format\n"
        "\t      -l : long output - list also file systems after each space\n"
        "\t      -g : geo output - aggregate space information along the "
        "instance geotree down to <depth>\n"
        "\t    --io : print IO statistics\n"
        "\t  --fsck : print filesystem check statistics\n\n"
        "space config <space-name> space.attr.<key> =[<>|]<value>              "
        " : configure a space extended attribute which is added to all "
        "directories referencing this space via sys.forced.space\n"
        "                                                                      "
        "  space.attr.sys.acl=<u:1000:rwx (the < sign indicates to add to the "
        "acl on the left side\n"
        "                                                                      "
        "  space.attr.sys.acl=>u:1000:rwx (the > sign indicates to add to the "
        "acl on the right side\n"
        "                                                                      "
        "  space.attr.sys.acl=|u:1000:rwx (the | sign indicates to set the acl "
        "if there is none defined\n"
        "                                                                      "
        "  space.attr.sys.foo=bar ( the sys.foo attribute is overwriting the "
        "local sys.foo attribute\n"
        "                                                                      "
        "  space.attr.sys.foo=|bar ( the sys.foo attribute is set only if "
        "there is no local sys.foo attribute\n"
        "space config <space-name> space.nominalsize=<value>                   "
        ": configure the nominal size for this space\n"
        "space config <space-name> space.balancer=on|off                       "
        ": enable/disable the space balancer [ default=off ]\n"
        "space config <space-name> space.balancer.threshold=<percent>          "
        ": configure the used bytes deviation which triggers balancing         "
        "    [ default=20 (%%)     ] \n"
        "space config <space-name> space.balancer.node.rate=<MB/s>             "
        ": configure the nominal transfer bandwidth per running transfer on a "
        "node [ default=25 (MB/s)   ]\n"
        "space config <space-name> space.balancer.node.ntx=<#>                 "
        ": configure the number of parallel balancing transfers per node       "
        "    [ default=2 (streams) ]\n"
        "space config <space-name> space.balancer.max-queue-jobs=<#>           "
        ": configure the maximum number of queued jobs allowed in the balancer "
        "thread pool [ default=1000 (jobs) ]\n"
        "space config <space-name> space.balancer.max-thread-pool-size=<#>     "
        ": configure the maximum number of threads to be used in the balancer "
        "thread pool [ default=100 (threads) ]\n"
        "space config <space-name> space.balancer.update.interval=<#>          "
        ": configure the update interval of the balancing statistics used for "
        "spawning transfers [ default=60 (seconds) min=1 max=300]\n"
        "space config <space-name> space.drainer.tx.minrate=<MB/s >            "
        ": configure the minimum transfer bandwidth per running transfer used "
        "for computing transfer timeout [ default=25 (MB/s) ]\n"
        "space config <space-name> space.drainer.fs.ntx=<#>                    "
        ": configure the number of parallel draining transfers per fs [ "
        "default=5 ]\n"
        "space config <space-name> space.groupbalancer=on|off                  "
        ": enable/disable the group balancer [ default=off ]\n"
        "space config <space-name> space.groupbalancer.ntx=<ntx>               "
        ": configure the number of parallel group balancer jobs per 10s [ "
        "default=10 ]\n"
        "space config <space-name> space.groupbalancer.engine=[value]          "
        ": configure the groupbalancer engine - std/minmax/freespace [ "
        "default=std ]\n"
        "space config <space-name> space.groupbalancer.min_threshold=<v>       "
        ": configure the groupbalancer min threshold(%%), groups below this "
        "will be picked as targets [default=60]\n"
        "space config <space-name> space.groupbalancer.max_threshold=<v>       "
        ": configure the groupbalancer max threshold(%%), groups above this "
        "will be picked as sources [default=95]\n"
        "space config <space-name> "
        "space.groupbalancer.min_file_size=<#K/M/G/T>: configure the min file "
        "size to move between groups [ default=1G ]\n"
        "space config <space-name> "
        "space.groupbalancer.max_file_size=<#K/M/G/T>: configure the max file "
        "size to move between groups [ default=16G ]\n"
        "space config <space-name> space.groupbalancer.file_attempts=<#>       "
        ": configure the no of attempts to find a file within sizes [ "
        "default=50 ]\n"
        "space config <space-name> space.groupbalancer.threshold=<threshold>   "
        ": [Deprecated use <..>.min/max_threshold (see above)] configure the "
        "threshold when a group is balanced\n"
        "space config <space-name> space.groupbalancer.blocklist=<list>        "
        ": comma list eg. group1, group2 of groups blocklisted (only available "
        "for freespace engine)\n"
        "space config <space-name> space.geobalancer=on|off                    "
        ": enable/disable the geo balancer [ default=off ]\n"
        "space config <space-name> space.geobalancer.ntx=<ntx>                 "
        ": configure the number of parallel geobalancer jobs [ default=0 ]\n"
        "space config <space-name> space.geobalancer.threshold=<threshold>     "
        ": configure the threshold when a geotag is balanced [ default=0 ] \n"
        "space config <space-name> space.groupdrainer=on|off                   "
        ": enable/disable the group drainer [ default=on ]\n"
        "space config <space-name> space.groupdrainer.threshold=<threshold>    "
        ": configure the threshold(%%) for picking target groups\n"
        "space config <space-name> space.groupdrainer.group_refresh_interval   "
        ": configure time in seconds for refreshing cached groups info "
        "[default=300]\n"
        "space config <space-name> space.groupdrainer.retry_interval           "
        ": configure time in seconds for retrying failed drains "
        "[default=4*3600]\n"
        "space config <space-name> space.groupdrainer.retry_count              "
        ": configure the amount of retries for failed drains [default=5]\n"
        "space config <space-name> space.groupdrainer.ntx                      "
        ": configure the max file transfer queue size [default=10000]\n"
        "space config <space-name> space.lru=on|off                            "
        ": enable/disable the LRU policy engine [ default=off ]\n"
        "space config <space-name> space.lru.interval=<sec>                    "
        ": configure the default lru scan interval\n"
        "space config <space-name> fs.max.ropen=<n>                            "
        ": allow more than <n> read streams per disk in the given space\n"
        "space config <space-name> fs.max.wopen=<n>                            "
        ": allow more than <n> write streams per disk in the given space\n"
        "space config <space-name> space.wfe=on|off|paused                     "
        ": enable/disable the Workflow Engine [ default=off ]\n"
        "space config <space-name> space.wfe.interval=<sec>                    "
        ": configure the default WFE scan interval\n"
        "space config <space-name> space.headroom=<size>                       "
        ": configure the default disk headroom if not defined on a filesystem "
        "(see fs for details)\n"
        "space config <space-name> space.scaninterval=<sec>                    "
        ": configure the default scan interval if not defined on a filesystem "
        "(see fs for details)\n"
        "space config <space-name> space.scan_rain_interval=<sec>              "
        ": configure the default rain scan interval if not defined on a "
        "filesystem (see fs for details)\n"
        "space config <space-name> space.scanrate=<MB/S>                       "
        ": configure the default scan rate if not defined on a filesystem     "
        "(see fs for details)\n"
        "space config <space-name> space.scan_disk_interva=<sec>               "
        ": time interval after which the disk scanner will run, default 4h\n"
        "space config <space-name> space.scan_ns_interval=<sec>                "
        ": time interval after which the namespace scanner will run, default 3 "
        "days\n"
        "space config <space-name> space.scan_ns_rate=entry/sec                "
        ": namespace scan rate in terms of number of stat requests per second "
        "done against the local disk\n"
        "space config <space-name> space.scheduler.type=<type>                 "
        ": configure the default scheduler for space, eg. geo, roundrobin, "
        "weightedrandom etc\n"
        "space config <space-name> space.drainperiod=<sec>                     "
        ": configure the default drain  period if not defined on a filesystem "
        "(see fs for details)\n"
        "space config <space-name> space.graceperiod=<sec>                     "
        ": configure the default grace  period if not defined on a filesystem "
        "(see fs for details)\n"
        "space config <space-name> space.filearchivedgc=on|off                 "
        ": enable/disable the 'file archived' garbage collector [ default=off "
        "]\n"
        "space config <space-name> space.tracker=on|off                        "
        ": enable/disable the space layout creation tracker [ default=off ]\n"
        "space config <space-name> space.inspector=on|off                      "
        ": enable/disable the file inspector [ default=off ]\n"
        "space config <space-name> space.inspector.interval=<sec>              "
        ": time interval after which the inspector will run, default 4h\n"
        "space config <space-name> space.inspector.price.currency=[0-5]        "
        ": currency printed by the cost evaluation ( 0=EOS, 1=CHF, 2=EUR, "
        "3=USD, 4=AUD, 5=YEN )\n"
        "space config <space-name> space.inspector.price.disk.tbyear=<price>   "
        ": set the price of a tb year of data on disk without redundancy "
        "(default=20)\n"
        "space config <space-name> space.inspector.price.tape.tbyear=<price>   "
        ": set the price of a tb year of data on disk without redundancy "
        "(default=10)\n"
        "space config <space-name> space.geo.access.policy.write.exact=on|off  "
        ": if 'on' use exact matching geo replica (if available), 'off' uses "
        "weighting [ for write case ]\n"
        "space config <space-name> space.geo.access.policy.read.exact=on|off   "
        ": if 'on' use exact matching geo replica (if available), 'off' uses "
        "weighting [ for read  case ]\n"
        "space config <space-name> fs.<key>=<value>                            "
        ": configure file system parameters for each filesystem in this space "
        "(see help of 'fs config' for details)\n"
        "space config <space-name> "
        "space.policy.[layout|nstripes|checksum|blockchecksum|blocksize|bw|"
        "schedule|iopriority|iotype]=<value>      \n"
        "                                                                      "
        ": configure default file layout creation settings as a space policy - "
        "a value='remove' deletes the space policy\n\n"
        "space config <space-name> space.altxs=on|off                          "
        ": enable/disable the alternative checksums computation when the file "
        "is uploaded\n"
        "space config <space-name> space.policy.recycle=on\n"
        "                                                                      "
        ": globally enforce using always a recycle bin\n\n"
        "TAPE REST API specific parameters:\n"
        "space config default tape.rest.api.switch=on|off                      "
        "         : enable/disable the tape REST API handler [ default=off ]\n"
        "space config default tape.rest.api.stage.switch=on|off                "
        "         : enable/disable the tape REST API STAGE resource [ "
        "default=off ]\n\n"
        "Tape specific configuration parameters:\n"
        "space config <space-name> space.tgc.qry.period.secs=<#>               "
        "  : tape-aware GC query period in seconds [ default=3600 ]\n"
        "                                                                      "
        "  => value must be > 0 and <= 86400\n"
        "space config <space-name> space.tgc.free.bytes.script=<path>          "
        "  : optional path to a script used to determine the number of free "
        "bytes in a given EOS space [ "
        "default='/usr/sbin/eos-get-free-bytes-in-space.sh' ]\n"
        "                                                                      "
        "  => an empty or invalid path means the compile time default way of "
        "determining free space will be used\n"
        "space config <space-name> space.tgc.avail.bytes=<#>                   "
        " : configure the number of available bytes the space should have [ "
        "default=0 ] \n"
        "space config <space-name> space.tgc.total.bytes=<#>                   "
        " : configure the total number of bytes the space should have before "
        "the tape-aware GC kicks in [ default=0 ] \n\n"
        "space config rm <space-name> <key>                   : remove the "
        "given key from the space configuration\n\n"
        "space define <space-name> [<groupsize> [<groupmod>]] : define how "
        "many filesystems can end up in one scheduling group <groupsize> [ "
        "default=0 ]\n"
        "                                                       => "
        "<groupsize>=0 means that no groups are built within a space, "
        "otherwise it should be the maximum number of nodes in a scheduling "
        "group\n"
        "                                                       => <groupmod> "
        "maximum number of groups in the space, which should be at least equal "
        "to the maximum number of filesystems per node\n\n"
        "space inspector [--current|-c] [--last|-l] [-m] [-p] [-e] [-s|--space "
        "<space_name>] [--all|-a] [--cost|-C] [--usage|-U] [--birth|-B] "
        "[--access|-A] [--vs|-V] [--layouts|-L] : show namespace inspector "
        "output\n"
        "\t  -c  : show current scan\n"
        "\t  -l  : show last complete scan\n"
        "\t  -m  : print last scan in monitoring format ( by default this "
        "enables --cost --usage --birth --access --layouts)\n"
        "\t  -A  : combined with -m prints access time distributions\n"
        "\t  -V  : combined with -m prints birth time vs access time "
        "distributions\n"
        "\t  -B  : combined with -m prints birth time distributions\n"
        "\t  -C  : combined with -m prints cost information (storage price per "
        "user/group)\n"
        "\t  -U  : combined with -m prints usage information (stored bytes per "
        "user/group)\n"
        "\t  -L  : combined with -m prints layout statistics\n"
        "\t  -a  : combined with -m or -C or -U removes the restriction to "
        "show only the top 10 user ranking\n"
        "\t  -p  : combined with -c or -l lists erroneous files\n"
        "\t  -e  : combined with -c or -l exports erroneous files on the MGM "
        "into /var/log/eos/mgm/FileInspector.<date>.list\n"
        "\t  -s  : select target space, by default \"default\" space is "
        "used\n\n"
        "space node-set <space-name> <node.key> <file-name> : store the "
        "contents of <file-name> into the node configuration variable "
        "<node.key> visible to all FSTs\n"
        "                                                     => if "
        "<file-name> matches file:<path> the file is loaded from the MGM and "
        "not from the client\n"
        "                                                     => local files "
        "cannot exceed 512 bytes - MGM files can be arbitrary length\n"
        "                                                     => the contents "
        "gets base64 encoded by default\n\n"
        "space node-get <space-name> <node.key> : get the value of <node.key> "
        "and base64 decode before output\n"
        "                                         => if the value for "
        "<node.key> is identical for all nodes in the referenced space, it is "
        "dumped only once, otherwise the value is dumped for each node "
        "separately\n\n"
        "space reset <space-name> "
        "[--egroup|mapping|drain|scheduledrain|schedulebalance|ns|"
        "nsfilesystemview|nsfilemap|nsdirectorymap] : reset different space "
        "attributes\n"
        "\t            --egroup : clear cached egroup information\n"
        "\t           --mapping : clear all user/group uid/gid caches\n"
        "\t             --drain : reset draining\n"
        "\t     --scheduledrain : reset drain scheduling map\n"
        "\t   --schedulebalance : reset balance scheduling map\n"
        "\t                --ns : resize all namespace maps\n"
        "\t  --nsfilesystemview : resize namespace filesystem view\n"
        "\t         --nsfilemap : resize namespace file map\n"
        "\t    --nsdirectorymap : resize namespace directory map\n\n"
        "space status <space-name> [-m] : print all defined variables for "
        "space\n\n"
        "space tracker : print all file replication tracking entries\n\n"
        "space set <space-name> on|off : enable/disable all groups under that "
        "space\n"
        "                                => <on> value will enable all nodes, "
        "<off> value won't affect nodes\n\n"
        "space rm <space-name> : remove space\n\n"
        "space quota <space-name> on|off : enable/disable quota\n\n"
        "space groupbalancer status <space-name> [--detail(-d)|-m] : print "
        "groupbalancer status\n\n"
        "space groupdrainer status <space-name> [--detail(-d)|-m]  : print "
        "groupdrainer status\n"
        "space groupdrainer reset <space-name> <--failed|--all>    : reset "
        "failed transfers/all caches\n");
  }
};
} // namespace

void
RegisterSpaceProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SpaceProtoCommand>());
}
