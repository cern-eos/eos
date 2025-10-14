// ----------------------------------------------------------------------
// File: ns-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_ns(char*);

namespace {
class NsProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "ns"; }
  const char* description() const override { return "Namespace Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_ns((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: ns [stat|mutex|compact|master|cache|benchmark]\n"
            "    print or configure basic namespace parameters\n"
            "  ns stat [-a] [-m] [-n] [--reset]\n"
            "    print namespace statistics\n"
            "    -a      : break down by uid/gid\n"
            "    -m      : display in monitoring format <key>=<value>\n"
            "    -n      : display numerical uid/gid(s)\n"
            "    --reset : reset namespace counters\n\n"
            "  ns mutex [<option>]\n"
            "    manage mutex monitoring. Option can be:\n"
            "    --toggletime     : toggle the timing\n"
            "    --toggleorder    : toggle the order\n"
            "    --toggledeadlock : toggle deadlock check\n"
            "    --smplrate1      : set timing sample rate at 1%% (default, no slow-down)\n"
            "    --smplrate10     : set timing sample rate at 10%% (medium slow-down)\n"
            "    --smplrate100    : set timing sample rate at 100%% (severe slow-down)\n"
            "    --setblockedtime <ms>\n"
            "                     : set minimum time when a mutex lock lasting longer than <ms> \n"
            "                       is reported in the log file [default=10000]\n\n"
            "  ns compact off|on <delay> [<interval>] [<type>]\n"
            "    enable online compaction after <delay> seconds\n"
            "    <interval> : if >0 then compaction is repeated automatically \n"
            "                 after so many seconds\n"
            "    <type>     : can be 'files', 'directories' or 'all'. By default  only the file\n"
            "                 changelog is compacted. The repair flag can be indicated by using:\n"
            "                 'files-repair', 'directories-repair' or 'all-repair'\n\n"
            "  ns master [<option>]\n"
            "    master/slave operations. Option can be:\n"
            "    <master_hostname> : set hostname of MGM master RW daemon\n"
            "    --log             : show master log\n"
            "    --log-clear       : clean master log\n"
            "    --enable          : enable the slave/master supervisor thread modifying stall/\n"
            "                        redirectorion rules\n"
            "    --disable         : disable supervisor thread\n\n"
            "  ns recompute_tree_size <path>|cid:<decimal_id>|cxid:<hex_id> [--depth <val>]\n"
            "    recompute the tree size of a directory and all its subdirectories\n"
            "    --depth : maximum depth for recomputation, default 0 i.e no limit\n\n"
            "  ns recompute_quotanode <path>|cid:<decimal_id>|cxid:<hex_id>\n"
            "    recompute the specified quotanode\n\n"
            "  ns update_quotanode <path>|cid:<decimal_id>|cxid:<hex_id> uid:<uid>|gid:<gid> bytes:<bytes> physicalbytes:<bytes> inodes:<inodes>\n"
            "    update the specified quotanode, with the specified (and unchecked) values\n\n"
            "  ns cache set|drop [-d|-f] [<max_num>] [<max_size>K|M|G...]\n"
            "    set the max number of entries or the max size of the cache. Use the\n"
            "    ns stat command to see the current values.\n"
            "    set        : update cache size for files or directories\n"
            "    drop       : drop cached file and/or directory entries\n"
            "    -d         : control the directory cache\n"
            "    -f         : control the file cache\n"
            "    <max_num>  : max number of entries\n"
            "    <max_size> : max size of the cache - not implemented yet\n\n"
            "  ns cache drop-single-file <id of file to drop>\n"
            "    force refresh of the given FileMD by dropping it from the cache\n\n"
            "  ns cache drop-single-container <id of container to drop>\n"
            "    force refresh of the given ContainerMD by dropping it from the cache\n\n"
            "  ns drain list|set [<key>=<value>]                                 \n"
            "    list : list the global drain configuration parameters           \n"
            "    set  : set one of the following drain configuration parameters  \n"
            "           max-thread-pool-size : max number of threads in drain pool\n"
            "                                  [default 100, minimum 5]          \n"
            "           max-fs-per-node      : max number of file systems per node that\n"
            "                                  can be drained in parallel [default 5]\n\n"
            "  ns reserve-ids <file id> <container id>\n"
            "    blacklist file and container IDs below the given threshold. The namespace\n"
            "    will not allocate any file or container with IDs less than, or equal to the\n"
            "    given blacklist thresholds.\n\n"
            "  ns benchmark <n-threads> <n-subdirs> <n-subfiles> [prefix=/benchmark]\n"
            "     run metadata benchmark inside the MGM - results are printed into the MGM logfile and the shell\n"
            "                n-threads  : number of parallel threads running a benchmark in the MGM\n"
            "                n-subdirs  : directories created by each threads\n"
            "                n-subfiles : number of files created in each sub-directory\n"
            "                prefix     : absolute directory where to write the benchmarkf iles - default is /benchmark\n\n"
            "     example: eos ns benchmark 100 10 10\n\n"
            " ns tracker list|clear --name tracker_type\n"
            "     list or clear the different file identifier trackers\n"
            "     tracker_type : one of the following: drain, balance, fsck, convert, all\n\n"
            " ns behaviour list|set|clear\n"
            "     modify the behaviour of internal mechanisms for the manager node\n"
            "     list                    : list all the behaviour changes enforced\n"
            "     set <behaviour> <value> : enforce given behavior\n"
            "     get <behaviour>         : get behaviour configuration\n"
            "     clear <behaviour>|all   : remove enforced behavior\n\n"
            "     The following behaviours are supported:\n"
            "       rain_min_fsid_entry : for RAIN files the entry server will deterministically\n"
            "         be the file system with the lowest fsid from the list of stripes\n"
            "         Accepted values: \"on\" or \"off\" [default off]\n");
  }
};
}

void RegisterNsProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<NsProtoCommand>());
}


