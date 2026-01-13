// ----------------------------------------------------------------------
// File: find-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NewfindHelper.hh"
#include <algorithm>
#include <memory>
#include <sstream>

namespace {
class FindProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "find";
  }
  const char*
  description() const override
  {
    return "Find files/directories";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
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
    // Reuse the same helper as the original newfind implementation
    NewfindHelper finder(*ctx.globalOpts);
    // Special schemes handled locally
    if (joined.find("root://") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("root://"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindXroot(path);
      return 0;
    } else if (joined.find("file:") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("file:"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindXroot(path);
      return 0;
    } else if (joined.find("as3:") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("as3:"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindAs3(path);
      return 0;
    }
    if (!finder.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = finder.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stdout,
        " usage\n"
        "find/newfind [OPTIONS] <path> : find files and directories\n"
        "OPTIONS can be filters, actions, or output modifiers for the found "
        "items\n"
        "Filters: [--maxdepth <n>] [--name <pattern>] [-f] [-d] [-0] [-g] "
        "[-uid <n>] [-nuid <n>]\n"
        "         [-gid <n>] [-ngid <n>] [-flag <n>] [-nflag <n>] "
        "[--ctime|--mtime +<n>|-<n>]\n"
        "         [-x <key>=<val>] [--faultyacl] [--stripediff]\n"
        "\t       --maxdepth <n> : descend only <n> levels\n"
        "\t     --name <pattern> : find by name, filtering by 'egrep' style "
        "regex match\n"
        "\t                -f,-d : find only files(-f) or directories (-d) in "
        "<path>\n"
        "\t                   -0 : find 0-size files only\n"
        "\t                   -g : find files with mixed scheduling groups\n"
        "\t   -uid <n>,-nuid <n> : find entries owned / not owned by a given "
        "user id number\n"
        "\t   -gid <n>,-ngid <n> : find entries owned / not owned by a given "
        "group id number\n"
        "\t -flag <n>,-nflag <n> : find entries with / without specified UNIX "
        "access flag, e.g. 755\n"
        "\t   --ctime <+n>, <-n> : find files with ctime older (+n) or younger "
        "(-n) than <n> days\n"
        "\t   --mtime <+n>, <-n> : find files with mtime older (+n) or younger "
        "(-n) than <n> days\n"
        "\t       -x <key>=<val> : find entries with <key>=<val>\n"
        "\t          --faultyacl : find files and directories with illegal "
        "ACLs\n"
        "\t         --stripediff : find files that do not have the nominal "
        "number of stripes(replicas)\n"
        "\t  --skip-version-dirs : skip version directories in the traversed "
        "hierarchy\n\n"
        "Actions: [-b] [--layoutstripes <n>] [--purge <n> ] [--fileinfo] "
        "[--format formatlist] [--cache] [--du]\n"
        "\t                   -b : query the server balance of the files "
        "found\n"
        "\t  --layoutstripes <n> : apply new layout with <n> stripes to the "
        "files found\n"
        "\t --purge <n> | atomic : remove versioned files keeping <n> versions "
        "(use --purge 0 to remove all old versions)\n"
        "\t                        To apply the settings of the extended "
        "attribute definition use --purge -1\n"
        "\t                        To remove all atomic upload left-overs "
        "older than a day use --purge atomic\n"
        "\t         [--fileinfo] : invoke `eos fileinfo` on the entry\n"
        "\t              --count : print aggregated number of file and "
        "directory including the search path\n"
        "\t         --childcount : print the number of children in each "
        "directory\n"
        "\t          --treecount : print the aggregated number of filesand "
        "directory children excluding the search path\n"
        "\t             --format : print with the given komma separated format "
        "list, redundant switches like\n"
        "\t                        --uid --checksum, which can be specified "
        "via the format are automatically disabled.\n"
        "\t                        Possible values for format tags are: "
        "uid,gid,size,checksum,checksumtype,etag,fxid,\n"
        "\t                        "
        "pxid,cxid,fid,pid,cid,atime,btime,ctime,mtime,type,mode,files,link,"
        "directories,\n"
        "\t                        attr.*,attr.<name> e.g. attr.sys.acl !\n"
        "\t              --cache : store all found entries in the in-memory "
        "namespace cache\n"
        "\t                 --du : create du-style output\n\n"
        "Output mode: [--xurl] [-p <key>] [--nrep] [--nunlink] [--size] "
        "[--online] [--hosts]\n"
        "             [--partition] [--fid] [--fs] [--checksum] [--ctime] "
        "[--mtime] [--uid] [--gid]\n"
        "\t                : print out the requested meta data as key value "
        "pairs\n"
        "The <path> argument ca be:\n"
        "\t path=file:...  :  do a find in the local file system (options "
        "ignored) - 'file:' is the current working directory\n"
        "\t path=root:...  :  do a find on a plain XRootD server (options "
        "ignored) - does not work on native XRootD clusters\n"
        "\t path=as3:...   :  do a find on an S3 bucket\n"
        "\t path=...       :  all other paths are considered to be EOS "
        "paths!\n");
  }
};

// Provide 'newfind' alias to the same implementation as 'find'
class NewfindAliasCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "newfind";
  }
  const char*
  description() const override
  {
    return "Find files/directories (new)";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    IConsoleCommand* findCmd = CommandRegistry::instance().find("find");
    if (!findCmd) {
      fprintf(stderr, "error: 'find' command not available\n");
      global_retc = EINVAL;
      return 0;
    }
    return findCmd->run(args, ctx);
  }
  void
  printHelp() const override
  {
    // Delegate to 'find' help
    IConsoleCommand* findCmd = CommandRegistry::instance().find("find");
    if (findCmd)
      findCmd->printHelp();
  }
};
} // namespace

void
RegisterFindProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FindProtoCommand>());
  CommandRegistry::instance().reg(std::make_unique<NewfindAliasCommand>());
}
