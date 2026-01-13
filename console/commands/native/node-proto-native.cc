// ----------------------------------------------------------------------
// File: node-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NodeHelper.hh"
#include <memory>
#include <sstream>

namespace {
class NodeProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "node";
  }
  const char*
  description() const override
  {
    return "Node configuration";
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
    NodeHelper helper(*ctx.globalOpts);
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
        "node ls [-s] [-b|--brief] [-m|-l|--sys|--io|--fsck] [<node>] : list "
        "all nodes or only <node>. <node> is a substring match and can be a "
        "comma seperated list\n"
        "\t      -s : silent mode\n"
        "\t      -b : display host names without domain names\n"
        "\t      -m : monitoring key=value output format\n"
        "\t      -l : long output - list also file systems after each node\n"
        "\t    --io : print IO statistics\n"
        "\t   --sys : print SYS statistics (memory + threads)\n"
        "\t  --fsck : print filesystem check statistcis\n\n"
        "node config <host:port> <key>=<value : configure file system "
        "parameters for each filesystem of this node\n"
        "\t    <key> : "
        "error.simulation=io_read|io_write|xs_read|xs_write|fmd_open|fake_"
        "write|close|unresponsive\n"
        "\t            If offset is given then the error will get triggered "
        "for requests past the given value.\n"
        "\t            Accepted format for offset: 8B, 10M, 20G etc.\n"
        "\t            fmd_open            : simulate a file metadata mismatch "
        "when opening a file\n"
        "\t            open_delay[_<sec>]  : add by default 120 sec delay per "
        "open operation\n"
        "\t            read_delay[_<sec>]  : add by default 10 sec delay per "
        "read operation\n"
        "\t            io_read[_<offset>]  : simulate read errors\n"
        "\t            io_write[_<offset>] : simulate write errors\n"
        "\t            xs_read             : simulate checksum errors when "
        "reading a file\n"
        "\t            xs_write[_<sec>]    : simulate checksum errors on write "
        "with an optional delay, default 0\n"
        "\t            fake_write          : do not really write data to disk\n"
        "\t            close               : return an error on close\n"
        "\t            close_commit_mgm    : simulate error during close "
        "commit to MGM\n"
        "\t            unresponsive        : emulate a write/close request "
        "taking 2 minutes\n"
        "\t            <none>              : disable error simulation (any "
        "value other than the previous ones is fine!)\n"
        "\t    <key> : publish.interval=<sec> - set the filesystem state "
        "publication interval to <sec> seconds\n"
        "\t    <key> : debug.level=<level>    - set the node into debug level "
        "<level> [default=notice] -> see debug --help for available levels\n"
        "\t    <key> : stripexs=on|off        - enable/disable synchronously "
        "stripe checksum computation\n"
        "\t    <key> : for other keys see help of 'fs config' for details\n\n"
        "node set <queue-name>|<host:port> on|off                 : "
        "activate/deactivate node\n\n"
        "node rm  <queue-name>|<host:port>                        : remove a "
        "node\n\n"
        "node txgw <queue-name>|<host:port> <on|off> : enable (on) or disable "
        "(off) node as a transfer gateway\n\n"
        "node proxygroupadd <group-name> <queue-name>|<host:port> : add a node "
        "to a proxy group\n\n"
        "node proxygrouprm <group-name> <queue-name>|<host:port> : rm a node "
        "from a proxy group\n\n"
        "node proxygroupclear <queue-name>|<host:port> : clear the list of "
        "groups a node belongs to\n\n"
        "node status <queue-name>|<host:port> : print's all defined variables "
        "for a node\n");
  }
};
} // namespace

void
RegisterNodeProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<NodeProtoCommand>());
}
