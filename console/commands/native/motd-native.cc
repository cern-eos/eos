// ----------------------------------------------------------------------
// File: motd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "common/SymKeys.hh"
#include <memory>
#include <sstream>
#include <unistd.h>
#include <fcntl.h>

namespace {
class MotdCommand : public IConsoleCommand {
public:
  const char* name() const override { return "motd"; }
  const char* description() const override { return "Message of the day"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    XrdOucString in = "mgm.cmd=motd";
    if (!args.empty()) {
      XrdOucString motdfile = args[0].c_str();
      int fd = open(motdfile.c_str(), O_RDONLY);
      if (fd >= 0) {
        char maxmotd[1024]; memset(maxmotd, 0, sizeof(maxmotd));
        size_t nread = read(fd, maxmotd, sizeof(maxmotd)); maxmotd[1023] = 0;
        XrdOucString b64out; if (nread > 0) { eos::common::SymKey::Base64Encode(maxmotd, strlen(maxmotd) + 1, b64out); }
        in += "&mgm.motd="; in += b64out.c_str();
        (void) close(fd);
      }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterMotdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MotdCommand>());
}


