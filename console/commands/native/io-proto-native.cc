// ----------------------------------------------------------------------
// File: io-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protoio(char*);

namespace {
class IoProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "io"; }
  const char* description() const override { return "IO Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_protoio((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            " usage:\n\n"
            "io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x] [--ss] [--sa] [--si] : print io statistics\n"
            "\t  -l : show summary information (this is the default if -a,-t,-d,-x is not selected)\n"
            "\t  -a : break down by uid/gid\n"
            "\t  -m : print in <key>=<val> monitoring format\n"
            "\t  -n : print numerical uid/gids\n"
            "\t  -t : print top user stats\n"
            "\t  -d : break down by domains\n"
            "\t  -x : break down by application\n"
            "\t  --ss : show table with transfer sample statistics\n"
            "\t  --sa : start collection of statistics given number of seconds ago\n"
            "\t  --si : collect statistics over given interval of seconds\n"
            "\t  Note: this tool shows data for finished transfers only (using storage node reports)\n"
            "\t  Example: asking for data of finished transfers which were transferred during interval [now - 180s, now - 120s]:\n"
            "\t           eos io stat -x --sa 120 --si 60\n\n"
            "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io statistics\n"
            "\t         no arg. : start the colleciton thread\n"
            "\t              -r : enable collection of io reports\n"
            "\t              -p : enable popularity accounting\n"
            "\t              -n : enable report namespace\n"
            "\t --udp <address> : add a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l)\n\n"
            "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of io statistics\n"
            "\t         no arg. : stop the collection thread\n"
            "\t              -r : disable collection of io reports\n"
            "\t              -p : disable popularity accounting\n"
            "\t              -n : disable report namespace\n"
            "\t --udp <address> : remove a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l)\n\n"
            "io report <path> : show contents of report namespace for <path>\n\n"
            "io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f] : show namespace IO ranking (popularity)\n"
            "\t      -a :  don't limit the output list\n"
            "\t      -n :  show ranking by number of accesses\n"
            "\t      -b :  show ranking by number of bytes\n"
            "\t    -100 :  show the first 100 in the ranking\n"
            "\t   -1000 :  show the first 1000 in the ranking\n"
            "\t  -10000 :  show the first 10000 in the ranking\n"
            "\t      -w :  show history for the last 7 days\n"
            "\t      -f :  show the 'hotfiles' which are the files with highest number of present file opens\n");
  }
};
}

void RegisterIoProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<IoProtoCommand>());
}


