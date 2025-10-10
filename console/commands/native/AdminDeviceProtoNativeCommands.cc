// ----------------------------------------------------------------------
// File: AdminDeviceProtoNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_daemon(char*);
extern int com_geosched(char*);
extern int com_inspector(char*);
extern int com_license(char*);
extern int com_map(char*);
extern int com_member(char*);
extern int com_accounting(char*);
extern int com_health(char*);
extern int com_reconnect(char*);
extern int com_report(char*);
extern int com_rtlog(char*);
extern int com_role(char*);
extern int com_scitoken(char*);
extern int com_tracker(char*);
extern int com_vid(char*);

namespace { template <int(*F)(char*), const char*(*N)(), const char*(*D)(), bool (*R)(const std::string&)> struct Gen;

static bool defaultRequires(const std::string& args) { return !wants_help(args.c_str()); }

class SimpleNativeCommand : public IConsoleCommand {
public:
  using CFunc = int(*)(char*);
  SimpleNativeCommand(const char* nm, const char* desc, CFunc f)
    : mName(nm), mDesc(desc?desc:""), mFunc(f) {}
  const char* name() const override { return mName.c_str(); }
  const char* description() const override { return mDesc.c_str(); }
  bool requiresMgm(const std::string& args) const override { return defaultRequires(args); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str();
    return mFunc((char*)joined.c_str());
  }
  void printHelp() const override {}
private:
  std::string mName; std::string mDesc; CFunc mFunc;
};
}

void RegisterAdminDeviceProtoNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("daemon",    "Handle service daemon",     &com_daemon));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("geosched",  "Geoscheduler Interface",    &com_geosched));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("inspector", "Interact with File Inspector", &com_inspector));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("license",   "Display Software License",  &com_license));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("map",       "Path mapping interface",    &com_map));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("member",    "Check Egroup membership",   &com_member));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("accounting","Accounting Interface",      &com_accounting));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("health",    "Health information about system", &com_health));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("reconnect", "Forces a re-authentication of the shell", &com_reconnect));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("report",    "Analyze report log files on the local machine", &com_report));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("rtlog",     "Get realtime log output from mgm & fst servers", &com_rtlog));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("role",      "Set the client role",       &com_role));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("scitoken",  "SciToken interface",        &com_scitoken));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("tracker",   "Interact with File Tracker", &com_tracker));
  CommandRegistry::instance().reg(std::make_unique<SimpleNativeCommand>("vid",       "Virtual ID System Configuration", &com_vid));
}

// Legacy symbols are provided by RemainingLegacyNativeCommands.cc


