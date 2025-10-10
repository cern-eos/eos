// ----------------------------------------------------------------------
// File: file-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class FileCommand : public IConsoleCommand {
public:
  const char* name() const override { return "file"; }
  const char* description() const override { return "File Handling"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty()) { printHelp(); global_retc = EINVAL; return 0; }
    XrdOucString cmd = args[0].c_str();
    std::vector<std::string> rest(args.begin()+1, args.end());
    XrdOucString in = "mgm.cmd=file";

    auto set_path_or_id = [&](XrdOucString path){
      if (Path2FileDenominator(path)) { in += "&mgm.file.id="; in += path; }
      else { path = abspath(path.c_str()); in += "&mgm.path="; in += path; }
    };

    if (cmd == "rename") {
      if (rest.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      in += "&mgm.subcmd=rename"; XrdOucString p = rest[0].c_str(); set_path_or_id(p); in += "&mgm.file.source="; in += p; in += "&mgm.file.target="; in += abspath(rest[1].c_str());
    } else if (cmd == "symlink") {
      if (rest.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      in += "&mgm.subcmd=symlink"; XrdOucString p = rest[0].c_str(); set_path_or_id(p); in += "&mgm.file.source="; in += p; in += "&mgm.file.target="; in += abspath(rest[1].c_str());
    } else if (cmd == "drop") {
      if (rest.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=drop"; set_path_or_id(p); in += "&mgm.file.fsid="; in += rest[1].c_str();
      if (rest.size() > 2 && rest[2] == "-f") in += "&mgm.file.force=1";
    } else if (cmd == "move") {
      if (rest.size() < 3) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=move"; set_path_or_id(p); in += "&mgm.file.sourcefsid="; in += rest[1].c_str(); in += "&mgm.file.targetfsid="; in += rest[2].c_str();
    } else if (cmd == "replicate") {
      if (rest.size() < 3) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=replicate"; set_path_or_id(p); in += "&mgm.file.sourcefsid="; in += rest[1].c_str(); in += "&mgm.file.targetfsid="; in += rest[2].c_str();
    } else if (cmd == "purge" || cmd == "version") {
      if (rest.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      in += "&mgm.subcmd="; in += cmd; XrdOucString p = rest[0].c_str(); in += "&mgm.path="; in += abspath(p.c_str()); in += "&mgm.purge.version="; if (rest.size() > 1) in += rest[1].c_str(); else in += "-1";
    } else if (cmd == "versions") {
      if (rest.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=versions"; set_path_or_id(p); in += "&mgm.grab.version="; in += (rest.size() > 1 ? rest[1].c_str() : "-1");
    } else if (cmd == "layout") {
      if (rest.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=layout"; set_path_or_id(p);
      if (rest[1] == "-stripes" && rest.size() > 2) { in += "&mgm.file.layout.stripes="; in += rest[2].c_str(); }
      else if (rest[1] == "-checksum" && rest.size() > 2) { in += "&mgm.file.layout.checksum="; in += rest[2].c_str(); }
      else if (rest[1] == "-type" && rest.size() > 2) { in += "&mgm.file.layout.type="; in += rest[2].c_str(); }
      else { printHelp(); global_retc = EINVAL; return 0; }
    } else if (cmd == "tag") {
      if (rest.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=tag"; set_path_or_id(p); in += "&mgm.file.tag.fsid="; in += rest[1].c_str();
    } else if (cmd == "convert") {
      if (rest.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=convert"; set_path_or_id(p);
      if (rest.size() > 1) { in += "&mgm.convert.layout="; in += rest[1].c_str(); }
      if (rest.size() > 2) { in += "&mgm.convert.space="; in += rest[2].c_str(); }
      if (rest.size() > 3) { in += "&mgm.convert.placementpolicy="; in += rest[3].c_str(); }
      if (rest.size() > 4) { in += "&mgm.convert.checksum="; in += rest[4].c_str(); }
      // Option handling (legacy supported --rewrite; --sync not supported)
      if (rest.size() > 5) {
        for (size_t i = 5; i < rest.size(); ++i) {
          if (rest[i] == "--rewrite") { in += "&mgm.option=rewrite"; }
          else if (rest[i] == "--sync") { fprintf(stderr, "error: --sync is currently not supported\n"); printHelp(); global_retc = EINVAL; return 0; }
        }
      }
    } else if (cmd == "verify") {
      if (rest.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=verify&mgm.path="; in += abspath(p.c_str());
      for (size_t i = 1; i < rest.size(); ++i) {
        const std::string& opt = rest[i];
        if (opt == "-checksum") in += "&mgm.file.compute.checksum=1";
        else if (opt == "-commitchecksum") in += "&mgm.file.commit.checksum=1";
        else if (opt == "-commitsize") in += "&mgm.file.commit.size=1";
        else if (opt == "-commitfmd") in += "&mgm.file.commit.fmd=1";
        else if (opt == "-rate") { if (i+1 < rest.size()) { in += "&mgm.file.verify.rate="; in += rest[++i].c_str(); } else { printHelp(); global_retc = EINVAL; return 0; } }
        else if (opt == "-resync") in += "&mgm.file.resync=1";
        else { // treat as filter fsid if numeric
          in += "&mgm.file.verify.filterid="; in += opt.c_str();
        }
      }
    } else if (cmd == "share") {
      if (rest.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=share&mgm.path="; in += abspath(p.c_str());
      unsigned long long expires = (28ull * 86400ull);
      if (rest.size() > 1) { in += "&mgm.file.expires="; in += rest[1].c_str(); }
      else { char buf[64]; snprintf(buf, sizeof(buf), "%llu", expires); in += "&mgm.file.expires="; in += buf; }
    } else if (cmd == "workflow") {
      if (rest.size() < 3) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = rest[0].c_str(); in += "&mgm.subcmd=workflow&mgm.path="; in += abspath(p.c_str()); in += "&mgm.workflow="; in += rest[1].c_str(); in += "&mgm.event="; in += rest[2].c_str();
    } else {
      printHelp(); global_retc = EINVAL; return 0;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterFileNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FileCommand>());
}


