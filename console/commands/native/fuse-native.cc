// ----------------------------------------------------------------------
// File: fuse-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace {
class FuseCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fuse";
  }
  const char*
  description() const override
  {
    return "Fuse Mounting";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    if (interactive) {
      fprintf(stderr, "error: don't call <fuse> from an interactive shell - "
                      "call via 'eos fuse ...'!\n");
      global_retc = -1;
      return 0;
    }
    if (!args.empty()) {
      std::ostringstream oss;
      for (size_t i = 0; i < args.size(); ++i) {
        if (i)
          oss << ' ';
        oss << args[i];
      }
      if (wants_help(oss.str().c_str())) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
    }
    if (args.empty() || (args[0] != "mount" && args[0] != "umount")) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString cmd = args[0].c_str();
    XrdOucString mountpoint = (args.size() > 1 ? args[1].c_str() : "");
    XrdCl::URL url(serveruri.c_str());
    XrdOucString params = "fsname=";
    params += (url.GetHostName() == "localhost") ? "localhost.localdomain"
                                                 : url.GetHostName().c_str();
    params += ":";
    params += url.GetPath().c_str();
    if (cmd == "mount") {
      if (!mountpoint.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      struct stat buf {};
      if (stat(mountpoint.c_str(), &buf)) {
        XrdOucString createdir = "mkdir -p ";
        createdir += mountpoint;
        createdir += " >& /dev/null";
        fprintf(stderr, ".... trying to create ... %s\n", mountpoint.c_str());
        int rc = system(createdir.c_str());
        if (WEXITSTATUS(rc))
          fprintf(stderr, "error: creation of mountpoint failed");
      }
      if (stat(mountpoint.c_str(), &buf)) {
        fprintf(stderr, "error: cannot create mountpoint %s !\n",
                mountpoint.c_str());
        return -1;
      }
#ifdef __APPLE__
      params += " -onoappledouble,allow_root,defer_permissions,volname=EOS,"
                "iosize=65536,fsname=eos@cern.ch";
#endif
      fprintf(stderr, "===> Mountpoint   : %s\n", mountpoint.c_str());
      fprintf(stderr, "===> Fuse-Options : %s\n", params.c_str());
      XrdOucString mount;
      mount = "eosxd ";
      mount += mountpoint.c_str();
      mount += " -o";
      mount += params;
      mount += " >& /dev/null";
      int rc = system(mount.c_str());
      if (WEXITSTATUS(rc)) {
        fprintf(stderr, "error: failed mount\n");
        return -1;
      }
      fprintf(stderr, "info: successfully mounted EOS [%s] under %s\n",
              serveruri.c_str(), mountpoint.c_str());
    } else {
      if (!mountpoint.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
#ifdef __APPLE__
      XrdOucString umount = "umount -f ";
      umount += mountpoint.c_str();
      umount += " >& /dev/null";
#else
      XrdOucString umount = "fusermount -z -u ";
      umount += mountpoint.c_str();
#endif
      int rc = system(umount.c_str());
      if (WEXITSTATUS(rc))
        fprintf(stderr, "error: umount failed\n");
    }
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage:\n"
                    "  fuse mount <mount-point>\n"
                    "  fuse umount <mount-point>\n"
                    "Mount uses server URI to derive fsname and prepares the "
                    "mountpoint.\n");
  }
};
} // namespace

void
RegisterFuseNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FuseCommand>());
}
