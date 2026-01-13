// ----------------------------------------------------------------------
// File: ls-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "namespace/utils/Mode.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdPosix/XrdPosixXrootd.hh>
#include <dirent.h>
#include <memory>
#include <sstream>
#include <sys/stat.h>

namespace {
class LsCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "ls";
  }
  const char*
  description() const override
  {
    return "List a directory";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }

  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    // Setup parser
    ConsoleArgParser p;
    p.setProgramName("ls")
        .setDescription("list directory <path>")
        .allowCombinedShortOptions(true)
        .allowAttachedValue(true)
        .collectUnknownTokens(true);

    // Flags (no values)
    p.addOption({"", 'l', false, false, "", "show long listing", ""});
    p.addOption({"", 'y', false, false, "",
                 "show long listing with backend(tape) status", ""});
    // Note: '-lh' was accepted historically; '-h' alone shows help in legacy
    p.addOption({"", 'a', false, false, "", "show hidden files", ""});
    p.addOption({"", 'i', false, false, "", "add inode information", ""});
    p.addOption(
        {"", 'c', false, false, "", "add checksum value (implies -l)", ""});
    p.addOption(
        {"", 'n', false, false, "", "show numerical user/group ids", ""});
    p.addOption(
        {"", 'F', false, false, "", "append indicator '/' to directories", ""});
    p.addOption({"", 's', false, false, "",
                 "checks only if the directory exists without listing", ""});
    p.addOption(
        {"no-globbing", 'N', false, false, "", "disables globbing", ""});

    // Parse
    auto r = p.parse(args);

    // Help handling consistent with legacy (-h or --help)
    if (!args.empty() && (args[0] == "-h" || args[0] == "--help")) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    // Build mgm.option string
    std::string option;
    auto add_flag = [&](char c) {
      option += '-';
      option.push_back(c);
    };
    if (r.has("l"))
      add_flag('l');
    if (r.has("y"))
      add_flag('y');
    if (r.has("a"))
      add_flag('a');
    if (r.has("i"))
      add_flag('i');
    if (r.has("c")) {
      add_flag('c');
      if (!r.has("l"))
        add_flag('l');
    }
    if (r.has("n"))
      add_flag('n');
    if (r.has("F"))
      add_flag('F');
    if (r.has("s"))
      add_flag('s');
    if (r.has("no-globbing") || r.has("N")) {
      option += "-N";
    }

    // Determine path from positionals (join to allow spaces)
    std::ostringstream pathoss;
    for (size_t i = 0; i < r.positionals.size(); ++i) {
      if (i)
        pathoss << ' ';
      pathoss << r.positionals[i];
    }
    std::string path = pathoss.str();
    if (path.empty())
      path = gPwd.c_str();

    // Unescape blanks (legacy behaviour: replace "\\ " -> " ")
    {
      std::string replaced;
      replaced.reserve(path.size());
      for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] == '\\' && i + 1 < path.size() && path[i + 1] == ' ') {
          replaced.push_back(' ');
          ++i;
        } else
          replaced.push_back(path[i]);
      }
      path.swap(replaced);
    }

    XrdOucString xpath = path.c_str();

    // S3 scheme handling (legacy behaviour): as3:
    if (xpath.beginswith("as3:")) {
      XrdOucString hostport;
      XrdOucString protocol;
      XrdOucString sPath;
      const char* v = 0;
      if (!(v = eos::common::StringConversion::ParseUrl(xpath.c_str(), protocol,
                                                        hostport))) {
        fprintf(stderr, "error: illegal url <%s>\n", xpath.c_str());
        global_retc = EINVAL;
        return 0;
      }
      sPath = v;
      if (hostport.length())
        setenv("S3_HOSTNAME", hostport.c_str(), 1);

      XrdOucString envString = xpath;
      int qpos = 0;
      if ((qpos = envString.find("?")) != STR_NPOS) {
        envString.erase(0, qpos + 1);
        XrdOucEnv env(envString.c_str());
        if (env.Get("s3.key"))
          setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
        if (env.Get("s3.id"))
          setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
        xpath.erase(xpath.find("?"));
        sPath.erase(sPath.find("?"));
      }
      const char* cstr = getenv("S3_ACCESS_KEY");
      if (cstr)
        setenv("S3_SECRET_ACCESS_KEY", cstr, 1);
      cstr = getenv("S3_ACESSS_ID");
      if (cstr)
        setenv("S3_ACCESS_KEY_ID", cstr, 1);
      if (!getenv("S3_ACCESS_KEY_ID") || !getenv("S3_HOSTNAME") ||
          !getenv("S3_SECRET_ACCESS_KEY")) {
        fprintf(stderr, "error: you have to set the S3 environment variables "
                        "S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use "
                        "a URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
        exit(-1);
      }
      XrdOucString s3env;
      s3env = "env S3_ACCESS_KEY_ID=";
      s3env += getenv("S3_ACCESS_KEY_ID");
      s3env += " S3_HOSTNAME=";
      s3env += getenv("S3_HOSTNAME");
      s3env += " S3_SECRET_ACCESS_KEY=";
      s3env += getenv("S3_SECRET_ACCESS_KEY");
      XrdOucString s3arg = sPath.c_str();
      XrdOucString listcmd = "bash -c \"";
      listcmd += s3env;
      listcmd += " s3 list ";
      listcmd += s3arg;
      listcmd += " ";
      listcmd += "\"";
      global_retc = system(listcmd.c_str());
      return 0;
    }

    // Local file or plain XRootD path handling
    if (xpath.beginswith("file:") || xpath.beginswith("root:")) {
      bool isXrd = xpath.beginswith("root:");
      XrdOucString protocol;
      XrdOucString hostport;
      XrdOucString sPath;
      const char* v = 0;
      if (!(v = eos::common::StringConversion::ParseUrl(xpath.c_str(), protocol,
                                                        hostport))) {
        global_retc = EINVAL;
        return 0;
      }
      sPath = v;
      std::string Path = v;
      if (sPath == "" && (protocol == "file")) {
        sPath = getenv("PWD");
        Path = getenv("PWD");
        if (!sPath.endswith("/")) {
          sPath += "/";
          Path += "/";
        }
      }
      XrdOucString url = "";
      eos::common::StringConversion::CreateUrl(
          protocol.c_str(), hostport.c_str(), Path.c_str(), url);
      DIR* dir =
          (isXrd) ? XrdPosixXrootd::Opendir(url.c_str()) : opendir(url.c_str());
      if (dir) {
        struct dirent* entry;
        while (
            (entry = (isXrd) ? XrdPosixXrootd::Readdir(dir) : readdir(dir))) {
          struct stat buf;
          XrdOucString curl = "";
          XrdOucString cpath = Path.c_str();
          cpath += entry->d_name;
          eos::common::StringConversion::CreateUrl(
              protocol.c_str(), hostport.c_str(), cpath.c_str(), curl);
          if (option.find("a") == std::string::npos) {
            if (entry->d_name[0] == '.')
              continue;
          }
          if (!((isXrd) ? XrdPosixXrootd::Stat(curl.c_str(), &buf)
                        : stat(curl.c_str(), &buf))) {
            if (option.find("l") == std::string::npos) {
              fprintf(stdout, "%s\n", entry->d_name);
            } else {
              char t_creat[14];
              char modestr[11];
              eos::modeToBuffer(buf.st_mode, modestr);
              XrdOucString suid = "";
              suid += (int)buf.st_uid;
              XrdOucString sgid = "";
              sgid += (int)buf.st_gid;
              XrdOucString sizestring = "";
              struct tm* t_tm;
              struct tm t_tm_local;
              t_tm = localtime_r(&buf.st_ctime, &t_tm_local);
              strftime(t_creat, 13, "%b %d %H:%M", t_tm);
              XrdOucString dirmarker = "";
              if (option.find("F") != std::string::npos)
                dirmarker = "/";
              if (modestr[0] != 'd')
                dirmarker = "";
              fprintf(stdout, "%s %3d %-8.8s %-8.8s %12s %s %s%s\n", modestr,
                      (int)buf.st_nlink, suid.c_str(), sgid.c_str(),
                      eos::common::StringConversion::GetSizeString(
                          sizestring, (unsigned long long)buf.st_size),
                      t_creat, entry->d_name, dirmarker.c_str());
            }
          }
        }
        (isXrd) ? XrdPosixXrootd::Closedir(dir) : closedir(dir);
      }
      global_retc = 0;
      return 0;
    }

    // EOS path via MGM
    XrdOucString ap = abspath(xpath.c_str());
    if (strlen(ap.c_str()) >= FILENAME_MAX) {
      fprintf(stderr, "error: path length longer than %i bytes", FILENAME_MAX);
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString esc =
        eos::common::StringConversion::curl_escaped(ap.c_str()).c_str();
    XrdOucString in = "mgm.cmd=ls";
    in += "&mgm.path=";
    in += esc;
    in += "&eos.encodepath=1";
    in += "&mgm.option=";
    in += option.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }

  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: ls [-laniyFN] [--no-globbing] <path>\n");
    fprintf(stderr, "list directory <path>\n\n");
    fprintf(stderr, "  -l : show long listing\n");
    fprintf(stderr, "  -y : show long listing with backend(tape) status\n");
    fprintf(stderr, "  -lh: show long listing with readable sizes\n");
    fprintf(stderr, "  -a : show hidden files\n");
    fprintf(stderr, "  -i : add inode information\n");
    fprintf(stderr, "  -c : add checksum value\n");
    fprintf(stderr, "  -n : show numerical user/group ids\n");
    fprintf(stderr, "  -F : append indicator '/' to directories \n");
    fprintf(stderr,
            "  -s : checks only if the directory exists without listing\n");
    fprintf(stderr, "  --no-globbing|-N : disables path globbing feature (e.g: "
                    "list a file containing '[]' characters)\n");
    fprintf(stderr, "         path=file:... : list on a local file system\n\n");
    fprintf(stderr, "path=root:... : list on a plain XRootD server (does not "
                    "work on native XRootD clusters\n");
    fprintf(
        stderr,
        "path=...      : all other paths are considered to be EOS paths!\n");
  }
};
} // namespace

void
RegisterLsNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LsCommand>());
}
