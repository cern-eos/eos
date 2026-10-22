// ----------------------------------------------------------------------
// File: ls-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "namespace/utils/Mode.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdPosix/XrdPosixXrootd.hh>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <memory>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

extern char** environ;

namespace {
struct LsOptions {
  CLI::App app{"List directories or files", "ls"};
  bool longListing = false, backend = false, all = false, inode = false;
  bool checksum = false, readable = false, directory = false, numeric = false;
  bool classify = false, exists = false, reverse = false, time = false;
  bool noGlobbing = false;
  std::vector<std::string> paths;

  LsOptions()
  {
    app.set_help_flag("--help", "Print this help message and exit");
    app.add_flag("-l", longListing, "Show long listing");
    app.add_flag("-y", backend, "Show long listing with backend (tape) status");
    app.add_flag("-a", all, "Show hidden files");
    app.add_flag("-i", inode, "Add inode information");
    app.add_flag("-c", checksum, "Add checksum value (implies -l)");
    app.add_flag("-h", readable,
                 "Show readable sizes in the long listing; alone, show help");
    app.add_flag("-d", directory,
                 "List directories themselves instead of their contents");
    app.add_flag("-n", numeric, "Show numeric user/group IDs");
    app.add_flag("-F", classify, "Append '/' to directories");
    app.add_flag("-s", exists, "Check directory existence without listing");
    app.add_flag("-N,--no-globbing", noGlobbing, "Disable globbing");
    app.add_flag("-r", reverse, "Accepted for compatibility; ignored");
    app.add_flag("-t", time, "Accepted for compatibility; ignored");
    app.add_option("path", paths, "Namespace paths (default: current directory)")
        ->check(eos::console::NonEmptyArgument);
    app.footer("file:... lists a local directory; root:... lists a plain XRootD server; "
               "as3:... lists an S3 path. Other paths refer to EOS.");
  }
};

bool
LsWantsHelp(const std::vector<std::string>& args)
{
  const auto delimiter = std::find(args.begin(), args.end(), "--");
  return std::find(args.begin(), delimiter, "--help") != delimiter ||
         (args.size() == 1 && args.front() == "-h");
}

int
ListPath(const std::string& path, const std::string& option, CommandContext& ctx)
{
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
      return EINVAL;
    }
    sPath = v;
    if (hostport.length()) {
      setenv("S3_HOSTNAME", hostport.c_str(), 1);
    }

    XrdOucString envString = xpath;
    int qpos = 0;
    if ((qpos = envString.find("?")) != STR_NPOS) {
      envString.erase(0, qpos + 1);
      XrdOucEnv env(envString.c_str());
      if (env.Get("s3.key")) {
        setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
      }
      if (env.Get("s3.id")) {
        setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
      }
      xpath.erase(xpath.find("?"));
      sPath.erase(sPath.find("?"));
    }
    const char* cstr = getenv("S3_ACCESS_KEY");
    if (cstr) {
      setenv("S3_SECRET_ACCESS_KEY", cstr, 1);
    }
    cstr = getenv("S3_ACESSS_ID");
    if (cstr) {
      setenv("S3_ACCESS_KEY_ID", cstr, 1);
    }
    if (!getenv("S3_ACCESS_KEY_ID") || !getenv("S3_HOSTNAME") ||
        !getenv("S3_SECRET_ACCESS_KEY")) {
      fprintf(stderr, "error: you have to set the S3 environment variables "
                      "S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use "
                      "a URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
      return EINVAL;
    }
    char* arguments[] = {const_cast<char*>("s3"), const_cast<char*>("list"),
                         const_cast<char*>(sPath.c_str()), nullptr};
    pid_t child = 0;
    const int spawned = posix_spawnp(&child, "s3", nullptr, nullptr, arguments, environ);
    if (spawned) {
      fprintf(stderr, "error: cannot execute s3: %s\n", strerror(spawned));
      return spawned;
    }
    int status = 0;
    while (waitpid(child, &status, 0) < 0) {
      if (errno != EINTR) {
        return errno;
      }
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
  }

  // Local file or plain XRootD path handling
  if (xpath.beginswith("file:") || xpath.beginswith("root:")) {
    bool isXrd = xpath.beginswith("root:");
    if (isXrd && !xpath.beginswith("root://")) {
      return EINVAL;
    }
    XrdOucString protocol;
    XrdOucString hostport;
    XrdOucString sPath;
    const char* v = 0;
    if (!(v = eos::common::StringConversion::ParseUrl(xpath.c_str(), protocol,
                                                      hostport))) {
      return EINVAL;
    }
    sPath = v;
    std::string Path = v;
    if (sPath == "" && (protocol == "file")) {
      const char* cwd = getenv("PWD");
      if (!cwd) {
        return EINVAL;
      }
      sPath = cwd;
      Path = cwd;
      if (!sPath.endswith("/")) {
        sPath += "/";
        Path += "/";
      }
    }
    XrdOucString url = "";
    eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
                                             Path.c_str(), url);
    if (!Path.empty() && Path.back() != '/') {
      Path += '/';
    }
    DIR* dir = (isXrd) ? XrdPosixXrootd::Opendir(url.c_str()) : opendir(url.c_str());
    if (dir) {
      struct dirent* entry;
      while ((entry = (isXrd) ? XrdPosixXrootd::Readdir(dir) : readdir(dir))) {
        struct stat buf;
        XrdOucString curl = "";
        XrdOucString cpath = Path.c_str();
        cpath += entry->d_name;
        eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
                                                 cpath.c_str(), curl);
        if (option.find("a") == std::string::npos) {
          if (entry->d_name[0] == '.') {
            continue;
          }
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
            if (option.find("F") != std::string::npos) {
              dirmarker = "/";
            }
            if (modestr[0] != 'd') {
              dirmarker = "";
            }
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
    if (!dir) {
      const int status = errno ? errno : ENOENT;
      fprintf(stderr, "error: cannot list %s: %s\n", path.c_str(), strerror(status));
      return status;
    }
    return 0;
  }

  // EOS path via MGM
  XrdOucString ap = abspath(xpath.c_str());
  if (strlen(ap.c_str()) >= FILENAME_MAX) {
    fprintf(stderr, "error: path length longer than %i bytes", FILENAME_MAX);
    return EINVAL;
  }
  XrdOucString esc = eos::common::StringConversion::curl_escaped(ap.c_str()).c_str();
  XrdOucString in = "mgm.cmd=ls";
  in += "&mgm.path=";
  in += esc;
  in += "&eos.encodepath=1";
  in += "&mgm.option=";
  in += option.c_str();
  return ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
}

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
    std::vector<std::string> arguments;
    return eos::console::TokenizeArguments(args.c_str(), arguments) &&
           requiresMgmArguments(arguments);
  }
  bool
  requiresMgmArguments(const std::vector<std::string>& arguments) const override
  {
    if (LsWantsHelp(arguments)) {
      return false;
    }
    LsOptions options;
    std::vector<std::string> reversed(arguments.rbegin(), arguments.rend());
    try {
      options.app.parse(reversed);
    } catch (const CLI::ParseError&) {
      return false;
    }
    return options.paths.empty() ||
           std::any_of(options.paths.begin(), options.paths.end(),
                       [](const std::string& path) {
                         return path.rfind("file:", 0) != 0 &&
                                path.rfind("root:", 0) != 0 && path.rfind("as3:", 0) != 0;
                       });
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    LsOptions options;
    if (LsWantsHelp(args) || !eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    std::string flags;
    const auto add = [&](bool enabled, char flag) {
      if (enabled) {
        flags += '-';
        flags += flag;
      }
    };
    add(options.longListing || options.checksum, 'l');
    add(options.backend, 'y');
    add(options.all, 'a');
    add(options.inode, 'i');
    add(options.checksum, 'c');
    add(options.readable, 'h');
    add(options.directory, 'd');
    add(options.numeric, 'n');
    add(options.classify, 'F');
    add(options.exists, 's');
    add(options.noGlobbing, 'N');
    if (options.paths.empty()) {
      options.paths.emplace_back(gPwd.c_str());
    }
    int status = 0;
    for (const auto& path : options.paths) {
      const int result = ListPath(path, flags, ctx);
      if (!status) {
        status = result;
      }
    }
    global_retc = status;
    return 0;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    LsOptions options;
    return CompleteCliArguments(
        options.app, args, {},
        {{options.app.get_option("path"), CompletionPath::EosOrFile}});
  }
  std::string
  helpText() const override
  {
    return LsOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterLsNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LsCommand>());
}
