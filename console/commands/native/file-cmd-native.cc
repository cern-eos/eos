// ----------------------------------------------------------------------
// File: file-native.cc
// ----------------------------------------------------------------------

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "common/FileId.hh"
#include <CLI/CLI.hpp>
#include "common/Fmd.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClURL.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <cstdlib>
#include <vector>
#include <cstring>
#include <errno.h>
#include <limits>
#include <memory>
#include <openssl/sha.h>
#include <set>
#include <sstream>
#include <time.h>

#ifdef __APPLE__
#define ECOMM 70
#endif

namespace {
std::string MakeFileHelp()
{
  return "Usage: file <subcmd> [args...]\n\n"
         "Subcommands:\n"
         "  adjustreplica <path|fid|fxid> [space [subgroup]] [--exclude-fs <fsid>]  adjust replica placement\n"
         "  check <path|fid|fxid> [%size%checksum%nrep%diskchecksum%force%output%silent]  verify replicas\n"
         "  convert <path|fid> [layout] [space] [policy] [checksum] [--rewrite]\n"
         "  copy [-f] [-s] [-c] <src> <dst>     synchronous third party copy\n"
         "  drop <path|fid> <fsid> [-f]          drop replica\n"
         "  info <identifier> [options]          show file info (path|fid:|fxid:|pid:|pxid:|inode:)\n"
         "  layout <path|fid> -stripes|-checksum|-type <val>  change layout\n"
         "  move <path|fid> <fsid1> <fsid2>      move replica between fsids\n"
         "  purge <path> [version]               purge versions\n"
         "  rename <src> <dst>                   rename path\n"
         "  rename_with_symlink <src> <dst-dir>  rename and create symlink\n"
         "  replicate <path|fid> <fsid1> <fsid2> replicate replica between fsids\n"
         "  share <path> [lifetime]              create share link\n"
         "  symlink <link-name> <target>         create symlink\n"
         "  tag <path|fid> +|-|~<fsid>           location tag ops\n"
         "  touch [-a] [-n] [-0] <path|fid|fxid> [linkpath|size [hexchecksum]]\n"
         "  touch -l <path|fid|fxid> [lifetime [audience=user|app]]\n"
         "  touch -u <path|fid|fxid>             remove lock\n"
         "  verify <path|fid> [opts]             verify file checks\n"
         "  version <path> [version]             create version\n"
         "  versions <path|fid> [grab-version]   list/grab versions\n"
         "  workflow <path> <workflow> <event>   trigger workflow\n";
}

void ConfigureFileApp(CLI::App& app, std::string& subcmd)
{
  app.name("file");
  app.description("File Handling");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeFileHelp();
      }));
  app.add_option("subcmd", subcmd,
                 "rename|rename_with_symlink|symlink|drop|touch|move|copy|"
                 "replicate|purge|version|versions|layout|tag|convert|verify|"
                 "adjustreplica|check|share|workflow|info")
      ->required();
}

void
AppendEncodedPath(XrdOucString& in, const XrdOucString& raw, bool absolutize)
{
  XrdOucString raw_copy = raw;
  if (raw_copy.beginswith("fid:") || raw_copy.beginswith("fxid:") ||
      raw_copy.beginswith("pid:") || raw_copy.beginswith("pxid:") ||
      raw_copy.beginswith("inode:") || raw_copy.beginswith("cid:") ||
      raw_copy.beginswith("cxid:")) {
    in += "&mgm.path=";
    in += raw;
    return;
  }
  XrdOucString path = raw;
  if (absolutize) {
    path = abspath(path.c_str());
  }
  XrdOucString esc =
      eos::common::StringConversion::curl_escaped(path.c_str()).c_str();
  in += "&mgm.path=";
  in += esc;
  in += "&eos.encodepath=1";
}

int
GetRemoteFmdFromLocalDb(const char* manager, const char* shexfid,
                        const char* sfsid, eos::common::FmdHelper& fmd)
{
  if ((!manager) || (!shexfid) || (!sfsid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getfmd&fst.getfmd.fid=";
  fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid=";
  fmdquery += sfsid;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug(
        "got replica file meta data from server %s for fxid=%s fsid=%s",
        manager, shexfid, sfsid);
  } else {
    rc = ECOMM;
    eos_static_err(
        "Unable to retrieve meta data from server %s for fxid=%s fsid=%s",
        manager, shexfid, sfsid);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info(
        "Unable to retrieve meta data on remote server %s for fxid=%s fsid=%s",
        manager, shexfid, sfsid);
    delete response;
    return ENODATA;
  }

  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(response->GetBuffer());

  if (!eos::common::EnvToFstFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }

  // very simple check
  if (fmd.mProtoFmd.fid() != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid "
                   "is %lu instead of %lu !",
                   fmd.mProtoFmd.fid(),
                   eos::common::FileId::Hex2Fid(shexfid));
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

int
RunFileCheck(XrdOucString path, const std::string& option, CommandContext& ctx)
{
  XrdOucString in = "mgm.cmd=file";

  bool absolutize = (!path.beginswith("fid:")) && (!path.beginswith("fxid:"));

  in += "&mgm.subcmd=getmdlocation";
  in += "&mgm.format=fuse";
  AppendEncodedPath(in, path, absolutize);

  // Eventually disable json format to avoid parsing issues
  bool old_json = json;
  if (old_json) {
    json = false;
  }

  XrdOucEnv* result = ctx.clientCommand(in, false, nullptr);

  if (old_json) {
    json = true;
  }

  if (!result) {
    fprintf(stderr, "error: getmdlocation query failed\n");
    global_retc = EINVAL;
    return 0;
  }

  int envlen = 0;
  std::unique_ptr<XrdOucEnv> newresult(new XrdOucEnv(result->Env(envlen)));
  delete result;

  if (!envlen) {
    fprintf(stderr, "error: couldn't get meta data information\n");
    global_retc = EIO;
    return 0;
  }

  char* ptr = newresult->Get("mgm.proc.retc");

  if (ptr) {
    int retc_getmdloc = 0;

    try {
      retc_getmdloc = std::stoi(ptr);
    } catch (...) {
      retc_getmdloc = EINVAL;
    }

    if (retc_getmdloc) {
      fprintf(stderr, "error: failed getmdlocation command, errno=%i",
              retc_getmdloc);
      global_retc = retc_getmdloc;
      return 0;
    }
  }

  XrdOucString ns_path = newresult->Get("mgm.nspath");
  XrdOucString checksumtype = newresult->Get("mgm.checksumtype");
  XrdOucString checksum = newresult->Get("mgm.checksum");
  uint64_t mgm_size = std::stoull(newresult->Get("mgm.size"));
  bool silent_cmd = ((option.find("%silent") != std::string::npos) ||
                     ctx.silent);

  if (!silent_cmd) {
    fprintf(stdout, "path=\"%s\" fxid=\"%4s\" size=\"%llu\" nrep=\"%s\" "
            "checksumtype=\"%s\" checksum=\"%s\"\n",
            ns_path.c_str(), newresult->Get("mgm.fid0"),
            (unsigned long long)mgm_size, newresult->Get("mgm.nrep"),
            checksumtype.c_str(), newresult->Get("mgm.checksum"));
  }

  std::string err_label;
  std::set<std::string> set_errors;
  int nrep_online = 0;
  int i = 0;

  for (i = 0; i < 255; ++i) {
    err_label = "none";
    XrdOucString repurl = "mgm.replica.url";
    repurl += i;
    XrdOucString repfid = "mgm.fid";
    repfid += i;
    XrdOucString repfsid = "mgm.fsid";
    repfsid += i;
    XrdOucString repbootstat = "mgm.fsbootstat";
    repbootstat += i;
    XrdOucString repfstpath = "mgm.fstpath";
    repfstpath += i;

    if (!newresult->Get(repurl.c_str())) {
      break;
    }

    // Query the FSTs for stripe info
    XrdCl::StatInfo* stat_info = 0;
    XrdCl::XRootDStatus status;
    std::ostringstream oss;
    oss << "root://" << newresult->Get(repurl.c_str()) << "//dummy";
    XrdCl::URL url(oss.str());

    if (!url.IsValid()) {
      fprintf(stderr, "error: URL is not valid: %s", oss.str().c_str());
      global_retc = EINVAL;
      return 0;
    }

    // Get XrdCl::FileSystem object
    std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

    if (!fs) {
      fprintf(stderr, "error: failed to get new FS object");
      global_retc = ECOMM;
      return 0;
    }

    XrdOucString bs = newresult->Get(repbootstat.c_str());
    bool down = (bs != "booted");

    if (down && (option.find("%force") == std::string::npos)) {
      err_label = "DOWN";
      set_errors.insert(err_label);

      if (!silent_cmd) {
        fprintf(stderr,
                "error: unable to retrieve file meta data from %s "
                "[ status=%s ]\n",
                newresult->Get(repurl.c_str()), bs.c_str());
      }

      continue;
    }

    // Do a remote stat using XrdCl::FileSystem
    uint64_t stat_size = std::numeric_limits<uint64_t>::max();
    XrdOucString statpath = newresult->Get(repfstpath.c_str());

    if (!statpath.beginswith("/")) {
      // base 64 encode this path
      XrdOucString statpath64;
      eos::common::SymKey::Base64(statpath, statpath64);
      statpath = "/#/";
      statpath += statpath64;
    }

    status = fs->Stat(statpath.c_str(), stat_info);

    if (!status.IsOK()) {
      err_label = "STATFAILED";
      set_errors.insert(err_label);
    } else {
      stat_size = stat_info->GetSize();
    }

    // Free memory
    delete stat_info;
    int retc = 0;
    eos::common::FmdHelper fmd;

    if ((retc = GetRemoteFmdFromLocalDb(newresult->Get(repurl.c_str()),
                                        newresult->Get(repfid.c_str()),
                                        newresult->Get(repfsid.c_str()),
                                        fmd))) {
      if (!silent_cmd) {
        fprintf(stderr, "error: unable to retrieve file meta data from %s [%d]\n",
                newresult->Get(repurl.c_str()), retc);
      }

      err_label = "NOFMD";
      set_errors.insert(err_label);
    } else {
      const auto& proto_fmd = fmd.mProtoFmd;
      XrdOucString cx = proto_fmd.checksum().c_str();

      for (unsigned int k = (cx.length() / 2); k < SHA256_DIGEST_LENGTH; ++k) {
        cx += "00";
      }

      std::string disk_cx = proto_fmd.diskchecksum().c_str();

      for (unsigned int k = (disk_cx.length() / 2); k < SHA256_DIGEST_LENGTH;
           ++k) {
        disk_cx += "00";
      }

      if (eos::common::LayoutId::IsRain(proto_fmd.lid()) == false) {
        // These checks make sense only for non-rain layouts
        if (proto_fmd.size() != mgm_size) {
          err_label = "SIZE";
          set_errors.insert(err_label);
        } else {
          if (proto_fmd.size() != (unsigned long long) stat_size) {
            err_label = "FSTSIZE";
            set_errors.insert(err_label);
          }
        }

        if (cx != checksum) {
          err_label = "CHECKSUM";
          set_errors.insert(err_label);
        }

        uint64_t disk_cx_val = 0ull;

        try {
          disk_cx_val = std::stoull(disk_cx.substr(0, 8), nullptr, 16);
        } catch (...) {
          // error during conversion
        }

        if ((disk_cx.length() > 0) && disk_cx_val &&
            ((disk_cx.length() < 8) || (!cx.beginswith(disk_cx.c_str())))) {
          err_label = "DISK_CHECKSUM";
          set_errors.insert(err_label);
        }

        if (!silent_cmd) {
          fprintf(stdout,
                  "nrep=\"%02d\" fsid=\"%s\" host=\"%s\" fstpath=\"%s\" "
                  "size=\"%llu\" statsize=\"%llu\" checksum=\"%s\" "
                  "diskchecksum=\"%s\" error_label=\"%s\"\n",
                  i, newresult->Get(repfsid.c_str()),
                  newresult->Get(repurl.c_str()),
                  newresult->Get(repfstpath.c_str()),
                  (unsigned long long)proto_fmd.size(),
                  (unsigned long long)(stat_size),
                  cx.c_str(), disk_cx.c_str(), err_label.c_str());
        }
      } else {
        // For RAIN layouts we only check for block-checksum errors
        if (proto_fmd.blockcxerror()) {
          err_label = "BLOCK_XS";
          set_errors.insert(err_label);
        }

        if (!silent_cmd) {
          fprintf(stdout,
                  "nrep=\"%02d\" fsid=\"%s\" host=\"%s\" fstpath=\"%s\" "
                  "size=\"%llu\" statsize=\"%llu\" error_label=\"%s\"\n",
                  i, newresult->Get(repfsid.c_str()),
                  newresult->Get(repurl.c_str()),
                  newresult->Get(repfstpath.c_str()),
                  (unsigned long long)proto_fmd.size(),
                  (unsigned long long)(stat_size), err_label.c_str());
        }
      }

      ++nrep_online;
    }
  }

  int nrep = 0;
  int stripes = 0;

  if (newresult->Get("mgm.stripes")) {
    stripes = atoi(newresult->Get("mgm.stripes"));
  }

  if (newresult->Get("mgm.nrep")) {
    nrep = atoi(newresult->Get("mgm.nrep"));
  }

  if (nrep != stripes) {
    if (set_errors.find("NOFMD") == set_errors.end()) {
      err_label = "NUM_REPLICAS";
      set_errors.insert(err_label);
    }
  }

  if (set_errors.size()) {
    if ((option.find("%output")) != std::string::npos) {
      fprintf(stdout, "INCONSISTENCY %s path=%-32s fxid=%s size=%llu "
              "stripes=%d nrep=%d nrepstored=%d nreponline=%d "
              "checksumtype=%s checksum=%s\n", set_errors.begin()->c_str(),
              path.c_str(), newresult->Get("mgm.fid0"),
              (unsigned long long) mgm_size, stripes, nrep, i, nrep_online,
              checksumtype.c_str(), newresult->Get("mgm.checksum"));
    }

    if (((option.find("%size") != std::string::npos) &&
         ((set_errors.find("SIZE") != set_errors.end() ||
           set_errors.find("FSTSIZE") != set_errors.end()))) ||
        ((option.find("%checksum") != std::string::npos) &&
         ((set_errors.find("CHECKSUM") != set_errors.end()) ||
          (set_errors.find("BLOCK_XS") != set_errors.end()))) ||
        ((option.find("%diskchecksum") != std::string::npos) &&
         (set_errors.find("DISK_CHECKSUM") != set_errors.end())) ||
        ((option.find("%nrep") != std::string::npos) &&
         ((set_errors.find("NOFMD") != set_errors.end()) ||
          (set_errors.find("NUM_REPLICAS") != set_errors.end())))) {
      global_retc = EFAULT;
    }
  }

  return 0;
}

class FileCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "file";
  }
  const char*
  description() const override
  {
    return "File Handling";
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
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string subcmd;
    ConfigureFileApp(app, subcmd);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> remaining = app.remaining();
    std::reverse(remaining.begin(), remaining.end());

    XrdOucString cmd = subcmd.c_str();
    std::vector<std::string> rest = remaining;
    XrdOucString in = "mgm.cmd=file";

    auto set_path_or_id = [&](XrdOucString path) {
      if (Path2FileDenominator(path)) {
        in += "&mgm.file.id=";
        in += path;
      } else {
        AppendEncodedPath(in, path, true);
      }
    };

    if (cmd == "rename") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=rename";
      XrdOucString p = abspath(rest[0].c_str());
      set_path_or_id(p);
      in += "&mgm.file.source=";
      in += p;
      in += "&mgm.file.target=";
      in += abspath(rest[1].c_str());
    } else if (cmd == "rename_with_symlink") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=rename_with_symlink";
      XrdOucString p = abspath(rest[0].c_str());
      set_path_or_id(p);
      in += "&mgm.file.source=";
      in += p;
      in += "&mgm.file.target=";
      in += abspath(rest[1].c_str());
    } else if (cmd == "symlink") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=symlink";
      XrdOucString p = abspath(rest[0].c_str());
      set_path_or_id(p);
      in += "&mgm.file.source=";
      in += p;
      in += "&mgm.file.target=";
      in += rest[1].c_str();
    } else if (cmd == "drop") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=drop";
      set_path_or_id(p);
      in += "&mgm.file.fsid=";
      in += rest[1].c_str();
      if (rest.size() > 2 && rest[2] == "-f")
        in += "&mgm.file.force=1";
    } else if (cmd == "touch") {
      std::string option;
      size_t idx = 0;
      for (; idx < rest.size(); ++idx) {
        if (!rest[idx].empty() && rest[idx][0] == '-') {
          std::string tmp = rest[idx];
          tmp.erase(std::remove(tmp.begin(), tmp.end(), '-'), tmp.end());
          option += tmp;
        } else {
          break;
        }
      }
      if (idx >= rest.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[idx].c_str());
      in += "&mgm.subcmd=touch";
      set_path_or_id(p);
      std::string fsid1 = (idx + 1 < rest.size()) ? rest[idx + 1] : "";
      std::string fsid2 = (idx + 2 < rest.size()) ? rest[idx + 2] : "";

      if (option.find('n') != std::string::npos) {
        in += "&mgm.file.touch.nolayout=true";
      }
      if (option.find('0') != std::string::npos) {
        in += "&mgm.file.touch.truncate=true";
      }
      if (option.find('a') != std::string::npos) {
        in += "&mgm.file.touch.absorb=true";
      }
      if (option.find('l') != std::string::npos) {
        in += "&mgm.file.touch.lockop=lock";
        if (!fsid1.empty()) {
          in += "&mgm.file.touch.lockop.lifetime=";
          in += fsid1.c_str();
          fsid1.clear();
        }
        if (!fsid2.empty()) {
          if ((fsid2 != "app") && (fsid2 != "user")) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
          if (fsid2 == "app") {
            // this is inverted logic because we set the wildcard
            in += "&mgm.file.touch.wildcard=user";
          } else {
            // this is inverted logic because we set the wildcard
            in += "&mgm.file.touch.wildcard=app";
          }
          fsid2.clear();
        }
      }
      if (option.find('u') != std::string::npos) {
        in += "&mgm.file.touch.lockop=unlock";
        fsid1.clear();
        fsid2.clear();
      }
      if (!fsid1.empty()) {
        if (!fsid1.empty() && fsid1[0] == '/') {
          in += "&mgm.file.touch.hardlinkpath=";
          in += fsid1.c_str();
        } else {
          in += "&mgm.file.touch.size=";
          in += fsid1.c_str();
        }
      }
      if (!fsid2.empty()) {
        in += "&mgm.file.touch.checksuminfo=";
        in += fsid2.c_str();
      }
    } else if (cmd == "move") {
      if (rest.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=move";
      set_path_or_id(p);
      in += "&mgm.file.sourcefsid=";
      in += rest[1].c_str();
      in += "&mgm.file.targetfsid=";
      in += rest[2].c_str();
    } else if (cmd == "copy") {
      std::string option;
      size_t idx = 0;
      for (; idx < rest.size(); ++idx) {
        if (!rest[idx].empty() && rest[idx][0] == '-') {
          std::string tmp = rest[idx];
          tmp.erase(std::remove(tmp.begin(), tmp.end(), '-'), tmp.end());
          option += tmp;
        } else {
          break;
        }
      }
      if (idx + 1 >= rest.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[idx].c_str());
      XrdOucString dest = rest[idx + 1].c_str();
      in += "&mgm.subcmd=copy";
      set_path_or_id(p);
      if (!option.empty()) {
        std::string checkoption = option;
        checkoption.erase(
            std::remove(checkoption.begin(), checkoption.end(), 'f'),
            checkoption.end());
        checkoption.erase(
            std::remove(checkoption.begin(), checkoption.end(), 's'),
            checkoption.end());
        checkoption.erase(
            std::remove(checkoption.begin(), checkoption.end(), 'c'),
            checkoption.end());
        if (!checkoption.empty()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        in += "&mgm.file.option=";
        in += option.c_str();
      }
      dest = abspath(dest.c_str());
      in += "&mgm.file.target=";
      in += dest;
    } else if (cmd == "replicate") {
      if (rest.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=replicate";
      set_path_or_id(p);
      in += "&mgm.file.sourcefsid=";
      in += rest[1].c_str();
      in += "&mgm.file.targetfsid=";
      in += rest[2].c_str();
    } else if (cmd == "purge" || cmd == "version") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=";
      in += cmd;
      XrdOucString p = abspath(rest[0].c_str());
      AppendEncodedPath(in, p, true);
      in += "&mgm.purge.version=";
      if (rest.size() > 1)
        in += rest[1].c_str();
      else
        in += "-1";
    } else if (cmd == "versions") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=versions";
      set_path_or_id(p);
      in += "&mgm.grab.version=";
      in += (rest.size() > 1 ? rest[1].c_str() : "-1");
    } else if (cmd == "layout") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=layout";
      set_path_or_id(p);
      if (rest[1] == "-stripes" && rest.size() > 2) {
        in += "&mgm.file.layout.stripes=";
        in += rest[2].c_str();
      } else if (rest[1] == "-checksum" && rest.size() > 2) {
        in += "&mgm.file.layout.checksum=";
        in += rest[2].c_str();
      } else if (rest[1] == "-type" && rest.size() > 2) {
        in += "&mgm.file.layout.type=";
        in += rest[2].c_str();
      } else {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
    } else if (cmd == "tag") {
      if (rest.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=tag";
      set_path_or_id(p);
      in += "&mgm.file.tag.fsid=";
      in += rest[1].c_str();
    } else if (cmd == "convert") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=convert";
      set_path_or_id(p);
      if (rest.size() > 1) {
        in += "&mgm.convert.layout=";
        in += rest[1].c_str();
      }
      if (rest.size() > 2) {
        in += "&mgm.convert.space=";
        in += rest[2].c_str();
      }
      if (rest.size() > 3) {
        in += "&mgm.convert.placementpolicy=";
        in += rest[3].c_str();
      }
      if (rest.size() > 4) {
        in += "&mgm.convert.checksum=";
        in += rest[4].c_str();
      }
      // Option handling (legacy supported --rewrite; --sync not supported)
      if (rest.size() > 5) {
        for (size_t i = 5; i < rest.size(); ++i) {
          if (rest[i] == "--rewrite") {
            in += "&mgm.option=rewrite";
          } else if (rest[i] == "--sync") {
            fprintf(stderr, "error: --sync is currently not supported\n");
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        }
      }
    } else if (cmd == "verify") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=verify";
      AppendEncodedPath(in, p, true);
      for (size_t i = 1; i < rest.size(); ++i) {
        const std::string& opt = rest[i];
        if (opt == "-checksum")
          in += "&mgm.file.compute.checksum=1";
        else if (opt == "-commitchecksum")
          in += "&mgm.file.commit.checksum=1";
        else if (opt == "-commitsize")
          in += "&mgm.file.commit.size=1";
        else if (opt == "-commitfmd")
          in += "&mgm.file.commit.fmd=1";
        else if (opt == "-rate") {
          if (i + 1 < rest.size()) {
            in += "&mgm.file.verify.rate=";
            in += rest[++i].c_str();
          } else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        } else if (opt == "-resync")
          in += "&mgm.file.resync=1";
        else { // treat as filter fsid if numeric
          in += "&mgm.file.verify.filterid=";
          in += opt.c_str();
        }
      }
    } else if (cmd == "adjustreplica") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=adjustreplica";
      set_path_or_id(p);
      std::vector<std::string> args(rest.begin() + 1, rest.end());
      int positional_index = 0;
      for (size_t i = 0; i < args.size(); ++i) {
        if (args[i] == "--exclude-fs") {
          if (i + 1 < args.size()) {
            in += "&mgm.file.excludefs=";
            in += args[i + 1].c_str();
            i++;
          } else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        } else {
          if (positional_index == 0) {
            in += "&mgm.file.desiredspace=";
            in += args[i].c_str();
            positional_index++;
          } else if (positional_index == 1) {
            in += "&mgm.file.desiredsubgroup=";
            in += args[i].c_str();
            positional_index++;
          } else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        }
      }
    } else if (cmd == "check") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string option;
      if (rest.size() > 1) {
        option = rest[1];
      }
      return RunFileCheck(rest[0].c_str(), option, ctx);
    } else if (cmd == "share") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=share";
      AppendEncodedPath(in, p, true);
      unsigned long long expires = (28ull * 86400ull);
      if (rest.size() > 1) {
        in += "&mgm.file.expires=";
        in += rest[1].c_str();
      } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "%llu", expires);
        in += "&mgm.file.expires=";
        in += buf;
      }
    } else if (cmd == "workflow") {
      if (rest.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(rest[0].c_str());
      in += "&mgm.subcmd=workflow";
      AppendEncodedPath(in, p, true);
      in += "&mgm.workflow=";
      in += rest[1].c_str();
      in += "&mgm.event=";
      in += rest[2].c_str();
    } else if (cmd == "info") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString path = rest[0].c_str();
      bool absolutize = (!path.beginswith("fid:")) && (!path.beginswith("fxid:")) &&
                        (!path.beginswith("pid:")) && (!path.beginswith("pxid:")) &&
                        (!path.beginswith("inode:"));
      XrdOucString fin = "mgm.cmd=fileinfo";
      AppendEncodedPath(fin, path, absolutize);
      XrdOucString option = "";
      for (size_t i = 1; i < rest.size(); ++i) {
        XrdOucString tok = rest[i].c_str();
        if (tok == "s")
          option += "silent";
        else
          option += tok;
      }
      if (option.length()) {
        fin += "&mgm.file.info.option=";
        fin += option;
      }
      // Print output unless silent
      if (option.find("silent") == STR_NPOS) {
        global_retc =
            ctx.outputResult(ctx.clientCommand(fin, false, nullptr), true);
      }
      return 0;
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeFileHelp().c_str());
  }
};
} // namespace

void
RegisterFileNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FileCommand>());
}
