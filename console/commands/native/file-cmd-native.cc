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
#include <cctype>
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
  std::ostringstream oss;
  oss << "Usage: file <subcmd> [args...]\n\n"
      << "'[eos] file ..' provides the file management interface of EOS.\n\n";
  oss << "adjustreplica [--nodrop] [<path>|fid:<fid-dec>|fxid:<fid-hex>] [space [subgroup]] [--exclude-fs <fsid>]\n"
      << "  Tries to bring files with replica layouts to the nominal replica level [need root].\n"
      << "  --exclude-fs <fsid>  exclude the given filesystem from being used for the replica adjustment\n\n";
  oss << "check [<path>|fid:<fid-dec>|fxid:<fid-hex>] [%size%checksum%nrep%diskchecksum%force%output%silent]\n"
      << "  Retrieves stat information from the physical replicas and verifies correctness.\n"
      << "  %size        return EFAULT if mismatch between the size meta data information\n"
      << "  %checksum    return EFAULT if mismatch between the checksum meta data information\n"
      << "  %nrep        return EFAULT if mismatch between layout number of replicas and existing replicas\n"
      << "  %diskchecksum  return EFAULT if mismatch between disk checksum on FST and reference checksum\n"
      << "  %silent      suppress all information for each replica to be printed\n"
      << "  %force       force to get the MD even if the node is down\n"
      << "  %output      print lines with inconsistency information\n\n";
  oss << "convert [<path>|fid:<fid-dec>|fxid:<fid-hex>] [<layout>:<stripes>|<layout-id>|<sys.attribute.name>] "
      << "[target-space] [placement-policy] [checksum] [--rewrite]\n"
      << "  Convert the layout of a file.\n"
      << "  <layout>:<stripes>   target layout and number of stripes\n"
      << "  <layout-id>          hexadecimal layout id\n"
      << "  <conversion-name>    name of sys.conversion.<name> in parent directory defining target layout\n"
      << "  <target-space>       optional name of target space or group e.g. default or default.3\n"
      << "  <placement-policy>   scattered, hybrid:<geotag>, gathered:<geotag>\n"
      << "  <checksum>           optional target checksum name (md5, adler, etc.)\n"
      << "  --rewrite            run conversion rewriting the file, creating new copies and dropping old\n\n";
  oss << "copy [-f] [-s] [-c] <src> <dst>\n"
      << "  Synchronous third party copy from <src> to <dst>.\n"
      << "  <src>  source file or directory (<path>|fid:<fid-dec>|fxid:<fid-hex>)\n"
      << "  <dst>  destination file (if source is file) or directory\n"
      << "  -f     force overwrite\n"
      << "  -s     don't print output\n"
      << "  -c     clone the file (keep ctime, mtime)\n\n";
  oss << "drop [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid> [-f]\n"
      << "  Drop the file from <fsid>. -f force removes replica without trigger/wait for deletion "
      << "(used to retire a filesystem).\n\n";
  oss << "info [<path>|fid:<fid-dec>|fxid:<fid-hex>|pid:<cid-dec>|pxid:<cid-hex>|inode:<inode-dec>] [options]\n"
      << "  Show file info. Options: --path, --fid, --fxid, --size, --checksum, --fullpath, "
      << "--proxy, -m, -s|--silent.\n\n";
  oss << "layout <path>|fid:<fid-dec>|fxid:<fid-hex> -stripes <n>\n"
      << "  Change the number of stripes of a file with replica layout to <n>.\n"
      << "layout <path>|fid:<fid-dec>|fxid:<fid-hex> -checksum <checksum-type>\n"
      << "  Change the checksum-type of a file to <checksum-type>.\n"
      << "layout <path>|fid:<fid-dec>|fxid:<fid-hex> -type <hex-layout-type>\n"
      << "  Change the layout-type of a file to <hex-layout-type> (as shown by file info).\n\n";
  oss << "move [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2>\n"
      << "  Move the file from <fsid1> to <fsid2>.\n\n";
  oss << "purge <path> [purge-version]\n"
      << "  Keep maximum <purge-version> versions of a file. If not specified apply sys.versioning attribute.\n\n";
  oss << "rename [<path>|fid:<fid-dec>|fxid:<fid-hex>] <new>\n"
      << "  Rename from <old> to <new> name (works for files and directories).\n\n";
  oss << "rename_with_symlink <source_file> <destination_dir>\n"
      << "  Rename/move source file to destination directory atomically:\n"
      << "  - move file to destination directory\n"
      << "  - create symlink in the source directory to the new location\n\n";
  oss << "replicate [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2>\n"
      << "  Replicate file part on <fsid1> to <fsid2>.\n\n";
  oss << "share <path> [lifetime]\n"
      << "  Create a share link. <lifetime> defaults to 28d (1, 1s, 1d, 1w, 1mo, 1y, ...).\n\n";
  oss << "symlink [-f] <name> <target>\n"
      << "  Create a symlink with <name> pointing to <target>. -f force overwrite.\n\n";
  oss << "tag <path>|fid:<fid-dec>|fxid:<fid-hex> +|-|~<fsid>\n"
      << "  Add/remove/unlink a filesystem location to/from a file in the location index "
      << "(does not move any data).\n"
      << "  Unlink keeps the location in the list of deleted files (gets a deletion request).\n\n";
  oss << "touch [-a] [-n] [-0] <path>|fid:<fid-dec>|fxid:<fid-hex> [linkpath|size [hexchecksum]]\n"
      << "  Create/touch a 0-size/0-replica file if <path> does not exist or update mtime of existing file.\n"
      << "  -n        disable placement logic (default uses placement)\n"
      << "  -0        truncate a file\n"
      << "  -a        absorb (adopt) a file from hardlink path - file disappears from given path and is taken under EOS FST control\n"
      << "  linkpath  hard- or softlink the touched file to a shared filesystem\n"
      << "  size      preset the size for a new touched file\n"
      << "  hexchecksum  checksum information for a new touched file\n"
      << "touch -l <path>|fid:<fid-dec>|fxid:<fid-hex> [<lifetime> [<audience>=user|app]]\n"
      << "  Touch and create an extended attribute lock with <lifetime> (default 24h).\n"
      << "  <audience> relaxes lock owner: same user or same app (default: both must match).\n"
      << "  EBUSY if lock held by another; second call by same caller extends lifetime.\n"
      << "  Use with 'eos -a application' to tag a client with an application for the lock.\n"
      << "touch -u <path>|fid:<fid-dec>|fxid:<fid-hex>\n"
      << "  Remove an extended attribute lock. No error if no lock; EBUSY if held by someone else.\n\n";
  oss << "verify <path>|fid:<fid-dec>|fxid:<fid-hex> [<fsid>] [-checksum] [-commitchecksum] [-commitsize] [-commitfmd] [-rate <rate>] [-resync]\n"
      << "  Verify a file against the disk images.\n"
      << "  <fsid>        verify only the replica on <fsid>\n"
      << "  -checksum     trigger checksum calculation during verification\n"
      << "  -commitchecksum  commit the computed checksum to the MGM\n"
      << "  -commitsize   commit the file size to the MGM\n"
      << "  -commitfmd    commit the FMD to the MGM\n"
      << "  -rate <rate>  restrict verification speed to <rate> per node\n"
      << "  -resync       ask all locations to resync their file md records\n\n";
  oss << "version <path> [purge-version]\n"
      << "  Create a new version of a file by cloning. <purge-version> defines max versions to keep.\n\n";
  oss << "versions <path>|fid:<fid-dec>|fxid:<fid-hex> [grab-version]\n"
      << "  List versions of a file, or grab a version [grab-version].\n\n";
  oss << "workflow <path>|fid:<fid-dec>|fxid:<fid-hex> <workflow> <event>\n"
      << "  Trigger workflow <workflow> with event <event> on <path>.\n";
  return oss.str();
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

    auto is_path_or_id = [](const std::string& s) -> bool {
      return !s.empty() && (s[0] == '/' || s.find("fid:") == 0 ||
                            s.find("fxid:") == 0 || s.find("pid:") == 0 ||
                            s.find("pxid:") == 0 || s.find("inode:") == 0 ||
                            s.find("cid:") == 0 || s.find("cxid:") == 0 ||
                            s[0] != '-');
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
      std::vector<std::string> positionals;
      bool force = false;

      for (const auto& arg : rest) {
        if (arg == "-f")
          force = true;
        else
          positionals.push_back(arg);
      }
      if (positionals.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::reverse(positionals.begin(), positionals.end());
      in += "&mgm.subcmd=symlink";
      XrdOucString p = abspath(positionals[0].c_str());
      set_path_or_id(p);
      in += "&mgm.file.source=";
      in += p;
      in += "&mgm.file.target=";
      in += positionals[1].c_str();
      if (force)
        in += "&mgm.file.force=1";
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
      bool rewrite = false;
      std::vector<std::string> positionals;

      for (const auto& arg : rest) {
        if (arg == "--rewrite")
          rewrite = true;
        else if (arg == "--sync") {
          fprintf(stderr, "error: --sync is currently not supported\n");
          printHelp();
          global_retc = EINVAL;
          return 0;
        } else
          positionals.push_back(arg);
      }
      if (positionals.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(positionals[0].c_str());
      in += "&mgm.subcmd=convert";
      set_path_or_id(p);
      if (positionals.size() > 1) {
        in += "&mgm.convert.layout=";
        in += positionals[1].c_str();
      }
      if (positionals.size() > 2) {
        in += "&mgm.convert.space=";
        in += positionals[2].c_str();
      }
      if (positionals.size() > 3) {
        in += "&mgm.convert.placementpolicy=";
        in += positionals[3].c_str();
      }
      if (positionals.size() > 4) {
        in += "&mgm.convert.checksum=";
        in += positionals[4].c_str();
      }
      if (rewrite)
        in += "&mgm.option=rewrite";
    } else if (cmd == "verify") {
      std::string path;
      std::string filter_fsid;
      std::string rate_val;

      for (size_t i = 0; i < rest.size(); ++i) {
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
            rate_val = rest[++i];
            in += "&mgm.file.verify.rate=";
            in += rate_val.c_str();
          } else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        } else if (opt == "-resync")
          in += "&mgm.file.resync=1";
        else if (!path.empty() && !opt.empty() &&
                   std::isdigit(static_cast<unsigned char>(opt[0]))) {
          filter_fsid = opt;
          in += "&mgm.file.verify.filterid=";
          in += filter_fsid.c_str();
        } else if (is_path_or_id(opt)) {
          if (path.empty())
            path = opt;
          else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
      }
      if (path.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = abspath(path.c_str());
      in += "&mgm.subcmd=verify";
      AppendEncodedPath(in, p, true);
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
        } else if (args[i] == "--nodrop") {
          in += "&mgm.file.nodrop=1";
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
      std::string option = (rest.size() > 1) ? rest[1] : "";
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
      std::string path;
      XrdOucString option = "";

      for (const auto& arg : rest) {
        if (is_path_or_id(arg)) {
          if (path.empty())
            path = arg;
          else {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        } else {
          XrdOucString tok = arg.c_str();
          if (tok == "s" || tok == "-s" || tok == "--silent")
            option += "silent";
          else
            option += tok;
        }
      }
      if (path.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString path_str = path.c_str();
      bool absolutize = (!path_str.beginswith("fid:")) && (!path_str.beginswith("fxid:")) &&
                        (!path_str.beginswith("pid:")) && (!path_str.beginswith("pxid:")) &&
                        (!path_str.beginswith("inode:"));
      XrdOucString fin = "mgm.cmd=fileinfo";
      AppendEncodedPath(fin, path_str, absolutize);
      if (option.length()) {
        fin += "&mgm.file.info.option=";
        fin += option;
      }
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
