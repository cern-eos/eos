// ----------------------------------------------------------------------
// File: file-proto-native.cc
// Author: Octavian-Mihai Matei - CERN
// ----------------------------------------------------------------------

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "common/FileId.hh"
#include "common/Fmd.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCompletion.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/FileHelper.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClURL.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <iomanip>
#include <limits>
#include <memory>
#include <openssl/sha.h>
#include <set>
#include <sstream>
#include <time.h>
#include <vector>

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
      << "(used to retire a filesystem).\n"
      << "drop [<path>|fid:<fid-dec>|fxid:<fid-hex>] cache\n"
      << "  Drop the read-through cache location from the file metadata and "
      << "truncate the journal on the cache FST.\n\n";
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

class FileProtoCommand : public IConsoleCommand {
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
  std::string
  helpText() const override
  {
    return MakeFileHelp();
  }
  std::vector<std::string>
  complete(const std::vector<std::string>& args) const override
  {
    return eos_help_completion_candidates(name(), helpText(), args);
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
      if (i) {
        oss << ' ';
      }
      if (args[i].find(' ') != std::string::npos) {
        oss << std::quoted(args[i]);
      } else {
        oss << args[i];
      }
    }
    std::string joined = oss.str();

    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    const std::string& cmd = args[0];
    std::vector<std::string> rest(args.begin() + 1, args.end());

    // 'file check' does client-side FST querying/stat'ing and doesn't fit
    // the oneof-dispatch pattern - stays on its original code path.
    if (cmd == "check") {
      if (rest.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      std::string option = (rest.size() > 1) ? rest[1] : "";
      return RunFileCheck(rest[0].c_str(), option, ctx);
    }

    FileHelper helper(*ctx.globalOpts,
                      [](const char* in) { return std::string(abspath(in)); });

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
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterFileProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FileProtoCommand>());
}
