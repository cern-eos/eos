// ----------------------------------------------------------------------
// File: fs-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/FsHelper.hh"
#include <memory>
#include <sstream>

namespace {
class FsProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fs";
  }
  const char*
  description() const override
  {
    return "File System configuration";
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    FsHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Confirmation is handled inside FsHelper where applicable
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: fs "
        "add|boot|config|dropdeletion|dropghosts|dropfiles|dumpmd|ls|mv|rm|"
        "status [OPTIONS]\n"
        "  Options:\n"
        "  fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] "
        "<mountpoint> [<space_info> [<status> [<sharedfs>]]]\n"
        "    add and assign a filesystem based on the unique identifier of the "
        "disk <uuid>\n"
        "    -m|--manual  : add with user specified <fsid> and <space>\n"
        "    <fsid>       : numeric filesystem id 1...65535\n"
        "    <uuid>       : unique string identifying current filesystem\n"
        "    <node-queue> : internal EOS identifier for a node e.g "
        "/eos/<host>:<port>/fst\n"
        "                   it is preferable to use the host:port syntax\n"
        "    <host>       : FQDN of host where filesystem is mounter\n"
        "    <port>       : FST XRootD port number [usually 1095]\n"
        "    <mountponit> : local path of the mounted filesystem e.g /data/\n"
        "    <space_info> : space or space.group location where to insert the "
        "filesystem,\n"
        "                   if nothing is specified then space \"default\" is "
        "used. E.g:\n"
        "                   default, default.7, ssd.3, spare\n"
        "    <status>     : set filesystem status after insertion e.g "
        "off|rw|ro|empty etc.\n"
        "    <sharedfs>   : set the name of a shared filesystem\n"
        "\n"
        "  fs boot <fsid>|<uuid>|<node-queue>|* [--syncdisk|--syncmgm]\n"
        "    boot - filesystem identified by <fsid> or <uuid>\n"
        "         - all filesystems on a node identified by <node-queue>\n"
        "         - all filesystems registered\n"
        "    --syncdisk   : do disk resynchronization during the booting\n"
        "    --syncmgm    : do MGM and disk resynchronization during the "
        "booting\n"
        "\n"
        "  fs clone <sourceid> <targetid>\n"
        "    replicate files from the source to the target filesystem\n"
        "    <sourceid>   : id of the source filesystem\n"
        "    <targetid>   : id of the target filesystem\n"
        "\n"
        "  fs compare <sourceid> <targetid>\n"
        "    compares and reports which files are present on one filesystem "
        "and not on the other\n"
        "    <sourceid>   : id of the source filesystem\n"
        "    <targetid>   : id of the target filesystem\n"
        "\n"
        "  fs config <fsid> <key>=<value>\n"
        "    configure the filesystem parameter, where <key> and <value> can "
        "be:\n"
        "    configstatus=rw|wo|ro|drain|draindead|off|empty [--comment "
        "\"<comment>\"]\n"
        "      rw        : set filesystem in read-write mode\n"
        "      wo        : set filesystem in write-only mode\n"
        "      ro        : set filesystem in read-only mode\n"
        "      drain     : set filesystem in drain mode\n"
        "      draindead : set filesystem in draindead mode, unusable for any "
        "read\n"
        "      off       : disable filesystem\n"
        "      empty     : empty filesystem, possible only if there are no\n"
        "                  more files stored on it\n"
        "      --comment : pass a reason for the status change\n"
        "    headroom=<size>\n"
        "      headroom to keep per filesystem. <size> can be (>0)[BMGT]\n"
        "    scaninterval=<seconds>\n"
        "      entry rescan interval (default 7 days), 0 disables scanning\n"
        "    scan_rain_interval=<seconds>\n"
        "      rain entry rescan interval (default 4 weeks), 0 disables "
        "scanning\n"
        "    scanrate=<MB/s>\n"
        "      maximum IO scan rate per filesystem\n"
        "    scan_disk_interval=<seconds>\n"
        "      disk consistency thread scan interval (default 4h)\n"
        "    scan_ns_interval=<seconds>\n"
        "      namespace consistency thread scan interval (default 3 days)\n"
        "    scan_ns_rate=<entries/s>\n"
        "      maximum scan rate of ns entries for the NS consistency. This\n"
        "      is bound by the maxium number of IOPS per disk.\n"
        "    fsck_refresh_interval=<sec>\n"
        "       time interval after which fsck inconsistencies are refreshed\n"
        "    scan_altxs_rate=<entries/s>\n"
        "       maximum scan rate of ns entries for checking if alternative\n"
        "       checksums have been computed.\n"
        "    scan_altxs_interval=<sec>\n"
        "       alternative checksum computing interval (default 30 days), 0 "
        "disables scanning\n"
        "    altxs_sync=0|1\n"
        "       enable synchronization of alternative checksums settings from "
        "namespace\n"
        "    altxs_sync_interval=<seconds>\n"
        "       time interval after which synchronization of alternative "
        "checksums\n"
        "       settings are refreshed. If 0 it is synchronized only once "
        "(default 0)\n"
        "    graceperiod=<seconds>\n"
        "      grace period before a filesystem with an operation error gets\n"
        "      automatically drained\n"
        "    drainperiod=<seconds>\n"
        "      period a drain job is allowed to finish the drain procedure\n"
        "    proxygroup=<proxy_grp_name>\n"
        "      schedule a proxy for the current filesystem by taking it from\n"
        "      the given proxy group. The special value \"<none>\" is the\n"
        "      same as no value and means no proxy scheduling\n"
        "    filestickyproxydepth=<depth>\n"
        "      depth of the subtree to be considered for file-stickyness. A\n"
        "      negative value means no file-stickyness\n"
        "    forcegeotag=<geotag>\n"
        "      set the filesystem's geotag, overriding the host geotag value.\n"
        "      The special value \"<none>\" is the same as no value and means\n"
        "      no override\n"
        "    s3credentials=<accesskey>:<secretkey>\n"
        "      the access and secret key pair used to authenticate\n"
        "      with the S3 storage endpoint\n"
        "    sharedfs=<name>\n"
        "      the sharedfs this filesystem is to be assigned to. To unlabel "
        "set the sharedfs name to 'none' !\n"
        "\n"
        "  fs dropdeletion <fsid> \n"
        "    drop all pending deletions on the filesystem\n"
        "\n"
        "  fs dropghosts <fsid> [--fxid fid1 [fid2] ...]\n"
        "    drop file ids (hex) without a corresponding metadata object in\n"
        "    the namespace that are still accounted in the file system view.\n"
        "    If no fxid is provided then all fids on the file system are "
        "checked.\n"
        "\n"
        "  fs dropfiles <fsid> [-f]\n"
        "    drop all files on the filesystem\n"
        "    -f : unlink/remove files from the namespace (you have to remove\n"
        "         the files from disk)\n"
        "\n"
        "  fs dumpmd <fsid> [--count] [--fid|--fxid|--path] [--size] [-m|-s]\n"
        "    dump/count file metadata entries on the given filesystem, only if "
        "less than 100k\n"
        "    --count : print only the number of entries on the file system "
        "[default]\n"
        "    --fid   : dump only the file ids in decimal\n"
        "    --fxid  : dump only the file ids in hexadecimal\n"
        "    --path  : dump only the file paths\n"
        "    --size  : dump only the file sizes\n"
        "    -m      : print full metadata record in env format\n"
        "    -s      : silent mode (will keep an internal reference)\n"
        "\n"
        "  fs ls [-m|-l|-e|--io|--fsck|[-d|--drain]|-D|-F] [-s] [-b|--brief] "
        "[[matchlist]]\n"
        "    list filesystems using the default output format\n"
        "    -m         : monitoring format\n"
        "    -b|--brief : display hostnames without domain names\n"
        "    -l         : display parameters in long format\n"
        "    -e         : display filesystems in error state\n"
        "    --io       : IO output format\n"
        "    --fsck     : display filesystem check statistics\n"
        "    -d|--drain : display filesystems in drain or draindead status\n"
        "                 along with drain progress and statistics\n"
        "    -D|--drain_jobs : \n"
        "                 display ongoing drain transfers, matchlist needs to "
        "be an integer\n"
        "                 representing the drain file system id\n"
        "    -F|--failed_drain_jobs : \n"
        "                 display failed drain transfers, matchlist needs to "
        "be an integer\n"
        "                 representing the drain file system id. This will "
        "only display\n"
        "                 information while the draining is ongoing\n"
        "    -s         : silent mode\n"
        "    [matchlist]\n"
        "       -> can be the name of a space or a comma separated list of\n"
        "          spaces e.g 'default,spare'\n"
        "       -> can be a grep style list to filter certain filesystems\n"
        "          e.g. 'fs ls -d drain,bootfailure'\n"
        "       -> can be a combination of space filter and grep e.g.\n"
        "          'fs ls -l default,drain,bootfailure'\n"
        "\n"
        "  fs mv [--force] <src_fsid|src_grp|src_space> "
        "<dst_grp|dst_space|node:port>\n"
        "    move filesystem(s) in different scheduling group, space or node\n"
        "    --force   : force mode - allows to move non-empty filesystems "
        "bypassing group \n"
        "                and node constraints\n"
        "    src_fsid  : source filesystem id\n"
        "    src_grp   : all filesystems from scheduling group are moved\n"
        "    src_space : all filesystems from space are moved\n"
        "    dst_grp   : destination scheduling group\n"
        "    dst_space : destination space - best match scheduling group\n"
        "    node:port : destination node, useful to serve a disk from a "
        "different\n"
        "                FST on the same node or failover an FST serving data "
        "from\n"
        "                a shared filesystem backend\n"
        "\n"
        "  fs rm <fsid>|<mnt>|<node-queue> <mnt>|<hostname> <mnt>\n"
        "    remove filesystem by various identifiers, where <mnt> is the \n"
        "    mountpoint\n"
        "\n"
        "  fs status [-r] [-l] <identifier>\n"
        "    return all status variables of a filesystem and calculates\n"
        "    the risk of data loss if this filesystem is removed\n"
        "    <identifier> can be: \n"
        "       <fsid> : filesystem id\n"
        "       [<host>] <mountpoint> : if host is not specified then it's\n"
        "       considered localhost\n"
        "    -l : list all files which are at risk and offline files\n"
        "    -r : show risk analysis\n"
        "\n"
        "  Examples: \n"
        "  fs ls --io -> list all filesystems with IO statistics\n"
        "  fs boot *  -> send boot request to all filesystems\n"
        "  fs dumpmd 100 --path -> dump all logical path names on filesystem "
        "100\n"
        "  fs mv 100 default.0 -> move filesystem 100 to scheduling group "
        "default.0\n");
  }
};
} // namespace

void
RegisterFsProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FsProtoCommand>());
}
