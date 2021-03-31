// ----------------------------------------------------------------------
// File: com_cp.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include <iomanip>
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/

extern int com_transfer(char* argin);

int
com_cp_usage()
{
  fprintf(stdout,
          "Usage: cp [--async] [--atomic] [--rate=<rate>] [--streams=<n>] [--depth=<d>] [--checksum] [--no-overwrite|-k] [--preserve|-p] [--recursive|-r|-R] [-s|--silent] [-a] [-n] [-S] [-d[=][<lvl>] <src> <dst>\n");
  fprintf(stdout, "'[eos] cp ..' provides copy functionality to EOS.\n");
  fprintf(stdout,
          "          <src>|<dst> can be root://<host>/<path>, a local path /tmp/../ or an eos path /eos/ in the connected instance\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout,
          "       --async         : run an asynchronous transfer via a gateway server (see 'transfer submit --sync' for the full options)\n");
  fprintf(stdout,
          "       --atomic        : run an atomic upload where files are only visible with the target name when their are completely uploaded [ adds ?eos.atomic=1 to the target URL ]\n");
  fprintf(stdout, "       --rate          : limit the cp rate to <rate>\n");
  fprintf(stdout, "       --streams       : use <#> parallel streams\n");
  fprintf(stdout, "       --depth         : depth for recursive copy\n");
  fprintf(stdout, "       --checksum      : output the checksums\n");
  fprintf(stdout,
          "       -a              : append to the target, don't truncate\n");
  fprintf(stdout, "       -p              : create destination directory\n");
  fprintf(stdout, "       -n              : hide progress bar\n");
  fprintf(stdout, "       -S              : print summary\n");
  fprintf(stdout,
          "   -d | --debug          : enable debug information (optional <lvl>=1|2|3)\n");
  fprintf(stdout,
          "   -s | --silent         : no output outside error messages\n");
  fprintf(stdout,
          "   -k | --no-overwrite   : disable overwriting of files\n");
  fprintf(stdout,
          "   -P | --preserve       : preserves file creation and modification time from the source\n");
  fprintf(stdout,
          "   -r | -R | --recursive : copy source location recursively\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "Remark: \n");
  fprintf(stdout,
          "       If you deal with directories always add a '/' in the end of source or target paths e.g. if the target should be a directory and not a file put a '/' in the end. To copy a directory hierarchy use '-r' and source and target directories terminated with '/' !\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "Examples: \n");
  fprintf(stdout,
          "       eos cp /var/data/myfile /eos/foo/user/data/                   : copy 'myfile' to /eos/foo/user/data/myfile\n");
  fprintf(stdout,
          "       eos cp /var/data/ /eos/foo/user/data/                         : copy all plain files in /var/data to /eos/foo/user/data/\n");
  fprintf(stdout,
          "       eos cp -r /var/data/ /eos/foo/user/data/                      : copy the full hierarchy from /var/data/ to /eos/foo/user/data/ => empty directories won't show up on the target!\n");
  fprintf(stdout,
          "       eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  : copy the full hierarchy and just printout the checksum information for each file copied!\n");
  fprintf(stdout, "\nS3:\n");
  fprintf(stdout, "      URLs have to be written as:\n");
  fprintf(stdout,
          "         as3://<hostname>/<bucketname>/<filename> as implemented in ROOT\n");
  fprintf(stdout,
          "      or as3:<bucketname>/<filename> with environment variable S3_HOSTNAME set\n");
  fprintf(stdout, "     and as3:....?s3.id=<id>&s3.key=<key>\n\n");
  fprintf(stdout, "      The access id can be defined in 3 ways:\n");
  fprintf(stdout,
          "      env S3_ACCESS_ID=<access-id>          [as used in ROOT  ]\n");
  fprintf(stdout,
          "      env S3_ACCESS_KEY_ID=<access-id>      [as used in libs3 ]\n");
  fprintf(stdout,
          "      <as3-url>?s3.id=<access-id>           [as used in EOS transfers ]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "      The access key can be defined in 3 ways:\n");
  fprintf(stdout,
          "      env S3_ACCESS_KEY=<access-key>        [as used in ROOT ]\n");
  fprintf(stdout,
          "      env S3_SECRET_ACCESS_KEY=<access-key> [as used in libs3 ]\n");
  fprintf(stdout,
          "      <as3-url>?s3.key=<access-key>         [as used in EOS transfers ]\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "      If <src> and <dst> are using S3, we are using the same credentials on both ends and the target credentials will overwrite source credentials!\n");
  return (EINVAL);
}

/* Helper types */
enum Protocol {
  HTTP, HTTPS, GSIFTP,
  S3, AS3, XROOT,
  EOS, LOCAL, UNKNOWN
};

struct File_t {
  XrdOucString name;
  XrdOucString opaque;
  Protocol protocol;
  timespec atime;
  timespec mtime;
  unsigned long long size;

  File_t() : name(""), opaque(""), protocol(Protocol::UNKNOWN), size(0) { }
};

/* Helper functions */
int run_eos_command(const char* cmdline, std::vector<XrdOucString>& result);
int run_command(const char* cmdline, std::vector<XrdOucString>& result);
const char* absolute_path(const char* path);
bool is_dir(const char* path, Protocol protocol, struct stat* buf = NULL);
XrdOucString process_symlink(XrdOucString path);
const char* setup_s3_environment(XrdOucString path, XrdOucString opaque);
const char* eos_roles_opaque();
int do_stat(const char* path, Protocol protocol, struct stat& buf);
int check_protocol_tool(const char* path);
Protocol get_protocol(XrdOucString path);
const char* protocol_to_string(Protocol protocol);
int parse_debug_level(XrdOucString option);

/* eos cp command */
int
com_cp(char* argin)
{
  XrdOucString rate = "";
  XrdOucString streams = "0";
  XrdOucString atomic = "";
  std::vector<XrdOucString> source_find_list;
  std::vector<XrdOucString> source_basepath_list;
  std::vector<File_t> source_list;
  File_t target;
  bool target_is_stdout;
  bool target_is_dir = false;
  bool recursive = false;
  bool summary = false;
  bool noprogress = false;
  bool append = false;
  bool makeparent = false;
  bool debug = false;
  int debug_level = 0;
  bool checksums = false;
  bool silent = false;
  bool nooverwrite = false;
  bool preserve = false;
  unsigned long long copysize = 0;
  unsigned long long copiedsize = 0;
  unsigned long depth = 0;
  struct timeval start_time, end_time;
  struct timezone tz;
  int files_copied = 0;
  int retc = 0;
  // Check if this is an 'async' command
  XrdOucString sarg = argin;

  if ((sarg.find("--async")) != STR_NPOS) {
    char fullcmd[4096];
    sarg.replace("--async", "submit --sync");
    snprintf(fullcmd, sizeof(fullcmd) - 1, "%s", sarg.c_str());
    return com_transfer(fullcmd);
  }

  // ----------------------------------------------------------------------------
  // Parse arguments
  // ----------------------------------------------------------------------------
  eos::common::StringTokenizer subtokenizer(argin);
  subtokenizer.GetLine();

  do {
    XrdOucString option = subtokenizer.GetToken();

    if (!option.length()) {
      break;
    }

    if (option.beginswith("--rate=")) {
      rate = option;
      rate.replace("--rate=", "");
    } else if (option.beginswith("--streams=")) {
      streams = option;
      streams.replace("--streams=", "");
    } else if ((option == "--recursive") ||
               (option == "-R") || (option == "-r")) {
      recursive = true;
    } else if (option == "-n") {
      noprogress = true;
    } else if (option == "-a") {
      append = true;
    } else if (option == "-p") {
      makeparent = true;
    } else if (option == "-S") {
      summary = true;
    } else if ((option == "-s") || (option == "--silent")) {
      silent = true;
    } else if ((option == "-k") || (option == "--no-overwrite")) {
      nooverwrite = true;
    } else if (option == "--checksum") {
      checksums = true;
    } else if ((option.beginswith("-d")) || (option.beginswith("--debug"))) {
      if ((debug_level = parse_debug_level(option)) < 0) {
        return com_cp_usage();
      }

      debug = true;
    } else if ((option == "--preserve") || (option == "-P")) {
      preserve = true;
    } else if (option == "--atomic") {
      atomic = "&eos.atomic=1";
    } else if (option.beginswith("--depth=")) {
      option.replace("--depth=", "");

      try {
        depth = std::stoul(option.c_str());
      } catch (...) {
        fprintf(stderr, "error: invalid value for <depth>=%s", option.c_str());
        return com_cp_usage();
      }
    } else if (option.beginswith("-")) {
      return com_cp_usage();
    } else {
      if ((!option.beginswith("/eos/")) || (!option.beginswith("root:/"))) {
        while (option.replace("#AND#", "&")) {}
      }

      source_find_list.emplace_back(option.c_str());
      break;
    }
  } while (true);

  if (silent || !hasterminal) {
    noprogress = true;
  }

  if (recursive) {
    makeparent = true;
  }

  // Store list of source locations + target destination
  XrdOucString nextarg = subtokenizer.GetToken();
  XrdOucString lastarg = subtokenizer.GetToken();

  while (lastarg.length()) {
    source_find_list.emplace_back(nextarg.c_str());
    nextarg = lastarg;
    lastarg = subtokenizer.GetToken();
  }

  target.name = nextarg;

  if (!target.name.length()) {
    fprintf(stderr, "warning: no target specified. Please view 'eos cp --help'.\n");
    global_retc = 0;
    return 0;
  }

  // --------------------------------------------------------------------------
  // Expand source list into final list to copy.
  // This means interpreting the '*' character in file names
  // and traversing directories for the recursive flag.
  // Every source path also has an associated base path,
  // which will get appended to the target.
  // --------------------------------------------------------------------------

  for (size_t i = 0; i < source_find_list.size(); i++) {
    std::vector<XrdOucString> files;
    XrdOucString source = source_find_list[i];
    XrdOucString source_opaque;
    XrdOucString basepath = "";
    Protocol protocol;
    std::string sprotocol = "";
    int opos = source.find("?");
    bool wildcard = false;
    files.clear();

    // Extract opaque info
    if (opos != STR_NPOS) {
      source_opaque = source;
      source_opaque.erase(0, opos + 1);
      source.erase(opos);
    }

    // Identify protocol
    protocol = get_protocol(source.c_str());

    if (protocol == Protocol::UNKNOWN) {
      fprintf(stderr, "warning: %s -- protocol not recognized. Skipping path..",
              source.c_str());
      continue;
    }

    // Convert local to absolute path
    const char* abs_path = absolute_path(source.c_str());
    source = abs_path;
    free((char*)abs_path);

    // Check if source is a directory
    if (!source.endswith("/") && is_dir(source.c_str(), protocol, NULL)) {
      source.append("/");
    }

    // Extract file name and parent path
    const char* filepath = source.c_str();

    // URLs need different processing in order to extract the path
    if ((protocol != Protocol::EOS) && (protocol != Protocol::LOCAL)) {
      XrdOucString sprot, hostport;
      filepath = eos::common::StringConversion::ParseUrl(source.c_str(),
                 sprot, hostport);

      if (!filepath) {
        fprintf(stderr, "error: cannot process file=%s [protocol=%s]\n",
                source.c_str(), protocol_to_string(protocol));
        continue;
      }
    }

    eos::common::Path cPath(filepath);
    basepath = cPath.GetParentPath();

    if ((source.find("*") != STR_NPOS) || (source.endswith("/"))) {
      std::string cmdtext;

      if ((protocol != Protocol::EOS) && (protocol != Protocol::LOCAL)) {
        fprintf(stderr, "error: %s -- path expansion not implemented for %s protocol."
                " Skipping path..\n", source.c_str(), protocol_to_string(protocol));
        continue;
      }

      // Get all paths matching wildcard
      if (source.find("*") != STR_NPOS) {
        // Will use 'ls -lF' combined with grep to identify matches
        // ls -l[F|p] <path> | awk 'NF == 9 {print $9}' [ | egrep "<match>" ]
        // Note: eos::common::Path removes trailing '/'!
        XrdOucString basename = cPath.GetName();

        if (source.endswith("/")) {
          basename.append("/");
        }

        // Wildcards are supported only in the basename
        if (basename.find("*") == STR_NPOS) {
          fprintf(stderr, "warning: %s -- wildcards not supported outside basename."
                  " Skipping path..\n", source.c_str());
          continue;
        }

        XrdOucString match = basename.c_str();
        wildcard = true;

        if (!match.beginswith("*")) {
          match.insert("^", 0);
        }

        if (!match.endswith("*"))   {
          match.append("$");
        }

        match.replace("*", ".*");
        // Construct command text
        cmdtext = "ls -l";
        cmdtext += (protocol == Protocol::EOS) ? "F " : "p ";
        cmdtext += basepath.c_str();
        cmdtext +=
          " | awk '{out=$9; for (i=10; i<=NF; i++) {out=out\" \"$i}; print out}' | egrep \"";
        cmdtext += match.c_str();
        cmdtext += "\"";
      } else if (source.endswith("/")) {
        // Get all files within directory

        // Will use 'find' to identify files
        // local file: find <path> [-maxdepth <depth>] -follow -type f
        // eos file:   find -f [--maxdepth <depth>] <path>
        if (!recursive) {
          fprintf(stderr, "warning: omitting directory %s\n", source.c_str());
          continue;
        }

        // Enclose source path in quotes, as the path may contain whitespace
        stringstream ss;
        ss.clear();
        ss << std::quoted(source.c_str());
        source = ss.str().c_str();
        // Capture only last directory
        // This will end up appended to the target
        std::string smaxdepth = " ";

        if (depth != 0) {
          smaxdepth = " -maxdepth ";
          smaxdepth += std::to_string(depth);
          smaxdepth += " ";

          if (protocol == Protocol::EOS) {
            smaxdepth.insert(1, "-");
          }
        }

        cmdtext = "find ";

        if (protocol == Protocol::EOS) {
          cmdtext += "-f";
          cmdtext += smaxdepth.c_str();
          cmdtext += source.c_str();
        } else {
          cmdtext += source.c_str();
          cmdtext += smaxdepth.c_str();
          cmdtext += "-follow -type f";
        }
      }

      cmdtext += " 2> /dev/null";

      if (debug) {
        fprintf(stderr, "[eos-cp] running: %s\n", cmdtext.c_str());
      }

      int rc = (protocol == Protocol::EOS)  ?
               run_eos_command(cmdtext.c_str(), files) :
               run_command(cmdtext.c_str(), files);

      if (rc && !files.size()) {
        fprintf(stderr, "warning: could not expand source: %s\n", source.c_str());
        global_retc = rc;
        return -1;
      }
    } else {
      files.emplace_back(source.c_str());
    }

    for (auto& file : files) {
      // Check if path expansion discovered a symlink
      if (file.find(" -> ") != STR_NPOS) {
        file = process_symlink(file.c_str());
      }

      if (wildcard) {
        file.insert(basepath.c_str(), 0);
        source_find_list.emplace_back(file.c_str());
        continue;
      }

      if (debug) {
        fprintf(stderr, "[eos-cp] Copy list: %s\n", file.c_str());
      }

      File_t source_file;
      source_file.name = file.c_str();
      source_file.opaque = source_opaque.c_str();
      source_file.protocol = protocol;
      source_list.emplace_back(source_file);
      source_basepath_list.emplace_back(basepath.c_str());
    }
  }

  // Check if there is any file in the list
  if (source_list.empty()) {
    fprintf(stderr, "warning: found zero files to copy!\n");
    global_retc = 0;
    return 0;
  }

  // --------------------------------------------------------------------------
  // Process target path
  // --------------------------------------------------------------------------
  bool target_exists;
  struct stat target_stat;
  target.protocol = get_protocol(target.name.c_str());

  // Make sure executable to reach target exists
  if (check_protocol_tool(target.name.c_str())) {
    return -1;
  }

  // Handle opaque information for target
  if (target.protocol != Protocol::LOCAL) {
    int qpos = target.name.find("?");

    if (qpos != STR_NPOS) {
      target.opaque = target.name.c_str();
      target.opaque.keep(qpos + 1);
      target.name.erase(qpos);
    }

    // Replace '&' with '#AND#' for EOS target
    if (target.protocol == Protocol::EOS) {
      target.name.replace("&", "#AND");
    }
  }

  // Detect whether target is stdout
  const char* abs_path = absolute_path(target.name.c_str());
  target.name = abs_path;
  free((char*)abs_path);
  target_is_stdout = (target.name == "-");

  if (!target_is_stdout) {
    // Detect whether target is a directory
    int stat_rc = do_stat(target.name.c_str(), target.protocol, target_stat);
    target_exists = (stat_rc == 0);
    target_is_dir = is_dir(target.name.c_str(), target.protocol, &target_stat);

    // If multiple source files target must be a directory
    if (source_list.size() > 1) {
      // Target doesn't exist, mark it as directory
      if (!target_exists) {
        target_is_dir = true;
      }

      // Target is not a directory
      if (!target_is_dir) {
        fprintf(stderr, "error: target must be a directory\n");
        global_retc = EINVAL;
        return -1;
      }
    }

    // Target doesn't exist but name suggests should be a directory
    if (!target_exists && target.name.endswith("/")) {
      target_is_dir = true;
    }

    // If target is a directory then the name should also reflect this
    if (target_is_dir && !target.name.endswith("/")) {
      target.name.append("/");
    }

    // Check rights to create target directory
    if (target_is_dir && !target_exists) {
      if (!makeparent) {
        fprintf(stderr, "error: target must be created. Please try with "
                "create flag '-p' or see 'eos cp --help' for more info.\n");
        global_retc = EINVAL;
        return -1;
      }
    }

    // Create target directory tree for EOS or local path
    if (makeparent) {
      if ((target.protocol == Protocol::EOS) ||
          (target.protocol == Protocol::LOCAL)) {
        XrdOucString mktarget;

        if (target.name.endswith("/")) {
          mktarget = target.name.c_str();
        } else {
          eos::common::Path cTarget(target.name.c_str());
          mktarget = cTarget.GetParentPath();
        }

        std::string cmdtext = "mkdir -p ";

        if (target.protocol == Protocol::LOCAL) {
          cmdtext += "--mode 755 ";
        }

        cmdtext += mktarget.c_str();
        std::vector<XrdOucString> tmp;
        int rc = (target.protocol == Protocol::EOS) ?
                 run_eos_command(cmdtext.c_str(), tmp) :
                 run_command(cmdtext.c_str(), tmp);

        if (rc) {
          fprintf(stderr, "error: failed to create target directory : %s\n",
                  mktarget.c_str());
          global_retc = rc;
          return -1;
        }
      }
    }
  } else {
    // Disable all output for stdout target
    silent = true;
    noprogress = true;
  }

  // Set up environment for S3 target
  if ((target.protocol == Protocol::AS3) ||
      (target.protocol == Protocol::S3)) {
    const char* url = setup_s3_environment(target.name, target.opaque);

    if (url == NULL) {
      return -1;
    }

    target.name = url;
  }

  // Expand '/eos/' shortcut for EOS protocol
  if ((target.protocol == Protocol::EOS) &&
      (target.name.beginswith("/eos/"))) {
    if (!serveruri.endswith("/")) {
      target.name.insert("/", 0);
    }

    target.name.insert(serveruri.c_str(), 0);
  }

  if (debug) {
    fprintf(stderr, "[eos-cp] # of source files: %lu\n", source_list.size());
    fprintf(stderr, "[eos-cp] Setting target %s [protocol=%s]\n",
            target.name.c_str(), protocol_to_string(target.protocol));
  }

  // --------------------------------------------------------------------------
  // Compute size for each source path
  // --------------------------------------------------------------------------
  // As needed, check whether tools to access these protocols can be found
  bool s3_tool = false;
  bool http_tool = false;
  bool gsiftp_tool = false;

  for (auto& source : source_list) {
    bool statok = false;
    struct stat buf;
    source.atime.tv_nsec = source.mtime.tv_nsec = 0;

    switch (source.protocol) {
    // ------------------------------------------
    // EOS, XRoot or local file
    // ------------------------------------------
    case Protocol::EOS:
    case Protocol::XROOT:
    case Protocol::LOCAL:
      if (!do_stat(source.name.c_str(), source.protocol, buf)) {
        // For symbolic links, EOS stat returns the size of the link.
        // Ignore the size attribute in this case
        if (source.protocol != Protocol::LOCAL && !S_ISREG(buf.st_mode)) {
          source.size = 0;

          if (debug || !silent) {
            fprintf(stderr,
                    "warning: disable size check for path=%s [EOS symbolic link]\n",
                    source.name.c_str());
          }
        } else {
          copysize += buf.st_size;
          source.size = (unsigned long long) buf.st_size;
        }

        // Store the a/m-time
        source.atime.tv_sec = buf.st_atime;
        source.mtime.tv_sec = buf.st_mtime;
        statok = true;
      }

      break;

    // ------------------------------------------
    // S3 file
    // ------------------------------------------
    case Protocol::AS3:
    case Protocol::S3: {
      if (!s3_tool) {
        if (check_protocol_tool(source.name.c_str())) {
          return -1;
        }

        s3_tool = true;
      }

      const char* url = setup_s3_environment(source.name, source.opaque);

      if (url == NULL) {
        return -1;
      }

      XrdOucString s3env = "env S3_ACCESS_KEY_ID=";
      s3env += getenv("S3_ACCESS_KEY_ID");
      s3env += " S3_HOSTNAME=";
      s3env += getenv("S3_HOSTNAME");
      s3env += " S3_SECRET_ACCESS_KEY=";
      s3env += getenv("S3_SECRET_ACCESS_KEY");
      // Execute 's3' command to retrieve size
      XrdOucString cmdtext = "bash -c \"";
      cmdtext += s3env;
      cmdtext += " s3 head ";
      cmdtext += url;
      cmdtext += " | grep Content-Length | awk '{print \\$2}' 2> /dev/null\"";

      if (debug) {
        fprintf(stderr, "[eos-cp] running %s\n", cmdtext.c_str());
      }

      long long size = eos::common::StringConversion::LongLongFromShellCmd(
                         cmdtext.c_str());

      if ((!size) || (size == LLONG_MAX)) {
        fprintf(stderr, "error: path=%s cannot obtain size of S3 source file "
                "or file size is 0!\n", source.name.c_str());
        global_retc = EIO;
        return -1;
      }

      copysize += size;
      source.size = (unsigned long long) size;
      source.atime.tv_sec = source.mtime.tv_sec = 0;
      statok = true;
      break;
    }

    // ------------------------------------------
    // HTTP(S) & GSIFTP file
    // ------------------------------------------
    case Protocol::GSIFTP:
    case Protocol::HTTP:
    case Protocol::HTTPS:
      if ((source.protocol == Protocol::HTTP ||
           source.protocol == Protocol::HTTPS) && (!http_tool)) {
        if (check_protocol_tool(source.name.c_str())) {
          return -1;
        }

        http_tool = true;
      } else if ((source.protocol == Protocol::GSIFTP) && (!gsiftp_tool)) {
        if (check_protocol_tool(source.name.c_str())) {
          return -1;
        }

        gsiftp_tool = true;
      }

      source.size = 0;
      source.atime.tv_sec = source.mtime.tv_sec = 0;

      if (debug || !silent) {
        fprintf(stderr,
                "warning: disabling size check for path=%s [protocol=%s]\n",
                source.name.c_str(), protocol_to_string(source.protocol));
      }

      statok = true;
      break;

    default:
      break;
    }

    if (!statok) {
      fprintf(stderr, "error: cannot get file size of path=%s [protocol=%s]\n",
              source.name.c_str(), protocol_to_string(source.protocol));
      global_retc = EINVAL;
      return -1;
    }

    if (debug) {
      fprintf(stderr, "[eos-cp] path=%s size=%llu [protocol=%s]\n",
              source.name.c_str(), source.size,
              protocol_to_string(source.protocol));
    }
  }

  if (debug || (!silent && source_list.size() > 1)) {
    XrdOucString ssize;
    fprintf(stderr, "[eos-cp] going to copy %lu files and %s\n", source_list.size(),
            eos::common::StringConversion::GetReadableSizeString(ssize, copysize, "B"));
  }

  // Mark start timestamp
  gettimeofday(&start_time, &tz);
  // --------------------------------------------------------------------------
  // Create 'eoscp' command for each source path
  // and effectively perform the copy operation
  // --------------------------------------------------------------------------
  int file_idx = -1;
  retc = 0;

  for (auto& source : source_list) {
    XrdOucString dest = target.name.c_str();
    // Processed target path + original target opaque info
    XrdOucString target_path = "";
    // Temporary file upload flag
    bool temporary_file = false;
    file_idx++;

    //------------------------------------
    // Process destination path
    //------------------------------------

    // Append source suffix to destination
    // The source suffix: <source_path> = <source_basepath/><source_suffix>
    if (target_is_dir) {
      XrdOucString source_suffix = source.name.c_str();
      int pos = source_suffix.find(source_basepath_list[file_idx].c_str());

      if (pos == STR_NPOS) {
        fprintf(stderr, "error: could not identify source suffix for path=%s\n",
                source.name.c_str());
        global_retc = EINVAL;
        return -1;
      }

      pos += source_basepath_list[file_idx].length();
      source_suffix.keep(pos);
      dest += source_suffix.c_str();
    }

    // Check that source and destination are different
    if (!strcmp(source.name.c_str(), dest.c_str())) {
      fprintf(stderr,
              "warning: source and target are the same path=%s. Skipping path..\n",
              source.name.c_str());
      continue;
    }

    // Add opaque info to destination
    if (target.opaque.length()) {
      dest += "?";
      dest += target.opaque.c_str();
    }

    target_path = dest.c_str();

    // Continue processing for non STDOUT targets
    if (!target_is_stdout) {
      // Check if destination exists
      if (nooverwrite) {
        if ((target.protocol == Protocol::LOCAL) ||
            (target.protocol == Protocol::EOS)) {
          struct stat tmp;

          if (!do_stat(dest.c_str(), target.protocol, tmp)) {
            fprintf(stderr, "warning: target=%s exists, but --no-overwrite "
                    "flag specified\n", dest.c_str());
            retc |= EEXIST;
            continue;
          }
        }
      }

      // Handle EOS specific opaque info
      if ((target.protocol == Protocol::EOS) ||
          (target.protocol == Protocol::XROOT)) {
        char opaque[1024];
        const char* roles = eos_roles_opaque();
        snprintf(opaque, sizeof(opaque) - 1,
                 "%ceos.targetsize=%llu&eos.bookingsize=%llu&eos.app=eoscp%s%s%s",
                 (target.opaque.length()) ? '&' : '?',
                 source.size, source.size, atomic.c_str(),
                 (roles) ? "&" : "",
                 (roles) ? roles : "");
        dest.append(opaque);
      }

      // Protocols for EOS, XRoot and local targets are supported directly
      // S3 targets will be uploaded via STDIN & STDOUT pipes
      // Remaining protocols will be copied to a temporary file
      if ((target.protocol == Protocol::HTTP) ||
          (target.protocol == Protocol::HTTPS) ||
          (target.protocol == Protocol::GSIFTP)) {
        char tmp_name[] = "/tmp/com_cp.XXXXXX";
        int tmp_fd = mkstemp(tmp_name);

        if (tmp_fd == -1) {
          fprintf(stderr, "error: failed to create temporary file "
                  "while preparing copy for path=%s [protocol=%s]\n",
                  dest.c_str(), protocol_to_string(target.protocol));
          global_retc = errno;
          return -1;
        }

        close(tmp_fd);
        temporary_file = true;
        dest = tmp_name;
      }
    }

    //------------------------------------
    // Process source path
    //------------------------------------

    // Expand '/eos/' shortcut for EOS protocol
    if ((source.protocol == Protocol::EOS) &&
        (source.name.beginswith("/eos/"))) {
      if (!serveruri.endswith("/")) {
        source.name.insert("/", 0);
      }

      source.name.insert(serveruri.c_str(), 0);
    }

    // Add opaque info to source
    if (source.opaque.length()) {
      source.name += "?";
      source.name += source.opaque.c_str();
    }

    if (debug) {
      fprintf(stderr, "\n[eos-cp] copying %s to %s\n",
              source.name.c_str(), target_path.c_str());
    }

    //------------------------------------
    // Prepare STDIN and STDOUT pipes
    //------------------------------------
    XrdOucString transfersize =
      ""; // used for STDIN pipes to specify the target size to eoscp
    XrdOucString cmdtext = "";
    bool rstdin = false;
    bool rstdout = false;

    if ((source.protocol == Protocol::EOS) ||
        (source.protocol == Protocol::XROOT)) {
      const char* roles = eos_roles_opaque();
      source.name += (source.opaque.length())  ?  "&"  :  "?";
      source.name += "eos.app=eoscp";
      source.name += (roles)  ?  "&"  :  "";
      source.name += (roles)  ?  roles  : "";
    } else if ((source.protocol != Protocol::LOCAL) &&
               (source.protocol != Protocol::UNKNOWN)) {
      bool old_noprogress = noprogress;
      noprogress = true;
      XrdOucString safesource = source.name.c_str();

      while (safesource.replace("'", "\\'")) {}

      safesource.replace("as3:", "", 0, 3);
      XrdOucString tool = "";

      if (source.protocol == Protocol::HTTP)    {
        tool = "curl ";
      }

      if (source.protocol == Protocol::HTTPS)   {
        tool = "curl -k ";
      }

      if (source.protocol == Protocol::GSIFTP)  {
        tool = "globus-url-copy ";
      }

      if ((source.protocol == Protocol::AS3) ||
          (source.protocol == Protocol::S3)) {
        tool = "s3 get ";
        noprogress = old_noprogress;
      }

      cmdtext += tool;
      cmdtext += "$'";
      cmdtext += safesource;
      cmdtext += "'";

      if (source.protocol == Protocol::GSIFTP) {
        cmdtext += " -";
      }

      cmdtext += " | ";
      rstdin = true;
    }

    if ((source.protocol == Protocol::AS3) ||
        (source.protocol == Protocol::S3)  ||
        (target.protocol == Protocol::AS3) ||
        (target.protocol == Protocol::S3)) {
      char ts[1024];
      snprintf(ts, sizeof(ts) - 1, "%llu ", source.size);
      transfersize = ts;
    }

    if ((target.protocol == Protocol::AS3) ||
        (target.protocol == Protocol::S3)) {
      rstdout = true;
    }

    //------------------------------------
    // Prepare eoscp transaction name
    //------------------------------------
    XrdOucString safename = source.name.c_str();
    int qpos = safename.rfind("?");

    if (qpos != STR_NPOS) {
      safename.erase(qpos);
    }

    if (source.protocol != Protocol::LOCAL) {
      XrdOucString sprot, hostport;
      const char* url = eos::common::StringConversion::ParseUrl(safename.c_str(),
                        sprot, hostport);

      if (url) {
        std::string surl = url;
        safename = surl.c_str();
      }
    }

    safename = eos::common::Path(safename.c_str()).GetName();;
    safename.replace("&", "#AND#");
    safename.replace("'", "\\'");
    //------------------------------------
    // Construct 'eoscp' command
    //------------------------------------
    cmdtext += "eoscp ";

    if (append) {
      cmdtext += "-a ";
    }

    if (debug_level) {
      cmdtext += (debug_level == 1) ? "-v " : "-d ";
    }

    if (!summary) {
      cmdtext += "-s ";
    }

    if (makeparent) {
      cmdtext += "-p ";
    }

    if (noprogress) {
      cmdtext += "-n ";
    }

    if (nooverwrite) {
      cmdtext += "-x ";
    }

    if (transfersize.length()) {
      cmdtext += "-T ";
      cmdtext += transfersize;
      cmdtext += " ";
    }

    if (rate.length()) {
      cmdtext += "-t ";
      cmdtext += rate.c_str();
      cmdtext += " ";
    }

    cmdtext += "-N $'";
    cmdtext += safename.c_str();
    cmdtext += "' ";

    if (rstdin) {
      cmdtext += "- ";
    } else {
      XrdOucString safesource = source.name.c_str();
      safesource.replace("'", "\\'");
      cmdtext += "$'";
      cmdtext += safesource;
      cmdtext += "' ";
    }

    if (rstdout) {
      cmdtext += "-";
    } else {
      XrdOucString safedest = dest.c_str();
      safedest.replace("'", "\\'");
      cmdtext += "$'";
      cmdtext += safedest;
      cmdtext += "'";
    }

    if ((target.protocol == Protocol::AS3) ||
        (target.protocol == Protocol::S3)) {
      // s3 can upload via STDIN
      XrdOucString s3dest = dest.c_str();
      s3dest.replace("as3:", "", 0, 3);
      cmdtext += " | s3 put ";
      cmdtext += s3dest.c_str();
      cmdtext += " contentLength=";
      cmdtext += transfersize.c_str();
      cmdtext += " > /dev/null";
    }

    if (debug) {
      fprintf(stderr, "[eos-cp] running: %s\n", cmdtext.c_str());
    }

    int lrc = system(cmdtext.c_str());

    // Check if we got a CONTROL-C
    if (lrc == EINTR) {
      fprintf(stderr, "<Control-C>\n");
      break;
    }

    if (WEXITSTATUS(lrc)) {
      fprintf(stderr, "error: failed copying path=%s\n", target_path.c_str());
      retc |= lrc;
      continue;
    }

    //------------------------------------
    // Check target size
    //------------------------------------

    if (((target.protocol == Protocol::EOS)    ||
         (target.protocol == Protocol::XROOT)  ||
         (target.protocol == Protocol::LOCAL)) && (!target_is_stdout)) {
      struct stat buf;

      if (!do_stat(target_path.c_str(), target.protocol, buf)) {
        if ((!source.size) ||
            (buf.st_size == (off_t)(append ? target_stat.st_size + source.size :
                                    (off_t) source.size)
            )
           ) {
          // Preserve creation and modification timestamps
          if ((preserve) && (source.atime.tv_sec > 0) && (source.mtime.tv_sec > 0)) {
            bool updateok;

            if (target.protocol == Protocol::LOCAL) {
              struct timeval times[2];
              times[0].tv_sec = source.atime.tv_sec;
              times[0].tv_usec = source.atime.tv_nsec / 1000;
              times[1].tv_sec = source.mtime.tv_sec;
              times[1].tv_usec = source.mtime.tv_nsec / 1000;
              updateok = (utimes(target_path.c_str(), times) == 0);
            } else {
              char update[1024];
              const char* roles = eos_roles_opaque();
              sprintf(update, "%ceos.app=eoscp%s%s&mgm.pcmd=utimes"
                      "&tv1_sec=%llu&tv1_nsec=%llu"
                      "&tv2_sec=%llu&tv2_nsec=%llu",
                      (target.opaque.length()) ? '&' : '?',
                      (roles) ? "&" : "",
                      (roles) ? roles : "",
                      (unsigned long long) source.atime.tv_sec,
                      (unsigned long long) source.atime.tv_nsec,
                      (unsigned long long) source.mtime.tv_sec,
                      (unsigned long long) source.mtime.tv_nsec);
              XrdOucString request = target_path.c_str();
              request += update;
              char value[4096];
              value[0] = 0;
              long long update_rc = XrdPosixXrootd::QueryOpaque(request.c_str(),
                                    value, 4096);
              updateok = (update_rc >= 0);

              // Parse the stat output
              if (updateok) {
                char tag[1024];
                int tmp_retc;
                int items = sscanf(value, "%1023s retc=%d", tag, &tmp_retc);
                updateok = ((items == 2) && (strcmp(tag, "utimes:") == 0));
              }
            }

            if (!updateok) {
              fprintf(stderr, "warning: creation/modification time "
                      "could not be preserved for path=%s\n",
                      target_path.c_str());
            }
          }

          // Verify checksum
          if ((checksums) && (target.protocol != Protocol::LOCAL)) {
            XrdOucString address = serveruri.c_str();
            address += "//dummy";
            XrdCl::URL url(address.c_str());

            if (!url.IsValid()) {
              fprintf(stderr, "error: invalid file system URL=%s "
                      "[attempting checksum]\n",
                      url.GetURL().c_str());
              global_retc = EINVAL;
              return -1;
            }

            auto* fs = new XrdCl::FileSystem(url);

            if (!fs) {
              fprintf(stderr, "error: failed to get new FS object "
                      "[attempting checksum]\n");
              global_retc = EINVAL;
              return -1;
            }

            XrdCl::Buffer arg;
            XrdCl::Buffer* response = nullptr;
            XrdCl::XRootDStatus status;
            std::string query_path = dest.c_str();
            std::string::size_type pos = query_path.rfind("//");

            if (pos != std::string::npos) {
              query_path.erase(0, pos + 1);
            }

            arg.FromString(query_path);
            status = fs->Query(XrdCl::QueryCode::Checksum, arg, response);

            if (status.IsOK()) {
              XrdOucString xsum = response->GetBuffer();
              xsum.replace("eos ", "");
              fprintf(stdout, "path=%s size=%llu checksum=%s\n",
                      source.name.c_str(), source.size, xsum.c_str());
            } else {
              fprintf(stdout, "warning: failed getting checksum for path=%s size=%llu\n",
                      source.name.c_str(), source.size);
            }

            delete response;
            delete fs;
          }
        } else {
          XrdOucString ssize1, ssize2;
          fprintf(stderr, "error: file size difference between source and target file "
                  "source=%s [%s] target=%s [%s]\n",
                  source.name.c_str(),
                  eos::common::StringConversion::GetReadableSizeString(ssize1,
                      source.size, "B"),
                  target_path.c_str(),
                  eos::common::StringConversion::GetReadableSizeString(ssize2,
                      (unsigned long long) buf.st_size, "B"));
          lrc |= 0xffff00;
        }
      } else {
        fprintf(stderr, "error: target file not created source=%s target=%s\n",
                source.name.c_str(), target_path.c_str());
        lrc |= 0xffff00;
      }
    }

    // Attempt to upload temporary file
    if (temporary_file) {
      if (target.protocol == Protocol::GSIFTP) {
        cmdtext = "globus-url-copy file://";
        cmdtext += dest.c_str();
        cmdtext += " ";
        cmdtext += target_path.c_str();

        if (silent || noprogress) {
          cmdtext += " >& /dev/null";
        }

        if (debug) {
          fprintf(stderr, "[eos-cp] running: %s\n", cmdtext.c_str());
        }

        int rc = system(cmdtext.c_str());

        if (WEXITSTATUS(rc)) {
          fprintf(stderr, "error: failed to upload %s [protocol=gsiftp]\n",
                  target_path.c_str());
          lrc |= 0xffff00;
        }
      }

      if ((target.protocol == Protocol::HTTP) ||
          (target.protocol == Protocol::HTTPS)) {
        fprintf(stderr, "error: file uploads not supported for %s protocol [path=%s]\n",
                protocol_to_string(target.protocol), target_path.c_str());
        lrc |= 0xffff00;
      }

      // Clean-up the temporary file
      unlink(dest.c_str());
    }

    if (!WEXITSTATUS(lrc)) {
      files_copied++;
      copiedsize += source.size;
    }

    retc |= lrc;
  }

  // Mark end timestamp
  gettimeofday(&end_time, &tz);

  if (debug || !silent) {
    float time_elapsed = (float)(((end_time.tv_sec - start_time.tv_sec) * 1000000 +
                                  (end_time.tv_usec - start_time.tv_usec)) / 1000000.0);
    unsigned long long copyrate = (copiedsize / time_elapsed);
    XrdOucString ssize1, ssize2;
    fprintf(stderr,
            "%s[eos-cp] copied %d/%d files and %s in %.02f seconds with %s\n",
            (retc) ? "#WARNING " : "",
            files_copied,
            (int) source_list.size(),
            eos::common::StringConversion::GetReadableSizeString(ssize1, copiedsize, "B"),
            time_elapsed,
            eos::common::StringConversion::GetReadableSizeString(ssize2, copyrate, "B/s"));
  }

  global_retc = WEXITSTATUS(retc);
  return global_retc;
}


// ----------------------------------------------------------------------------
// Helper functions implementation
// ----------------------------------------------------------------------------

/**
 * Convenience function to be used by 'eos cp' to query EOS for file names.
 * The output of the command is placed into the result vector.
 * @param cmdline the eos command to be executed
 * @param result reference to the result vector
 * @return error code of the command
 */
int run_eos_command(const char* cmdline, std::vector<XrdOucString>& result)
{
  XrdOucString cmd = "eos -b ";

  if (user_role.length() && group_role.length()) {
    cmd += "--role ";
    cmd += user_role;
    cmd += " ";
    cmd += group_role;
    cmd += " ";
  }

  cmd += cmdline;
  return run_command(cmd.c_str(), result);
}

/**
 * Convenience function to be used by 'eos cp' to execute a command.
 * The output of the command is placed into the result vector.
 * @param cmdline the bash command to be executed
 * @param result reference to the result vector
 * @return error code of the command
 */
int run_command(const char* cmdline, std::vector<XrdOucString>& result)
{
  FILE* fp = popen(cmdline, "r");
  char line[4096];
  int rc;

  if (!fp) {
    fprintf(stderr, "error: failed executing command %s\n", cmdline);
    return errno;
  }

  while (fgets(line, sizeof(line), fp)) {
    int size = strlen(line);

    if (line[size - 1] == '\n') {
      line[size - 1] = '\0';
    }

    result.emplace_back(line);
  }

  rc = pclose(fp);
  return WEXITSTATUS(rc);
}

/**
 * Converts from local to absolute path.
 * This function makes the distinction between local or EOS paths.
 * Any other protocol will be left untouched.
 * Function is aware of interactive eos shell environment.
 * Local files will have the 'file:' prefix removed.
 * @param path the given path
 * @return abspath the absolute path
 */
const char* absolute_path(const char* path)
{
  Protocol protocol = get_protocol(path);

  if (protocol != Protocol::EOS && protocol != Protocol::LOCAL) {
    return strdup(path);
  }

  if (strcmp(path, "-") == 0) {
    return strdup(path);
  }

  XrdOucString spath = path;

  if (protocol == Protocol::LOCAL && spath.beginswith("file:")) {
    spath.erase(0, 5);
  }

  if (!spath.beginswith("/")) {
    XrdOucString abspath = "";

    if (interactive) {
      // Construct absolute path within eos shell
      abspath.insert(gPwd.c_str(), 0);
    } else {
      // Construct absolute path within regular shell
      abspath.insert("/", 0);
      abspath.insert(getenv("PWD"), 0);
    }

    spath.insert(abspath.c_str(), 0);
  }

  // Note: eos::common::Path expects an absolute path!
  // Note: eos::common::Path removes trailing '/'!
  std::string trailing_slash = "";

  if ((spath.endswith("/")) && (!spath.endswith("/./")) &&
      (!spath.endswith("/../"))) {
    trailing_slash = "/";
  }

  // Sanitize '.' and '..' entries
  spath = eos::common::Path(spath.c_str()).GetFullPath().c_str();
  spath += trailing_slash.c_str();
  return strdup(spath.c_str());
}

/**
 * Given a symlink path of the following format 'link -> target',
 * will return the name of the 'link'.
 * @param path the path to check
 * @return path the processed symlink name
 */
XrdOucString process_symlink(XrdOucString path)
{
  int pos = path.find(" -> ");

  if (pos != STR_NPOS) {
    path.erase(pos);
  }

  return path;
}

/**
 * Will check whether the given path is a directory or not.
 * For local and EOS protocols, stat information is used.
 * The stat structure may be passed, otherwise it is constructed.
 * Function is aware of interactive eos shell environment.
 * @param path the path to check
 * @param protocol the protocol to access the path
 * @param buf stat structure
 * @return true if directory, false otherwise
 */
bool is_dir(const char* path, Protocol protocol, struct stat* buf)
{
  if (protocol != Protocol::EOS && protocol != Protocol::LOCAL) {
    XrdOucString spath = path;
    return spath.endswith("/");
  }

  int rc = 0;

  struct stat tmpbuf{};
  if (buf == nullptr) {
    buf = &tmpbuf;
    const char* abs_path = absolute_path(path);
    rc = do_stat(abs_path, protocol, *buf);
    free(const_cast<char*>(abs_path));
  }

  return (rc == 0)  ?  S_ISDIR(buf->st_mode)  :  false;
}

/**
 * Returns eos roles opaque info from the global user variables.
 * @return roles opaque info containing eos roles
 */
const char* eos_roles_opaque()
{
  if (user_role.length() && group_role.length()) {
    XrdOucString roles = "eos.ruid=";
    roles += user_role;
    roles += "&eos.rgid=";
    roles += group_role;
    return strdup(roles.c_str());
  }

  return NULL;
}

/**
 * Perform stat on a given path.
 * Function makes the distinction between local or EOS paths.
 * @param path the path to stat
 * @param protocol the protocol to access the path
 * @param buf stat structure to fill
 * @return rc stat error code
 */
int do_stat(const char* path, Protocol protocol, struct stat& buf)
{
  const char* abs_path = absolute_path(path);
  int rc = -1;

  if (protocol == Protocol::EOS || protocol == Protocol::XROOT) {
    // Stat EOS file
    XrdOucString url = abs_path;
    const char* roles = eos_roles_opaque();

    // Expand '/eos/' shortcut for EOS protocol
    if (url.beginswith("/eos/")) {
      url = serveruri.c_str();
      url += (!url.endswith("/"))  ?  "/"  :  "";
      url += abs_path;
    }

    if (roles) {
      url += (url.find("?") == STR_NPOS)  ?  "?"  :  "&";
      url += eos_roles_opaque();
    }

    rc = XrdPosixXrootd::Stat(url.c_str(), &buf);
  } else if (protocol == Protocol::LOCAL) {
    // Stat local file
    rc = stat(abs_path, &buf);
  }

  free((char*)abs_path);
  return rc;
}

/**
 * Given an S3 path, will parse and remove the opaque info.
 * The following environment variables are set:
 * S3_ACCESS_KEY_ID <br/>
 * S3_SECRET_ACCESS_KEY <br/>
 * S3_HOSTNAME <br/>
 * @param path the S3 path
 * @param opaque the opaque info to parse for S3 info
 * @return url the S3 url
 */
const char* setup_s3_environment(XrdOucString path, XrdOucString opaque)
{
  XrdOucString sprot, hostport;
  XrdOucString url = eos::common::StringConversion::ParseUrl(path.c_str(),
                     sprot, hostport);

  if (!url.length()) {
    fprintf(stderr, "error: could not parse S3 url=%s", path.c_str());
    global_retc = EINVAL;
    return 0;
  }

  if (opaque.length()) {
    XrdOucEnv env(opaque.c_str());

    // Extract opaque S3 tags if present
    if (env.Get("s3.id"))  {
      setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
    }

    if (env.Get("s3.key")) {
      setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
    }
  }

  if (hostport.length()) {
    setenv("S3_HOSTNAME", hostport.c_str(), 1);
  }

  // Apply the ROOT compatibility environment variables
  if (getenv("S3_ACCESS_ID")) {
    setenv("S3_ACCESS_KEY_ID", getenv("S3_ACCESS_ID"), 1);
  }

  if (getenv("S3_ACCESS_KEY")) {
    setenv("S3_SECRET_ACCESS_KEY", getenv("S3_ACCESS_KEY"), 1);
  }

  // Check S3 environment
  if ((!getenv("S3_HOSTNAME")) || (!getenv("S3_ACCESS_KEY_ID")) ||
      (!getenv("S3_SECRET_ACCESS_KEY"))) {
    fprintf(stderr, "error: S3 environment not set up for %s\n", path.c_str());
    fprintf(stderr, "You have to set the following environment variables: "
            "S3_ACCESS_KEY_ID or S3_ACCESS_ID\n"
            "S3_SECRET_ACCESS_KEY or S3_ACCESS_KEY\n"
            "S3_HOSTNAME (or use path with URI)");
    global_retc = EINVAL;
    return 0;
  }

  return url.c_str();
}

/**
 * Check if required tools are available to access the given path.
 * @param path the path to access
 */
int check_protocol_tool(const char* path)
{
  Protocol protocol = get_protocol(path);
  std::string tool = "";
  char cmd[128];

  if (protocol == Protocol::HTTP || protocol == Protocol::HTTPS) {
    tool = "curl";
  } else if (protocol == Protocol::AS3 || protocol == Protocol::S3) {
    tool = "s3";
  } else if (protocol == Protocol::GSIFTP) {
    tool = "globus-url-copy";
  } else {
    return 0;
  }

  sprintf(cmd, "which %s >& /dev/null", tool.c_str());
  int rc = system(cmd);

  if (WEXITSTATUS(rc)) {
    fprintf(stderr, "error: %s executable not found in PATH\n", tool.c_str());

    if (tool == "s3") {
      fprintf(stderr, " error: please install S3 executable from libs3\n");
    }

    global_retc = WEXITSTATUS(rc);
  }

  return WEXITSTATUS(rc);
}

/**
 * Returns the protocol for a given path.
 * Function is aware of interactive eos shell environment.
 */
Protocol get_protocol(XrdOucString path)
{
  if (path.beginswith("/eos/")) {
    return Protocol::EOS;
  } else if (path.beginswith("http://")) {
    return Protocol::HTTP;
  } else if (path.beginswith("https://")) {
    return Protocol::HTTPS;
  } else if (path.beginswith("gsiftp://")) {
    return Protocol::GSIFTP;
  } else if (path.beginswith("root://")) {
    return Protocol::XROOT;
  } else if (path.beginswith("as3:")) {
    return Protocol::AS3;
  } else if (path.beginswith("s3://")) {
    return Protocol::S3;
  } else if (path.beginswith("file:")) {
    return Protocol::LOCAL;
  } else if (path.beginswith("/") || (path.find(":/") == STR_NPOS)) {
    return (interactive)  ?  Protocol::EOS  :  Protocol::LOCAL;
  }

  return Protocol::UNKNOWN;
}

/**
 * Returns a string representation of the protocol.
 */
const char* protocol_to_string(Protocol protocol)
{
  if (protocol == Protocol::EOS)       {
    return "eos";
  } else if (protocol == Protocol::HTTP)      {
    return "http";
  } else if (protocol == Protocol::HTTPS)     {
    return "https";
  } else if (protocol == Protocol::GSIFTP)    {
    return "gsiftp";
  } else if (protocol == Protocol::XROOT)     {
    return "root";
  } else if (protocol == Protocol::AS3)       {
    return "as3";
  } else if (protocol == Protocol::S3)        {
    return "s3";
  } else if (protocol == Protocol::LOCAL)     {
    return "local";
  }

  return "unknown";
}

/**
 * Parse and returns debug level from option string or -1 if invalid.
 * Option format: -d[=][1|2|3]
 */
int parse_debug_level(XrdOucString option)
{
  if (option.beginswith("-d")) {
    option.erase(0, 2);
  } else if (option.beginswith("--debug")) {
    option.erase(0, 7);
  }

  if (option.length() && ((option[0] == ' ') || (option[0] == '='))) {
    option.erase(0, 1);
  }

  if (!option.length()) {
    return 0;
  }

  int level = 0;

  try {
    level = std::stoul(option.c_str());
  } catch (...) { }

  if (level < 1 || level > 3) {
    fprintf(stderr, "error: invalid value for <debug level>=%s\n", option.c_str());
    return -1;
  }

  return level - 1;
}
