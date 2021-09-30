//------------------------------------------------------------------------------
//! @file eosfuse.cc
//! @author Andreas-Joachim Peters CERN
//! @brief EOS C++ Fuse low-level implementation (3rd generation)
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016CERN/Switzerland                                  *
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

#include "common/StacktraceHere.hh"
#ifndef __APPLE__
#include "common/ShellCmd.hh"
#endif
#include "kv/RocksKV.hh"
#include "eosfuse.hh"
#include "misc/fusexrdlogin.hh"
#include "misc/filename.hh"
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <sstream>
#include <memory>
#include <algorithm>
#include <thread>
#include <iterator>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include <dirent.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sched.h>

#ifdef HAVE_RICHACL
extern "C" { /* this 'extern "C"' brace will eventually end up in the .h file, then it can be removed */
#include <sys/richacl.h>
}
#include "misc/richacl.hh"
#endif

#include <sys/resource.h>
#include <sys/types.h>
#include <sys/file.h>

#include "common/XattrCompat.hh"

#ifdef __APPLE__
#define O_DIRECT 0
#define EKEYEXPIRED 127
#define SI_LOAD_SHIFT 16
#else
#include <sys/resource.h>
#endif

#include "common/Timing.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/StringConversion.hh"
#include "auth/Logbook.hh"
#include "md/md.hh"
#include "md/kernelcache.hh"
#include "kv/kv.hh"
#include "data/cache.hh"
#include "data/cachehandler.hh"

#if ( FUSE_USE_VERSION > 28 )
#include "misc/EosFuseSessionLoop.hh"
#endif

#define _FILE_OFFSET_BITS 64

const char* k_mdino = "sys.eos.mdino";
const char* k_nlink = "sys.eos.nlink";
const char* k_fifo = "sys.eos.fifo";
EosFuse* EosFuse::sEosFuse = 0;

/* -------------------------------------------------------------------------- */
EosFuse::EosFuse()
{
  sEosFuse = this;
  fusesession = 0;
  fusechan = 0;
  SetTrace(false);
}

/* -------------------------------------------------------------------------- */
EosFuse::~EosFuse()
{
}

/* -------------------------------------------------------------------------- */
static void
/* -------------------------------------------------------------------------- */
chmod_to_700_or_die(const std::string& path)
/* -------------------------------------------------------------------------- */
{
  if (path.empty()) {
    return;
  }

  if (chmod(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
    fprintf(stderr, "error: failed to make path=%s RWX for root - errno=%d",
            path.c_str(), errno);
    exit(-1);
  }
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
EosFuse::UsageGet()
{
  std::string usage = "usage CLI   : eosxd get <key> [<path>]\n";
  usage += "\n";
  usage +=
    "                     eos.btime <path>                   : show inode birth time\n";
  usage +=
    "                     eos.ttime <path>                   : show lastest mtime in tree\n";
  usage +=
    "                     eos.tsize <path>                   : show size of directory tree\n";
  usage +=
    "                     eos.dsize <path>                   : show total size of files inside a directory \n";
  usage +=
    "                     eos.name <path>                    : show EOS instance name for given path\n";
  usage +=
    "                     eos.md_ino <path>                  : show inode number valid on MGM \n";
  usage +=
    "                     eos.hostport <path>                : show MGM connection host + port for given path\n";
  usage +=
    "                     eos.mgmurl <path>                  : show MGM URL for a given path\n";
  usage +=
    "                     eos.stats <path>                   : show mount statistics\n";
  usage +=
    "                     eos.stacktrace <path>              : test thread stack trace functionality\n";
  usage +=
    "                     eos.quota <path>                   : show user quota information for a given path\n";
  usage +=
    "                     eos.reconnect <mount>              : reconnect and dump the connection credentials\n";
  usage +=
    "                     eos.reconnectparent <mount>        : reconnect parent process and dump the connection credentials\n";
  usage +=
    "                     eos.identity <mount>               : show credential assignment of the calling process\n";
  usage +=
    "                     eos.identityparent <mount>         : show credential assignment of the executing shell\n";
  usage += "\n";
  usage +=
    " as root             system.eos.md  <path>              : dump meta data for given path\n";
  usage +=
    "                     system.eos.cap <path>              : dump cap for given path\n";
  usage +=
    "                     system.eos.caps <mount>            : dump all caps\n";
  usage +=
    "                     system.eos.vmap <mount>            : dump virtual inode translation table\n";
  usage += "\n";
  return usage;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
EosFuse::UsageSet()
{
  std::string usage = "usage CLI   : eosxd set <key> <value> [<path>]\n";
  usage += "\n";
  usage +=
    " as root             system.eos.debug <level> <mount>   : set debug level with <level>=crit|warn|err|notice|info|debug|trace\n";
  usage +=
    "                     system.eos.dropcap - <mount>       : drop capability of the given path\n";
  usage +=
    "                     system.eos.dropcaps - <mount>      : drop call capabilities for given mount\n";
  usage +=
    "                     system.eos.resetstat - <mount>     : reset the statistic counters\n";
  usage +=
    "                     system.eos.log <mode> <mount>      : make log file public or private with <mode>=public|private\n";
  usage += "\n";
  return usage;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
EosFuse::UsageMount()
{
  std::string usage =
    "usage FS    : eosxd -ofsname=<host><remote-path> <mnt-path>\n";
  usage +=
    "                     eosxd -ofsname=<config-name> <mnt-path>\n";
  usage +=
    "                        with configuration file /etc/eos/fuse.<config-name>.conf\n";
  usage +=
    "                     mount -t fuse eosxd -ofsname=<host><remote-path> <mnt-path>\n";
  usage +=
    "                     mount -t fuse eosxd -ofsname=<config-name> <mnt-path>\n";
  usage += "\n";
  return usage;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
EosFuse::UsageHelp()
{
  std::string usage =
    "usage HELP  : eosxd [-h|--help|help]                    : get help\n";
  return usage;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
EosFuse::run(int argc, char* argv[], void* userdata)
/* -------------------------------------------------------------------------- */
{
  eos::common::Logging::GetInstance().LB->suspend();      /* no log thread yet */
  eos_static_debug("");
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("RunForkHandler", 1);
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  fuse_opt_parse(&args, NULL, NULL, NULL);
  char* local_mount_dir = 0;
  int err = 0;
  std::string no_fsync_list;
  std::string nowait_flush_exec_list;
  // check the fsname to choose the right JSON config file
  std::string fsname = "";

  if (argc == 1) {
    fprintf(stderr, "%s%s%s%s", UsageGet().c_str(), UsageSet().c_str(),
            UsageMount().c_str(), UsageHelp().c_str());
    exit(0);
  }

  for (int i = 0; i < argc; i++) {
    std::string option = argv[i];
    size_t npos;
    size_t epos;

    if ((option == "-h") ||
        (option == "help") ||
        (option == "--help")) {
      fprintf(stderr, "%s%s%s%s", UsageGet().c_str(), UsageSet().c_str(),
              UsageMount().c_str(), UsageHelp().c_str());
      exit(0);
    }

    if (option == "get") {
      if ((i + 1) >= argc) {
        fprintf(stderr, "%s\n", UsageGet().c_str());
        exit(-1);
      }

      std::string tag = argv[i + 1];
#ifndef __APPLE__
      std::string path = ((i + 2) >= argc) ? get_current_dir_name() : argv[i + 2];
#else
      std::string path = ((i + 2) >= argc) ? getenv("PWD") : argv[i + 2];
#endif
      std::string systemline = "getfattr --absolute-names --only-values -n ";
      systemline += tag;
      systemline += " ";
      systemline += path;
      int rc = system(systemline.c_str());
      exit(WEXITSTATUS(rc));
    }

    if (option == "set") {
      if ((i + 2) >= argc) {
        fprintf(stderr, "%s\n", UsageSet().c_str());
        exit(-1);
      }

      std::string tag = argv[i + 1];
      std::string value = argv[i + 2];
#ifndef __APPLE__
      std::string path = ((i + 3) >= argc) ? get_current_dir_name() : argv[i + 3];
#else
      std::string path = ((i + 3) >= argc) ? getenv("PWD") : argv[i + 3];
#endif
      std::string systemline = "setfattr -n ";
      systemline += tag;
      systemline += " -v ";
      systemline += value;
      systemline += " ";
      systemline += path;
      int rc = system(systemline.c_str());
      exit(WEXITSTATUS(rc));
    }

    if ((npos = option.find("fsname=")) != std::string::npos) {
      epos = option.find(",", npos);
      fsname = option.substr(npos + std::string("fsname=").length(),
                             (epos != std::string::npos) ?
                             epos - npos - std::string("fsname=").length() : -1);
      break;
    }
  }

  fprintf(stderr, "# fsname='%s'\n", fsname.c_str());

  if (getuid() == 0) {
    // the root mount always adds the 'allow_other' option
    fuse_opt_add_arg(&args, "-oallow_other");
    fprintf(stderr, "# -o allow_other enabled on shared mount\n");
  }

  fprintf(stderr, "# -o big_writes enabled\n");
  fuse_opt_add_arg(&args, "-obig_writes");
  std::string jsonconfig = "/etc/eos/fuse";
  std::string default_ssskeytab = "/etc/eos/fuse.sss.keytab";
  std::string jsonconfiglocal;

  if (geteuid()) {
    if (getenv("HOME")) {
      jsonconfig = getenv("HOME");
    } else {
      fprintf(stderr,"# warning: HOME environment not defined\n");
      jsonconfig = ".";
    }
    jsonconfig += "/.eos/fuse";
    if (getenv("HOME")) {
      default_ssskeytab = getenv("HOME");
    } else {
      default_ssskeytab = ".";
    }
    default_ssskeytab += "/.eos/fuse.sss.keytab";
  }

  if (fsname.length()) {
    if (((fsname.find("@") == std::string::npos)) &&
        ((fsname.find(":") == std::string::npos))) {
      jsonconfig += ".";
      jsonconfig += fsname;
    }
  }

  jsonconfiglocal = jsonconfig;
  jsonconfiglocal += ".local.conf";
  jsonconfig += ".conf";
#ifndef __APPLE__

  if (::access("/bin/fusermount", X_OK)) {
    fprintf(stderr, "error: /bin/fusermount is not executable for you!\n");
    exit(-1);
  }

#endif

  if (getuid() == 0) {
    unsetenv("KRB5CCNAME");
    unsetenv("X509_USER_PROXY");
  }

  cacheconfig cconfig;
  // ---------------------------------------------------------------------------------------------
  // The logic of configuration works liks that:
  // - every configuration value has a corresponding default value
  // - the configuration file name is taken from the fsname option given on the command line
  //   e.g. root> eosxd -ofsname=foo loads /etc/eos/fuse.foo.conf
  //        root> eosxd              loads /etc/eos/fuse.conf
  //        user> eosxd -ofsname=foo loads $HOME/.eos/fuse.foo.conf
  // One can avoid to use configuration files if the defaults are fine providing the remote host and remote mount directory via the fsname
  //   e.g. root> eosxd -ofsname=eos.cern.ch:/eos/ $HOME/eos mounts the /eos/ directory from eos.cern.ch shared under $HOME/eos/
  //   e.g. user> eosxd -ofsname=user@eos.cern.ch:/eos/user/u/user/ $home/eos mounts /eos/user/u/user from eos.cern.ch private under $HOME/eos/
  //   If this is a user-private mount the syntax 'foo@cern.ch' should be used to distinguish private mounts of individual users in the 'df' output
  //
  //   Please note, that root mounts are by default shared mounts with kerberos configuration,
  //   user mounts are private mounts with kerberos configuration
  // --------------------------------------------------------------------------------------------
  // XrdCl::* options we read from our config file
  std::vector<std::string> xrdcl_options;
  xrdcl_options.push_back("TimeoutResolution");
  xrdcl_options.push_back("ConnectionWindow");
  xrdcl_options.push_back("ConnectionRetry");
  xrdcl_options.push_back("StreamErrorWindow");
  xrdcl_options.push_back("RequestTimeout");
  xrdcl_options.push_back("StreamTimeout");
  xrdcl_options.push_back("RedirectLimit");
  std::string mountpoint;
  std::string store_directory;
  config.options.foreground = 0;
  config.options.automounted = 0;

  for (int i = 1; i < argc; ++i) {
    std::string opt = argv[i];
    std::string opt0 = argv[i - 1];

    if ((opt[0] != '-') && (opt0 != "-o")) {
      mountpoint = opt;
    }

    if (opt == "-f") {
      config.options.foreground = 1;
    }
  }

  try {
    // parse JSON configuration
    Json::Value root;
    Json::Reader reader;
    struct stat configstat;
    bool has_config = false;

    if (!::stat(jsonconfig.c_str(), &configstat)) {
      std::ifstream configfile(jsonconfig, std::ifstream::binary);

      if (reader.parse(configfile, root, false)) {
        fprintf(stderr, "# JSON parsing successful\n");
        has_config = true;
      } else {
        fprintf(stderr, "error: invalid configuration file %s - %s\n",
                jsonconfig.c_str(), reader.getFormattedErrorMessages().c_str());
        exit(EINVAL);
      }
    } else {
      fprintf(stderr, "# no config file - running on default values\n");
    }

    if (!::stat(jsonconfiglocal.c_str(), &configstat)) {
      Json::Value localjson;
      std::ifstream configfile(jsonconfiglocal, std::ifstream::binary);

      if (reader.parse(configfile, localjson, false)) {
        fprintf(stderr, "# JSON parsing successful\n");
        has_config = true;
      } else {
        fprintf(stderr, "error: invalid configuration file %s - %s\n",
                jsonconfiglocal.c_str(), reader.getFormattedErrorMessages().c_str());
        exit(EINVAL);
      }

      Merge(root, localjson);
    } else {
      fprintf(stderr, "# no config file for local overwrites\n");
    }

    if (!root.isMember("hostport")) {
      if (has_config) {
        fprintf(stderr,
                "error: please configure 'hostport' in your configuration file '%s'\n",
                jsonconfig.c_str());
        exit(EINVAL);
      }

      if (!fsname.length()) {
        fprintf(stderr,
                "error: please configure the EOS endpoint via fsname=<user>@<host\n");
        exit(EINVAL);
      }

      if ((fsname.find(".") == std::string::npos)) {
        fprintf(stderr,
                "error: when running without a configuration file you need to configure the EOS endpoint via fsname=<host>.<domain> - the domain has to be added!\n");
        exit(EINVAL);
      }

      size_t pos_add;

      if ((pos_add = fsname.find("@")) != std::string::npos) {
        std::string fsuser = fsname;
        fsname.erase(0, pos_add + 1);
        fsuser.erase(pos_add);

        if ((fsuser == "gw") || (fsuser == "smb")) {
          root["auth"]["krb5"] = 0;

          if (fsuser == "smb") {
            // enable overlay mode
            if (!root["options"].isMember("overlay-mode")) {
              root["options"]["overlay-mode"] = "0777";
              fprintf(stderr, "# enabling overlay-mode 0777 for smb export\n");
            }
          }
        }
      }

      size_t pos_colon;
      std::string remotemount;

      if ((pos_colon = fsname.find(":")) != std::string::npos) {
        remotemount = fsname.substr(pos_colon + 1);
        fsname.erase(pos_colon);
        root["remotemountdir"] = remotemount;
        fprintf(stderr, "# extracted remote mount dir from fsname is '%s'\n",
                remotemount.c_str());
      }

      root["hostport"] = fsname;
      fprintf(stderr, "# extracted connection host from fsname is '%s'\n",
              fsname.c_str());
    }

    if (!root.isMember("mdcachedir")) {
      if (geteuid()) {
        root["mdcachedir"] = "/var/tmp/eos/fusex/md-cache/";
      } else {
        root["mdcachedir"] = "/var/cache/eos/fusex/md-cache/";
      }

      fprintf(stderr, "# enabling swapping inodes with md-cache in '%s'\n",
              root["mdcachedir"].asString().c_str());
    }

    // apply some default settings for undefined entries.
    {
      if (!root.isMember("name")) {
        XrdOucString id = mountpoint.c_str();

        while (id.replace("/", "-")) {
        }

        fsname += id.c_str();
        root["name"] = fsname;
      }

      if (!root.isMember("hostport")) {
        root["hostport"] = "localhost";
      }

      if (!root.isMember("mdzmqidentity")) {
        if (geteuid()) {
          root["mdzmqidentity"] = "userd";
        } else {
          root["mdzmqidentity"] = "eosxd";
        }
      }

      if (!root.isMember("remotemountdir")) {
        root["remotemountdir"] = "/eos/";
      }

      if (!root.isMember("localmountdir")) {
        root["localmountdir"] = "/eos/";
      }

      if (!root["options"].isMember("debuglevel")) {
        root["options"]["debuglevel"] = 4;
      }

      if (!root["options"].isMember("backtrace")) {
        root["options"]["backtrace"] = 1;
      }

      if (!root["options"].isMember("md-kernelcache")) {
        root["options"]["md-kernelcache"] = 1;
      }

      if (!root["options"].isMember("leasetime")) {
        root["options"]["leasetime"] = 300;
      }

      if (!root["options"].isMember("md-kernelcache.enoent.timeout")) {
        root["options"]["md-kernelcache.enoent.timeout"] = 0;
      }

      if (!root["options"].isMember("md-backend.timeout")) {
        root["options"]["md-backend.timeout"] = 86400;
      }

      if (!root["options"].isMember("md-backend.put.timeout")) {
        root["options"]["md-backend.put.timeout"] = 120;
      }

      if (!root["options"].isMember("data-kernelcache")) {
        root["options"]["data-kernelcache"] = 1;
      }

      if (!root["options"].isMember("rename-is-sync")) {
        root["options"]["rename-is-sync"] = 1;
      }

      if (!root["options"].isMember("rm-is-sync")) {
        root["options"]["rm-is-sync"] = 0;
      }

      if (!root["options"].isMember("global-flush")) {
        root["options"]["global-flush"] = 1;
      }

      if (!root["options"].isMember("global-locking")) {
        root["options"]["global-locking"] = 1;
      }

      if (!root["options"].isMember("flush-wait-open")) {
        root["options"]["flush-wait-open"] = 1;
      }

      if (!root["options"].isMember("flush-wait-open-size")) {
        root["options"]["flush-wait-open-size"] = 262144;
      }

      if (!root["options"].isMember("flush-wait-umount")) {
        root["options"]["flush-wait-umount"] = 120;
      }

      if (!root["options"].isMember("show-tree-size")) {
        root["options"]["show-tree-size"] = 0;
      }

      if (!root["options"].isMember("hide-versions")) {
        root["options"]["hide-versions"] = 1;
      }

      if (!root["auth"].isMember("krb5")) {
        root["auth"]["krb5"] = 1;
      }

      if (!root["auth"].isMember("sss")) {
        root["auth"]["sss"] = 1;
      }

      if (!root["auth"].isMember("oauth2")) {
        root["auth"]["oauth2"] = 1;
      }

      if (!root["auth"].isMember("ignore-containerization")) {
        root["auth"]["ignore-containerization"] = 0;
      }

      if (!root["auth"].isMember("credential-store")) {
        if (geteuid()) {
          root["auth"]["credential-store"] = "/var/tmp/eos/fusex/credential-store/";
        } else {
          root["auth"]["credential-store"] = "/var/cache/eos/fusex/credential-store/";
        }
      }

      if ((root["auth"]["sss"] == 1) || (root["auth"]["oauth2"] == 1)) {
        if (!root["auth"].isMember("ssskeytab")) {
          root["auth"]["ssskeytab"] = default_ssskeytab;
          config.ssskeytab = root["auth"]["ssskeytab"].asString();
          struct stat buf;

          if (stat(config.ssskeytab.c_str(), &buf)) {
            fprintf(stderr,
                    "warning: sss keytabfile '%s' does not exist - disabling sss/oauth2\n",
                    config.ssskeytab.c_str());
            root["auth"]["sss"] = 0;
            root["auth"]["oauth2"] = 0;
          }
        } else {
          config.ssskeytab = root["auth"]["ssskeytab"].asString();
        }
      }

      if (!root["inline"].isMember("max-size")) {
        root["inline"]["max-size="] = 0;
      }

      if (!root["inline"].isMember("default-compressor")) {
        root["inline"]["default-compressor"] = "none";
      }

      if (!root["auth"].isMember("shared-mount")) {
        if (geteuid()) {
          root["auth"]["shared-mount"] = 0;
        } else {
          root["auth"]["shared-mount"] = 1;
        }
      }

      if (!root["options"].isMember("fd-limit")) {
        if (!geteuid()) {
          root["options"]["fd-limit"] = 524288;
        } else {
          root["options"]["fd-limit"] = 4096;
        }
      }

      if (!root["options"].isMember("no-fsync")) {
        root["options"]["no-fsync"].append(".db");
        root["options"]["no-fsync"].append(".db-journal");
        root["options"]["no-fsync"].append(".sqlite");
        root["options"]["no-fsync"].append(".sqlite-journal");
        root["options"]["no-fsync"].append(".db3");
        root["options"]["no-fsync"].append(".db3-journal");
        root["options"]["no-fsync"].append(".o");
      }

      if (!root["options"].isMember("flush-nowait-executables")) {
        root["options"]["flush-nowait-executables"].append("/tar");
        root["options"]["flush-nowait-executables"].append("/touch");
      }
    }

    if (!root["options"].isMember("cpu-core-affinity")) {
      root["options"]["cpu-core-affinity"] = 1;
    }

    if (!root["options"].isMember("no-xattr")) {
      root["options"]["no-xattr"] = 0;
    }

    if (!root["options"].isMember("no-link")) {
      root["options"]["no-link"] = 0;
    }

    if (!root["options"].isMember("nocache-graceperiod")) {
      root["options"]["nocache-graceperiod"] = 5;
    }

    if (!root["auth"].isMember("forknoexec-heuristic")) {
      root["auth"]["forknoexec-heuristic"] = 1;
    }

    if (!root["options"].isMember("rm-rf-protect-levels")) {
      root["options"]["rm-rf-protect-levels"] = 0;
    }

    if (!root["options"].isMember("rm-rf-bulk")) {
      root["options"]["rm-rf-bulk"] = 0;
    }

    if (!root["options"].isMember("write-size-flush-interval")) {
      root["options"]["write-size-flush-interval"] = 10;
    }

    if (!root["options"].isMember("submounts")) {
      root["options"]["submounts"] = 0;

      if (!root["options"].isMember("inmemory-inodes")) {
        root["options"]["inmemory-inodes"] = 16384;
      }

      // xrdcl default options
      XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
      XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionWindow", 10);
      XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionRetry", 0);
      XrdCl::DefaultEnv::GetEnv()->PutInt("StreamErrorWindow", 60);
      XrdCl::DefaultEnv::GetEnv()->PutInt("RequestTimeout", 30);
      XrdCl::DefaultEnv::GetEnv()->PutInt("StreamTimeout", 60);
      XrdCl::DefaultEnv::GetEnv()->PutInt("RedirectLimit", 3);

      for (auto it = xrdcl_options.begin(); it != xrdcl_options.end(); ++it) {
        if (root["xrdcl"].isMember(*it)) {
          XrdCl::DefaultEnv::GetEnv()->PutInt(it->c_str(),
                                              root["xrdcl"][it->c_str()].asInt());

          if (*it == "RequestTimeout") {
            int rtimeout = root["xrdcl"][it->c_str()].asInt();

            if (rtimeout > XrdCl::Proxy::chunk_timeout()) {
              XrdCl::Proxy::chunk_timeout(rtimeout + 60);
            }
          }
        }
      }

      if (root["xrdcl"].isMember("LogLevel")) {
        XrdCl::DefaultEnv::GetEnv()->PutString("LogLevel",
                                               root["xrdcl"]["LogLevel"].asString());
        setenv((char*) "XRD_LOGLEVEL", root["xrdcl"]["LogLevel"].asString().c_str(), 1);
        XrdCl::DefaultEnv::ReInitializeLogging();
      }

      // recovery setting
      if (!root["recovery"].isMember("read")) {
        root["recovery"]["read"] = 1;
      }

      if (!root["recovery"].isMember("read-open")) {
        root["recovery"]["read-open"] = 1;
      }

      if (!root["recovery"].isMember("read-open-noserver")) {
        root["recovery"]["read-open-noserver"] = 1;
      }

      if (!root["recovery"].isMember("read-open-noserver-retrywindow")) {
        root["recovery"]["read-open-noserver-retrywindow"] = 15;
      }

      if (!root["recovery"].isMember("write")) {
        root["recovery"]["write"] = 1;
      }

      if (!root["recovery"].isMember("write-open")) {
        root["recovery"]["write-open"] = 1;
      }

      if (!root["recovery"].isMember("write-open-noserver")) {
        root["recovery"]["write-open-noserver"] = 1;
      }

      if (!root["recovery"].isMember("write-open-noserver-retrywindow")) {
        root["recovery"]["write-open-noserver-retrywindow"] = 15;
      }

      // fuzzing settings
      if (!root["fuzzing"].isMember("open-async-submit")) {
        root["fuzzing"]["open-async-submit"] = 0;
      }

      if (!root["fuzzing"].isMember("open-async-return")) {
        root["fuzzing"]["open-async-return"] = 0;
      }

      if (!root["fuzzing"].isMember("open-async-submit-fatal")) {
        root["fuzzing"]["open-async-submit-fatal"] = 0;
      }

      if (!root["fuzzing"].isMember("open-async-return-fatal")) {
        root["fuzzing"]["open-async-return-fatal"] = 0;
      }

      config.name = root["name"].asString();
      config.hostport = root["hostport"].asString();
      config.remotemountdir = root["remotemountdir"].asString();
      config.localmountdir = root["localmountdir"].asString();
      config.statfilesuffix = root["statfilesuffix"].asString();
      config.statfilepath = root["statfilepath"].asString();
      config.appname = "fuse";

      if (root["appname"].asString().length()) {
        if (root["appname"].asString().find("&") == std::string::npos) {
          config.appname += "::";
          config.appname += root["appname"].asString();
        } else {
          fprintf(stderr, "error: appname cannot contain '&' character!\n");
          exit(EINVAL);
        }
      }

      config.options.debug = root["options"]["debug"].asInt();
      config.options.debuglevel = root["options"]["debuglevel"].asInt();
      config.options.enable_backtrace = root["options"]["backtrace"].asInt();
      config.options.libfusethreads = root["options"]["libfusethreads"].asInt();
      config.options.md_kernelcache = root["options"]["md-kernelcache"].asInt();
      config.options.md_kernelcache_enoent_timeout =
        root["options"]["md-kernelcache.enoent.timeout"].asDouble();
      config.options.md_backend_timeout =
        root["options"]["md-backend.timeout"].asDouble();
      config.options.md_backend_put_timeout =
        root["options"]["md-backend.put.timeout"].asDouble();
      config.options.data_kernelcache = root["options"]["data-kernelcache"].asInt();
      config.options.rename_is_sync = root["options"]["rename-is-sync"].asInt();
      config.options.rmdir_is_sync = root["options"]["rmdir-is-sync"].asInt();
      config.options.global_flush = root["options"]["global-flush"].asInt();
      config.options.flush_wait_open = root["options"]["flush-wait-open"].asInt();
      config.options.flush_wait_open_size =
        root["options"]["flush-wait-open-size"].asInt();
      config.options.flush_wait_umount = root["options"]["flush-wait-umount"].asInt();
      config.options.global_locking = root["options"]["global-locking"].asInt();
      config.options.overlay_mode = strtol(
                                      root["options"]["overlay-mode"].asString().c_str(), 0, 8);

      if (config.options.overlay_mode & 1) {
        config.options.x_ok = 0;
      } else {
        config.options.x_ok = X_OK;
      }

      config.options.fdlimit = root["options"]["fd-limit"].asInt();
      config.options.rm_rf_protect_levels =
        root["options"]["rm-rf-protect-levels"].asInt();
      config.options.rm_rf_bulk =
        root["options"]["rm-rf-bulk"].asInt();
      config.options.show_tree_size = root["options"]["show-tree-size"].asInt();
      config.options.hide_versions = root["options"]["hide-versions"].asInt();
      config.options.protect_directory_symlink_loops =
        root["options"]["protect-directory-symlink-loops"].asInt();
      config.options.cpu_core_affinity = root["options"]["cpu-core-affinity"].asInt();
      config.options.no_xattr = root["options"]["no-xattr"].asInt();
      config.options.no_eos_xattr_listing =
        root["options"]["no-eos-xattr-listing"].asInt();
      config.options.no_hardlinks = root["options"]["no-link"].asInt();
      config.options.write_size_flush_interval =
        root["options"]["write-size-flush-interval"].asInt();
      config.options.inmemory_inodes = root["options"]["inmemory-inodes"].asInt();
      config.options.flock = false;
#ifdef FUSE_SUPPORTS_FLOCK
      config.options.flock = true;
#endif

      if (config.options.no_xattr) {
        disable_xattr();
      }

      if (config.options.no_hardlinks) {
        disable_link();
      }

      config.options.nocache_graceperiod =
        root["options"]["nocache-graceperiod"].asInt();
      config.options.leasetime = root["options"]["leasetime"].asInt();
      config.options.submounts = root["options"]["submounts"].asInt();
      config.recovery.read = root["recovery"]["read"].asInt();
      config.recovery.read_open = root["recovery"]["read-open"].asInt();
      config.recovery.read_open_noserver =
        root["recovery"]["read-open-noserver"].asInt();
      config.recovery.read_open_noserver_retrywindow =
        root["recovery"]["read-open-noserver-retrywindow"].asInt();
      config.recovery.write = root["recovery"]["write"].asInt();
      config.recovery.write_open = root["recovery"]["write-open"].asInt();
      config.recovery.write_open_noserver =
        root["recovery"]["write-open-noserver"].asInt();
      config.recovery.write_open_noserver_retrywindow =
        root["recovery"]["write-open-noserver-retrywindow"].asInt();
      config.fuzzing.open_async_submit = root["fuzzing"]["open-async-submit"].asInt();
      config.fuzzing.open_async_return = root["fuzzing"]["open-async-return"].asInt();
      config.fuzzing.read_async_return = root["fuzzing"]["read-async-return"].asInt();
      config.fuzzing.open_async_submit_fatal = (bool)
          root["fuzzing"]["open-async-submit-fatal"].asInt();
      config.fuzzing.open_async_return_fatal = (bool)
          root["fuzzing"]["open-async-return-fatal"].asInt();
      XrdCl::Fuzzing::Configure(config.fuzzing.open_async_submit,
                                config.fuzzing.open_async_return,
                                config.fuzzing.open_async_submit_fatal,
                                config.fuzzing.open_async_return_fatal,
                                config.fuzzing.read_async_return);
      config.mdcachedir = root["mdcachedir"].asString();
      config.mqtargethost = root["mdzmqtarget"].asString();
      config.mqidentity = root["mdzmqidentity"].asString();
      config.mqname = config.mqidentity;
      config.auth.fuse_shared = root["auth"]["shared-mount"].asInt();
      config.auth.use_user_krb5cc = root["auth"]["krb5"].asInt();
      config.auth.use_user_oauth2 = root["auth"]["oauth2"].asInt();
      config.auth.ignore_containerization =
        root["auth"]["ignore-containerization"].asInt();
      config.auth.use_user_gsiproxy = root["auth"]["gsi"].asInt();
      config.auth.use_user_sss = root["auth"]["sss"].asInt();
      config.auth.credentialStore = root["auth"]["credential-store"].asString();

      if (config.auth.use_user_sss || config.auth.use_user_oauth2) {
        // store keytab location for this mount
        setenv("XrdSecSSSKT", root["auth"]["ssskeytab"].asString().c_str(), 1);
      }

      config.auth.tryKrb5First = !((bool)root["auth"]["gsi-first"].asInt());
      config.auth.environ_deadlock_timeout =
        root["auth"]["environ-deadlock-timeout"].asInt();
      config.auth.forknoexec_heuristic = root["auth"]["forknoexec-heuristic"].asInt();

      if (config.auth.environ_deadlock_timeout <= 0) {
        config.auth.environ_deadlock_timeout = 500;
      }

      config.inliner.max_size = root["inline"]["max-size"].asInt();
      config.inliner.default_compressor =
        root["inline"]["default-compressor"].asString();

      if ((config.inliner.default_compressor != "none") &&
          (config.inliner.default_compressor != "zlib")) {
        std::cerr <<
                  "inline default compressor value can only be 'none' or 'zlib'."
                  << std::endl;
        exit(EINVAL);
      }

      for (Json::Value::iterator it = root["options"]["no-fsync"].begin();
           it != root["options"]["no-fsync"].end(); ++it) {
        config.options.no_fsync_suffixes.push_back(it->asString());
        no_fsync_list += it->asString();
        no_fsync_list += ",";
      }

      for (Json::Value::iterator it =
             root["options"]["flush-nowait-executables"].begin();
           it != root["options"]["flush-nowait-executables"].end(); ++it) {
        config.options.nowait_flush_executables.push_back(it->asString());
        nowait_flush_exec_list += it->asString();
        nowait_flush_exec_list += ",";
      }

      // reset mdcachedir if compiled without rocksdb support
#ifndef HAVE_ROCKSDB

      if (!config.mdcachedir.empty()) {
        std::cerr <<
                  "Options mdcachedir is unavailable, fusex was compiled without rocksdb support."
                  << std::endl;
        config.mdcachedir = "";
      }

#endif // HAVE_ROCKSDB

      if (config.mdcachedir.length()) {
        // add the instance name to all cache directories
        if (config.mdcachedir.rfind("/") != (config.mdcachedir.size() - 1)) {
          config.mdcachedir += "/";
        }

        config.mdcachedir += config.name.length() ? config.name : "default";
      }

      // the store directory is the tree before we append individual UUIDs for each mount
      store_directory = config.mdcachedir;

      // default settings
      if (!config.statfilesuffix.length()) {
        config.statfilesuffix = "stats";
      }

      if (!config.mqtargethost.length()) {
        std::string h = config.hostport;

        if (h.find(":") != std::string::npos) {
          h.erase(h.find(":"));
        }

        config.mqtargethost = "tcp://" + h + ":1100";
      }

      {
        config.mqidentity.insert(0, "fuse://");
        config.mqidentity += "@";
        char hostname[4096];

        if (gethostname(hostname, sizeof(hostname))) {
          fprintf(stderr, "error: failed to get hostname!\n");
          exit(EINVAL);
        }

        config.clienthost = hostname;
        config.mqidentity += hostname;
        char suuid[40];
        uuid_t uuid;
        uuid_generate_time(uuid);
        uuid_unparse(uuid, suuid);
        config.clientuuid = suuid;
        config.mqidentity += "//";
        config.mqidentity += suuid;
        config.mqidentity += ":";
        char spid[16];
        snprintf(spid, sizeof(spid), "%d", getpid());
        config.mqidentity += spid;

        if (config.mdcachedir.length()) {
          config.mdcachedir += "/";
          config.mdcachedir += suuid;
        }
      }

      if (config.options.fdlimit > 0) {
        struct rlimit newrlimit;
        newrlimit.rlim_cur = config.options.fdlimit;
        newrlimit.rlim_max = config.options.fdlimit;

        if ((setrlimit(RLIMIT_NOFILE, &newrlimit) != 0) && (!geteuid())) {
          fprintf(stderr, "warning: unable to set fd limit to %lu - errno %d\n",
                  config.options.fdlimit, errno);
        }
      }

      struct rlimit nofilelimit;

      if (getrlimit(RLIMIT_NOFILE, &nofilelimit) != 0) {
        fprintf(stderr, "error: unable to get fd limit - errno %d\n", errno);
        exit(EINVAL);
      }

      fprintf(stderr, "# File descriptor limit: %lu soft, %lu hard\n",
              nofilelimit.rlim_cur, nofilelimit.rlim_max);
      // store the current limit
      config.options.fdlimit = nofilelimit.rlim_cur;
      // data caching configuration
      cconfig.type = cache_t::INVALID;
      cconfig.clean_on_startup = true;

      if (root["cache"]["type"].asString() == "disk") {
        cconfig.type = cache_t::DISK;
      } else if (root["cache"]["type"].asString() == "memory") {
        cconfig.type = cache_t::MEMORY;
      } else {
        if (root["cache"]["type"].asString().length()) {
          fprintf(stderr, "error: invalid cache type configuration\n");
          exit(EINVAL);
        } else {
          cconfig.type = cache_t::DISK;
        }
      }

      if (!root["cache"].isMember("read-ahead-bytes-nominal")) {
        root["cache"]["read-ahead-bytes-nominal"] = 256 * 1024;
      }

      if (!root["cache"].isMember("read-ahead-bytes-max")) {
        root["cache"]["read-ahead-bytes-max"] = 2 * 1024 * 1024;
      }

      if (!root["cache"].isMember("read-ahead-blocks-max")) {
        root["cache"]["read-ahead-blocks-max"] = 16;
      }

      if (!root["cache"].isMember("read-ahead-strategy")) {
        root["cache"]["read-ahead-strategy"] = "dynamic";
      }

      if (!root["cache"].isMember("read-ahead-sparse-ratio")) {
        root["cache"]["read-ahead-sparse-ratio"] = 0.0;
      }

      // auto-scale read-ahead and write-back buffer
      uint64_t best_io_buffer_size = meminfo.get().totalram / 8;

      if (best_io_buffer_size > 128 * 1024 * 1024) {
        best_io_buffer_size = 128 * 1024 * 1024;
      } else {
        // we take 1/8 of the total available memory, if we don't have one GB available
        best_io_buffer_size /= 8;
      }

      if (!root["cache"].isMember("max-read-ahead-buffer")) {
        fprintf(stderr, "# allowing max read-ahead buffers of %lu bytes\n",
                best_io_buffer_size);
        root["cache"]["max-read-ahead-buffer"] = (Json::Value::UInt64)
            best_io_buffer_size;
      }

      if (!root["cache"].isMember("max-write-buffer")) {
        fprintf(stderr, "# allowing max write-back buffers of %lu bytes\n",
                best_io_buffer_size);
        root["cache"]["max-write-buffer"] = (Json::Value::UInt64)best_io_buffer_size;
      }

      cconfig.location = root["cache"]["location"].asString();
      cconfig.journal = root["cache"]["journal"].asString();
      cconfig.default_read_ahead_size =
        root["cache"]["read-ahead-bytes-nominal"].asInt();
      cconfig.max_read_ahead_size = root["cache"]["read-ahead-bytes-max"].asInt();
      cconfig.max_read_ahead_blocks = root["cache"]["read-ahead-blocks-max"].asInt();
      cconfig.read_ahead_strategy = root["cache"]["read-ahead-strategy"].asString();
      cconfig.read_ahead_sparse_ratio = root["cache"]["read-ahead-sparse-ratio"].asFloat();

      if ((cconfig.read_ahead_strategy != "none") &&
          (cconfig.read_ahead_strategy != "static") &&
          (cconfig.read_ahead_strategy != "dynamic")) {
        fprintf(stderr,
                "error: invalid read-ahead-strategy specified - only 'none' 'static' 'dynamic' allowed\n");
        exit(EINVAL);
      }

      cconfig.max_inflight_read_ahead_buffer_size =
        root["cache"]["max-read-ahead-buffer"].asInt();
      cconfig.max_inflight_write_buffer_size =
        root["cache"]["max-write-buffer"].asInt();

      // set defaults for journal and file-start cache
      if (geteuid()) {
        if (!cconfig.location.length()) {
          cconfig.location = "/var/tmp/eos/fusex/cache/";
	  if (getenv("USER")) {
	    cconfig.location += getenv("USER");
	  } else {
	    cconfig.location += std::to_string(geteuid());
	  }
          cconfig.location += "/";
        }

        if (!cconfig.journal.length()) {
          cconfig.journal = "/var/tmp/eos/fusex/cache/";
	  if (getenv("USER")) {
	    cconfig.journal += getenv("USER");
	  } else {
	    cconfig.location +=std::to_string(geteuid());
	  }
          cconfig.journal += "/";
        }

        // default cache size 512 MB
        if (!root["cache"]["size-mb"].asString().length()) {
          root["cache"]["size-mb"] = 512;
        }

        // default cache size 64k inodes
        if (!root["cache"]["size-ino"].asString().length()) {
          root["cache"]["size-ino"] = 65536;
        }

        // default journal cache size 2 G
        if (!root["cache"]["journal-mb"].asString().length()) {
          root["cache"]["journal-mb"] = 2048;
        }

        // default journal size 64k inodes
        if (!root["cache"]["journal-ino"].asString().length()) {
          root["cache"]["journal-ino"] = 65536;
        }

        // default cleaning threshold
        if (!root["cache"]["clean-threshold"].asString().length()) {
          root["cache"]["clean-threshold"] = 85.0;
        }

        if (!root["cache"]["file-cache-max-kb"].asString().length()) {
          root["cache"]["file-cache-max-kb"] = 256;
        }
      } else {
        if (!cconfig.location.length()) {
          cconfig.location = "/var/cache/eos/fusex/cache/";
        }

        if (!cconfig.journal.length()) {
          cconfig.journal = "/var/cache/eos/fusex/cache/";
        }

        // default cache size 1 GB
        if (!root["cache"]["size-mb"].asString().length()) {
          root["cache"]["size-mb"] = 1000;
        }

        // default cache size 64k indoes
        if (!root["cache"]["size-ino"].asString().length()) {
          root["cache"]["size-ino"] = 65536;
        }

        // default cleaning threshold
        if (!root["cache"]["clean-threshold"].asString().length()) {
          root["cache"]["clean-threshold"] = 85.0;
        }

        if (!root["cache"]["file-cache-max-kb"].asString().length()) {
          root["cache"]["file-cache-max-kb"] = 256;
        }
      }

      if (cconfig.location == "OFF") {
        // disable file-start cache
        cconfig.location = "";
      }

      if (cconfig.journal == "OFF") {
        // disable journal
        cconfig.journal = "";
      }

      if (cconfig.location.length()) {
        if (cconfig.location.rfind("/") != (cconfig.location.size() - 1)) {
          cconfig.location += "/";
        }

        cconfig.location += config.name.length() ? config.name : "default";
      }

      if (cconfig.journal.length()) {
        if (cconfig.journal.rfind("/") != (cconfig.journal.size() - 1)) {
          cconfig.journal += "/";
        }

        cconfig.journal += config.name.length() ? config.name : "default";
      }

      config.auth.credentialStore += config.name.length() ? config.name : "default";
      // apply some defaults for all existing options
      // by default create all the specified cache paths
      std::string mk_cachedir = "mkdir -p " + config.mdcachedir;
      std::string mk_journaldir = "mkdir -p " + cconfig.journal;
      std::string mk_locationdir = "mkdir -p " + cconfig.location;
      std::string mk_credentialdir = "mkdir -p " + config.auth.credentialStore;

      if (config.mdcachedir.length()) {
        system(mk_cachedir.c_str());
        size_t slashes = std::count(config.mdcachedir.begin(), config.mdcachedir.end(),
                                    '/');

        // just some paranoid safety to avoid wiping by accident something we didn't intend to wipe
        if ((slashes > 2)  &&
            config.mdcachedir[config.mdcachedir.length() - 37] == '/') {
          config.mdcachedir_unlink = config.mdcachedir;
        }
      }

      if (cconfig.journal.length()) {
        system(mk_journaldir.c_str());
      }

      if (cconfig.location.length()) {
        system(mk_locationdir.c_str());
      }

      if (config.auth.credentialStore.length()) {
        system(mk_credentialdir.c_str());
      }

      // make the cache directories private to root
      chmod_to_700_or_die(config.mdcachedir);
      chmod_to_700_or_die(cconfig.journal);
      chmod_to_700_or_die(cconfig.location);
      chmod_to_700_or_die(config.auth.credentialStore);
      {
        char list[64];
#ifndef __APPLE__

        if (::listxattr(cconfig.location.c_str(), list, sizeof(list))) {
#else

        if (::listxattr(cconfig.location.c_str(), list, sizeof(list), 0)) {
#endif

          if (errno == ENOTSUP) {
            fprintf(stderr,
                    "error: eosxd requires XATTR support on partition %s errno=%d\n",
                    cconfig.location.c_str(), errno);
            exit(-1);
          }
        }
      }

      cconfig.total_file_cache_size = root["cache"]["size-mb"].asUInt64() * 1024 *
                                      1024;
      cconfig.total_file_cache_inodes = root["cache"]["size-ino"].asUInt64();
      cconfig.total_file_journal_size = root["cache"]["journal-mb"].asUInt64() *
                                        1024 * 1024;
      cconfig.total_file_journal_inodes = root["cache"]["journal-ino"].asUInt64();
      cconfig.per_file_cache_max_size = root["cache"]["file-cache-max-kb"].asUInt64()
                                        * 1024;
      cconfig.per_file_journal_max_size =
        root["cache"]["file-journal-max-kb"].asUInt64() * 1024;
      cconfig.clean_threshold = root["cache"]["clean-threshold"].asDouble();
      int rc = 0;

      if ((rc = cachehandler::instance().init(cconfig))) {
        exit(rc);
      }
    }
    {
      if (!mountpoint.length()) {
        // we allow to take the mountpoint from the json file if it is not given on the command line
        fuse_opt_add_arg(&args, config.localmountdir.c_str());
        mountpoint = config.localmountdir.c_str();
      } else {
        config.localmountdir = mountpoint;
      }

      if (mountpoint.length()) {
        DIR* d = 0;
        struct stat d_stat;

        // sanity check of the mount directory
        if (!(d = ::opendir(mountpoint.c_str()))) {
          // check for a broken mount
          if ((errno == ENOTCONN) || (errno == ENOENT)) {
            // force an 'umount -l '
            std::string systemline = "umount -l ";
            systemline += mountpoint;
            fprintf(stderr, "# dead mount detected - forcing '%s'\n", systemline.c_str());
            system(systemline.c_str());
          }

          if (stat(mountpoint.c_str(), &d_stat)) {
            if (errno == ENOENT) {
              fprintf(stderr, "error: mountpoint '%s' does not exist\n", mountpoint.c_str());
              exit(-1);
            } else {
              fprintf(stderr, "error: failed to stat '%s' - errno = %d\n", mountpoint.c_str(),
                      errno);
              exit(-1);
            }
          }
        } else {
          closedir(d);
        }
      }
    }
    std::string nodelay = getenv("XRD_NODELAY") ? getenv("XRD_NODELAY") : "";

    if (nodelay == "1") {
      fprintf(stderr, "# Running with XRD_NODELAY=1 (nagle algorithm is disabled)\n");
    } else {
      putenv((char*) "XRD_NODELAY=1");
      fprintf(stderr, "# Disabling nagle algorithm (XRD_NODELAY=1)\n");
    }

    if (!getenv("MALLOC_CONF")) {
      fprintf(stderr, "# Setting MALLOC_CONF=dirty_decay_ms:0\n");
      putenv((char*) "MALLOC_CONF=dirty_decay_ms:0");
    } else {
      fprintf(stderr, "# MALLOC_CONF=%s\n", getenv("MALLOC_CONF"));
    }

    int debug;
    {
      // C-style fuse configuration optionss
      struct eosxd_options {
        int autofs;
      };
      struct eosxd_options fuse_opts;
      fuse_opts.autofs = 0;
#define OPTION(t, p)    \
  { t, offsetof(struct eosxd_options, p), 1 }
      static struct fuse_opt eosxd_options_spec[] = {
        OPTION("autofs", autofs),
        FUSE_OPT_END
      };

      if (fuse_opt_parse(&args, &fuse_opts, eosxd_options_spec, NULL) == -1) {
        exit(errno ? errno : -1);
      }

      if (fuse_parse_cmdline(&args, &local_mount_dir, NULL, &debug) == -1) {
        exit(errno ? errno : -1);
      }

      config.options.automounted = fuse_opts.autofs;
    }

    if ((fusechan = fuse_mount(local_mount_dir, &args)) == NULL) {
      fprintf(stderr, "error: fuse_mount failed\n");
      exit(errno ? errno : -1);
    }

    if (fuse_daemonize(config.options.foreground) != -1) {
#ifndef __APPLE__
      eos::common::ShellCmd cmd("echo eos::common::ShellCmd init 2>&1");
      eos::common::cmd_status st = cmd.wait(5);
      int rc = st.exit_code;

      if (rc) {
        fprintf(stderr,
                "warning: failed to run shell command\n");
      }

      if (!geteuid()) {
        // change the priority of this process to maximum
        if (setpriority(PRIO_PROCESS, getpid(), -PRIO_MAX / 2) < 0) {
          fprintf(stderr,
                  "error: failed to renice this process '%u', to maximum priority '%d'\n",
                  getpid(), -PRIO_MAX / 2);
        }

        if (config.options.cpu_core_affinity > 0) {
          cpu_set_t cpuset;
          CPU_ZERO(&cpuset);
          CPU_SET(config.options.cpu_core_affinity - 1, &cpuset);
          sched_setaffinity(getpid(), sizeof(cpu_set_t), &cpuset);
          fprintf(stderr, "# Setting CPU core affinity to core %d\n",
                  config.options.cpu_core_affinity - 1);
        }
      }

#endif
      fusexrdlogin::initializeProcessCache(config.auth);

      if (config.options.foreground) {
        if (nodelay != "1") {
          fprintf(stderr,
                  "# warning: nagle algorithm is still enabled (export XRD_NODELAY=1 before running in foreground)\n");
        }
      }

      // Open log file
      if (getuid()) {
        char logfile[1024];

        if (getenv("EOS_FUSE_LOGFILE")) {
          snprintf(logfile, sizeof(logfile) - 1, "%s",
                   getenv("EOS_FUSE_LOGFILE"));
        } else {
          snprintf(logfile, sizeof(logfile) - 1, "/tmp/eos-fuse.%d.log",
                   getuid());
        }

        config.logfilepath = logfile;

        if (!config.statfilepath.length()) {
          config.statfilepath = logfile;
          config.statfilepath += ".";
          config.statfilepath += config.statfilesuffix;
        }

        // Running as a user ... we log into /tmp/eos-fuse.$UID.log
        if (!(fstderr = freopen(logfile, "a+", stderr))) {
          fprintf(stdout, "error: cannot open log file %s\n", logfile);
        } else {
          if (::chmod(logfile, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) {
            fprintf(stderr, "error: cannot change permission of log file %s\n", logfile);
            exit(-1);
          }
        }
      } else {
        // Running as root ... we log into /var/log/eos/fuse
        std::string log_path = "/var/log/eos/fusex/fuse.";

        if (getenv("EOS_FUSE_LOG_PREFIX") || fsname.length()) {
          if (getenv("EOS_FUSE_LOG_PREFIX")) {
            log_path += getenv("EOS_FUSE_LOG_PREFIX");
          } else {
            log_path += fsname;
          }

          if (!config.statfilepath.length()) config.statfilepath = log_path +
                "." + config.statfilesuffix;

          log_path += ".log";
        } else {
          if (!config.statfilepath.length()) config.statfilepath = log_path +
                config.statfilesuffix;

          log_path += "log";
        }

        eos::common::Path cPath(log_path.c_str());
        cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);
        config.logfilepath = log_path;
        ;

        if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr))) {
          fprintf(stderr, "error: cannot open log file %s\n", cPath.GetPath());
        } else if (::chmod(cPath.GetPath(), S_IRUSR | S_IWUSR)) {
          fprintf(stderr, "error: failed to chmod %s\n", cPath.GetPath());
        }
      }

      if (fstderr) {
        setvbuf(fstderr, (char*) NULL, _IONBF, 0);
      }

      eos::common::Logging::GetInstance().SetUnit("FUSE@eosxd");
      eos::common::Logging::GetInstance().gShortFormat = true;
      eos::common::Logging::GetInstance().SetFilter("DumpStatistic");
      eos::common::Logging::GetInstance().SetIndexSize(512);

      if (config.options.debug) {
        eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
      } else {
        if (config.options.debuglevel) {
          eos::common::Logging::GetInstance().SetLogPriority(config.options.debuglevel);
        } else {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO);
        }
      }

      eos::common::Logging::GetInstance().SetIndexSize(512);
      eos::common::Logging::GetInstance().EnableRateLimiter();
      fprintf(stderr, "Logging: suspended %d running %d in q %d\n",
              eos::common::Logging::GetInstance().LB->log_suspended,
              eos::common::Logging::GetInstance().LB->log_thread_started,
              eos::common::Logging::GetInstance().LB->log_buffer_in_q);
      eos::common::Logging::GetInstance().LB->resume();
      eos_static_debug("");
      fprintf(stderr, "Logging: suspended %d running %d in q %d\n",
              eos::common::Logging::GetInstance().LB->log_suspended,
              eos::common::Logging::GetInstance().LB->log_thread_started,
              eos::common::Logging::GetInstance().LB->log_buffer_in_q);
      // initialize mKV in case no cache is configured to act as no-op
      mKV.reset(new NoKV());
#ifdef HAVE_ROCKSDB

      if (!config.mdcachedir.empty()) {
        RocksKV* kv = new RocksKV();
        // clean old stale DBs
        kv->clean_stores(store_directory, config.clientuuid);

        if (kv->connect(config.name, config.mdcachedir) != 0) {
          fprintf(stderr, "error: failed to open rocksdb KV cache - path=%s",
                  config.mdcachedir.c_str());
          exit(EINVAL);
        }

        mKV.reset(kv);
      }

#endif // HAVE_ROCKSDB
      mdbackend.init(config.hostport, config.remotemountdir,
                     config.options.md_backend_timeout,
                     config.options.md_backend_put_timeout);
      mds.init(&mdbackend);
      caps.init(&mdbackend, &mds);
      datas.init();

      if (config.mqtargethost.length()) {
        if (mds.connect(config.mqtargethost, config.mqidentity, config.mqname,
                        config.clienthost, config.clientuuid)) {
          fprintf(stderr,
                  "error: failed to connect to mgm/zmq - connect-string=%s connect-identity=%s connect-name=%s",
                  config.mqtargethost.c_str(), config.mqidentity.c_str(), config.mqname.c_str());
          exit(EINVAL);
        }
      }

      if (cachehandler::instance().init_daemonized()) {
        exit(errno);
      }

      fusestat.Add("getattr", 0, 0, 0);
      fusestat.Add("setattr", 0, 0, 0);
      fusestat.Add("setattr:chown", 0, 0, 0);
      fusestat.Add("setattr:chmod", 0, 0, 0);
      fusestat.Add("setattr:utimes", 0, 0, 0);
      fusestat.Add("setattr:truncate", 0, 0, 0);
      fusestat.Add("lookup", 0, 0, 0);
      fusestat.Add("opendir", 0, 0, 0);
      fusestat.Add("readdir", 0, 0, 0);
      fusestat.Add("releasedir", 0, 0, 0);
      fusestat.Add("statfs", 0, 0, 0);
      fusestat.Add("mknod", 0, 0, 0);
      fusestat.Add("mkdir", 0, 0, 0);
      fusestat.Add("rm", 0, 0, 0);
      fusestat.Add("unlink", 0, 0, 0);
      fusestat.Add("rmdir", 0, 0, 0);
      fusestat.Add("rename", 0, 0, 0);
      fusestat.Add("access", 0, 0, 0);
      fusestat.Add("open", 0, 0, 0);
      fusestat.Add("create", 0, 0, 0);
      fusestat.Add("read", 0, 0, 0);
      fusestat.Add("write", 0, 0, 0);
      fusestat.Add("release", 0, 0, 0);
      fusestat.Add("fsync", 0, 0, 0);
      fusestat.Add("forget", 0, 0, 0);
      fusestat.Add("flush", 0, 0, 0);
      fusestat.Add("getxattr", 0, 0, 0);
      fusestat.Add("setxattr", 0, 0, 0);
      fusestat.Add("listxattr", 0, 0, 0);
      fusestat.Add("removexattr", 0, 0, 0);
      fusestat.Add("readlink", 0, 0, 0);
      fusestat.Add("symlink", 0, 0, 0);
      fusestat.Add("link", 0, 0, 0);
      fusestat.Add(__SUM__TOTAL__, 0, 0, 0);
      tDumpStatistic.reset(&EosFuse::DumpStatistic, this);
      tStatCirculate.reset(&EosFuse::StatCirculate, this);
      tMetaCacheFlush.reset(&metad::mdcflush, &mds);
      tMetaSizeFlush.reset(&metad::mdsizeflush, &mds);
      tMetaStackFree.reset(&metad::mdstackfree, &mds);
      tMetaCommunicate.reset(&metad::mdcommunicate, &mds);
      tCapFlush.reset(&cap::capflush, &caps);

      // wait that we get our heartbeat sent ...
      for (size_t i = 0; i < 50; ++i) {
        if (mds.is_visible()) {
          break;
        } else {
          eos_static_notice("waiting for established heart-beat : %u", i);
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }

      eos_static_warning("********************************************************************************");
      eos_static_warning("eosxd started version %s - FUSE protocol version %d",
                         VERSION, FUSE_USE_VERSION);
      eos_static_warning("eos-instance-url       := %s", config.hostport.c_str());
      eos_static_warning("thread-pool            := %s",
                         config.options.libfusethreads ? "libfuse" : "custom");
      eos_static_warning("zmq-connection         := %s", config.mqtargethost.c_str());
      eos_static_warning("zmq-identity           := %s", config.mqidentity.c_str());
      eos_static_warning("fd-limit               := %lu", config.options.fdlimit);

      if (config.auth.use_user_sss) {
        eos_static_warning("sss-keytabfile         := %s", config.ssskeytab.c_str());
      }

      eos_static_warning("options                := backtrace=%d md-cache:%d md-enoent:%.02f md-timeout:%.02f md-put-timeout:%.02f data-cache:%d rename-sync:%d rmdir-sync:%d flush:%d flush-w-open:%d flush-w-open-sz:%ld flush-w-umount:%d locking:%d no-fsync:%s flush-nowait-exec:%s ol-mode:%03o show-tree-size:%d hide-versions:%d protect-symlink-loops:%d core-affinity:%d no-xattr:%d no-eos-xattr-listing: %d no-link:%d nocache-graceperiod:%d rm-rf-protect-level=%d rm-rf-bulk=%d t(lease)=%d t(size-flush)=%d submounts=%d ino(in-mem)=%d flock:%d",
                         config.options.enable_backtrace,
                         config.options.md_kernelcache,
                         config.options.md_kernelcache_enoent_timeout,
                         config.options.md_backend_timeout,
                         config.options.md_backend_put_timeout,
                         config.options.data_kernelcache,
                         config.options.rename_is_sync,
                         config.options.rmdir_is_sync,
                         config.options.global_flush,
                         config.options.flush_wait_open,
                         config.options.flush_wait_open_size,
                         config.options.flush_wait_umount,
                         config.options.global_locking,
                         no_fsync_list.c_str(),
                         nowait_flush_exec_list.c_str(),
                         config.options.overlay_mode,
                         config.options.show_tree_size,
                         config.options.hide_versions,
                         config.options.protect_directory_symlink_loops,
                         config.options.cpu_core_affinity,
                         config.options.no_xattr,
                         config.options.no_eos_xattr_listing,
                         config.options.no_hardlinks,
                         config.options.nocache_graceperiod,
                         config.options.rm_rf_protect_levels,
                         config.options.rm_rf_bulk,
                         config.options.leasetime,
                         config.options.write_size_flush_interval,
                         config.options.submounts,
                         config.options.inmemory_inodes,
                         config.options.flock
                        );
      eos_static_warning("cache                  := rh-type:%s rh-nom:%d rh-max:%d rh-blocks:%d rh-sparse-ratio:%.01f max-rh-buffer=%lu max-wr-buffer=%lu tot-size=%ld tot-ino=%ld jc-size=%ld jc-ino=%ld dc-loc:%s jc-loc:%s clean-thrs:%02f%%%",
                         cconfig.read_ahead_strategy.c_str(),
                         cconfig.default_read_ahead_size,
                         cconfig.max_read_ahead_size,
                         cconfig.max_read_ahead_blocks,
			 cconfig.read_ahead_sparse_ratio,
                         cconfig.max_inflight_read_ahead_buffer_size,
                         cconfig.max_inflight_write_buffer_size,
                         cconfig.total_file_cache_size,
                         cconfig.total_file_cache_inodes,
                         cconfig.total_file_journal_size,
                         cconfig.total_file_journal_inodes,
                         cconfig.location.c_str(),
                         cconfig.journal.c_str(),
                         cconfig.clean_threshold);
      eos_static_warning("read-recovery          := enabled:%d ropen:%d ropen-noserv:%d ropen-noserv-window:%u",
                         config.recovery.read,
                         config.recovery.read_open,
                         config.recovery.read_open_noserver,
                         config.recovery.read_open_noserver_retrywindow);
      eos_static_warning("write-recovery         := enabled:%d wopen:%d wopen-noserv:%d wopen-noserv-window:%u",
                         config.recovery.write,
                         config.recovery.write_open,
                         config.recovery.write_open_noserver,
                         config.recovery.write_open_noserver_retrywindow);
      eos_static_warning("file-inlining          := emabled:%d max-size=%lu compressor=%s",
                         config.inliner.max_size ? 1 : 0,
                         config.inliner.max_size,
                         config.inliner.default_compressor.c_str());
      eos_static_warning("fuzzing                := open-async-submit:%lu(fatal:%lu) open-async-return:%lu(fatal:%lu) read-async-return:%lu",
                         config.fuzzing.open_async_submit,
                         config.fuzzing.open_async_submit_fatal,
                         config.fuzzing.open_async_return,
                         config.fuzzing.open_async_return_fatal,
                         config.fuzzing.read_async_return);
      std::string xrdcl_option_string;
      std::string xrdcl_option_loglevel;

      for (auto it = xrdcl_options.begin(); it != xrdcl_options.end(); ++it) {
        xrdcl_option_string += *it;
        xrdcl_option_string += ":";
        int value = 0;
        std::string svalue;
        XrdCl::DefaultEnv::GetEnv()->GetInt(it->c_str(), value);
        xrdcl_option_string += eos::common::StringConversion::GetSizeString(svalue,
                               (unsigned long long) value);
        xrdcl_option_string += " ";
      }

      XrdCl::DefaultEnv::GetEnv()->GetString("LogLevel", xrdcl_option_loglevel);
      eos_static_warning("xrdcl-options          := %s log-level='%s' fusex-chunk-timeout=%d",
                         xrdcl_option_string.c_str(), xrdcl_option_loglevel.c_str(),
                         XrdCl::Proxy::sChunkTimeout);
      fusesession = fuse_lowlevel_new(&args,
                                      &(get_operations()),
                                      sizeof(operations), NULL);

      if ((fusesession != NULL)) {
        if (fuse_set_signal_handlers(fusesession) != -1) {
          fuse_session_add_chan(fusesession, fusechan);

          if (getenv("EOS_FUSE_NO_MT") &&
              (!strcmp(getenv("EOS_FUSE_NO_MT"), "1"))) {
            err = fuse_session_loop(fusesession);
          } else {
#if ( FUSE_USE_VERSION <= 28 )
            err = fuse_session_loop_mt(fusesession);
#else

            if (config.options.libfusethreads) {
              err = fuse_session_loop_mt(fusesession);
            } else {
              EosFuseSessionLoop loop(10, 20, 10, 20);
              err = loop.Loop(fusesession);
            }

#endif
          }
        }
      }

      if (config.options.flush_wait_umount) {
        datas.terminate(config.options.flush_wait_umount);
      }

      eos_static_warning("eosxd stopped version %s - FUSE protocol version %d",
                         VERSION, FUSE_USE_VERSION);
      eos_static_warning("********************************************************************************");
      tDumpStatistic.join();
      tStatCirculate.join();
      tMetaCacheFlush.join();
      tMetaSizeFlush.join();
      tMetaStackFree.join();
      tMetaCommunicate.join();
      tCapFlush.join();
      {
        // rename the stats file
        std::string laststat = config.statfilepath;
        laststat += ".last";
        ::rename(config.statfilepath.c_str(), laststat.c_str());
      }

      if (Instance().Config().options.submounts) {
        Mounter().terminate();
      }

      // remove the session and channel object after all threads are joined
      if (fusesession) {
        fuse_remove_signal_handlers(fusesession);

        if (fusechan) {
          fuse_session_remove_chan(fusechan);
        }

        fuse_session_destroy(fusesession);
      }

      fuse_unmount(local_mount_dir, fusechan);
      mKV.reset();

      if (config.mdcachedir_unlink.length()) {
        // clean rocksdb directory
        std::string rmline = "rm -rf ";
        rmline += config.mdcachedir_unlink.c_str();
        system(rmline.c_str());
      }
    } else {
      fprintf(stderr, "error: failed to daemonize\n");
      exit(errno ? errno : -1);
    }

    return err ? 1 : 0;
  }
  catch (Json::Exception const&) {
    fprintf(stderr, "error: catched json config exception");
    exit(-1);
  }

  eos::common::Logging::GetInstance().shutDown(true);
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::umounthandler(int sig, siginfo_t* si, void* ctx)
/* -------------------------------------------------------------------------- */
{
  if (Instance().Config().options.submounts) {
    Instance().Mounter().terminate();
  }

  eos::common::handleSignal(sig, si, ctx);
  static char systemline[4096];
  snprintf(systemline,sizeof(systemline),"fusermount -u -z %s",EosFuse::Instance().Config().localmountdir.c_str());
  system(systemline);
  fprintf(stderr, "# umounthandler: executing %s\n", systemline);
  fprintf(stderr,
          "# umounthandler: sighandler received signal %d - emitting signal %d again\n",
          sig, sig);
  signal(SIGSEGV, SIG_DFL);
  signal(SIGABRT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
#ifndef __APPLE__
  pthread_t thread = pthread_self();
  pthread_kill(thread, sig);
#else
  kill(getpid(), sig);
#endif
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::init(void* userdata, struct fuse_conn_info* conn)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");

  if (EosFuse::instance().config.options.enable_backtrace) {
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = EosFuse::umounthandler;

    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
      char msg[1024];
      snprintf(msg, sizeof(msg), "failed to install SEGV handler");
      throw std::runtime_error(msg);
    }

    if (sigaction(SIGABRT, &sa, NULL) == -1) {
      char msg[1024];
      snprintf(msg, sizeof(msg), "failed to install SEGV handler");
      throw std::runtime_error(msg);
    }

    if (sigaction(SIGTERM, &sa, NULL) == -1) {
      char msg[1024];
      snprintf(msg, sizeof(msg), "failed to install SEGV handler");
      throw std::runtime_error(msg);
    }

    if (EosFuse::instance().config.options.enable_backtrace == 2) {
      setenv("EOS_ENABLE_BACKWARD_STACKTRACE", "1", 1);
    }
  }

  conn->want |= FUSE_CAP_EXPORT_SUPPORT | FUSE_CAP_POSIX_LOCKS |
                FUSE_CAP_BIG_WRITES;
}

void
EosFuse::destroy(void* userdata)
{
  eos_static_debug("");
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::DumpStatistic(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("started statistic dump thread");
  char ino_stat[16384];
  time_t start_time = time(NULL);

  while (!assistant.terminationRequested()) {
    meminfo.update();
    eos::common::LinuxStat::linux_stat_t osstat;
#ifndef __APPLE__
    eos::common::LinuxMemConsumption::linux_mem_t mem;

    if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem)) {
      eos_static_err("failed to get the MEM usage information");
    }

    if (!eos::common::LinuxStat::GetStat(osstat)) {
      eos_static_err("failed to get the OS usage information");
    }

#endif
    eos_static_debug("dumping statistics");
    XrdOucString out;
    fusestat.PrintOutTotal(out);
    std::string sout = out.c_str();
    time_t now = time(NULL);
    snprintf(ino_stat, sizeof(ino_stat),
             "# -----------------------------------------------------------------------------------------------------------\n"
             "ALL        inodes              := %lu\n"
             "ALL        inodes stack        := %lu\n"
             "ALL        inodes-todelete     := %lu\n"
             "ALL        inodes-backlog      := %lu\n"
             "ALL        inodes-ever         := %lu\n"
             "ALL        inodes-ever-deleted := %lu\n"
             "ALL        inodes-open         := %lu\n"
             "ALL        inodes-vmap         := %lu\n"
             "ALL        inodes-caps         := %lu\n"
             "ALL        inodes-tracker      := %lu\n"
	     "ALL        proxies             := %d\n"
             "# -----------------------------------------------------------------------------------------------------------\n",
             this->getMdStat().inodes(),
             this->getMdStat().inodes_stacked(),
             this->getMdStat().inodes_deleted(),
             this->getMdStat().inodes_backlog(),
             this->getMdStat().inodes_ever(),
             this->getMdStat().inodes_deleted_ever(),
             this->datas.size(),
             this->mds.vmaps().size(),
             this->caps.size(),
             this->Tracker().size(),
	     XrdCl::Proxy::Proxies()
            );
    sout += ino_stat;
    std::string s1;
    std::string s2;
    std::string s3;
    std::string s4;
    std::string s5;
    std::string s6;
    std::string s7;
    std::string s8;
    std::string blocker;
    uint64_t    blocker_inode;
    static std::string last_blocker = "";
    static uint64_t last_blocker_inode = 0;
    static double last_blocked_ms = 0;
    {
      unsigned long long rbytes, wbytes = 0;
      unsigned long nops = 0;
      float total_rbytes, total_wbytes = 0;
      int sum = 0;
      unsigned long totalram, freeram, loads0 = 0;
      {
        XrdSysMutexHelper sLock(getFuseStat().Mutex);
        rbytes = this->getFuseStat().GetTotal("rbytes");
        wbytes = this->getFuseStat().GetTotal("wbytes");
        nops = this->getFuseStat().GetOps();
        total_rbytes = this->getFuseStat().GetTotalAvg5("rbytes") / 1000.0 / 1000.0;
        total_wbytes = this->getFuseStat().GetTotalAvg5("wbytes") / 1000.0 / 1000.0;
        sum = (int) this->getFuseStat().GetTotalAvg5(":sum");
      }
      {
        std::lock_guard<std::mutex> lock(meminfo.mutex());
        totalram = meminfo.getref().totalram;
        freeram = meminfo.getref().freeram;
        loads0 = meminfo.getref().loads[0];
      }
      double blocked_ms = this->Tracker().blocked_ms(blocker, blocker_inode);
      snprintf(ino_stat, sizeof(ino_stat),
               "ALL        threads             := %llu\n"
               "ALL        visze               := %s\n"
               "ALL        rss                 := %s\n"
               "ALL        pid                 := %d\n"
               "ALL        log-size            := %lu\n"
               "ALL        wr-buf-inflight     := %s\n"
               "ALL        wr-buf-queued       := %s\n"
               "ALL        wr-nobuff           := %lu\n"
               "ALL        ra-buf-inflight     := %s\n"
               "ALL        ra-buf-queued       := %s\n"
               "ALL        ra-xoff             := %lu\n"
               "ALL        ra-nobuff           := %lu\n"
               "ALL        rd-buf-inflight     := %s\n"
               "ALL        rd-buf-queued       := %s\n"
               "ALL        version             := %s\n"
               "ALL        fuseversion         := %d\n"
               "ALL        starttime           := %lu\n"
               "ALL        uptime              := %lu\n"
               "ALL        total-mem           := %lu\n"
               "ALL        free-mem            := %lu\n"
               "ALL        load                := %lu\n"
               "ALL        total-rbytes        := %llu\n"
               "ALL        total-wbytes        := %llu\n"
               "ALL        total-io-ops        := %lu\n"
               "ALL        read--mb/s          := %.02f\n"
               "ALL        write-mb/s          := %.02f\n"
               "ALL        iops                := %d\n"
               "ALL        xoffs               := %lu\n"
               "ALL        instance-url        := %s\n"
               "ALL        client-uuid         := %s\n"
               "ALL        server-version      := %s\n"
               "ALL        automounted         := %d\n"
               "ALL        max-inode-lock-ms   := %.02f [%s]\n"
               "# -----------------------------------------------------------------------------------------------------------\n",
               osstat.threads,
               eos::common::StringConversion::GetReadableSizeString(s1, osstat.vsize, "b"),
               eos::common::StringConversion::GetReadableSizeString(s2, osstat.rss, "b"),
               getpid(),
               this->sizeLogFile(),
               eos::common::StringConversion::GetReadableSizeString(s3,
                   XrdCl::Proxy::sWrBufferManager.inflight(), "b"),
               eos::common::StringConversion::GetReadableSizeString(s4,
                   XrdCl::Proxy::sWrBufferManager.queued(), "b"),
               XrdCl::Proxy::sWrBufferManager.nobuf(),
               eos::common::StringConversion::GetReadableSizeString(s5,
                   XrdCl::Proxy::sRaBufferManager.inflight(), "b"),
               eos::common::StringConversion::GetReadableSizeString(s6,
                   XrdCl::Proxy::sRaBufferManager.queued(), "b"),
               XrdCl::Proxy::sRaBufferManager.xoff(),
               XrdCl::Proxy::sRaBufferManager.nobuf(),
               eos::common::StringConversion::GetReadableSizeString(s7,
                   data::datax::sBufferManager.inflight(), "b"),
               eos::common::StringConversion::GetReadableSizeString(s8,
                   data::datax::sBufferManager.queued(), "b"),
               VERSION,
               FUSE_USE_VERSION,
               start_time,
               now - start_time,
               totalram,
               freeram,
               loads0,
               rbytes,
               wbytes,
               nops,
               total_rbytes,
               total_wbytes,
               sum,
               Instance().datas.get_xoff(),
               EosFuse::Instance().config.hostport.c_str(),
               EosFuse::Instance().config.clientuuid.c_str(),
               EosFuse::Instance().mds.server_version().c_str(),
               EosFuse::Instance().Config().options.automounted,
               blocked_ms,
               blocker.c_str()
              );

      if (blocker_inode != 1) {
        if (blocker.length() && last_blocker.empty()) {
          eos_static_warning("IO blocked on ino=%#lx for op=%s since %.02f ms",
                             blocker_inode, blocker.c_str(), blocked_ms);
        }

        if (blocker.empty() && last_blocker.length()) {
          eos_static_warning("IO unblock on ino=%#lx for op=%s since %.02f ms",
                             last_blocker_inode, last_blocker.c_str(), last_blocked_ms);
        }
      }

      last_blocker_inode = blocker_inode;
      last_blocker = blocker;
      last_blocked_ms = blocked_ms;
    }
    sout += ino_stat;
    std::ofstream dumpfile(EosFuse::Instance().config.statfilepath);
    dumpfile << sout;
    this->statsout.set(sout);
    shrinkLogFile();
    assistant.wait_for(std::chrono::seconds(1));
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::StatCirculate(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("started stat circulate thread");
  fusestat.Circulate(assistant);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);
  struct fuse_entry_param e;
  metad::shared_md md = Instance().mds.getlocal(req, ino);

  if (ino != 1) {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || (md->deleted() && !md->lookup_is())) {
      rc = md->deleted() ? ENOENT : md->err();
    } else {
      fuse_ino_t cap_ino = S_ISDIR(md->mode()) ? ino : md->pid();
      cap::shared_cap pcap = Instance().caps.acquire(req, cap_ino ? cap_ino : 1,
                             S_IFDIR | Instance().Config().options.x_ok);
      double cap_lifetime = 0;
      XrdSysMutexHelper capLock(pcap->Locker());

      if (pcap->errc()) {
        rc = pcap->errc();
        capLock.UnLock();
      } else {
        cap_lifetime = pcap->lifetime();

        if (md->needs_refresh()) {
          md->Locker().UnLock();
          std::string authid = pcap->authid();
          capLock.UnLock();
          md = Instance().mds.get(req, ino);
          md->Locker().Lock();

          if (!md->id() || (md->deleted() && !md->lookup_is())) {
            rc = md->deleted() ? ENOENT : md->err();
          }
        } else {
          capLock.UnLock();
        }

        if (!rc) {
          md->convert(e, cap_lifetime);
          eos_static_info("%s", md->dump(e).c_str());
        }
      }
    }
  } else {
    // mountpoint stat does not require a cap
    md->convert(e);
    eos_static_info("%s", md->dump(e).c_str());
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_attr(req, &e.attr, e.attr_timeout);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int op,
                 struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("ino=%d", ino);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);
  cap::shared_cap pcap;
  metad::shared_md md;
  bool md_update_sync = false;     /* wait for MD update for return code */
  md = Instance().mds.get(req, ino);
  md->Locker().Lock();

  if (op == 0) {
    rc = EINVAL;
  } else if (!md->id() || (md->deleted() && !md->lookup_is())) {
    rc = md->deleted() ? ENOENT : md->err();
  } else {
    fuse_ino_t cap_ino = S_ISDIR(md->mode()) ? ino : md->pid();

    if (op & FUSE_SET_ATTR_MODE) {
      // chmod permissions are derived from the parent in case of a directory or file
      // otherwise we trap ourselfs when revoking W_OK
      if (S_ISDIR(md->mode())) {
        cap_ino = md->pid();
      }

      // retrieve cap for mode setting
      pcap = Instance().caps.acquire(req, cap_ino,
                                     M_OK);
    } else if ((op & FUSE_SET_ATTR_UID) || (op & FUSE_SET_ATTR_GID)) {
      // retrieve cap for owner setting
      pcap = Instance().caps.acquire(req, cap_ino,
                                     C_OK);
    } else if (op & FUSE_SET_ATTR_SIZE) {
      // retrieve cap for write
      pcap = Instance().caps.acquire(req, cap_ino,
                                     W_OK);
    } else if ((op & FUSE_SET_ATTR_ATIME)
               || (op & FUSE_SET_ATTR_MTIME)
               || (op & FUSE_SET_ATTR_ATIME_NOW)
               || (op & FUSE_SET_ATTR_MTIME_NOW)
              ) {
      // retrieve cap for write
      pcap = Instance().caps.acquire(req, cap_ino,
                                     W_OK);

      if (pcap->errc()) {
        // retrieve cap for set utime
        pcap = Instance().caps.acquire(req, cap_ino,
                                       SU_OK);
      }
    }

    if (pcap->errc()) {
      // don't fail chown not changing the owner,
      if ((op & FUSE_SET_ATTR_UID) && (md->uid() == (int) attr->st_uid)) {
        rc = 0;
      } else {
        if ((op & FUSE_SET_ATTR_GID) && (md->uid() == (int) attr->st_gid)) {
          rc = 0;
        } else {
          rc = pcap->errc();
        }
      }
    } else {
      if (op & FUSE_SET_ATTR_MODE) {
        /*
          EACCES Search permission is denied on a component of the path prefix.

          EFAULT path points outside your accessible address space.

          EIO    An I/O error occurred.

          ELOOP  Too many symbolic links were encountered in resolving path.

          ENAMETOOLONG
           path is too long.

          ENOENT The file does not exist.

          ENOMEM Insufficient kernel memory was available.

          ENOTDIR
           A component of the path prefix is not a directory.

          EPERM  The  effective  UID does not match the owner of the file,
           and the process is not privileged (Linux: it does not

           have the CAP_FOWNER capability).

          EROFS  The named file resides on a read-only filesystem.

          The general errors for fchmod() are listed below:

          EBADF  The file descriptor fd is not valid.

          EIO    See above.

          EPERM  See above.

          EROFS  See above.
         */
        ADD_FUSE_STAT("setattr:chmod", req);
        EXEC_TIMING_BEGIN("setattr:chmod");

        struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);
	md->set_ctime(tsnow.tv_sec);
	md->set_ctime_ns(tsnow.tv_nsec);
        md->set_mode(attr->st_mode);

        if (S_ISDIR(md->mode())) {
          // if this is a directory we have to revoke a potential existing cap for that directory
          cap::shared_cap cap = Instance().caps.get(req, md->id());
          cap->invalidate();

          if (Instance().mds.has_flush(ino)) {
            // we have also to wait for the upstream flush
            Instance().mds.wait_flush(req, md);
          }
        }

        EXEC_TIMING_END("setattr:chmod");
      }

      if ((op & FUSE_SET_ATTR_UID) || (op & FUSE_SET_ATTR_GID)) {
        /*
          EACCES Search permission is denied on a component of the path prefix.

          EFAULT path points outside your accessible address space.

          ELOOP  Too many symbolic links were encountered in resolving path.

          ENAMETOOLONG
           path is too long.

          ENOENT The file does not exist.

          ENOMEM Insufficient kernel memory was available.

          ENOTDIR
           A component of the path prefix is not a directory.

          EPERM  The calling process did not have the required permissions
           (see above) to change owner and/or group.

          EROFS  The named file resides on a read-only filesystem.

          The general errors for fchown() are listed below:

          EBADF  The descriptor is not valid.

          EIO    A low-level I/O error occurred while modifying the inode.

          ENOENT See above.

          EPERM  See above.

          EROFS  See above.
         */
        ADD_FUSE_STAT("setattr:chown", req);
        EXEC_TIMING_BEGIN("setattr:chown");

        if (op & FUSE_SET_ATTR_UID) {
          md->set_uid(attr->st_uid);
        }

        if (op & FUSE_SET_ATTR_GID) {
          md->set_gid(attr->st_gid);
        }

	struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);
	md->set_ctime(tsnow.tv_sec);
	md->set_ctime_ns(tsnow.tv_nsec);

        if (S_ISDIR(md->mode())) {
          // if this is a directory we have to revoke a potential existing cap for that directory
          cap::shared_cap cap = Instance().caps.get(req, md->id());
          cap->invalidate();

          if (Instance().mds.has_flush(ino)) {
            // we have also to wait for the upstream flush
            Instance().mds.wait_flush(req, md);
          }
        }

        md_update_sync = true;
        EXEC_TIMING_END("setattr:chown");
      }

      if (
        (op & FUSE_SET_ATTR_ATIME)
        || (op & FUSE_SET_ATTR_MTIME)
        || (op & FUSE_SET_ATTR_ATIME_NOW)
        || (op & FUSE_SET_ATTR_MTIME_NOW)
      ) {
        /*
        EACCES Search permission is denied for one of the directories in
        the  path  prefix  of  path

        EACCES times  is  NULL,  the caller's effective user ID does not match
        the owner of the file, the caller does not have
        write access to the file, and the caller is not privileged
        (Linux: does not have either the CAP_DAC_OVERRIDE or
        the CAP_FOWNER capability).

        ENOENT filename does not exist.

        EPERM  times is not NULL, the caller's effective UID does not
        match the owner of the file, and the caller is not priv‐
        ileged (Linux: does not have the CAP_FOWNER capability).

        EROFS  path resides on a read-only filesystem.
         */
        ADD_FUSE_STAT("setattr:utimes", req);
        EXEC_TIMING_BEGIN("setattr:utimes");
        eos_static_debug("setattr:utimes %d", fi ? fi->fh : -1);
        struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);

        if (op & FUSE_SET_ATTR_ATIME) {
          md->set_atime(attr->ATIMESPEC.tv_sec);
          md->set_atime_ns(attr->ATIMESPEC.tv_nsec);
          md->set_ctime(tsnow.tv_sec);
          md->set_ctime_ns(tsnow.tv_nsec);
        }

        if (op & FUSE_SET_ATTR_MTIME) {
          md->set_mtime(attr->MTIMESPEC.tv_sec);
          md->set_mtime_ns(attr->MTIMESPEC.tv_nsec);
          md->set_ctime(tsnow.tv_sec);
          md->set_ctime_ns(tsnow.tv_nsec);
        }

        if ((op & FUSE_SET_ATTR_ATIME_NOW) ||
            (op & FUSE_SET_ATTR_MTIME_NOW)) {
          if (op & FUSE_SET_ATTR_ATIME_NOW) {
            md->set_atime(tsnow.tv_sec);
            md->set_atime_ns(tsnow.tv_nsec);
            md->set_ctime(tsnow.tv_sec);
            md->set_ctime_ns(tsnow.tv_nsec);
          }

          if (op & FUSE_SET_ATTR_MTIME_NOW) {
            md->set_mtime(tsnow.tv_sec);
            md->set_mtime_ns(tsnow.tv_nsec);
            md->set_ctime(tsnow.tv_sec);
            md->set_ctime_ns(tsnow.tv_nsec);
          }
        }

        std::string cookie = md->Cookie();
        Instance().datas.update_cookie(md->id(), cookie);
        EXEC_TIMING_END("setattr:utimes");
      }

      if (op & FUSE_SET_ATTR_SIZE) {
        /*
        EACCES Search  permission is denied for a component of the path
        prefix, or the named file is not writable by the user.

        EFAULT Path points outside the process's allocated address space.

        EFBIG  The argument length is larger than the maximum file size.

        EINTR  While blocked waiting to complete, the call was interrupted
        by a signal handler; see fcntl(2) and signal(7).

        EINVAL The argument length is negative or larger than the maximum
        file size.

        EIO    An I/O error occurred updating the inode.

        EISDIR The named file is a directory.

        ELOOP  Too many symbolic links were encountered in translating the
        pathname.

        ENAMETOOLONG
        A component of a pathname exceeded 255 characters, or an
        entire pathname exceeded 1023 characters.

        ENOENT The named file does not exist.

        ENOTDIR
        A component of the path prefix is not a directory.

        EPERM  The underlying filesystem does not support extending a file
        beyond its current size.

        EROFS  The named file resides on a read-only filesystem.

        ETXTBSY
        The file is a pure procedure (shared text) file that is
        being executed.

        For ftruncate() the same errors apply, but instead of things that
        can be wrong with path, we now have things that  can
        be wrong with the file descriptor, fd:

        EBADF  fd is not a valid descriptor.

        EBADF or EINVAL
        fd is not open for writing.

        EINVAL fd does not reference a regular file.
         */
        ADD_FUSE_STAT("setattr:truncate", req);
        EXEC_TIMING_BEGIN("setattr:truncate");
        int rc = 0;

        if (!md->id() || (md->deleted() && !md->lookup_is())) {
          rc = ENOENT;
        } else {
          if ((md->mode() & S_IFDIR)) {
            rc = EISDIR;
          } else {
            if (fi && fi->fh) {
              // ftruncate
              data::data_fh* io = (data::data_fh*) fi->fh;

              if (io) {
                if (!md->creator() || (md->creator() &&
                                       ((off_t) md->size() != attr->st_size))) {
                  // no need to truncate if we still have the creator key
                  eos_static_debug("ftruncate size=%lu", (size_t) attr->st_size);
                  rc |= io->ioctx()->truncate(req, attr->st_size);
                  io->ioctx()->inline_file(attr->st_size);
                  struct timespec tsnow;
                  eos::common::Timing::GetTimeSpec(tsnow);
                  md->set_mtime(tsnow.tv_sec);
                  md->set_mtime_ns(tsnow.tv_nsec);
                  md->set_ctime(tsnow.tv_sec);
                  md->set_ctime_ns(tsnow.tv_nsec);
                  rc |= io->ioctx()->flush(req);
                  rc = rc ? (errno ? errno : rc) : 0;
                }
              } else {
                rc = EIO;
              }
            } else {
              // truncate
              eos_static_debug("truncate size=%lu", (size_t) attr->st_size);
              std::string cookie = md->Cookie();
              data::shared_data io = Instance().datas.get(req, md->id(), md);

              if (!md->creator() || (md->creator() &&
                                     ((off_t) md->size() != attr->st_size))) {
                rc = io->attach(req, cookie, true);
                eos_static_debug("calling truncate");
                rc |= io->truncate(req, attr->st_size);
                io->inline_file(attr->st_size);
                rc |= io->flush(req);
                rc |= io->detach(req, cookie, true);
                rc = rc ? (errno ? errno : rc) : 0;
                Instance().datas.release(req, md->id());
                struct timespec tsnow;
                eos::common::Timing::GetTimeSpec(tsnow);
                md->set_mtime(tsnow.tv_sec);
                md->set_mtime_ns(tsnow.tv_nsec);
                md->set_ctime(tsnow.tv_sec);
                md->set_ctime_ns(tsnow.tv_nsec);
              } else {
                Instance().datas.release(req, md->id());
              }
            }

            if (!rc) {
              ssize_t size_change = (int64_t)(attr->st_size) - (int64_t) md->size();

              if (size_change > 0) {
                Instance().caps.book_volume(pcap, size_change);
              } else {
                Instance().caps.free_volume(pcap, size_change);
              }

              md->set_size(attr->st_size);
            }
          }
        }

        EXEC_TIMING_END("setattr:truncate");
      }
    }
  }

  if (md_update_sync && rc == 0) {
    if (Instance().mds.has_flush(md->id())) {
      Instance().mds.wait_flush(req, md);
    }

    md->setop_update();
    Instance().mds.update(req, md, pcap->authid());

    if (Instance().mds.has_flush(md->id())) {
      Instance().mds.wait_flush(req, md);
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("id %ld err %d op %d del %d", md->id(), md->err(), md->getop(),
                       md->deleted());
    }

    rc = md->deleted() ? ENOENT : md->err();
  }

  if (rc) {
    md->Locker().UnLock();
    fuse_reply_err(req, rc);
  } else {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    md->convert(e, pcap->lifetime());
    eos_static_info("%s", md->dump(e).c_str());

    if (!md_update_sync) {
      Instance().mds.update(req, md, pcap->authid());
    }

    md->Locker().UnLock();
    fuse_reply_attr(req, &e.attr, e.attr_timeout);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());
}

void
EosFuse::lookup(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug(name);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);
  struct fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  {
    metad::shared_md md;
    md = Instance().mds.lookup(req, parent, name);

    if (md->id() && !md->deleted()) {
      cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                             Instance().Config().options.x_ok);
      XrdSysMutexHelper mLock(md->Locker());
      md->set_pid(parent);
      eos_static_info("%s", md->dump(e).c_str());
      md->lookup_inc();
      {
        auto attrMap = md->attr();

        // fetch necessary hardlink target
        if (attrMap.count(k_mdino)) {
          uint64_t mdino = std::stoull(attrMap[k_mdino]);
          uint64_t local_ino = EosFuse::Instance().mds.vmaps().forward(mdino);
          metad::shared_md tmd = EosFuse::Instance().mds.get(req, local_ino, "");
        }
      }
      md->convert(e, pcap->lifetime());
    } else {
      // negative cache entry
      e.ino = 0;

      if (Instance().Config().options.md_kernelcache_enoent_timeout) {
        e.attr_timeout = Instance().Config().options.md_kernelcache_enoent_timeout;
        e.entry_timeout = Instance().Config().options.md_kernelcache_enoent_timeout;
      } else {
        cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                               Instance().Config().options.x_ok);
        e.entry_timeout = pcap->lifetime();
        metad::shared_md pmd = Instance().mds.getlocal(req, parent);

        if (pmd && pmd->id()) {
          // remember negative lookups
          XrdSysMutexHelper mLock(pmd->Locker());
          pmd->local_enoent().insert(name);
        }
      }

      if (e.entry_timeout) {
        rc = 0;
      } else {
        rc = md->deleted() ? ENOENT : md->err();
      }
    }

    if (md->err()) {
      if (EOS_LOGS_DEBUG) {
        eos_static_debug("returning errc=%d for ino=%#lx name=%s md-name=%s\n",
                         md->err(), parent, name, md->name().c_str());
      }

      rc = md->err();
    }
  }
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);

  if (e.ino) {
    eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                      dump(id, parent, 0, rc, name).c_str());
  } else {
    eos_static_notice("t(ms)=%.03f ENOENT pino=%#lx name=%s lifetime=%.02f",
                      timing.RealTime(), parent, name, e.entry_timeout);
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_entry(req, &e);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
EosFuse::listdir(fuse_req_t req, fuse_ino_t ino, metad::shared_md& md)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  int rc = 0;
  fuse_id id(req);
  // retrieve cap
  cap::shared_cap pcap = Instance().caps.acquire(req, ino,
                         S_IFDIR | R_OK, true);
  XrdSysMutexHelper cLock(pcap->Locker());

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    // retrieve md
    std::string authid = pcap->authid();
    cLock.UnLock();
    md = Instance().mds.get(req, ino, authid, true);

    if (!md) {
      // this is weired, but instead of SEGV we throw an IO error
      rc = EIO;
    } else {
      if (!md->pid() && (md->id() != 1)) {
        if (md->err()) {
          rc = md->err();
        } else {
          rc = ENOENT;
        }
      }
    }
  }

  return rc;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EXEC_TIMING_BEGIN(__func__);
  ADD_FUSE_STAT(__func__, req);
  fuse_ino_t pino = 0;
  std::string name;
  int rc = 0;
  fuse_id id(req);
  metad::shared_md md;
  bool do_listdir = true;
  {
    Track::Monitor mon("opendir", Instance().Tracker(), ino);

    if (Instance().Config().options.rm_rf_protect_levels &&
        Instance().Config().options.rm_rf_bulk &&
        isRecursiveRm(req, true, true)) {
      md = Instance().mds.get(req, ino);

      if (md && md->attr().count("sys.recycle")) {
        do_listdir = false;
        eos_static_warning("Running recursive rm (pid = %d)", fuse_req_ctx(req)->pid);
        // bulk rm only when a recycle bin is configured
        {
          XrdSysMutexHelper mLock(md->Locker());
          name = md->name();

          if (!md->id() || md->deleted()) {
            rc = md->deleted() ? ENOENT : md->err();
          } else {
            if (!md->get_rmrf()) {
              rc = Instance().mds.rmrf(req, md);
            }
          }

          if (!rc) {
            if (!md->get_rmrf()) {
              if (EOS_LOGS_DEBUG) {
                eos_static_warning("rm-rf marks for deletion");
              }

              md->set_rmrf();
            }
          } else {
            md->unset_rmrf();
          }
        }

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("rm-rf gave retc=%d", rc);
        }

        if (!rc) {
          metad::shared_md pmd = Instance().mds.getlocal(req, md->pid());

          if (pmd) {
            pmd->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                          name));
            pmd->mutable_children()->erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                             name));
            pino = pmd->id();
          }

          rc = 0;

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("rm-rf returns 0");
          }
        }
      }
    }

    if (do_listdir) {
      rc = listdir(req, ino, md);
    }

    if (!rc) {
      XrdSysMutexHelper mLock(md->Locker());

      if (!md->id() || md->deleted()) {
        rc = md->deleted() ? ENOENT : md->err();
      } else {
        if (EOS_LOGS_DEBUG) {
          eos_static_debug("%s", md->dump().c_str());
        }

        if (Instance().Config().options.rm_rf_protect_levels &&
            isRecursiveRm(req) &&
            Instance().mds.calculateDepth(md) <=
            Instance().Config().options.rm_rf_protect_levels) {
          eos_static_warning("Blocking recursive rm (pid = %d)", fuse_req_ctx(req)->pid);
          rc = EPERM; // you shall not pass, muahahahahah
        } else {
          auto md_fh = new opendir_t;
          md_fh->md = md;
          md->opendir_inc();
          // fh contains a dummy 0 pointer
          eos_static_debug("adding ino=%08lx p-ino=%08lx", md->id(), md->pid());
          fi->fh = (unsigned long) md_fh;
        }
      }
    }
  }

  // rm-rf might need to tell the kernel cache  that this directory is gone
  if (pino && EosFuse::Instance().Config().options.md_kernelcache) {
    kernelcache::inval_entry(pino, name.c_str());
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_open(req, fi);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
EosFuse::readdir_filler(fuse_req_t req, EosFuse::opendir_t* md,
                        mode_t& pmd_mode, uint64_t& pmd_id)
/* -------------------------------------------------------------------------- */
{
  int rc = 0;
  metad::shared_md pmd = md->md;
  // avoid to have more than one md object locked at a time
  XrdSysMutexHelper mLock(pmd->Locker());
  pmd_id = pmd->id();
  pmd_mode = pmd->mode();

  // make sure, the meta-data object contains listing information
  // it might have been invalidated by a callback)

  do {
    if (pmd->type() == pmd->MDLS) {
      break;
    }

    pmd->Locker().UnLock();
    // refresh the listing
    eos_static_debug("refresh listing int=%#lx", pmd_id);
    rc = listdir(req, pmd_id, pmd);
    pmd->Locker().Lock();
  } while ((!rc) && (pmd->type() != pmd->MDLS));

  if (md->pmd_children.size() != pmd->local_children().size() ||
      ((md->pmd_mtime.tv_sec != (int64_t) pmd->mtime()) ||
       (md->pmd_mtime.tv_nsec != (int64_t) pmd->mtime_ns()))) {
    auto pmap = pmd->local_children();
    auto it = pmap.begin();
    // make a copy of the listing for subsequent readdir operations
    eos_static_debug("copying children map");
    md->pmd_children.clear();
    bool fillchildset = false;

    if (!md->readdir_items.size()) {
      fillchildset = true;
    }

    std::set<std::string> listing_diff;

    for (; it != pmap.end(); ++it) {
      if (!fillchildset) {
        listing_diff.insert(it->first);
      }

      std::string encname = eos::common::StringConversion::EncodeInvalidUTF8(
                              it->first);
      md->pmd_children[encname] = it->second;

      if (fillchildset) {
        md->readdir_items.push_back(encname);
      }
    }

    if (!fillchildset) {
      // compute difference to previous listing
      for (size_t i = 0; i < md->readdir_items.size(); ++i) {
        listing_diff.erase(md->readdir_items[i]);
      }
    }

    // append all new items
    for (auto i = listing_diff.begin(); i != listing_diff.end(); ++i) {
      md->readdir_items.push_back(*i);
    }

    // store mtime for the current state
    md->pmd_mtime.tv_sec = pmd->mtime();
    md->pmd_mtime.tv_nsec = pmd->mtime_ns();
  }

  if (!md->pmd_children.size()) {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("%s", Instance().mds.dump_md(pmd, false).c_str());
    }
  }

  return rc;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
/*
EBADF  Invalid directory stream descriptor fi->fh
 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);

  if (!fi->fh) {
    fuse_reply_err(req, EBADF);
    rc = EBADF;
  } else {
    // get the shared pointer from the open file descriptor
    opendir_t* md = (opendir_t*) fi->fh;
    metad::shared_md pmd = md->md;
    mode_t pmd_mode;
    uint64_t pmd_id;
    // refresh the current directory state
    rc = readdir_filler(req, md, pmd_mode, pmd_id);
    // only one readdir at a time
    XrdSysMutexHelper lLock(md->items_lock);
    eos_static_info("off=%lu size-%lu", off, md->pmd_children.size());
    fuse_ino_t cino = pmd_id;
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(struct stat));
    md->b.reset();
    // ---------------------------------------------------------------------- //
    // root directory has only . while all the other have . and ..
    // ---------------------------------------------------------------------- //
    size_t off_shift = (cino > 1) ? 2 : 1;

    // ---------------------------------------------------------------------- //
    // "."
    // ---------------------------------------------------------------------- //
    if (off == 0) {
      // at offset=0 add the '.' directory
      std::string bname = ".";
      eos_static_debug("list: %#lx %s", cino, bname.c_str());
      mode_t mode = pmd_mode;
      stbuf.st_ino = cino;
      stbuf.st_mode = mode;
      size_t a_size = fuse_add_direntry(req, md->b.ptr, size - md->b.size,
                                        bname.c_str(), &stbuf, ++off);
      eos_static_info("name=%s ino=%08lx mode=%#lx bytes=%u/%u",
                      bname.c_str(), cino, mode, a_size, size - md->b.size);
      md->b.ptr += a_size;
      md->b.size += a_size;
    }

    // ---------------------------------------------------------------------- //
    // ".."
    // ---------------------------------------------------------------------- //
    if (off == 1) {
      // at offset=1 add the '..' directory
      metad::shared_md ppmd = Instance().mds.get(req, pmd->pid(), "", false, 0, 0,
                              true);

      // don't add a '..' at root
      if ((cino > 1) && ppmd && (ppmd->id() == pmd->pid())) {
        fuse_ino_t cino = 0;
        mode_t mode = 0;
        {
          XrdSysMutexHelper ppLock(ppmd->Locker());
          cino = ppmd->id();
          mode = ppmd->mode();
        }
        std::string bname = "..";
        eos_static_debug("list: %#lx %s", cino, bname.c_str());
        stbuf.st_ino = cino;
        stbuf.st_mode = mode;
        size_t a_size = fuse_add_direntry(req, md->b.ptr, size - md->b.size,
                                          bname.c_str(), &stbuf, ++off);
        eos_static_info("name=%s ino=%08lx mode=%#lx bytes=%u/%u",
                        bname.c_str(), cino, mode, a_size, size - md->b.size);
        md->b.ptr += a_size;
        md->b.size += a_size;
      }
    }

    memset(&stbuf, 0, sizeof(struct stat));

    // ---------------------------------------------------------------------- //
    // the 'rest' of a listing
    // ---------------------------------------------------------------------- //

    for (size_t i = off - off_shift; i < md->readdir_items.size(); ++i) {
      std::string d_name = md->readdir_items[i];
      std::string bname = eos::common::StringConversion::DecodeInvalidUTF8(d_name);
      auto it = md->pmd_children.find(d_name);
      fuse_ino_t cino = it->second;
      metad::shared_md cmd = Instance().mds.get(req, cino, "", 0, 0, 0, true);

      if (!cmd) {
        continue;
      }

      eos_static_debug("list: %#lx %s (d=%d)", cino, it->first.c_str(),
                       cmd->deleted());

      if (strncmp(d_name.c_str(), "...eos.ino...",
                  13) == 0) { /* hard link deleted inodes */
        off++;
        continue;
      }

      mode_t mode;
      {
        XrdSysMutexHelper cLock(cmd->Locker());
        mode = cmd->mode();

        // skip deleted entries or hidden entries
        if (cmd->deleted()) {
          continue;
        }
      }
      stbuf.st_ino = cino;
      {
        auto attrMap = cmd->mutable_attr();

        if (attrMap->count(k_mdino)) {
          uint64_t mdino = std::stoull((*attrMap)[k_mdino]);
          uint64_t local_ino = Instance().mds.vmaps().forward(mdino);

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("hlnk %s id %#lx mdino '%s' (%lx) local_ino %#lx",
                             cmd->name().c_str(), cmd->id(), (*attrMap)[k_mdino].c_str(), mdino, local_ino);
          }

          stbuf.st_ino = local_ino;
          metad::shared_md target = Instance().mds.get(req, local_ino, "", 0, 0, 0,
                                    true);
          mode = target->mode();
        }
      }
      stbuf.st_mode = mode;
      size_t a_size = fuse_add_direntry(req, md->b.ptr, size - md->b.size,
                                        bname.c_str(), &stbuf, ++off);

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("name=%s id=%#lx ino=%#lx mode=%#o bytes=%u/%u ",
                         bname.c_str(), cino, stbuf.st_ino, mode, a_size, size - md->b.size);
      }

      if (a_size > (size - md->b.size)) {
        off--;
        break;
      }

      md->b.ptr += a_size;
      md->b.size += a_size;
    }

    if (md->b.size) {
      fuse_reply_buf(req, md->b.buffer(), md->b.size);
    } else {
      fuse_reply_buf(req, md->b.buffer(), 0);
    }

    eos_static_info("size=%lu off=%llu reply-size=%lu \n",
                    size, off, md->b.size);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EXEC_TIMING_BEGIN(__func__);
  ADD_FUSE_STAT(__func__, req);
  int rc = 0;
  fuse_id id(req);
  opendir_t* md = (opendir_t*) fi->fh;

  if (md) {
    // The following two lines act as a barrier to ensure the last readdir() has
    // released items_lock. From the point of view of the FUSE kernel module,
    // once we call fuse_reply_buf inside readdir, that syscall is over, and it
    // is free to call releasedir. This creates a race condition where we try to
    // delete md while readdir still holds items_lock - the following two lines
    // prevent this.
    md->items_lock.Lock();
    md->items_lock.UnLock();
    md->md->opendir_dec(1);
    delete md;
    fi->fh = 0;
  }

  EXEC_TIMING_END(__func__);
  fuse_reply_err(req, 0);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::statfs(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);
  struct statvfs svfs;
  memset(&svfs, 0, sizeof(struct statvfs));
  rc = Instance().mds.statvfs(req, &svfs);

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_statfs(req, &svfs);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode)
/* -------------------------------------------------------------------------- */
/*
EACCES The parent directory does not allow write permission to the process,
or one of the directories in pathname  did

not allow search permission.  (See also path_resolution(7).)

EDQUOT The user's quota of disk blocks or inodes on the filesystem has been
exhausted.

EEXIST pathname  already exists (not necessarily as a directory).
This includes the case where pathname is a symbolic
link, dangling or not.

EFAULT pathname points outside your accessible address space.

ELOOP  Too many symbolic links were encountered in resolving pathname.

EMLINK The number of links to the parent directory would exceed LINK_MAX.

ENAMETOOLONG
pathname was too long.

ENOENT A directory component in pathname does not exist or is a dangling
symbolic link.

ENOMEM Insufficient kernel memory was available.

ENOSPC The device containing pathname has no room for the new directory.

ENOSPC The new directory cannot be created because the user's disk quota is
exhausted.

ENOTDIR
A component used as a directory in pathname is not, in fact, a directory.

EPERM  The filesystem containing pathname does not support the creation of
directories.

EROFS  pathname refers to a file on a read-only filesystem.
 */
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug(name);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("mkdir", Instance().Tracker(), parent, true);
  int rc = 0;
  fuse_id id(req);
  struct fuse_entry_param e;
  // do a parent check
  cap::shared_cap pcap1 = Instance().caps.acquire(req, parent,
                          S_IFDIR | X_OK , true);
  cap::shared_cap pcap2 = Instance().caps.acquire(req, parent,
                          S_IFDIR | X_OK | W_OK, true);

  if (pcap1->errc()) {
    rc = pcap1->errc();
  } else {
    metad::shared_md md;
    metad::shared_md pmd;
    uint64_t del_ino = 0;
    md = Instance().mds.lookup(req, parent, name);
    pmd = Instance().mds.get(req, parent, pcap2->authid());
    {
      std::string implied_cid;
      {
        // logic avoiding a mkdir/rmdir/mkdir sync/async race
        {
          XrdSysMutexHelper pLock(pmd->Locker());
          auto it = pmd->get_todelete().find(
                      eos::common::StringConversion::EncodeInvalidUTF8(name));

          if ((it != pmd->get_todelete().end()) && it->second) {
            del_ino = it->second;
          }
        }

        if (del_ino) {
          Instance().mds.wait_upstream(req, del_ino);
        }
      }
      XrdSysMutexHelper mLock(md->Locker());

      if (md->id() && !md->deleted()) {
        rc = EEXIST;
      } else {
        if (pcap2->errc()) {
          rc = pcap2->errc();
        } else {
          md->set_id(0);
          md->set_md_ino(0);
          md->set_err(0);
          md->set_mode(mode | S_IFDIR);
          struct timespec ts;
          eos::common::Timing::GetTimeSpec(ts);
          md->set_name(name);
          md->set_atime(ts.tv_sec);
          md->set_atime_ns(ts.tv_nsec);
          md->set_mtime(ts.tv_sec);
          md->set_mtime_ns(ts.tv_nsec);
          md->set_ctime(ts.tv_sec);
          md->set_ctime_ns(ts.tv_nsec);
          md->set_btime(ts.tv_sec);
          md->set_btime_ns(ts.tv_nsec);
          // need to update the parent mtime
          md->set_pmtime(ts.tv_sec);
          md->set_pmtime_ns(ts.tv_nsec);
          pmd->set_mtime(ts.tv_sec);
          pmd->set_mtime_ns(ts.tv_nsec);
          md->set_uid(pcap2->uid());
          md->set_gid(pcap2->gid());
          /* xattr inheritance */
          auto attrMap = md->mutable_attr();
          auto pattrMap = pmd->attr();

          for (const auto& it : pattrMap) {
            eos_static_debug("adding xattr[%s]=%s", it.first.c_str(), it.second.c_str());
            (*attrMap)[it.first] = it.second;
          }

          md->set_nlink(2);
          md->set_creator(true);
          md->set_type(md->EXCL);
          std::string imply_authid = eos::common::StringConversion::random_uuidstring();
          eos_static_info("generating implied authid %s => %s", pcap2->authid().c_str(),
                          imply_authid.c_str());
          implied_cid = Instance().caps.imply(pcap2, imply_authid, mode,
                                              (fuse_ino_t) md->id());
          md->cap_inc();
          md->set_implied_authid(imply_authid);
          rc = Instance().mds.add_sync(req, pmd, md, pcap2->authid());
          md->set_type(md->MD);

          if (!rc) {
            Instance().mds.insert(req, md, pcap2->authid());
            memset(&e, 0, sizeof(e));
            md->convert(e, pcap2->lifetime());
            md->lookup_inc();
            pmd->local_enoent().erase(name);
            eos_static_info("%s", md->dump(e).c_str());
          }
        }
      }
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_entry(req, &e);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::unlink(fuse_req_t req, fuse_ino_t parent, const char* name)
/* -------------------------------------------------------------------------- */
/*
EACCES Write access to the directory containing pathname is not allowed for the process's effective UID, or one of the
directories in pathname did not allow search permission.  (See also path_resolution(7).)

EBUSY  The file pathname cannot be unlinked because it is being used by the system or another process; for example, it
is a mount point or the NFS client software created it to represent an  active  but  otherwise  nameless  inode
("NFS silly renamed").

EFAULT pathname points outside your accessible address space.

EIO    An I/O error occurred.

EISDIR pathname refers to a directory.  (This is the non-POSIX value returned by Linux since 2.1.132.)

ELOOP  Too many symbolic links were encountered in translating pathname.

ENAMETOOLONG
pathname was too long.

ENOENT A component in pathname does not exist or is a dangling symbolic link, or pathname is empty.

ENOMEM Insufficient kernel memory was available.

ENOTDIR
A component used as a directory in pathname is not, in fact, a directory.

EPERM  The  system  does  not allow unlinking of directories, or unlinking of directories requires privileges that the
calling process doesn't have.  (This is the POSIX prescribed error return; as noted above, Linux returns EISDIR
for this case.)

EPERM (Linux only)
The filesystem does not allow unlinking of files.

EPERM or EACCES
The  directory  containing pathname has the sticky bit (S_ISVTX) set and the process's effective UID is neither
the UID of the file to be deleted nor that of the directory containing it, and the process  is  not  privileged
(Linux: does not have the CAP_FOWNER capability).

EROFS  pathname refers to a file on a read-only filesystem.

 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("parent=%#lx name=%s", parent, name);
  }

  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  fuse_ino_t hardlink_target_ino = 0;
  Track::Monitor pmon("unlink", Instance().Tracker(), parent, true);
  int rc = 0;
  fuse_id id(req);
  // retrieve cap
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                         S_IFDIR | X_OK | D_OK, true);

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    metad::shared_md pmd = NULL /* Parent */, tmd = NULL /*Hard link target */;
    std::string sname = name;
    uint64_t freesize = 0;

    if (sname == ".") {
      rc = EINVAL;
    }

    if (sname.length() > 1024) {
      rc = ENAMETOOLONG;
    }

    fuse_ino_t del_ino = 0;

    if (!rc) {
      metad::shared_md md;
      md = Instance().mds.lookup(req, parent, name);
      XrdSysMutexHelper lLock(md->Locker());

      if (!Instance().Config().options.rename_is_sync) {
        if (Instance().mds.has_flush(md->id())) {
          Instance().mds.wait_flush(req, md);
        }
      }

      if (!md->id() || md->deleted()) {
        rc = ENOENT;
      }

      if ((!rc) && ((md->mode() & S_IFDIR))) {
        rc = EISDIR;
      }

      if (!rc) {
        if (Instance().Config().options.rm_rf_protect_levels &&
            isRecursiveRm(req) &&
            (Instance().mds.calculateDepth(md) <=
             Instance().Config().options.rm_rf_protect_levels)) {
          eos_static_warning("Blocking recursive rm (pid = %d )", fuse_req_ctx(req)->pid);
          rc = EPERM; // you shall not pass, muahahahahah
        } else {
          del_ino = md->id();
          int nlink =
            0; /* nlink has 0-origin (0 = simple file, 1 = inode has two names) */
          auto attrMap = md->attr();
          pmd = Instance().mds.get(req, parent, pcap->authid());

          if (attrMap.count(k_mdino)) { /* This is a hard link */
            uint64_t mdino = std::stoull(attrMap[k_mdino]);
            uint64_t local_ino = Instance().mds.vmaps().forward(mdino);
            tmd = Instance().mds.get(req, local_ino,
                                     pcap->authid()); /* the target of the link */
            hardlink_target_ino = tmd->id();
            {
              // if a hardlink is deleted, we should remove the local shadow entry
              char nameBuf[256];
              snprintf(nameBuf, sizeof(nameBuf), "...eos.ino...%lx", hardlink_target_ino);
              std::string newname = nameBuf;
              XrdSysMutexHelper pLock(pmd->Locker());

              if (pmd->local_children().count(
                    eos::common::StringConversion::EncodeInvalidUTF8(newname))) {
                pmd->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                              newname));
                pmd->set_nchildren(pmd->nchildren() - 1);
              }
            }
          }

          freesize = md->size();

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("hlnk unlink %s new nlink %d %s", name, nlink,
                             Instance().mds.dump_md(md, false).c_str());
          }

          // we have to signal the unlink always to 'the' target inode of a hardlink
          if (hardlink_target_ino) {
            Instance().datas.unlink(req, hardlink_target_ino);
          } else {
            Instance().datas.unlink(req, md->id());
          }

          Instance().mds.remove(req, pmd, md, pcap->authid());

          if (attrMap.count(k_nlink)) {
            // this is a target for hardlinks and we want to invalidate in the kernel cache
            hardlink_target_ino = md->id();
            md->force_refresh();
          }
        }
      }
    }

    if (!rc) {
      if (hardlink_target_ino || Instance().Config().options.rmdir_is_sync) {
        eos_static_warning("waiting for flush of ino=%#lx", del_ino);

        if (del_ino) {
          Instance().mds.wait_upstream(req, del_ino);

          if (hardlink_target_ino) {
            // refetch a possible shadow inode and unmask the local deletion
            metad::shared_md smd = EosFuse::Instance().mds.get(req, del_ino, "");
            smd->setop_none();
          }
        }
      }

      XrdSysMutexHelper pLock(pcap->Locker());
      Instance().caps.free_volume(pcap, freesize);
      Instance().caps.free_inode(pcap);
      eos_static_debug("freeing %llu bytes on cap ", freesize);
    }
  }

  fuse_reply_err(req, rc);

  // the link count has changed and we have to tell the kernel cache
  if (hardlink_target_ino &&
      EosFuse::Instance().Config().options.md_kernelcache) {
    eos_static_warning("invalidating inode ino=%#lx", hardlink_target_ino);
    kernelcache::inval_inode(hardlink_target_ino, true);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rmdir(fuse_req_t req, fuse_ino_t parent, const char* name)
/* -------------------------------------------------------------------------- */
/*
EACCES Write access to the directory containing pathname was not allowed,
or one of the directories in the path prefix
of pathname did not allow search permission.

EBUSY  pathname is currently in use by the system or some process that
prevents its  removal.   On  Linux  this  means
pathname is currently used as a mount point or is the root directory of
the calling process.

EFAULT pathname points outside your accessible address space.

EINVAL pathname has .  as last component.

ELOOP  Too many symbolic links were encountered in resolving pathname.

ENAMETOOLONG
pathname was too long.

ENOENT A directory component in pathname does not exist or is a dangling
symbolic link.

ENOMEM Insufficient kernel memory was available.

ENOTDIR
pathname, or a component used as a directory in pathname, is not,
in fact, a directory.

ENOTEMPTY
pathname contains entries other than . and .. ; or, pathname has ..
as its final component.  POSIX.1-2001 also
allows EEXIST for this condition.

EPERM  The directory containing pathname has the sticky bit (S_ISVTX) set and
the process's effective user ID is  nei‐
ther  the  user  ID  of  the file to be deleted nor that of the
directory containing it, and the process is not
privileged (Linux: does not have the CAP_FOWNER capability).

EPERM  The filesystem containing pathname does not support the removal of
directories.

EROFS  pathname refers to a directory on a read-only filesystem.

 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("rmdir", Instance().Tracker(), parent, true);
  int rc = 0;
  fuse_id id(req);
  // retrieve cap
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                         S_IFDIR | X_OK | D_OK, true);

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    std::string sname = name;

    if (sname == ".") {
      rc = EINVAL;
    }

    if (sname.length() > 1024) {
      rc = ENAMETOOLONG;
    }

    fuse_ino_t del_ino = 0;

    if (!rc) {
      metad::shared_md md;
      metad::shared_md pmd;
      md = Instance().mds.lookup(req, parent, name);
      Track::Monitor mon("rmdir", Instance().Tracker(), md->id(), true);
      XrdSysMutexHelper mLock(md->Locker());

      if (!md->id() || md->deleted()) {
        rc = ENOENT;
      }

      if ((!rc) && (!(md->mode() & S_IFDIR))) {
        rc = ENOTDIR;
      }

      eos_static_info("link=%d", md->nlink());

      if ((!rc) && (md->local_children().size())) {
        eos_static_warning("not empty local children");
        rc = ENOTEMPTY;
      }

      if ((!rc && md->nchildren())) {
        // if we still see children, we wait that we have sent all our MD updates upstream and refetch it
        md->Locker().UnLock();
        Instance().mds.wait_upstream(req, md->id());
        md->force_refresh();
        // if we still see children, we wait that we have sent all our MD updates upstream and refetch it
        md = Instance().mds.lookup(req, parent, name);
        md->Locker().Lock();

        if (md->nchildren()) {
          eos_static_warning("not empty children after refresh");
          rc = ENOTEMPTY;
        }
      }

      if (!rc) {
        pmd = Instance().mds.get(req, parent, pcap->authid());
        Instance().mds.remove(req, pmd, md, pcap->authid());
        del_ino = md->id();
      }
    }

    if (!rc) {
      if (Instance().Config().options.rmdir_is_sync) {
        Instance().mds.wait_upstream(req, del_ino);
      }
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

#ifdef _FUSE3
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                fuse_ino_t newparent, const char* newname, unsigned int flags)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                fuse_ino_t newparent, const char* newname)
/* -------------------------------------------------------------------------- */
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  // Need to pay attention to lock order here. This is the only (?) function where
  // we have to lock more than two inodes at the same time.
  //
  // Two racing requests with inverted source/target directories,
  // eg "mv dir1/file1 dir2/file2" and "mv dir2/file3 dir1/file4" can deadlock
  // us if we simply lock in order of source -> target.
  //
  // Instead, lock in order of increasing inode - both racing requests will
  // use the same locking order, and no deadlock can occur.
  fuse_ino_t first = std::min(parent, newparent);
  fuse_ino_t second = std::max(parent, newparent);
  Track::Monitor monp("rename", Instance().Tracker(), first, true);
  Track::Monitor monn("rename", Instance().Tracker(), second, true,
                      first == second);
  int rc = 0;
  fuse_id id(req);
  // do a parent check
  cap::shared_cap p1cap = Instance().caps.acquire(req, parent,
                          S_IFDIR | W_OK | X_OK, true);
  cap::shared_cap p2cap = Instance().caps.acquire(req, newparent,
                          S_IFDIR | W_OK | X_OK, true);

  if (p1cap->errc()) {
    rc = p1cap->errc();
  }

  if (!rc && p2cap->errc()) {
    rc = p2cap->errc();
  }

  if (!Instance().caps.share_quotanode(p1cap, p2cap)) {
    // cross-quota node move
    rc = EXDEV;
  }

  if (!rc) {
    metad::shared_md md;
    metad::shared_md p1md;
    metad::shared_md p2md;
    md = Instance().mds.lookup(req, parent, name);
    p1md = Instance().mds.get(req, parent, p1cap->authid());
    p2md = Instance().mds.get(req, newparent, p2cap->authid());
    uint64_t md_ino = 0;
    uint64_t del_ino = 0;
    {
      // logic avoiding a delete/rename sync.async race
      {
        XrdSysMutexHelper pLock(p2md->Locker());
        auto it = p2md->get_todelete().find(
                    eos::common::StringConversion::EncodeInvalidUTF8(newname));

        if ((it != p2md->get_todelete().end()) && it->second) {
          del_ino = it->second;
        }
      }

      if (del_ino) {
        Instance().mds.wait_upstream(req, del_ino);
      }

      XrdSysMutexHelper mLock(md->Locker());

      if (md->deleted()) {
        // we need to wait that this entry is really gone
        Instance().mds.wait_flush(req, md);
      }

      if (!md->id() || md->deleted()) {
        rc = md->deleted() ? ENOENT : md->err();
      } else {
        md_ino = md->id();
      }

      // If this is a move between directories of a directory then make sure
      // there is no destination directory with the same name that is not
      // empty.
      if (S_ISDIR(md->mode()) && (p1md->id() != p2md->id())) {
        metad::shared_md dst_same_name = Instance().mds.lookup(req, newparent, name);

        if (dst_same_name) {
          XrdSysMutexHelper dst_dir_lock(dst_same_name->Locker());

          if (dst_same_name->local_children().size()) {
            rc = ENOTEMPTY;
          }
        }
      }
    }

    if (!rc) {
      Track::Monitor mone("rename", Instance().Tracker(), md_ino, true);
      std::string new_name = newname;
      Instance().mds.mv(req, p1md, p2md, md, newname, p1cap->authid(),
                        p2cap->authid());

      if (Instance().Config().options.rename_is_sync) {
        XrdSysMutexHelper mLock(md->Locker());
        Instance().mds.wait_flush(req, md);
      }
    }
  }

  EXEC_TIMING_END(__func__);
  fuse_reply_err(req, rc);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s new-parent-ino=%#lx target-name=%s",
                    timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str(), newparent, newname);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::access(fuse_req_t req, fuse_ino_t ino, int mask)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("access", Instance().Tracker(), ino);
  int rc = 0;
  fuse_id id(req);
  metad::shared_md md = Instance().mds.getlocal(req, ino);
  metad::shared_md pmd = md;
  mode_t mode = 0;
  mode_t pmode = mask;
  bool is_deleted = false;
  fuse_ino_t pino = 0;
  {
    XrdSysMutexHelper mLock(md->Locker());
    pino = (md->id() == 1) ? md->id() : md->pid();
    mode = md->mode();
    is_deleted = md->deleted();
  }
  pmode &= ~F_OK;

  if (!Instance().Config().options.x_ok) {
    // if X_OK is maked, X_OK is set to 0
    pmode &= ~X_OK;
  }

  if (md->id() == 0) {
    rc = is_deleted ? ENOENT : EIO;
  } else {
    if (S_ISREG(mode)) {
      pmd = Instance().mds.getlocal(req, pino);
    }

    if (pmd->id() == 0) {
      rc = EIO;
    } else {
      // We need a fresh cap for pmd
      cap::shared_cap pcap = Instance().caps.acquire(req, pmd->id(), S_IFDIR | pmode);
      XrdSysMutexHelper mLock(pcap->Locker());

      if (pcap->errc()) {
        rc = pcap->errc();

        if (rc == EPERM) {
          rc = EACCES;
        }
      }

      if (S_ISREG(mode)) {
        // check the execution bits
        if (mask & X_OK) {
          bool allowed = false;

          if (pcap->uid() == md->uid()) {
            // check user X permission
            if (mode & S_IXUSR) {
              allowed = true;
            }
          }

          if (pcap->gid() == md->gid()) {
            // check group X permission
            if (mode & S_IXGRP) {
              allowed = true;
            }
          }

          // check other X permision
          if (mode & S_IXOTH) {
            allowed = true;
          }

          if (!allowed) {
            rc = EACCES;
          }
        }
      }
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("flags=%x sync=%d", fi->flags, (fi->flags & O_SYNC) ? 1 : 0);
  // FMODE_EXEC: "secret" internal flag which can be set only by the kernel when it's
  // reading a file destined to be used as an image for an execve.
#define FMODE_EXEC 0x20
  ExecveAlert execve(fi->flags & FMODE_EXEC);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("open", Instance().Tracker(), ino, true);
  int rc = 0;
  fuse_id id(req);
  int mode = R_OK;

  if (fi->flags & (O_RDWR | O_WRONLY)) {
    mode = U_OK;
  }

  {
    metad::shared_md md;
    md = Instance().mds.get(req, ino);
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted()) {
      rc = md->deleted() ? ENOENT : md->err();
    } else {
      fuse_ino_t cap_ino = md->pid();

      if (md->attr().count("user.acl")) { /* file with own ACL */
        cap_ino = md->id();
      } else {
	// screen for sqash image access, they only retrieve X_OK on the parent directories
	eos::common::Path cPath(md->name());
	if ( ( mode == R_OK ) && (cPath.isSquashFile()) ) {
	  mode = X_OK;
	}
      }

      cap::shared_cap pcap = Instance().caps.acquire(req, cap_ino, mode);
      XrdSysMutexHelper capLock(pcap->Locker());

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("id=%#lx cap-ino=%#lx mode=%#o", md->id(), cap_ino, mode);

        if ((!S_ISDIR(md->mode())) && md->attr().count("user.acl")) {
          eos_static_debug("file cap %s", pcap->dump().c_str());
        }
      }

      if (pcap->errc()) {
        rc = pcap->errc();
      } else {
        uint64_t pquota = 0;

        if (mode == U_OK) {
          if (!(pquota = Instance().caps.has_quota(pcap, 1024 * 1024))) {
            rc = EDQUOT;
          } else {
            Instance().caps.open_writer_inode(pcap);
          }
        }

        if (!rc) {
          int cache_flag = 0;
          std::string md_name = md->name();
          uint64_t md_ino = md->md_ino();
          uint64_t md_pino = md->md_pino();
          std::string cookie = md->Cookie();

          if (md->attr().count("sys.file.cache")) {
            cache_flag |= O_CACHE;
          }

          capLock.UnLock();
          struct fuse_entry_param e;
          memset(&e, 0, sizeof(e));
          md->convert(e, pcap->lifetime());
          mLock.UnLock();
          data::data_fh* io = data::data_fh::Instance(Instance().datas.get(req, md->id(),
                              md), md, (mode == U_OK));
          capLock.Lock(&pcap->Locker());
          io->set_authid(pcap->authid());

          if (pquota < pcap->max_file_size()) {
            io->set_maxfilesize(pquota);
          } else {
            io->set_maxfilesize(pcap->max_file_size());
          }

          io->cap_ = pcap;
          capLock.UnLock();
          // attach a datapool object
          fi->fh = (uint64_t) io;
          io->ioctx()->set_remote(Instance().Config().hostport,
                                  md_name,
                                  md_ino,
                                  md_pino,
                                  req,
                                  (mode == U_OK));
          bool outdated = (io->ioctx()->attach(req, cookie,
                                               fi->flags | cache_flag) == EKEYEXPIRED);
          fi->keep_cache = outdated ? 0 : Instance().Config().options.data_kernelcache;

          if (md->creator()) {
            fi->keep_cache = Instance().Config().options.data_kernelcache;
          }

          // files which have been broadcasted from a remote update are not cached during the first default:5 seconds
          if ((time(NULL) - md->bc_time()) <
              EosFuse::Instance().Config().options.nocache_graceperiod) {
            fi->keep_cache = false;
          }

          fi->direct_io = 0;
          eos_static_info("%s data-cache=%d", md->dump(e).c_str(), fi->keep_cache);
        }
      }
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_open(req, fi);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
               mode_t mode, dev_t rdev)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);

  if (S_ISREG(mode) || S_ISFIFO(mode)) {
    create(req, parent, name, mode, 0);
  } else {
    rc = ENOSYS;
  }

  if (rc) {
    fuse_reply_err(req, rc);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::create(fuse_req_t req, fuse_ino_t parent, const char* name,
                mode_t mode, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
/*
EACCES The  requested  access to the file is not allowed, or search permission is denied for one of the directories in
the path prefix of pathname, or the file did not exist yet and write access to  the  parent  directory  is  not
allowed.  (See also path_resolution(7).)

EDQUOT Where  O_CREAT  is  specified,  the  file  does not exist, and the user's quota of disk blocks or inodes on the
filesystem has been exhausted.

EEXIST pathname already exists and O_CREAT and O_EXCL were used.

EFAULT pathname points outside your accessible address space.

EFBIG  See EOVERFLOW.

EINTR  While blocked waiting to complete an open of a slow device (e.g., a FIFO; see fifo(7)),  the  call  was  inter‐
rupted by a signal handler; see signal(7).

EINVAL The filesystem does not support the O_DIRECT flag. See NOTES for more information.

EISDIR pathname refers to a directory and the access requested involved writing (that is, O_WRONLY or O_RDWR is set).

ELOOP  Too  many symbolic links were encountered in resolving pathname, or O_NOFOLLOW was specified but pathname was a
symbolic link.

EMFILE The process already has the maximum number of files open.

ENAMETOOLONG
pathname was too long.

ENFILE The system limit on the total number of open files has been reached.

ENODEV pathname refers to a device special file and no corresponding device exists.  (This is a Linux kernel  bug;  in
this situation ENXIO must be returned.)

ENOENT O_CREAT  is not set and the named file does not exist.  Or, a directory component in pathname does not exist or
is a dangling symbolic link.

ENOMEM Insufficient kernel memory was available.

ENOSPC pathname was to be created but the device containing pathname has no room for the new file.

ENOTDIR
A component used as a directory in pathname is not, in fact, a directory,  or  O_DIRECTORY  was  specified  and
pathname was not a directory.

ENXIO  O_NONBLOCK  |  O_WRONLY is set, the named file is a FIFO and no process has the file open for reading.  Or, the
file is a device special file and no corresponding device exists.

EOVERFLOW
pathname refers to a regular file that is too large to be opened.  The usual scenario here is that an  applica‐
tion  compiled  on  a  32-bit  platform  without -D_FILE_OFFSET_BITS=64 tried to open a file whose size exceeds
(2<<31)-1 bits; see also O_LARGEFILE above.  This is the error specified by  POSIX.1-2001;  in  kernels  before
2.6.24, Linux gave the error EFBIG for this case.

EPERM  The  O_NOATIME  flag was specified, but the effective user ID of the caller did not match the owner of the file
and the caller was not privileged (CAP_FOWNER).

EROFS  pathname refers to a file on a read-only filesystem and write access was requested.

ETXTBSY
pathname refers to an executable image which is currently being executed and write access was requested.

EWOULDBLOCK
The O_NONBLOCK flag was specified, and an incompatible lease was held on the file (see fcntl(2)).
 */
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  fuse_ino_t pino = 0;
  {
    COMMONTIMING("_start_", &timing);
    Track::Monitor mon("create", Instance().Tracker(), parent, true);

    if (fi) {
      eos_static_debug("flags=%x", fi->flags);
    }

    ADD_FUSE_STAT(__func__, req);
    EXEC_TIMING_BEGIN(__func__);
    int rc = 0;
    fuse_id id(req);
    // do a parent check
    cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                           S_IFDIR | W_OK, true);
    struct fuse_entry_param e;
    XrdSysMutexHelper capLock(pcap->Locker());

    if (pcap->errc()) {
      rc = pcap->errc();
    } else {
      capLock.UnLock();
      {
        if (!Instance().caps.has_quota(pcap, 1024 * 1024)) {
          rc = EDQUOT;
        }
      }

      if (!rc) {
        metad::shared_md md;
        metad::shared_md pmd;
	bool obfuscate = false;

        md = Instance().mds.lookup(req, parent, name);
        pmd = Instance().mds.get(req, parent, pcap->authid());
        {
          uint64_t del_ino = 0;
          // logic avoiding a create/unlink/create sync/async race
          {
            XrdSysMutexHelper pLock(pmd->Locker());
            auto it = pmd->get_todelete().find(
                        eos::common::StringConversion::EncodeInvalidUTF8(name));

            if ((it != pmd->get_todelete().end()) && it->second) {
              del_ino = it->second;
            }

	    obfuscate = pmd->obfuscate();
          }

          if (del_ino) {
            Instance().mds.wait_upstream(req, del_ino);
          }
        }

        XrdSysMutexHelper mLock(md->Locker());

        if (md->id() && !md->deleted()) {
          rc = EEXIST;
        } else {
          if (md->deleted()) {
            // we need to wait that this entry is really gone
            Instance().mds.wait_flush(req, md);
          }

          md->set_err(0);
          md->set_mode(mode | (S_ISFIFO(mode) ? S_IFIFO : S_IFREG));
          md->set_fullpath(pmd->fullpath() + "/" + name);

          if (S_ISFIFO(mode)) {
            (*md->mutable_attr())[k_fifo] = "";
          }

          struct timespec ts;

          eos::common::Timing::GetTimeSpec(ts);

          md->set_name(name);

          md->set_atime(ts.tv_sec);

          md->set_atime_ns(ts.tv_nsec);

          md->set_mtime(ts.tv_sec);

          md->set_mtime_ns(ts.tv_nsec);

          md->set_ctime(ts.tv_sec);

          md->set_ctime_ns(ts.tv_nsec);

          md->set_btime(ts.tv_sec);

          md->set_btime_ns(ts.tv_nsec);

          // need to update the parent mtime
          md->set_pmtime(ts.tv_sec);

          md->set_pmtime_ns(ts.tv_nsec);

          md->set_uid(pcap->uid());

          md->set_gid(pcap->gid());

          md->set_type(md->EXCL);

	  if (obfuscate) {
	    // create a random uuid
	    char suuid[40];
	    uuid_t uuid;
	    uuid_generate_random(uuid);
	    uuid_unparse(uuid, suuid);

	    md->set_obfuscate_key(std::string(suuid));
	    eos_static_err("obfuscating with key='%s'\n", suuid);
	  }

          rc = Instance().mds.add_sync(req, pmd, md, pcap->authid());

          md->set_type(md->MD);

          if (!rc) {
            Instance().mds.insert(req, md, pcap->authid());
            md->set_nlink(1);
            md->set_creator(true);
            // avoid lock-order violation
            {
              mLock.UnLock();
              XrdSysMutexHelper mLockParent(pmd->Locker());
              pmd->set_mtime(ts.tv_sec);
              pmd->set_mtime_ns(ts.tv_nsec);

              // get file inline size from parent attribute
              if (pmd->attr().count("sys.file.inline.maxsize")) {
                auto maxsize = (*pmd->mutable_attr())["sys.file.inline.maxsize"];
                md->set_inlinesize(strtoull(maxsize.c_str(), 0, 10));
              }

              mLockParent.UnLock();
              mLock.Lock(&md->Locker());
            }
            memset(&e, 0, sizeof(e));
            Instance().caps.book_inode(pcap);
            Instance().caps.open_writer_inode(pcap);
            md->convert(e, pcap->lifetime());
            md->lookup_inc();

            if (fi) {
              // -----------------------------------------------------------------------
              // FUSE caches the file for reads on the same filedescriptor in the buffer
              // cache, but the pages are released once this filedescriptor is released.
              fi->keep_cache = Instance().Config().options.data_kernelcache;

              if ((fi->flags & O_DIRECT) ||
                  (fi->flags & O_SYNC)) {
                fi->direct_io = 1;
              } else {
                fi->direct_io = 0;
              }

              std::string md_name = md->name();
              uint64_t md_ino = md->md_ino();
              uint64_t md_pino = md->md_pino();
              std::string cookie = md->Cookie();
              mLock.UnLock();
              data::data_fh* io = data::data_fh::Instance(Instance().datas.get(req, md->id(),
                                  md), md, true);
              io->set_authid(pcap->authid());
              io->set_maxfilesize(pcap->max_file_size());
              io->cap_ = pcap;
              // attach a datapool object
              fi->fh = (uint64_t) io;
              io->ioctx()->set_remote(Instance().Config().hostport,
                                      md_name,
                                      md_ino,
                                      md_pino,
                                      req,
                                      true);
              io->ioctx()->attach(req, cookie, fi->flags);
              mLock.Lock(&md->Locker());
            }

            pmd->local_enoent().erase(name);
            pino = pmd->id();
          }

          eos_static_info("%s", md->dump(e).c_str());
        }
      }
    }

    if (rc) {
      fuse_reply_err(req, rc);
    } else {
      if (fi)
        // create
      {
        fuse_reply_create(req, &e, fi);
      } else
        // mknod
      {
        fuse_reply_entry(req, &e);
      }
    }

    EXEC_TIMING_END(__func__);
    COMMONTIMING("_stop_", &timing);
    eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                      dump(id, parent, 0, rc).c_str());
  }

  // after creating a file we assign a new mtime to our parent directory
  if (pino && EosFuse::Instance().Config().options.md_kernelcache) {
    // now the mtime is wrong 'on-top' of use
    kernelcache::inval_inode(pino, false);
  }
}

void
/* -------------------------------------------------------------------------- */
EosFuse::read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  Track::Monitor mon("read", Instance().Tracker(), ino);
  eos_static_debug("inode=%llu size=%li off=%llu",
                   (unsigned long long) ino, size, (unsigned long long) off);
  eos_static_debug("");
  fuse_id id(req);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  data::data_fh* io = (data::data_fh*) fi->fh;
  ssize_t res = 0;
  int rc = 0;

  if (io) {
    char* buf = 0;

    if ((res = io->ioctx()->peek_pread(req, buf, size, off)) == -1) {
      rc = errno ? errno : EIO;
    } else {
      eos_static_debug("reply res=%lu", res);
      if (io->md->obfuscate_key().length()) {
	// un-obfuscate
	eos_static_debug("secret=%s", fusexrdlogin::secret(req).c_str());
	io->md->unobfuscate_buffer(buf, res, off, fusexrdlogin::secret(req));
      }
      fuse_reply_buf(req, buf, res);
    }

    io->ioctx()->release_pread();
  } else {
    rc = ENXIO;
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    ADD_IO_STAT("rbytes", res);
  }

  eos_static_debug("t(ms)=%.03f %s", timing.RealTime(),
                   dump(id, ino, 0, rc).c_str());
  EXEC_TIMING_END(__func__);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
               off_t off, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  Track::Monitor mon("write", Instance().Tracker(), ino, true);
  eos_static_debug("inode=%lld size=%lld off=%lld buf=%lld",
                   (long long) ino, (long long) size,
                   (long long) off, (long long) buf);
  eos_static_debug("");
  fuse_id id(req);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  data::data_fh* io = (data::data_fh*) fi->fh;
  int rc = 0;

  char* obuf = 0;

  if (io->md->obfuscate_key().length()) {
    // duplicate buffer
    obuf = (char*) malloc(size);
    if (!obuf) {
      // EOM
      io = nullptr;
    } else {
      memcpy(obuf, buf, size);
      eos_static_debug("secret=%s", fusexrdlogin::secret(req).c_str());
      // obfuscate
      io->md->obfuscate_buffer(obuf, buf, size, off, fusexrdlogin::secret(req));
      // make the data object read from the obfuscated buffer
      buf = obuf;
    }
  }

  if (io) {
    eos_static_debug("max-file-size=%llu", io->maxfilesize());

    if ((off + size) > io->maxfilesize()) {
      eos_static_err("io-error: maximum file size exceeded inode=%lld size=%lld off=%lld buf=%lld max-size=%llu",
                     ino, size, off, buf, io->maxfilesize());
      rc = EFBIG;
    } else {
      if (!EosFuse::instance().getCap().has_quota(io->cap_, size)) {
        eos_static_err("quota-error: inode=%lld size=%lld off=%lld buf=%lld", ino, size,
                       off, buf);
        rc = EDQUOT;
      } else {
        if (io->ioctx()->pwrite(req, buf, size, off) == -1) {
          eos_static_err("io-error: inode=%lld size=%lld off=%lld buf=%lld errno=%d", ino,
                         size,
                         off, buf, errno);
          rc = errno ? errno : EIO;

          if (rc == EDQUOT) {
            eos_static_err("quota-error: inode=%lld ran out of quota - setting cap to EDQUOT",
                           ino);
            EosFuse::instance().getCap().set_volume_edquota(io->cap_);
          }
        } else {
          {
            XrdSysMutexHelper mLock(io->mdctx()->Locker());
            io->mdctx()->set_size(io->ioctx()->size());
            {
              struct timespec tsnow;
              eos::common::Timing::GetTimeSpec(tsnow);
              io->md->set_mtime(tsnow.tv_sec);
              io->md->set_mtime_ns(tsnow.tv_nsec);
              io->md->set_ctime(tsnow.tv_sec);
              io->md->set_ctime_ns(tsnow.tv_nsec);
            }
            io->set_update();
            // flush size updates every 5 seconds
            time_t now = time(NULL);

            if (Instance().mds.should_flush_write_size()) {
              if (Instance().Config().options.write_size_flush_interval) {
                if (io->ioctx()->is_wopen(req)) {
                  // only start updating the MGM size if the file could be opened on FSTs
                  if (io->next_size_flush.load() && (io->next_size_flush.load() < now)) {
                    // if (io->cap_->valid()) // we want updates also after cap expiration
                    Instance().mds.update(req, io->md, io->authid());
                    io->next_size_flush.store(now +
                                              Instance().Config().options.write_size_flush_interval,
                                              std::memory_order_seq_cst);
                  } else {
                    if (!io->next_size_flush.load()) {
                      io->next_size_flush.store(now +
                                                Instance().Config().options.write_size_flush_interval,
                                                std::memory_order_seq_cst);
                    }
                  }
                }
              }
            }
          }
          fuse_reply_write(req, size);
        }
      }
    }
  } else {
    rc = ENXIO;
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    ADD_IO_STAT("wbytes", size);
  }

  if (obuf) {
    free (obuf);
  }

  eos_static_debug("t(ms)=%.03f %s", timing.RealTime(),
                   dump(id, ino, 0, rc).c_str());
  EXEC_TIMING_END(__func__);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("release", Instance().Tracker(), ino, true);
  int rc = 0;
  fuse_id id(req);

  if (fi->fh) {
    data::data_fh* io = (data::data_fh*) fi->fh;

    if (io->flocked) {
      // unlock all locks for that owner
      struct flock lock;
      lock.l_type = F_UNLCK;
      lock.l_start = 0;
      lock.l_len = -1;
      lock.l_pid = fuse_req_ctx(req)->pid;
      rc |= Instance().mds.setlk(req, io->mdctx(), &lock, 0);
    }

    std::string cookie = "";
    io->ioctx()->detach(req, cookie, io->rw);
    Instance().caps.close_writer_inode(io->cap_);
    delete io;
    Instance().datas.release(req, ino);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  fuse_reply_err(req, rc);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
               struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("datasync=%d", datasync);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("fsync", Instance().Tracker(), ino);
  int rc = 0;
  fuse_id id(req);
  data::data_fh* io = (data::data_fh*) fi->fh;

  if (io) {
    {
      std::string fname = "";
      {
        XrdSysMutexHelper mLock(io->md->Locker());
        fname = io->md->name();
      }

      if (filename::matches_suffix(fname,
                                   Instance().Config().options.no_fsync_suffixes)) {
        if (EOS_LOGS_DEBUG) {
          eos_static_info("name=%s is in no-fsync list - suppressing fsync call",
                          fname.c_str());
        }
      } else {
        if (Instance().Config().options.global_flush) {
          Instance().mds.begin_flush(req, io->md,
                                     io->authid()); // flag an ongoing flush centrally
        }

        struct timespec tsnow;

        eos::common::Timing::GetTimeSpec(tsnow);

        XrdSysMutexHelper mLock(io->md->Locker());

        io->md->set_mtime(tsnow.tv_sec);

        if (!rc) {
          // step 2 call sync - this currently flushed all open filedescriptors - should be ok
          rc = io->ioctx()->sync(); // actually wait for writes to be acknowledged
          rc = rc ? (errno ? errno : EIO) : 0;
        } else {
          rc = errno ? errno : EIO;
        }

        if (Instance().Config().options.global_flush) {
          Instance().mds.end_flush(req, io->md,
                                   io->authid()); // unflag an ongoing flush centrally
        }
      }
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("ino=%#lx nlookup=%d", ino, nlookup);
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  int rc = 0;
  fuse_id id(req);
  rc = Instance().mds.forget(req, ino, nlookup);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s nlookup=%d", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str(), nlookup);

  if (!rc) {
    Instance().Tracker().forget(ino);
  }

  fuse_reply_none(req);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("flush", Instance().Tracker(), ino, true);
  int rc = 0;
  fuse_id id(req);
  data::data_fh* io = (data::data_fh*) fi->fh;
  bool invalidate_inode = false;

  if (io) {
    if (io->has_update()) {
      cap::shared_cap pcap;
      {
        XrdSysMutexHelper mLock(io->md->Locker());
        auto map = io->md->attr();

        if (map.count("user.acl") > 0) { /* file has it's own ACL */
          mLock.UnLock();
          cap::shared_cap ccap = Instance().caps.acquire(req, io->md->id(), W_OK, true);
          rc = ccap->errc();

          if (rc == 0) {
            pcap = Instance().caps.acquire(req, io->md->pid(), S_IFDIR | X_OK, true);
          }
        } else {
          mLock.UnLock();
          pcap = Instance().caps.acquire(req, io->md->pid(), S_IFDIR | W_OK, true);
        }
      }
      XrdSysMutexHelper capLock(pcap->Locker());

      if (rc == 0 && pcap->errc() != 0) {
        rc = pcap->errc();
      }

      if (rc == 0) {
        {
          ssize_t size_change = (int64_t) io->md->size() - (int64_t) io->opensize();

          if (size_change > 0) {
            Instance().caps.book_volume(pcap, size_change);
          } else {
            Instance().caps.free_volume(pcap, size_change);
          }

          eos_static_debug("booking %ld bytes on cap ", size_change);
        }
        capLock.UnLock();
        struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);

        // possibly inline the file in extended attribute before mds update
        if (io->ioctx()->inline_file()) {
          eos_static_debug("file is inlined");
        } else {
          eos_static_debug("file is not inlined");
        }

        XrdSysMutexHelper mLock(io->md->Locker());
        auto map = io->md->attr();

        // actually do the flush
        if ((rc = io->ioctx()->flush(req))) {
          // if we have a flush error, we don't update the MD record
          invalidate_inode = true;
          io->md->set_size(io->opensize());
        } else {
          Instance().mds.update(req, io->md, io->authid());
        }

        std::string cookie = io->md->Cookie();
        io->ioctx()->store_cookie(cookie);
        capLock.Lock(&pcap->Locker());

        if (!Instance().caps.has_quota(pcap, 0)) {
          // we signal an error to the client if the quota get's exceeded although
          // we let the file be complete
          rc = EDQUOT;
        }
      }
    }

    // unlock all locks for that owner
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_start = 0;
    lock.l_len = -1;
    lock.l_pid = fi->lock_owner;

    if (io->flocked) {
      lock.l_pid = fuse_req_ctx(req)->pid;
    }

    rc |= Instance().mds.setlk(req, io->mdctx(), &lock, 0);
  }

  fuse_reply_err(req, rc);

  if (invalidate_inode) {
    eos_static_warning("invalidating ino=%#lx after flush error", ino);
    kernelcache::inval_inode(ino, true);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);

  // report slow flush
  if (Instance().Trace() || (timing.RealTime() > 2000)) {
    std::string path = Instance().mds.calculateLocalPath(io->md);
    std::string s;
    eos_static_warning("flush of '%s' took %.03fms\n%s",
                       Instance().Prefix(path).c_str(),
                       timing.RealTime(),
                       io->ioctx()->Dump(s));
  }

  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

#ifdef __APPLE__
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  size_t size, uint32_t position)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  size_t size)
/* -------------------------------------------------------------------------- */
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  std::string key = xattr_name;
  eos_static_debug("ino=%#x %s", ino,
                   key.c_str()); /* key in case xattr_name == NULL */
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("getxattr", Instance().Tracker(), ino);
  int rc = 0;
  fuse_id id(req);
  cap::shared_cap pcap;
  std::string value;
  bool local_getxattr = false;
  // the root user has a bypass to be able to retrieve information in
  // realtime
  {
    static std::string s_md = "system.eos.md";
    static std::string s_refresh = "system.eos.refreshls";
    static std::string s_cap = "system.eos.cap";
    static std::string s_ls_caps = "system.eos.caps";
    static std::string s_ls_vmap = "system.eos.vmap";

    if (key.substr(0, s_md.length()) == s_md) {
      local_getxattr = true;
      metad::shared_md md;
      pcap = Instance().caps.get(req, ino);
      md = Instance().mds.get(req, ino, pcap->authid());
      {
        value = Instance().mds.dump_md(md);
      }
    }

    if (key.substr(0, s_refresh.length()) == s_refresh) {
      local_getxattr = true;
      metad::shared_md md;
      md->set_type(md->MD);
      {
        value = "info: force refresh for next listing";
      }
    }

    if (key.substr(0, s_cap.length()) == s_cap) {
      local_getxattr = true;
      pcap = Instance().caps.get(req, ino);
      {
        value = pcap->dump();
      }
    }

    if (fuse_req_ctx(req)->uid == 0) {
      if (key.substr(0, s_ls_caps.length()) == s_ls_caps) {
        local_getxattr = true;
        value = Instance().caps.ls();
      }

      if (key.substr(0, s_ls_vmap.length()) == s_ls_vmap) {
        local_getxattr = true;
        value = Instance().mds.vmaps().dump();
      }
    }

    if ((size) && (value.size() > size)) {
      value.erase(size - 4);
      value += "...";
    }
  }

  if (!local_getxattr) {
    {
      metad::shared_md md;
      metad::shared_md pmd;
      static std::string s_sec = "security.";
      static std::string s_acl_a = "system.posix_acl_access";
      static std::string s_acl_d = "system.posix_acl_default";
      static std::string s_apple = "com.apple";
      static std::string s_racl = "system.richacl";

      // don't return any security attribute
      if (key.substr(0, s_sec.length()) == s_sec) {
        rc = ENOATTR;
      } else {
        // don't return any posix acl attribute
        if ((key == s_acl_a) || (key == s_acl_d)) {
          rc = ENOATTR;
        }

#ifdef __APPLE__
        else

          // don't return any finder attribute
          if (key.substr(0, s_apple.length()) == s_apple) {
            rc = ENOATTR;
          }

#endif
      }

      if (key == "eos.name") {
        value = Instance().Config().name;
      } else if (key == "eos.hostport") {
        value = Instance().Config().hostport;
      } else if (key == "eos.stacktrace") {
        value = getStacktrace();
      } else if (key == "eos.mgmurl") {
        std::string mgmurl = "root://";
        mgmurl += Instance().Config().hostport;
        value = mgmurl;
      } else if (key == "eos.reconnect") {
        Logbook logbook(true);
        const struct fuse_ctx* ctx = fuse_req_ctx(req);
        ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(ctx->pid,
                                   ctx->uid, ctx->gid, true, logbook);
        value = logbook.toString();

        if (size == 0) {
          // just make sure, the string does not get longer with the next call
          value += value;
        }
      } else if (key == "eos.reconnectparent") {
        const struct fuse_ctx* ctx = fuse_req_ctx(req);
        ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(ctx->pid,
                                   ctx->uid, ctx->gid, false);
        pid_t ppid = snapshot->getProcessInfo().getParentId();
        Logbook logbook(true);
        ProcessSnapshot snapshotParent =
          fusexrdlogin::processCache->retrieve(ppid,
                                               ctx->uid, ctx->gid, true, logbook);
        value = logbook.toString();

        if (size == 0) {
          // just make sure, the string does not get longer with the next call
          value += value;
        }
      } else if (key == "eos.identity") {
        const struct fuse_ctx* ctx = fuse_req_ctx(req);
        ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(ctx->pid,
                                   ctx->uid, ctx->gid, false);

        if (snapshot) {
          value = snapshot->getBoundIdentity()->describe();
        }
      } else if (key == "eos.identityparent") {
        const struct fuse_ctx* ctx = fuse_req_ctx(req);
        ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(ctx->pid,
                                   ctx->uid, ctx->gid, false);
        pid_t ppid = snapshot->getProcessInfo().getParentId();
        ProcessSnapshot snapshotParent =
          fusexrdlogin::processCache->retrieve(
            ppid, ctx->uid, ctx->gid, false);

        if (snapshotParent) {
          value = snapshotParent->getBoundIdentity()->describe();
        }
      } else if (!rc) {
        md = Instance().mds.get(req, ino);
        XrdSysMutexHelper mLock(md->Locker());

        if (!md->id() || md->deleted()) {
          rc = md->deleted() ? ENOENT : md->err();
        } else {
          auto map = md->attr();

          if (key.substr(0, 8) == "eos.sys.") {
            key.erase(0, 4);
          }

          if (key.substr(0, 4) == "eos.") {
            if (key == "eos.md_ino") {
              std::string md_ino;
              value = eos::common::StringConversion::GetSizeString(md_ino,
                      (unsigned long long) md->md_ino());
            }

            if (key == "eos.btime") {
              char btime[256];
              snprintf(btime, sizeof(btime), "%lu.%lu", md->btime(), md->btime_ns());
              value = btime;
            }

            if (key == "eos.ttime") {
              char ttime[256];

              if (S_ISDIR(md->mode())) {
                snprintf(ttime, sizeof(ttime), "%lu.%lu", md->ttime(), md->ttime_ns());
              } else {
                snprintf(ttime, sizeof(ttime), "%lu.%lu", md->mtime(), md->mtime_ns());
              }

              value = ttime;
            }

            if (key == "eos.tsize") {
              char tsize[256];
              snprintf(tsize, sizeof(tsize), "%lu", md->size());
              value = tsize;
            }

            if (key == "eos.dsize") {
              uint64_t sumsize = 0;
              mLock.UnLock();
              rc = listdir(req, ino, md);

              if (!rc) {
                for (auto it = md->local_children().begin(); it != md->local_children().end();
                     ++it) {
                  fuse_ino_t cino = it->second;
                  metad::shared_md cmd = Instance().mds.get(req, cino, "", 0, 0, 0, true);
                  XrdSysMutexHelper mLock(cmd->Locker());

                  if (cmd->id()) {
                    if (S_ISREG(cmd->mode())) {
                      sumsize += cmd->size();
                    }
                  }
                }
              }

              char dsize[256];
              snprintf(dsize, sizeof(dsize), "%lu", sumsize);
              value = dsize;
            }

            if (key == "eos.checksum") {
              rc = Instance().mdbackend.getChecksum(req, md->md_ino(), value);
            }

            if (key == "eos.stats") {
              value = Instance().statsout.get();
            }

            if (key == "eos.url.xroot") {
              value = "root://";
              value += Instance().Config().hostport;
              value += "/";
              value += md->fullpath().c_str();
            }

            if (key == "eos.quota") {
              pcap = Instance().caps.acquire(req, ino,
                                             R_OK);

              if (pcap->errc()) {
                rc = pcap->errc();
              } else {
                cap::shared_quota q = Instance().caps.quota(pcap);
                XrdSysMutexHelper qLock(q->Locker());
                char qline[1024];
                snprintf(qline, sizeof(qline),
                         "instance             uid     gid        vol-avail        ino-avail        max-fsize                         endpoint                        writer\n"
                         "%-16s %7u %7u %16lu %16lu %16lu %32s %d\n",
                         Instance().Config().name.c_str(),
                         pcap->uid(),
                         pcap->gid(),
                         q->volume_quota(),
                         q->inode_quota(),
                         pcap->max_file_size(),
                         Instance().Config().hostport.c_str(),
                         q->writer());
                value = qline;
              }
            }
          } else {
            if (S_ISDIR(md->mode())) {
              // retrieve the appropriate cap of this inode
              pcap = Instance().caps.acquire(req, ino,
                                             R_OK);
            } else {
              // retrieve the appropriate cap of the parent inode
              pcap = Instance().caps.acquire(req, md->pid(), R_OK);
            }

            if (pcap->errc()) {
              rc = pcap->errc();
            } else {
#ifdef HAVE_RICHACL

              if (key == s_racl) {
                struct richacl* a = NULL;

                if (map.count("user.acl") > 0 && map["user.acl"].length() > 0) {
                  const char* eosacl = map["user.acl"].c_str();
                  eos_static_debug("eosacl '%s'", eosacl);

                  if (!S_ISDIR(md->mode()) || map.count("sys.eval.useracl") > 0) {
                    a = eos2racl(eosacl, md);
                  }
                }

                metad::shared_md pmd = Instance().mds.getlocal(req, md->pid());

                if (pmd != NULL) {
                  /* decode parent ACL for merge */
                  auto pmap = pmd->attr();
                  struct richacl* pa = NULL;

                  if (pmap.count("sys.eval.useracl") > 0 && pmap.count("user.acl") > 0) {
                    const char* peosacl = pmap["user.acl"].c_str();
                    pa = eos2racl(peosacl, pmd);
                  }

                  if (pa == NULL) {
                    pa = richacl_from_mode(md->mode()); /* Always returns an ACL */
                  }

                  a = richacl_merge_parent(a, md, pa, pmd);
                  richacl_free(pa);

                  if (a == NULL) {
                    rc = ENOMEM;  /* a has been freed */
                  }

                  if (rc == 0) {
                    size_t sz = richacl_xattr_size(a);
                    value.assign(sz, '\0'); /* allocate and clear result buffer */
                    richacl_to_xattr(a, (void*) value.c_str());
                    char* a_t = richacl_to_text(a, 0);
                    eos_static_debug("eos2racl returned raw size %d, decoded: %s", sz, a_t);
                    free(a_t);
                    richacl_free(a);
                  }
                } else { /* unsupported EOS Acl */
                  size_t xx = 0;
                  value.assign((char*) &xx, sizeof(xx)); /* Invalid xattr */
                }

                if (EOS_LOGS_DEBUG) {
                  eos_static_debug("racl getxattr %d", value.length());
                }
              } else
#endif /*HAVE_RICHACL*/
                if (!map.count(key)) {
                  rc = ENOATTR;
                } else {
                  value = map[key];
                }
            }
          }
        }
      }
    }
  }

  if (!rc && (size != 0)) {
    if (value.size() > size) {
      rc = ERANGE;
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    if (size == 0) {
      fuse_reply_xattr(req, value.size());
    } else {
      fuse_reply_buf(req, value.c_str(), value.size());
    }
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc, xattr_name).c_str());
}

#ifdef __APPLE__
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  const char* xattr_value, size_t size, int flags,
                  uint32_t position)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  const char* xattr_value, size_t size, int flags)
/* -------------------------------------------------------------------------- */
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  std::string key = xattr_name;
  eos_static_debug(key.c_str()); /* key in case xattr_name == NULL */
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("setxattr", Instance().Tracker(), ino, true);
  int rc = 0;
  fuse_id id(req);
  cap::shared_cap pcap;
  std::string value;
  bool local_setxattr = false;
  value.assign(xattr_value, size);
#ifdef notdefHAVE_RICHACL

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("value: '%s' l=%d", escape(value).c_str(), size);
  }

#endif /*HAVE_RICHACL*/
  // the root user has a bypass to be able to change th fuse configuration in
  // realtime
  {
    static std::string s_debug = "system.eos.debug";
    static std::string s_dropcap = "system.eos.dropcap";
    static std::string s_dropallcap = "system.eos.dropallcap";
    static std::string s_resetstat = "system.eos.resetstat";
    static std::string s_log = "system.eos.log";

    if (key.substr(0, s_debug.length()) == s_debug) {
      local_setxattr = true;
      // only root can do this configuration changes

      if (fuse_req_ctx(req)->uid == 0) {
        rc = EINVAL;

        if (value == "crit") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_CRIT);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "warn") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_WARNING);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "error") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_ERR);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "notice") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_NOTICE);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "info") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "debug") {
          eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
          Instance().SetTrace(false);
          rc = 0;
        }

        if (value == "trace") {
          Instance().SetTrace(true);
          rc = 0;
        }
      } else {
        rc = EPERM;
      }
    }

    if (key.substr(0, s_dropcap.length()) == s_dropcap) {
      local_setxattr = true;
      cap::shared_cap pcap = Instance().caps.get(req, ino);

      if (pcap->id()) {
        Instance().caps.forget(pcap->capid(req, ino));
      }
    }

    if (key.substr(0, s_dropallcap.length()) == s_dropallcap) {
      local_setxattr = true;

      if (fuse_req_ctx(req)->uid == 0) {
        Instance().caps.reset();
      } else {
        rc = EPERM;
      }
    }

    if (fuse_req_ctx(req)->uid == 0) {
      if (key.substr(0, s_resetstat.length()) == s_resetstat) {
        local_setxattr = true;
        Instance().getFuseStat().Clear();
        fuse_reply_err(req, 0);
        // avoid to show this call in stats again
        return ;
      }
    }

    if (key.substr(0, s_log.length()) == s_log) {
      local_setxattr = true;

      if (value == "public") {
        ::chmod(Instance().Config().logfilepath.c_str(),
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      }

      if (value == "private") {
        ::chmod(Instance().Config().logfilepath.c_str(), S_IRUSR | S_IWUSR);
      }
    }
  }

  if (!local_setxattr) {
    metad::shared_md md;
    md = Instance().mds.get(req, ino);
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted()) {
      rc = md->deleted() ? ENOENT : md->err();
    } else {
      // retrieve the appropriate cap
      if (S_ISDIR(md->mode())) {
        pcap = Instance().caps.acquire(req, ino,
                                       SA_OK);
      } else {
        pcap = Instance().caps.acquire(req, md->pid(),
                                       SA_OK);
      }

      if (pcap->errc()) {
        rc = pcap->errc();
      } else {
        static std::string s_sec = "security.";
        static std::string s_acl = "system.posix_acl_access";
        static std::string s_apple = "com.apple";
        static std::string s_racl = "system.richacl";

        if (key.substr(0, 4) == "eos.") {
          // eos attributes are silently ignored
          rc = 0;
        }  else

          // ignore silently any security attribute
          if (key.substr(0, s_sec.length()) == s_sec) {
            rc = 0;
          } else

            // ignore silently any posix acl attribute
            if (key == s_acl) {
              rc = 0;
            }

#ifdef __APPLE__
            else

              // ignore silently any finder attribute
              if (key.substr(0, s_apple.length()) == s_apple) {
                rc = 0;
              }

#endif
              else if (key == s_racl) {
#ifdef HAVE_RICHACL
                struct richacl* a = richacl_from_xattr(xattr_value, size);
                richacl_compute_max_masks(a);

                if (EOS_LOGS_DEBUG) {
                  char* a_t = richacl_to_text(a, RICHACL_TEXT_SHOW_MASKS);
                  eos_static_debug("acl a_t '%s' ", a_t);
                  free(a_t);
                }

                int new_mode = richacl_masks_to_mode(a);
                char eosAcl[512];
                racl2eos(a, eosAcl, sizeof(eosAcl), md);
                eos_static_debug("acl eosacl '%s'", eosAcl);
                auto map = md->mutable_attr();
                rc = 0; /* assume green light */

                // assert user acls are enabled
                if (!map->count("sys.eval.useracl")) {
                  if (S_ISDIR(md->mode())) {
                    rc = EPERM;
                  } else {
                    metad::shared_md pmd = Instance().mds.getlocal(req, md->pid());
                    auto pmap = pmd->mutable_attr();

                    if (pmap->count("sys.eval.useracl") == 0) {
                      rc = EPERM;
                    }
                  }
                }

                if (rc == 0) {
                  new_mode |= (md->mode() & ~0777);
                  eos_static_debug("set new mode %#o", new_mode);
                  md->set_mode(new_mode);
                  (*map)["user.acl"] = std::string(eosAcl);
                  Instance().mds.update(req, md, pcap->authid());
                  pcap->invalidate();

                  if (Instance().mds.has_flush(ino)) {
                    Instance().mds.wait_flush(req, md); // wait for upstream flush
                  }
                }

#else /*HAVE_RICHACL*/
                rc = EINVAL;                          // fail loudly if not supported
#endif /*HAVE_RICHACL*/
              } else {
                auto map = md->mutable_attr();
                bool exists = false;

                if ((*map).count(key)) {
                  exists = true;
                }

                if (exists && (flags == XATTR_CREATE)) {
                  rc = EEXIST;
                } else if (!exists && (flags == XATTR_REPLACE)) {
                  rc = ENOATTR;
                } else {
                  (*map)[key] = value;
                  Instance().mds.update(req, md, pcap->authid());
                }
              }
      }
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc, xattr_name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("listxattr", Instance().Tracker(), ino);
  int rc = 0;
  fuse_id id(req);
  cap::shared_cap pcap;
  std::string attrlist;
  size_t attrlistsize = 0;
  metad::shared_md md;
  md = Instance().mds.get(req, ino);

  // retrieve the appropriate cap
  if (S_ISDIR(md->mode())) {
    pcap = Instance().caps.acquire(req, ino,
                                   X_OK, true);
  } else {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   X_OK, true);
  }

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted()) {
      rc = md->deleted() ? ENOENT : md->err();
    } else {
      auto map = md->attr();
      attrlist = "";

      for (auto it = map.begin(); it != map.end(); ++it) {
        if (it->first.substr(0, 4) == "sys.") {
          if (Instance().Config().options.no_eos_xattr_listing) {
            continue;
          }

          attrlist += "eos.";
          attrlistsize += strlen("eos.");
        }

        attrlistsize += it->first.length() + 1;
        attrlist += it->first;
        attrlist += '\0';
      }

      if (!Instance().Config().options.no_eos_xattr_listing) {
        // add 'eos.btime'
        attrlist += "eos.btime";
        attrlist += '\0';
        attrlistsize += strlen("eos.btime") + 1;
        // add 'eos.ttime'
        attrlist += "eos.ttime";
        attrlist += '\0';
        attrlistsize += strlen("eos.ttime") + 1;
        // add 'eos.tsize'
        attrlist += "eos.tsize";
        attrlist += '\0';
        attrlistsize += strlen("eos.tsize") + 1;
        // add "eos.url.xroot";
        attrlist += "eos.url.xroot";
        attrlist += '\0';
        attrlistsize += strlen("eos.url.xroot") + 1;
      }

      if (!Instance().Config().options.no_eos_xattr_listing) {
        // for files add 'eos.checksum'
        if (S_ISREG(md->mode())) {
          attrlist += "eos.checksum";
          attrlist += '\0';
          attrlistsize += strlen("eos.checksum") + 1;
          attrlist += "eos.md_ino";
          attrlist += '\0';
          attrlistsize += strlen("eos.md_ino") + 1;
        }
      }

      if (size != 0) {
        if (attrlistsize > size) {
          rc = ERANGE;
        }
      }
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    if (size == 0) {
      fuse_reply_xattr(req, attrlistsize);
    } else {
      fuse_reply_buf(req, attrlist.c_str(), attrlist.length());
    }
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::removexattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("removexattr", Instance().Tracker(), ino);
  int rc = 0;
  fuse_id id(req);
  cap::shared_cap pcap;
  metad::shared_md md;
  md = Instance().mds.get(req, ino);

  // retrieve the appropriate cap
  if (S_ISDIR(md->mode())) {
    pcap = Instance().caps.acquire(req, ino,
                                   SA_OK, true);
  } else {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   SA_OK, true);
  }

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted()) {
      rc = md->deleted() ? ENOENT : md->err();
    } else {
      std::string key = xattr_name;
      static std::string s_sec = "security.";
      static std::string s_acl = "system.posix_acl";
      static std::string s_apple = "com.apple";
      static std::string s_racl = "system.richacl";

      // ignore silently any security attribute
      if (key.substr(0, s_sec.length()) == s_sec) {
        rc = 0;
      } else

        // ignore silently any posix acl attribute
        if (key == s_acl) {
          rc = 0;
        }

#ifdef __APPLE__
        else

          // ignore silently any finder attribute
          if (key.substr(0, s_apple.length()) == s_apple) {
            rc = 0;
          }

#endif
          else {
#ifdef HAVE_RICHACL

            if (key == s_racl) {
              key = "user.acl";
            }

#endif
            auto map = md->mutable_attr();
            bool exists = false;

            if ((*map).count(key)) {
              exists = true;
            }

            if (!exists) {
              rc = ENOATTR;
            } else {
              (*map).erase(key);
              Instance().mds.update(req, md, pcap->authid());
            }
          }
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::readlink(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
/*
 EACCES Search permission is denied for a component of the path prefix.  (See also path_resolution(7).)

 EFAULT buf extends outside the process’s allocated address space.

 EINVAL bufsiz is not positive.

 EINVAL The named file is not a symbolic link.

 EIO    An I/O error occurred while reading from the file system.

 ELOOP  Too many symbolic links were encountered in translating the pathname.

 ENAMETOOLONG
  A pathname, or a component of a pathname, was too long.

 ENOENT The named file does not exist.

 ENOMEM Insufficient kernel memory was available.

 ENOTDIR
  A component of the path prefix is not a directory.
 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("readlink", Instance().Tracker(), ino);
  int rc = 0;
  std::string target;
  fuse_id id(req);
  cap::shared_cap pcap;
  metad::shared_md md;
  md = Instance().mds.get(req, ino);

  if (!md->id() || md->deleted()) {
    rc = md->deleted() ? ENOENT : md->err();

    if (rc == EPERM) {
      rc = EACCES;
    }
  } else {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   Instance().Config().options.x_ok, true);

    if (pcap->errc()) {
      rc = pcap->errc();
    } else {
      XrdSysMutexHelper mLock(md->Locker());

      if (!md->id() || md->deleted()) {
        rc = ENOENT;
      } else {
        if (!(md->mode() & S_IFLNK)) {
          // no a link
          rc = EINVAL;
        } else {
          target = md->target();
        }
      }
    }

    if (Instance().Config().options.protect_directory_symlink_loops) {
      std::string localpath = Instance().Prefix(Instance().mds.calculateLocalPath(
                                md));

      if ((target.front() == '/')) {
        if (localpath.substr(0, target.size()) == target) {
          target = "/#_invalidated_link";
        }
      } else {
        std::string targetpath = localpath;
        targetpath += "/";
        targetpath += target;
        eos::common::Path tPath(targetpath);
        targetpath = tPath.GetPath();

        if (localpath.substr(0, targetpath.size()) == targetpath) {
          target = "#_invalidated_link";
        }
      }
    }

    if (Instance().Config().options.submounts) {
      if (target.substr(0, 6) == "mount:") {
        std::string env;

        // if not shared, set the caller credentials
        if (0) {
          env = fusexrdlogin::environment(req);
        }

        std::string localpath = Instance().Prefix(Instance().mds.calculateLocalPath(
                                  md));
        rc = Instance().Mounter().mount(target, localpath, env);
      }

      if (target.substr(0, 11) == "squashfuse:") {
        std::string env;
        //    env = fusexrdlogin::environment(req);
        std::string localpath = Instance().Prefix(Instance().mds.calculateLocalPath(
                                  md));
        rc = Instance().Mounter().squashfuse(target, localpath, env);
      }
    }
  }

  if (!rc) {
    fuse_reply_readlink(req, target.c_str());
  } else {
    fuse_reply_err(req, errno);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                 const char* name)
/* -------------------------------------------------------------------------- */
/*
 EACCES Write access to the directory containing newpath is denied, or one of the directories in the path
  prefix of newpath did not allow search permission.  (See also path_resolution(7).)

 EEXIST newpath already exists.

 EFAULT oldpath or newpath points outside your accessible address space.

 EIO    An I/O error occurred.

 ELOOP  Too many symbolic links were encountered in resolving newpath.

 ENAMETOOLONG
  oldpath or newpath was too long.

 ENOENT A directory component in newpath does not exist or is a dangling symbolic link, or oldpath is the
  empty string.

 ENOMEM Insufficient kernel memory was available.

 ENOSPC The device containing the file has no room for the new directory entry.

 ENOTDIR
  A component used as a directory in newpath is not, in fact, a directory.

 EPERM  The file system containing newpath does not support the creation of symbolic links.

 EROFS  newpath is on a read-only file system.

 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("symlink", Instance().Tracker(), parent, true);
  int rc = 0;
  fuse_id id(req);
  struct fuse_entry_param e;
  // do a parent check
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                         S_IFDIR | W_OK | X_OK, true);

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    metad::shared_md md;
    metad::shared_md pmd;
    md = Instance().mds.lookup(req, parent, name);
    pmd = Instance().mds.get(req, parent, pcap->authid());
    XrdSysMutexHelper mLock(md->Locker());

    if (md->id() && !md->deleted()) {
      rc = EEXIST;
    } else {
      {
        uint64_t del_ino = 0;
        // logic avoiding a create/unlink/create sync/async race
        {
          XrdSysMutexHelper pLock(pmd->Locker());
          auto it = pmd->get_todelete().find(
                      eos::common::StringConversion::EncodeInvalidUTF8(name));

          if ((it != pmd->get_todelete().end()) && it->second) {
            del_ino = it->second;
          }
        }

        if (del_ino) {
          Instance().mds.wait_upstream(req, del_ino);
        }
      }
      md->set_mode(S_IRWXU | S_IRWXG | S_IRWXO | S_IFLNK);
      md->set_target(link);
      md->set_err(0);
      struct timespec ts;
      eos::common::Timing::GetTimeSpec(ts);
      md->set_name(name);
      md->set_atime(ts.tv_sec);
      md->set_atime_ns(ts.tv_nsec);
      md->set_mtime(ts.tv_sec);
      md->set_mtime_ns(ts.tv_nsec);
      md->set_ctime(ts.tv_sec);
      md->set_ctime_ns(ts.tv_nsec);
      md->set_btime(ts.tv_sec);
      md->set_btime_ns(ts.tv_nsec);
      md->set_uid(pcap->uid());
      md->set_gid(pcap->gid());
      md->lookup_inc();
      md->set_type(md->EXCL);
      rc = Instance().mds.add_sync(req, pmd, md, pcap->authid());
      md->set_type(md->MD);

      if (!rc) {
        Instance().mds.insert(req, md, pcap->authid());
        pmd->local_enoent().erase(name);
      }

      memset(&e, 0, sizeof(e));
      md->convert(e, pcap->lifetime());
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_entry(req, &e);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
              const char* newname)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("hlnk newname=%s ino=%#lx parent=%#lx", newname, ino, parent);
  }

  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("link", Instance().Tracker(), parent, true);
  int rc = 0;
  fuse_id id(req);
  struct fuse_entry_param e;
  // do a parent check
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                         S_IFDIR | X_OK | W_OK, true);

  if (pcap->errc()) {
    rc = pcap->errc();
  } else {
    metad::shared_md md; /* the new name */
    metad::shared_md pmd; /* the parent directory for the new name */
    metad::shared_md tmd; /* the link target */
    md = Instance().mds.lookup(req, parent, newname);
    pmd = Instance().mds.get(req, parent, pcap->authid());
    XrdSysMutexHelper mLock(md->Locker());

    if (md->id() && !md->deleted()) {
      rc = EEXIST;
    } else {
      {
        uint64_t del_ino = 0;
        // logic avoiding a create/unlink/create sync/async race
        {
          XrdSysMutexHelper pLock(pmd->Locker());
          auto it = pmd->get_todelete().find(
                      eos::common::StringConversion::EncodeInvalidUTF8(newname));

          if ((it != pmd->get_todelete().end()) && it->second) {
            del_ino = it->second;
          }
        }

        if (del_ino) {
          Instance().mds.wait_upstream(req, del_ino);
        }
      }
      tmd = Instance().mds.get(req, ino, pcap->authid()); /* link target */

      if (tmd->id() == 0 || tmd->deleted()) {
        rc = ENOENT;
      } else if (tmd->pid() != parent) {
        rc = EXDEV; /* only same parent supported */
      } else {
        XrdSysMutexHelper tmLock(tmd->Locker());

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("hlnk tmd id=%ld %s", tmd->id(), tmd->name().c_str());
        }

        md->set_mode(tmd->mode());
        md->set_err(0);
        struct timespec ts;
        eos::common::Timing::GetTimeSpec(ts);
        md->set_name(newname);
        char tgtStr[64];
        snprintf(tgtStr, sizeof(tgtStr), "////hlnk%ld",
                 tmd->md_ino()); /* This triggers the hard link and specifies the target inode */
        md->set_target(tgtStr);
        md->set_atime(tmd->atime());
        md->set_atime_ns(tmd->atime_ns());
        md->set_mtime(tmd->mtime());
        md->set_mtime_ns(tmd->mtime_ns());
        md->set_ctime(tmd->ctime());
        md->set_ctime_ns(tmd->ctime_ns());
        md->set_btime(tmd->btime());
        md->set_btime_ns(tmd->btime_ns());
        md->set_uid(tmd->uid());
        md->set_gid(tmd->gid());
        md->set_size(tmd->size());
        // increase the link count of the target
        auto attrMap = tmd->attr();
        size_t nlink = 1;

        if (attrMap.count(k_nlink)) {
          nlink += std::stol(attrMap[k_nlink]);
        }

        auto wAttrMap = tmd->mutable_attr();
        (*wAttrMap)[k_nlink] = std::to_string(nlink);
        eos_static_debug("setting link count to %d", nlink);
        auto sAttrMap = md->mutable_attr();
        (*sAttrMap)[k_mdino] = std::to_string(tmd->md_ino());
        tmd->set_nlink(nlink + 1);
        tmLock.UnLock();
        rc = Instance().mds.add_sync(req, pmd, md, pcap->authid());

        if (!rc) {
          Instance().mds.insert(req, md, pcap->authid());
        }

        md->set_target("");
        mLock.UnLock();

        if (!rc) {
          XrdSysMutexHelper tmLock(tmd->Locker());
          memset(&e, 0, sizeof(e));
          tmd->convert(e, pcap->lifetime());

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("hlnk tmd %s %s", tmd->name().c_str(), tmd->dump(e).c_str());
          }

          pmd->local_enoent().erase(newname);
          // reply with the target entry
          fuse_reply_entry(req, &e);
        }
      }
    }
  }

  if (rc) {
    fuse_reply_err(req, rc);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
}

void
/* -------------------------------------------------------------------------- */
EosFuse::getlk(fuse_req_t req, fuse_ino_t ino,
               struct fuse_file_info* fi, struct flock* lock)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("getlk", Instance().Tracker(), ino);
  fuse_id id(req);
  int rc = 0;

  if (!Instance().Config().options.global_locking) {
    // use default local locking
    rc = EOPNOTSUPP;
  } else {
    // use global locking
    data::data_fh* io = (data::data_fh*) fi->fh;

    if (io) {
      rc = Instance().mds.getlk(req, io->mdctx(), lock);
    } else {
      rc = ENXIO;
    }
  }

  eos_static_info("%u %u %u %lu %lu rc=%d", lock->l_type,
		  lock->l_whence,
		  lock->l_pid,
		  lock->l_start,
		  lock->l_len,
		  rc);

  if (rc) {
    fuse_reply_err(req, rc);
  } else {
    fuse_reply_lock(req, lock);
  }

  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::setlk(fuse_req_t req, fuse_ino_t ino,
               struct fuse_file_info* fi,
               struct flock* lock, int sleep)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("setlk", Instance().Tracker(), ino, true);
  fuse_id id(req);
  int rc = 0;

  if (!Instance().Config().options.global_locking) {
    // use default local locking
    rc = EOPNOTSUPP;
  } else {
    // use global locking
    data::data_fh* io = (data::data_fh*) fi->fh;

    if (io) {
      size_t w_ms = 10;

      do {
        // we currently implement the polling lock on client side due to the
        // thread-per-link model of XRootD
        rc = Instance().mds.setlk(req, io->mdctx(), lock, sleep);

        if (rc && sleep) {
          std::this_thread::sleep_for(std::chrono::milliseconds(w_ms));
          // do exponential back-off with a hard limit at 1s
          w_ms *= 2;

          if (w_ms > 1000) {
            w_ms = 1000;
          }

          continue;
        }

        break;
      } while (rc);
    } else {
      rc = ENXIO;
    }
  }

  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

#ifdef FUSE_SUPPORTS_FLOCK
void
/* -------------------------------------------------------------------------- */
EosFuse::flock(fuse_req_t req, fuse_ino_t ino,
               struct fuse_file_info* fi, int op)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  ADD_FUSE_STAT(__func__, req);
  EXEC_TIMING_BEGIN(__func__);
  Track::Monitor mon("flock", Instance().Tracker(), ino, true);
  fuse_id id(req);
  int rc = 0;

  if (!Instance().Config().options.global_locking) {
    // use default local locking
    rc = EOPNOTSUPP;
  } else {
    if (op) {
      // use global locking
      data::data_fh* io = (data::data_fh*) fi->fh;
      
      if (io) {
	metad::shared_md md = io->mdctx();
	size_t w_ms = 10;
	int sleep = 1;
	struct flock lock;
	lock.l_len = 0;
	lock.l_start = 0;
	
	if (op & LOCK_NB) {
	  sleep = 0;
	}
	
	if (op & LOCK_SH) {
	  lock.l_type = F_RDLCK;
	} else if (op & LOCK_EX) {
	  lock.l_type = F_WRLCK;
	} else if (op & LOCK_UN) {
	  lock.l_type = F_UNLCK;
      } else if (op & LOCK_MAND) {
	  // mandatory locking used by samba
	  if ( op & LOCK_READ) {
	    // 1st approximation
	    lock.l_type = F_RDLCK;
	  } else if (op & LOCK_WRITE) {
	    // 1st approximation
	    lock.l_type = F_RDLCK;
	  } else if (op & LOCK_RW) {
	    // 1st approximation
	    lock.l_type = F_RDLCK;
	  } else {
	    // 1st approximation
	    lock.l_type = F_WRLCK;
	  }
	} else {
	  eos_static_notice("unsupported lock operation op:=%x", op);
	  rc = EINVAL;
	}
	
	lock.l_pid = fuse_req_ctx(req)->pid;
	
	if (!rc) {
	  do {
	    // we currently implement the polling lock on client side due to the
	    // thread-per-link model of XRootD
	    rc = Instance().mds.setlk(req, md, &lock, sleep);
	    
	    if (rc && sleep) {
	      std::this_thread::sleep_for(std::chrono::milliseconds(w_ms));
	      // do exponential back-off with a hard limit at 1s
	      w_ms *= 2;
	      
	      if (w_ms > 1000) {
		w_ms = 1000;
	      }
	      
	      continue;
	    }
	    
	    break;
	  } while (rc);
	}
	
	if (!rc) {
	  io->flocked = true;
	}
      } else {
	rc = ENXIO;
      }
    } else {
      // consider a no-op
      rc = 0;
    }
  }
  
  fuse_reply_err(req, rc);
  EXEC_TIMING_END(__func__);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f op=%x %s", timing.RealTime(),op,
                    dump(id, ino, 0, rc).c_str());
}
#endif

/* -------------------------------------------------------------------------- */
void
EosFuse::getHbStat(eos::fusex::statistics& hbs)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("get statistics");
  eos::common::LinuxStat::linux_stat_t osstat;
#ifndef __APPLE__
  eos::common::LinuxMemConsumption::linux_mem_t mem;

  if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem)) {
    eos_static_err("failed to get the MEM usage information");
  }

  if (!eos::common::LinuxStat::GetStat(osstat)) {
    eos_static_err("failed to get the OS usage information");
  }

#endif
  hbs.set_inodes(getMdStat().inodes());
  hbs.set_inodes_todelete(getMdStat().inodes_deleted());
  hbs.set_inodes_backlog(getMdStat().inodes_backlog());
  hbs.set_inodes_ever(getMdStat().inodes_ever());
  hbs.set_inodes_ever_deleted(getMdStat().inodes_deleted_ever());
  hbs.set_threads(osstat.threads);
  hbs.set_vsize_mb(osstat.vsize / 1000.0 / 1000.0);
  hbs.set_rss_mb(osstat.rss / 1000.0 / 1000.0);
  hbs.set_open_files(Instance().datas.size());
  {
    std::lock_guard<std::mutex> lock(meminfo.mutex());
    hbs.set_free_ram_mb(meminfo.getref().freeram / 1000.0 / 1000.0);
    hbs.set_total_ram_mb(meminfo.getref().totalram / 1000.0 / 1000.0);
    hbs.set_load1(1.0 * meminfo.getref().loads[0] / (1 << SI_LOAD_SHIFT));
  }
  {
    XrdSysMutexHelper sLock(getFuseStat().Mutex);
    hbs.set_rbytes(getFuseStat().GetTotal("rbytes"));
    hbs.set_wbytes(getFuseStat().GetTotal("wbytes"));
    hbs.set_nio(getFuseStat().GetOps());
    hbs.set_rd_rate_60_mb(getFuseStat().GetTotalAvg60("rbytes") / 1000.0 / 1000.0);
    hbs.set_wr_rate_60_mb(getFuseStat().GetTotalAvg60("wbytes") / 1000.0 / 1000.0);
    hbs.set_iops_60(getFuseStat().GetTotalAvg60(":sum"));
  }
  hbs.set_wr_buf_mb(XrdCl::Proxy::sWrBufferManager.inflight() / 1000.0 / 1000.0);
  hbs.set_ra_buf_mb(XrdCl::Proxy::sRaBufferManager.inflight() / 1000.0 / 1000.0);
  hbs.set_xoff(Instance().datas.get_xoff());
  hbs.set_raxoff(XrdCl::Proxy::sRaBufferManager.xoff());
  hbs.set_ranobuf(XrdCl::Proxy::sRaBufferManager.nobuf());
  hbs.set_pid(getpid());
  hbs.set_logfilesize(sizeLogFile());
  hbs.set_wrnobuf(XrdCl::Proxy::sWrBufferManager.nobuf());
}

/* -------------------------------------------------------------------------- */
bool
EosFuse::isRecursiveRm(fuse_req_t req, bool forced, bool notverbose)
/* -------------------------------------------------------------------------- */
{
#ifndef __APPLE__
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(ctx->pid,
                             ctx->uid, ctx->gid, false);

  if (snapshot && snapshot->getProcessInfo().getRmInfo().isRm() &&
      snapshot->getProcessInfo().getRmInfo().isRecursive()) {
    bool result = true;

    if (forced) {
      // check if this is rm -rf style
      result = snapshot->getProcessInfo().getRmInfo().isForce();
    }

    if (notverbose) {
      result &= (!snapshot->getProcessInfo().getRmInfo().isVerbose());
    }

    return result;
  }

#endif
  return false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::TrackMgm(const std::string& lasturl)
/* -------------------------------------------------------------------------- */
{
  static std::mutex lTrackMgmMutex;
  std::lock_guard<std::mutex> sequenzerMutex(lTrackMgmMutex);
  std::string currentmgm = lastMgmHostPort.get();
  XrdCl::URL lastUrl(lasturl);
  std::string newmgm = lastUrl.GetHostName();
  std::string sport;
  newmgm += ":";
  newmgm += eos::common::StringConversion::GetSizeString(sport,
            (unsigned long long) lastUrl.GetPort());
  eos_static_debug("current-mgm:%s last-url:%s", currentmgm.c_str(),
                   newmgm.c_str());

  if (currentmgm != newmgm) {
    // for the first call currentmgm is an empty string, so we assume there is no failover needed
    if (currentmgm.length()) {
      // let's failover the ZMQ connection
      size_t p_pos = config.mqtargethost.rfind(":");
      std::string new_mqtargethost = config.mqtargethost;

      if ((p_pos != std::string::npos) && (p_pos > 6)) {
        new_mqtargethost.erase(6, p_pos - 6);
      } else {
        new_mqtargethost.erase(4);
      }

      lastMgmHostPort.set(newmgm);
      newmgm.erase(newmgm.find(":"));
      new_mqtargethost.insert(6, newmgm);
      // instruct a new ZMQ connection
      mds.connect(new_mqtargethost);
      eos_static_warning("reconnecting mqtarget=%s => mqtarget=%s",
                         config.mqtargethost.c_str(), new_mqtargethost.c_str());
    } else {
      // just store the first time we see the connected endpoint url
      lastMgmHostPort.set(newmgm);
    }
  }
}

/* -------------------------------------------------------------------------- */
std::string
EosFuse::Prefix(std::string path)
/* -------------------------------------------------------------------------- */
{
  std::string fullpath = Config().localmountdir;

  if (fullpath.back() == '/') {
    fullpath.pop_back();
  }

  return (fullpath + path);
}
