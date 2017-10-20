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

#ifdef ROCKSDB_FOUND
#include "kv/RocksKV.hh"
#endif

#include "eosfuse.hh"
#include "misc/fusexrdlogin.hh"

#include <string>
#include <map>
#include <set>
#include <iostream>
#include <sstream>
#include <memory>
#include <algorithm>
#include <thread>
#include <iterator>

#include <dirent.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <sys/resource.h>
#include <sys/types.h>
#ifdef __APPLE__
#define O_DIRECT 0
#define EKEYEXPIRED 127
#include <sys/xattr.h>
#else
#include <attr/xattr.h>
#endif

#include <json/json.h>

#include "common/Timing.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/StringConversion.hh"
#include "md/md.hh"
#include "kv/kv.hh"
#include "data/cache.hh"

#if ( FUSE_USE_VERSION > 28 )
#include "EosFuseSessionLoop.hh"
#endif

#define _FILE_OFFSET_BITS 64

EosFuse* EosFuse::sEosFuse = 0;

/* -------------------------------------------------------------------------- */
EosFuse::EosFuse()
{
  sEosFuse = this;
  fusesession = 0;
  fusechan = 0;
}

/* -------------------------------------------------------------------------- */
EosFuse::~EosFuse()
{
  eos_static_warning("");
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
EosFuse::run(int argc, char* argv[], void *userdata)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");

  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  fuse_opt_parse(&args, NULL, NULL, NULL);

  char* local_mount_dir = 0;
  int err = 0;

  // check the fsname to choose the right JSON config file

  std::string fsname = "";
  for (int i = 0; i < argc; i++)
  {
    std::string option = argv[i];
    size_t npos;
    size_t epos;

    if ((npos = option.find("fsname=")) != std::string::npos)
    {
      epos = option.find(",", npos);

      fsname = option.substr(npos + std::string("fsname=").length(),
                             (epos != std::string::npos) ?
                             epos - npos-std::string("fsname=").length() : -1);
      break;
    }
  }

  fprintf(stderr, "# fsname='%s'\n", fsname.c_str());

  if (getuid() == 0)
  {
    // the root mount always adds the 'allow_other' option
    fuse_opt_add_arg(&args, "-oallow_other");
    fprintf(stderr, "# -o allow_other enabled on shared mount\n");
  }
  fprintf(stderr, "# -o big_writes enabled\n");
  fuse_opt_add_arg(&args, "-obig_writes");

  std::string jsonconfig = "/etc/eos/fuse";

  if (fsname.length())
  {
    jsonconfig += ".";
    jsonconfig += fsname;
  }
  jsonconfig += ".conf";

#ifndef __APPLE__
  if (::access("/bin/fusermount", X_OK))
  {
    fprintf(stderr, "error: /bin/fusermount is not executable for you!\n");
    exit(-1);
  }
#endif

  if (getuid() <= DAEMONUID)
  {
    unsetenv("KRB5CCNAME");
    unsetenv("X509_USER_PROXY");
  }

  cachehandler::cacheconfig cconfig;

  {
    // parse JSON configuration
    Json::Value root;
    Json::Reader reader;
    std::ifstream configfile(jsonconfig, std::ifstream::binary);
    if (reader.parse(configfile, root, false))
    {
      fprintf(stderr, "# JSON parsing successfull\n");
    }
    else
    {
      fprintf(stderr, "error: invalid configuration file %s - %s\n",
              jsonconfig.c_str(), reader.getFormattedErrorMessages().c_str());
      exit(EINVAL);
    }


    const Json::Value jname = root["name"];
    config.name = root["name"].asString();
    config.hostport = root["hostport"].asString();
    config.remotemountdir = root["remotemountdir"].asString();
    config.localmountdir = root["localmountdir"].asString();
    config.statfilesuffix = root["statfilesuffix"].asString();
    config.statfilepath = root["statfilepath"].asString();
    config.options.debug = root["options"]["debug"].asInt();
    config.options.lowleveldebug = root["options"]["lowleveldebug"].asInt();
    config.options.debuglevel = root["options"]["debuglevel"].asInt();
    config.options.libfusethreads = root["options"]["libfusethreads"].asInt();
    config.options.md_kernelcache = root["options"]["md-kernelcache"].asInt();
    config.options.md_kernelcache_enoent_timeout = root["options"]["md-kernelcache.enoent.timeout"].asDouble();
    config.options.md_backend_timeout = root["options"]["md-backend.timeout"].asDouble();
    config.options.data_kernelcache = root["options"]["data-kernelcache"].asInt();
    config.options.mkdir_is_sync = root["options"]["mkdir-is-sync"].asInt();
    config.options.create_is_sync = root["options"]["create-is-sync"].asInt();
    config.options.symlink_is_sync = root["options"]["symlink-is-sync"].asInt();
    config.options.global_flush = root["options"]["global-flush"].asInt();
    config.options.global_locking = root["options"]["global-locking"].asInt();
    config.options.fdlimit = root["options"]["fd-limit"].asInt();
    config.mdcachehost = root["mdcachehost"].asString();
    config.mdcacheport = root["mdcacheport"].asInt();
    config.mdcachedir = root["mdcachedir"].asString();

    config.mqtargethost = root["mdzmqtarget"].asString();
    config.mqidentity = root["mdzmqidentity"].asString();
    config.mqname = config.mqidentity;
    config.auth.fuse_shared  = root["auth"]["shared-mount"].asInt();
    config.auth.use_user_krb5cc = root["auth"]["krb5"].asInt();
    config.auth.use_user_gsiproxy = root["auth"]["gsi"].asInt();
    config.auth.tryKrb5First = !((bool)root["auth"]["gsi-first"].asInt());

    // disallow mdcachedir if compiled without rocksdb support
#ifndef ROCKSDB_FOUND
    if(!config.mdcachedir.empty()) {
      std::cerr << "Options mdcachedir is unavailable, fusex was compiled without rocksdb support." << std::endl;
      exit(EINVAL);
    }
#endif

    // disallow conflicting options
    if(!config.mdcachedir.empty() && (config.mdcacheport != 0 || !config.mdcachehost.empty()) ) {
      std::cerr << "Options (mdcachehost, mdcacheport) conflict with (mdcachedir) - only one type of mdcache is allowed." << std::endl;
      exit(EINVAL);
    }

    // default settings
    if (!config.statfilesuffix.length())
    {
      config.statfilesuffix="stats";
    }
    if (!config.mdcacheport)
    {
      config.mdcacheport = 6379;
    }
    if (!config.mqtargethost.length())
    {
      std::string h = config.hostport;
      h.erase(h.find(":"));
      config.mqtargethost="tcp://" + h + ":1100";
    }

    {
      config.mqidentity.insert(0, "fuse://");
      config.mqidentity += "@";
      char hostname[4096];
      if (gethostname(hostname, sizeof (hostname)))
      {
        fprintf(stderr, "error: failed to get hostname!\n");
        exit(EINVAL);
      }

      config.clienthost = hostname;
      config.mqidentity += hostname;

      char suuid[40];
      uuid_t uuid;
      uuid_generate_time(uuid);
      uuid_unparse(uuid, suuid);
      config.clientuuid=suuid;

      config.mqidentity += "//";
      config.mqidentity += suuid;
      config.mqidentity += ":";
      char spid[16];
      snprintf(spid, sizeof (spid), "%d", getpid());
      config.mqidentity += spid;
    }

    if(config.options.fdlimit > 0) {

      struct rlimit newrlimit;
      newrlimit.rlim_cur = config.options.fdlimit;
      newrlimit.rlim_max = config.options.fdlimit;

      if(setrlimit(RLIMIT_NOFILE, &newrlimit) != 0) {
        fprintf(stderr, "error: unable to set fd limit to %d - errno %d\n", config.options.fdlimit, errno);
        exit(EINVAL);
      }
    }

    struct rlimit nofilelimit;
    if(getrlimit(RLIMIT_NOFILE, &nofilelimit) != 0) {
      fprintf(stderr, "error: unable to get fd limit - errno %d\n", errno);
      exit(EINVAL);
    }
    fprintf(stderr, "# File descriptor limit: %lu soft, %lu hard\n", nofilelimit.rlim_cur, nofilelimit.rlim_max);

    // data caching configuration
    cconfig.type = cachehandler::cache_t::INVALID;

    if (!config.mdcachehost.length())
    {
      cconfig.clean_on_startup = true;
    }
    else
    {
      cconfig.clean_on_startup = false;
    }

    if (root["cache"]["type"].asString() == "disk")
    {
      cconfig.type = cachehandler::cache_t::DISK;
    }
    else if (root["cache"]["type"].asString() == "memory")
    {
      cconfig.type = cachehandler::cache_t::MEMORY;
    }
    else
    {
      fprintf(stderr, "error: invalid cache type configuration\n");
    }

    cconfig.location = root["cache"]["location"].asString();
    cconfig.journal = root["cache"]["journal"].asString();
    cconfig.total_file_cache_size = root["cache"]["size-mb"].asUInt64()*1024 * 1024;
    cconfig.total_file_journal_size = root["cache"]["journal-mb"].asUInt64()*1024 * 1024;
    cconfig.per_file_cache_max_size = root["cache"]["file-cache-max-kb"].asUInt64()*1024;
    cconfig.per_file_journal_max_size = root["cache"]["file-journal-max-kb"].asUInt64()*1024;

    int rc = 0;

    if ( (rc = cachehandler::instance().init(cconfig)))
    {
      exit(rc);
    }
  }

  {
    std::string mountpoint;
    for (int i=1; i< argc; ++i)
    {
      std::string opt = argv[i];
      if (opt[0]!='-')
      {
	mountpoint = opt;
      }
      if (opt == "-f")
      {
	config.options.foreground = 1;
      }
    }

    if (!mountpoint.length())
    {
      // we allow to take the mountpoint from the json file if it is not given on the command line
      fuse_opt_add_arg(&args, config.localmountdir.c_str());
      mountpoint = config.localmountdir.c_str();
    }

    if (mountpoint.length())
    {
      // sanity check of the mount directory
      struct stat buf;
      if ( stat( mountpoint.c_str(), &buf) )
      {
	// check for a broken mount
	if ( errno == ENOTCONN )
	{
	  // force an 'umount -l '
	  std::string systemline="umount -l ";
	  systemline += mountpoint;
	  fprintf(stderr,"# dead mount detected - forcing '%s'\n", systemline.c_str());
	  system(systemline.c_str());
	}
      }
    }
  }

  int debug;
  if (fuse_parse_cmdline(&args, &local_mount_dir, NULL, &debug) == -1)
  {
    exit(errno?errno:-1);
  }
  if ((fusechan = fuse_mount(local_mount_dir, &args)) == NULL)
  {
    fprintf(stderr,"error: fuse_mount failed\n");
    exit(errno?errno:-1);
  }

  if (fuse_daemonize(config.options.foreground) != -1)
  {
    fusexrdlogin::initializeProcessCache(config.auth);

    FILE* fstderr;
    // Open log file
    if (getuid())
    {
      char logfile[1024];
      if (getenv("EOS_FUSE_LOGFILE"))
      {
        snprintf(logfile, sizeof ( logfile) - 1, "%s",
                 getenv("EOS_FUSE_LOGFILE"));
      }
      else
      {
        snprintf(logfile, sizeof ( logfile) - 1, "/tmp/eos-fuse.%d.log",
                 getuid());
      }

      if (!config.statfilepath.length())
      {
        config.statfilepath = logfile;
        config.statfilepath += ".";
        config.statfilepath += config.statfilesuffix;
      }

      // Running as a user ... we log into /tmp/eos-fuse.$UID.log
      if (!(fstderr = freopen(logfile, "a+", stderr)))
        fprintf(stderr, "error: cannot open log file %s\n", logfile);
      else
        ::chmod(logfile, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }
    else
    {
      // Running as root ... we log into /var/log/eos/fuse
      std::string log_path = "/var/log/eos/fusex/fuse.";
      if (getenv("EOS_FUSE_LOG_PREFIX") || fsname.length())
      {
        if (getenv("EOS_FUSE_LOG_PREFIX"))
        {
          log_path += getenv("EOS_FUSE_LOG_PREFIX");
        }
        else
        {
          log_path += fsname;
        }
        if (!config.statfilepath.length()) config.statfilepath = log_path +
                "." + config.statfilesuffix;
        log_path += ".log";
      }
      else
      {
        if (!config.statfilepath.length()) config.statfilepath = log_path +
                config.statfilesuffix;
        log_path += "log";
      }

      eos::common::Path cPath(log_path.c_str());
      cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);

      if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr)))
        fprintf(stderr, "error: cannot open log file %s\n", cPath.GetPath());
      else
        if (::chmod(cPath.GetPath(), S_IRUSR | S_IWUSR))
      {
        fprintf(stderr, "error: failed to chmod %s\n", cPath.GetPath());
      }
    }

    if (fstderr)
      setvbuf(fstderr, (char*) NULL, _IONBF, 0);

#ifdef EOSCITRINE
    eos::common::Logging::GetInstance().SetUnit ("FUSE@eosxd");
    eos::common::Logging::GetInstance().gShortFormat = true;
    eos::common::Logging::GetInstance().SetFilter("DumpStatistic");
    if (config.options.debug)
    {
      eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
    }
    else
    {
      if (config.options.debuglevel)
        eos::common::Logging::GetInstance().SetLogPriority(config.options.debuglevel);
      else
        eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO);
    }
#else
    eos::common::Logging::Init ();
    eos::common::Logging::SetUnit ("FUSE@eosxd");
    eos::common::Logging::gShortFormat = true;
    eos::common::Logging::SetFilter("DumpStatistic");
    if (config.options.debug)
    {
      eos::common::Logging::SetLogPriority(LOG_DEBUG);
    }
    else
    {
      if (config.options.debuglevel)
        eos::common::Logging::SetLogPriority(config.options.debuglevel);
      else
        eos::common::Logging::SetLogPriority(LOG_INFO);
    }
#endif

    // initialize mKV in case no cache is configured to act as no-op
    mKV.reset(new RedisKV());

#ifdef ROCKSDB_FOUND
    if(!config.mdcachedir.empty()) {
      RocksKV *kv = new RocksKV();
      if(kv->connect(config.name, config.mdcachedir) != 0) {
        fprintf(stderr, "error: failed to open rocksdb KV cache - path=%s",
                config.mdcachedir.c_str());
        exit(EINVAL);
      }
      mKV.reset(kv);
    }
#endif

    if (config.mdcachehost.length())
    {
      RedisKV *kv = new RedisKV();
      if (kv->connect(config.name, config.mdcachehost, config.mdcacheport ?
                      config.mdcacheport : 6379) != 0)
      {
        fprintf(stderr, "error: failed to connect to md cache - connect-string=%s",
                config.mdcachehost.c_str());
        exit(EINVAL);
      }
      mKV.reset(kv);
    }

    mdbackend.init(config.hostport, config.remotemountdir);
    mds.init(&mdbackend);
    caps.init(&mdbackend, &mds);
    datas.init();

    if (config.mqtargethost.length())
    {
      if (mds.connect(config.mqtargethost, config.mqidentity, config.mqname, config.clienthost, config.clientuuid))
      {
        fprintf(stderr, "error: failed to connect to mgm/zmq - connect-string=%s connect-identity=%s connect-name=%s",
                config.mqtargethost.c_str(), config.mqidentity.c_str(), config.mqname.c_str());
        exit(EINVAL);
      }
    }

    if ( cachehandler::instance().init_daemonized())
    {
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
    fusestat.Add(__SUM__TOTAL__, 0, 0, 0);

    tDumpStatistic = std::thread(EosFuse::DumpStatistic);
    //tDumpStatistic.detach();
    tStatCirculate = std::thread(EosFuse::StatCirculate);
    //tStatCirculate.detach();
    tMetaCacheFlush = std::thread(&metad::mdcflush, &mds);
    //tMetaCacheFlush.detach();
    tMetaCommunicate = std::thread(&metad::mdcommunicate, &mds);
    //tMetaCommunicate.detach();
    tCapFlush = std::thread(&cap::capflush, &caps);

    eos_static_warning("********************************************************************************");
    eos_static_warning("eosdx started version %s - FUSE protocol version %d", VERSION, FUSE_USE_VERSION);
    eos_static_warning("eos-instance-url       := %s", config.hostport.c_str());
    eos_static_warning("thread-pool            := %s", config.options.libfusethreads ? "libfuse" : "custom");
    eos_static_warning("zmq-connection         := %s", config.mqtargethost.c_str());
    eos_static_warning("zmq-identity           := %s", config.mqidentity.c_str());
    eos_static_warning("options                := md-cache:%d md-enoent:%.02f md-timeout:%.02f data-cache:%d mkdir-sync:%d create-sync:%d symlink-sync:%d flush:%d locking:%d",
		       config.options.md_kernelcache,
                       config.options.md_kernelcache_enoent_timeout,
                       config.options.md_backend_timeout,
                       config.options.data_kernelcache,
                       config.options.mkdir_is_sync,
                       config.options.create_is_sync,
		       config.options.symlink_is_sync,
                       config.options.global_flush,
                       config.options.global_locking
		       );


    fusesession = fuse_lowlevel_new(&args,
                                    &(get_operations()),
                                    sizeof (operations), NULL);

    if ((fusesession != NULL))
    {
      if (fuse_set_signal_handlers(fusesession) != -1)
      {
        fuse_session_add_chan(fusesession, fusechan);

        if (getenv("EOS_FUSE_NO_MT") &&
            (!strcmp(getenv("EOS_FUSE_NO_MT"), "1")))
        {
          err = fuse_session_loop(fusesession);
        }
        else
        {
#if ( FUSE_USE_VERSION <= 28 )
          err = fuse_session_loop_mt(fusesession);
#else
          if (config.options.libfusethreads)
          {
            err = fuse_session_loop_mt(fusesession);
          }
          else
          {
            EosFuseSessionLoop loop( 10, 20, 10, 20 );
            err = loop.Loop( fusesession );
          }
#endif
        }
        fuse_remove_signal_handlers(fusesession);
        fuse_session_remove_chan(fusechan);
      }
      fuse_session_destroy(fusesession);
    }


    eos_static_warning("eosdx stopped version %s - FUSE protocol version %d", VERSION, FUSE_USE_VERSION);
    eos_static_warning("********************************************************************************");

#if ( __GNUC_MINOR__ == 8 ) && ( __GNUC_PATCHLEVEL__ == 2 )
    {
      fuse_unmount(local_mount_dir, fusechan);
      pthread_kill(tDumpStatistic.native_handle(), 9);
      pthread_kill(tStatCirculate.native_handle(), 9);
      pthread_kill(tMetaCacheFlush.native_handle(), 9);
      pthread_kill(tMetaCommunicate.native_handle(), 9);
      pthread_kill(tCapFlush.native_handle(), 9);
    }
#else
    {
      // C++11 is poor for threading
      pthread_cancel(tDumpStatistic.native_handle());
      pthread_cancel(tStatCirculate.native_handle());
      pthread_cancel(tMetaCacheFlush.native_handle());
      pthread_cancel(tMetaCommunicate.native_handle());
      pthread_cancel(tCapFlush.native_handle());

      tDumpStatistic.join();
      tStatCirculate.join();
      tMetaCacheFlush.join();
      tMetaCommunicate.join();
      tCapFlush.join();
    }
#endif
    mds.terminate();
    fuse_unmount(local_mount_dir, fusechan);
  }
  else
  {
    fprintf(stderr,"error: failed to daemonize\n");
    exit(errno?errno:-1);
  }

  return err ? 1 : 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::umounthandler(int sig, siginfo_t *si, void *unused)
/* -------------------------------------------------------------------------- */
{
  eos_static_warning("sighandler received signal %d - emitting signal 2", sig);
  signal(SIGSEGV, SIG_DFL);
  kill (getpid(), 2);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::init(void *userdata, struct fuse_conn_info *conn)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  struct sigaction sa;

  sa.sa_flags = SA_SIGINFO;
  sigemptyset(&sa.sa_mask);
  sa.sa_sigaction = EosFuse::umounthandler;
  if (sigaction(SIGSEGV, &sa, NULL) == -1)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "failed to install SEGV handler");
    throw std::runtime_error(msg);
  }
  if (sigaction(SIGABRT, &sa, NULL) == -1)
  {
    char msg[1024];
    snprintf(msg, sizeof (msg), "failed to install SEGV handler");
    throw std::runtime_error(msg);
  }

  conn->want = FUSE_CAP_EXPORT_SUPPORT | FUSE_CAP_POSIX_LOCKS | FUSE_CAP_BIG_WRITES ;
  conn->capable = FUSE_CAP_EXPORT_SUPPORT | FUSE_CAP_POSIX_LOCKS | FUSE_CAP_BIG_WRITES ;
}

void
EosFuse::destroy(void *userdata)
{
  eos_static_debug("");
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::DumpStatistic()
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("started statistic dump thread");
  XrdSysTimer sleeper;
  char ino_stat[16384];
  time_t start_time = time(NULL);

  while (1)
  {
    eos::common::LinuxMemConsumption::linux_mem_t mem;
    eos::common::LinuxStat::linux_stat_t osstat;

    if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem))
    {
      eos_static_err("failed to get the MEM usage information");
    }


    if (!eos::common::LinuxStat::GetStat(osstat))
    {
      eos_static_err("failed to get the OS usage information");
    }

    eos_static_debug("dumping statistics");
    XrdOucString out;
    EosFuse::Instance().getFuseStat().PrintOutTotal(out);
    std::string sout = out.c_str();

    time_t now = time(NULL);

    snprintf(ino_stat, sizeof (ino_stat),
             "# -----------------------------------------------------------------------------------------------------------\n"
             "ALL        inodes              := %lu\n"
             "ALL        inodes-todelete     := %lu\n"
             "ALL        inodes-backlog      := %lu\n"
             "ALL        inodes-ever         := %lu\n"
             "ALL        inodes-ever-deleted := %lu\n"
             "# -----------------------------------------------------------------------------------------------------------\n",
             EosFuse::Instance().getMdStat().inodes(),
             EosFuse::Instance().getMdStat().inodes_deleted(),
             EosFuse::Instance().getMdStat().inodes_backlog(),
             EosFuse::Instance().getMdStat().inodes_ever(),
             EosFuse::Instance().getMdStat().inodes_deleted_ever());

    sout += ino_stat;

    std::string s1;
    std::string s2;

    snprintf(ino_stat, sizeof (ino_stat),
             "ALL        threads             := %llu\n"
             "ALL        visze               := %s\n"
             "All        rss                 := %s\n"
             "All        version             := %s\n"
             "ALl        fuseversion         := %d\n"
             "All        starttime           := %lu\n"
             "All        uptime              := %lu\n"
             "All        instance-url        := %s\n"
             "# -----------------------------------------------------------------------------------------------------------\n",
             osstat.threads,
             eos::common::StringConversion::GetReadableSizeString(s1, osstat.vsize, "b"),
             eos::common::StringConversion::GetReadableSizeString(s2, osstat.rss, "b"),
             VERSION,
             FUSE_USE_VERSION,
             start_time,
             now - start_time,
             EosFuse::Instance().config.hostport.c_str()
             );

    sout += ino_stat;

    std::ofstream dumpfile(EosFuse::Instance().config.statfilepath);
    dumpfile << sout;
    sleeper.Snooze(1);
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::StatCirculate()
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("started stat circulate thread");
  Stat::Instance().Circulate();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
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

  metad::shared_md md = Instance().mds.get(req, ino);

  {
    XrdSysMutexHelper mLock(md->Locker());
    if (!md->id() || (md->deleted() && !md->lookup_is()))
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      cap::shared_cap pcap = Instance().caps.acquire(req, md->pid() ? md->pid() : 1,
                                                     S_IFDIR | X_OK | R_OK);
      XrdSysMutexHelper capLock(pcap->Locker());
      if (pcap->errc())
      {
        rc = pcap->errc();
      }
      else
      {
        md->convert(e);
        eos_static_info("%s", md->dump(e).c_str());
      }
    }
  }

  if (rc)
    fuse_reply_err(req, rc);
  else
    fuse_reply_attr (req, &e.attr, e.attr_timeout);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int op,
                 struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  int rc = 0;

  fuse_id id(req);

  cap::shared_cap pcap;


  metad::shared_md md;

  md = Instance().mds.get(req, ino);

  md->Locker().Lock();

  if (!md->id() || (md->deleted() && !md->lookup_is()))
  {
    rc = md->deleted()? ENOENT : md->err();
  }
  else
  {

    fuse_ino_t cap_ino =  S_ISDIR(md->mode()) ? ino : md->pid();


    if (op & FUSE_SET_ATTR_MODE)
    {
      // retrieve cap for mode setting
      pcap = Instance().caps.acquire(req, cap_ino,
                                     M_OK);
    }
    else
      if ((op & FUSE_SET_ATTR_UID) || (op & FUSE_SET_ATTR_GID))
    {
      // retrieve cap for owner setting
      pcap = Instance().caps.acquire(req, cap_ino,
                                     C_OK);
    }
    else
      if (op & FUSE_SET_ATTR_SIZE)
    {
      // retrieve cap for write
      pcap = Instance().caps.acquire(req, cap_ino,
                                     W_OK);
    }
    else
      if ( (op & FUSE_SET_ATTR_ATIME)
          || (op & FUSE_SET_ATTR_MTIME)
          || (op & FUSE_SET_ATTR_ATIME_NOW)
          || (op & FUSE_SET_ATTR_MTIME_NOW)
          )
    {
      // retrieve cap for write
      pcap = Instance().caps.acquire(req, cap_ino,
                                     W_OK);
    }

    if (pcap->errc())
    {
      rc = pcap->errc();
    }
    else
    {

      if (op & FUSE_SET_ATTR_MODE)
      {
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

        md->set_mode(attr->st_mode);

        EXEC_TIMING_END("setattr:chmod");
      }

      if ((op & FUSE_SET_ATTR_UID) || (op & FUSE_SET_ATTR_GID))
      {
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

        if (op & FUSE_SET_ATTR_UID)
          md->set_uid(attr->st_uid);
        if (op & FUSE_SET_ATTR_GID)
          md->set_gid(attr->st_gid);

        EXEC_TIMING_END("setattr:chown");
      }

      if (
          (op & FUSE_SET_ATTR_ATIME)
          || (op & FUSE_SET_ATTR_MTIME)
          || (op & FUSE_SET_ATTR_ATIME_NOW)
          || (op & FUSE_SET_ATTR_MTIME_NOW)
          )
      {
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

        struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);
        if (op & FUSE_SET_ATTR_ATIME)
        {
          md->set_atime(attr->ATIMESPEC.tv_sec);
          md->set_atime_ns(attr->ATIMESPEC.tv_nsec);
          md->set_ctime(tsnow.tv_sec);
          md->set_ctime_ns(tsnow.tv_nsec);
        }
        if (op & FUSE_SET_ATTR_MTIME)
        {
          md->set_mtime(attr->MTIMESPEC.tv_sec);
          md->set_mtime_ns(attr->MTIMESPEC.tv_nsec);
          md->set_ctime(tsnow.tv_sec);
          md->set_ctime_ns(tsnow.tv_nsec);
        }

        if ( ( op & FUSE_SET_ATTR_ATIME_NOW) ||
            ( op & FUSE_SET_ATTR_MTIME_NOW) )
        {
          if (op & FUSE_SET_ATTR_ATIME_NOW)
          {
            md->set_atime(tsnow.tv_sec);
            md->set_atime_ns(tsnow.tv_nsec);
            md->set_ctime(tsnow.tv_sec);
            md->set_ctime_ns(tsnow.tv_nsec);
          }

          if (op & FUSE_SET_ATTR_MTIME_NOW)
          {
            md->set_mtime(tsnow.tv_sec);
            md->set_mtime_ns(tsnow.tv_nsec);
            md->set_ctime(tsnow.tv_sec);
            md->set_ctime_ns(tsnow.tv_nsec);
          }
        }
        EXEC_TIMING_END("setattr:utimes");
      }


      if (op & FUSE_SET_ATTR_SIZE)
      {
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

        if (!md->id() || (md->deleted() && !md->lookup_is()))
        {
          rc = ENOENT;
        }
        else
        {
          if ((md->mode() & S_IFDIR))
          {
            rc = EISDIR;
          }
          else
          {
            if (fi && fi->fh)
            {
              // ftruncate
              data::data_fh* io = (data::data_fh*) fi->fh;

              if (io)
              {
                eos_static_debug("ftruncate size=%lu", (size_t) attr->st_size);
                rc |= io->ioctx()->truncate(req, attr->st_size);
                rc |= io->ioctx()->flush(req);
              }
              else
              {
                rc = EIO;
              }
            }
            else
            {
              // truncate

              eos_static_debug("truncate size=%lu", (size_t) attr->st_size);
              std::string cookie=md->Cookie();
              data::shared_data io = Instance().datas.get(req, md->id(), md);
              rc = io->attach(req, cookie, true);
              eos_static_debug("calling truncate");
              rc |= io->truncate(req, attr->st_size);
              rc |= io->flush(req);
              rc |= io->detach(req, cookie, true);
              Instance().datas.release(req, md->id());
            }
            if (!rc)
            {
              ssize_t size_change = (int64_t) (attr->st_size) - (int64_t) md->size();
              if (size_change > 0)
                Instance().caps.book_volume(pcap, size_change);
              else
                Instance().caps.free_volume(pcap, size_change);
              md->set_size(attr->st_size);
            }
          }
        }
        EXEC_TIMING_END("setattr:truncate");
      }
    }
  }


  if (rc)
  {
    md->Locker().UnLock();
    fuse_reply_err (req, rc);
  }
  else
  {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof (e));
    md->convert(e);
    eos_static_info("%s", md->dump(e).c_str());
    Instance().mds.update(req, md, pcap->authid());
    md->Locker().UnLock();
    fuse_reply_attr (req, &e.attr, e.attr_timeout);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);

  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());
}

void
EosFuse::lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  int rc = 0;

  fuse_id id(req);

  struct fuse_entry_param e;
  memset(&e, 0, sizeof (e));
  {
    metad::shared_md md;
    md = Instance().mds.lookup(req, parent, name);

    if (md->id() && !md->deleted())
    {
      XrdSysMutexHelper mLock(md->Locker());
      md->set_pid(parent);
      md->convert(e);
      eos_static_info("%s", md->dump(e).c_str());
      md->lookup_inc();

      cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                     R_OK);
    }
    else
    {
      // negative cache entry
      e.ino = 0;
      e.attr_timeout = Instance().Config().options.md_kernelcache_enoent_timeout;
      e.entry_timeout = Instance().Config().options.md_kernelcache_enoent_timeout;
      if (e.entry_timeout)
        rc = 0;
      else
        rc = md->deleted()? ENOENT : md->err();
    }
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f name=%s %s", timing.RealTime(), name,
                    dump(id, parent, 0, rc).c_str());

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_entry(req, &e);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN(__func__);

  ADD_FUSE_STAT(__func__, req);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;

  fuse_id id(req);

  // retrieve cap

  cap::shared_cap pcap = Instance().caps.acquire(req, ino,
                                                 S_IFDIR | X_OK | R_OK, true);

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    // retrieve md
    metad::shared_md md = Instance().mds.get(req, ino, pcap->authid(), true);

    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      eos_static_info("%s", md->dump().c_str());
      auto md_fh = new opendir_t;
      md_fh->md = md;
      // fh contains a dummy 0 pointer
      eos_static_debug("adding ino=%08lx p-ino=%08lx", md->id(), md->pid());
      fi->fh = (unsigned long) md_fh;
    }
  }
  if (rc)
    fuse_reply_err(req, rc);
  else
    fuse_reply_open (req, fi);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
/*
EBADF  Invalid directory stream descriptor fi->fh
 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  EXEC_TIMING_BEGIN(__func__);

  ADD_FUSE_STAT(__func__, req);

  int rc = 0;

  fuse_id id(req);

  if (!fi->fh)
  {
    fuse_reply_err (req, EBADF);
    rc = EBADF;
  }
  else
  {
    // get the shared pointer from the open file descriptor
    opendir_t* md = (opendir_t*) fi->fh;
    metad::shared_md pmd = md->md;

    std::map<std::string, uint64_t> pmd_children;

    mode_t pmd_mode;
    uint64_t pmd_id;
    {
      // avoid to have more than one md object locked at a time
      XrdSysMutexHelper mLock(pmd->Locker());
      pmd_mode =pmd->mode();
      pmd_id = pmd->id();
      auto pmap = pmd->children();
      auto it = pmap.begin();
      for ( ; it != pmap.end(); ++it)
      {
	pmd_children[it->first] = it->second;
      }
      if (!pmd_children.size())
      {
	eos_static_debug("%s",Instance().mds.dump_md(pmd,false).c_str());
      }
    }
    // only one readdir at a time
    XrdSysMutexHelper lLock(md->items_lock);

    auto it = pmd_children.begin();

    eos_static_info("off=%lu size-%lu", off, pmd_children.size());

    char b[size];

    char* b_ptr = b;
    off_t b_size = 0;

    // the root directory adds only '.', all other add '.' and '..' for off=0
    if (off == 0)
    {
      // at offset=0 add the '.' directory
      std::string bname = ".";
      fuse_ino_t cino = pmd_id;

      eos_static_debug("list: %08x %s", cino, bname.c_str());
      mode_t mode = pmd_mode;

      struct stat stbuf;
      memset (&stbuf, 0, sizeof ( struct stat ));
      stbuf.st_ino = cino;
      stbuf.st_mode = mode;

      size_t a_size = fuse_add_direntry (req, b_ptr , size - b_size,
                                         bname.c_str(), &stbuf, ++off);

      eos_static_info("name=%s ino=%08lx mode=%08x bytes=%u/%u",
                      bname.c_str(), cino, mode, a_size, size - b_size);


      b_ptr += a_size;
      b_size += a_size;

      // at offset=0 add the '..' directory
      metad::shared_md ppmd = Instance().mds.get(req, pmd->pid(), "" , 0, 0, 0, true);
      if (ppmd && (ppmd->id() == pmd->pid()))
      {
        fuse_ino_t cino = 0;
	mode_t mode = 0;
	{
	  XrdSysMutexHelper ppLock(ppmd->Locker());
	  cino = ppmd->id();
	  mode = ppmd->mode();
	}
        std::string bname = "..";

        eos_static_debug("list: %08x %s", cino, bname.c_str());

        struct stat stbuf;
        memset (&stbuf, 0, sizeof ( struct stat ));
        stbuf.st_ino = cino;
        stbuf.st_mode = mode;

        size_t a_size = fuse_add_direntry (req, b_ptr , size - b_size,
                                           bname.c_str(), &stbuf, ++off);

        eos_static_info("name=%s ino=%08lx mode=%08x bytes=%u/%u",
                        bname.c_str(), cino, mode, a_size, size - b_size);

        b_ptr += a_size;
        b_size += a_size;
      }
    }

    // add regular children
    for ( ; it != pmd_children.end(); ++it)
    {
      // skip entries we have shown already
      if (md->readdir_items.count(it->first))
	continue;

      std::string bname = it->first;
      fuse_ino_t cino = it->second;

      metad::shared_md cmd = Instance().mds.get(req, cino, "" , 0, 0, 0, true);
      eos_static_debug("list: %08x %s (d=%d)", cino, it->first.c_str(),cmd->deleted());

      mode_t mode;
      {
	XrdSysMutexHelper cLock(cmd->Locker());
	mode = cmd->mode();
	// skip deleted entries or hidden entries
        if (cmd->deleted())
	  continue;
      }

      struct stat stbuf;
      memset (&stbuf, 0, sizeof ( struct stat ));
      stbuf.st_ino = cino;
      stbuf.st_mode = mode;

      size_t a_size = fuse_add_direntry (req, b_ptr , size - b_size,
                                         bname.c_str(), &stbuf, ++off);

      eos_static_info("name=%s ino=%08lx mode=%08x bytes=%u/%u",
                      bname.c_str(), cino, mode, a_size, size - b_size);

      if (a_size > (size - b_size))
        break;

      // add to the shown list
      md->readdir_items.insert(it->first);

      b_ptr += a_size;
      b_size += a_size;
    }

    if (b_size)
      fuse_reply_buf (req, b, b_size);
    else
      fuse_reply_err (req, 0);

    eos_static_debug("size=%lu off=%llu reply-size=%lu", size, off, b_size);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
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
  if (md)
  {
    // The following two lines act as a barrier to ensure the last readdir() has
    // released items_lock. From the point of view of the FUSE kernel module,
    // once we call fuse_reply_buf inside readdir, that syscall is over, and it
    // is free to call releasedir. This creates a race condition where we try to
    // delete md while readdir still holds items_lock - the following two lines
    // prevent this.

    md->items_lock.Lock();
    md->items_lock.UnLock();

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
  memset(&svfs, 0, sizeof (struct statvfs));

  rc = Instance().mds.statvfs(req, &svfs);

  if (rc)
    fuse_reply_err (req, rc);

  else
    fuse_reply_statfs (req, &svfs);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
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

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), parent, true);

  int rc = 0;

  fuse_id id(req);

  struct fuse_entry_param e;

  // do a parent check
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                 S_IFDIR | X_OK | W_OK, true);


  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    metad::shared_md md;
    metad::shared_md pmd;
    md = Instance().mds.lookup(req, parent, name);
    pmd = Instance().mds.get(req, parent, pcap->authid());

    {
      std::string implied_cid;


      XrdSysMutexHelper mLock(md->Locker());
      if (md->id() && !md->deleted())
      {
        rc = EEXIST;
      }
      else
      {
	if (md->deleted())
	{
	  // we need to wait that this entry is really gone
	  Instance().mds.wait_flush(req, md);
	}

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
        md->set_uid(pcap->uid());
        md->set_gid(pcap->gid());
        md->set_id(Instance().mds.insert(req, md, pcap->authid()));

        std::string imply_authid = eos::common::StringConversion::random_uuidstring();
        eos_static_info("generating implied authid %s => %s", pcap->authid().c_str(), imply_authid.c_str());

        implied_cid = Instance().caps.imply(pcap, imply_authid, mode, (fuse_ino_t) md->id());
        md->cap_inc();
        md->set_implied_authid(imply_authid);
      }

      if (!rc)
      {
        if (Instance().Config().options.mkdir_is_sync)
        {
          md->set_type(md->EXCL);
          rc = Instance().mds.add_sync(pmd, md, pcap->authid());
        }
        else
        {
          Instance().mds.add(pmd, md, pcap->authid());
        }
        if (!rc)
        {
          memset(&e, 0, sizeof (e));
          md->convert(e);
          md->lookup_inc();
          eos_static_info("%s", md->dump(e).c_str());
        }
        else
        {
          cap::Instance().forget(implied_cid);
        }
      }
    }
  }

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_entry(req, &e);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
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

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor pmon (__func__, Instance().Tracker(), parent, true);

  int rc = 0;

  fuse_id id(req);

  // retrieve cap

  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                 S_IFDIR | X_OK | D_OK, true);

  if (pcap->errc())
  {
    rc = pcap->errc();

  }
  else
  {
    std::string sname = name;
    uint64_t freesize = 0;
    if (sname == ".")
    {
      rc = EINVAL;
    }

    if (sname.length() > 1024)
    {
      rc = ENAMETOOLONG;
    }

    if (!rc)
    {
      metad::shared_md md;
      metad::shared_md pmd;

      md = Instance().mds.lookup(req, parent, name);

      XrdSysMutexHelper mLock(md->Locker());

      if (!md->id() || md->deleted())
      {
        rc = ENOENT;
      }

      if ((!rc) && ( (md->mode() & S_IFDIR)))
      {
        rc = EISDIR;
      }

      if (!rc)
      {
        freesize = md->size();
        pmd = Instance().mds.get(req, parent, pcap->authid());
        Instance().datas.unlink(req, md->id());
        Instance().mds.remove(pmd, md, pcap->authid());
      }
    }

    if (!rc)
    {
      XrdSysMutexHelper pLock(pcap->Locker());
      Instance().caps.free_volume(pcap, freesize);
      eos_static_debug("freeing %llu bytes on cap ", freesize);
    }
  }

  fuse_reply_err (req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rmdir(fuse_req_t req, fuse_ino_t parent, const char * name)
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

  Track::Monitor mon (__func__, Instance().Tracker(), parent);

  int rc = 0;

  fuse_id id(req);

  // retrieve cap

  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                 S_IFDIR | X_OK | D_OK, true);

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    std::string sname = name;

    if (sname == ".")
    {
      rc = EINVAL;
    }

    if (sname.length() > 1024)
    {
      rc = ENAMETOOLONG;
    }

    if (!rc)
    {
      metad::shared_md md;
      metad::shared_md pmd;

      md = Instance().mds.lookup(req, parent, name);

      XrdSysMutexHelper mLock(md->Locker());

      if (!md->id() || md->deleted())
      {
        rc = ENOENT;
      }

      if ((!rc) && (! (md->mode() & S_IFDIR)))
      {
        rc = ENOTDIR;
      }

      eos_static_info("link=%d", md->nlink());

      if ((!rc) && (md->children().size() || md->nchildren()))
      {
        rc = ENOTEMPTY;
      }

      if (!rc)
      {
        pmd = Instance().mds.get(req, parent, pcap->authid());
        Instance().mds.remove(pmd, md, pcap->authid());
      }
    }
  }

  fuse_reply_err (req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());
}

#ifdef _FUSE3
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                fuse_ino_t newparent, const char *newname, unsigned int flags)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                fuse_ino_t newparent, const char *newname)
/* -------------------------------------------------------------------------- */
#endif
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor monp (__func__, Instance().Tracker(), parent);
  Track::Monitor monn (__func__, Instance().Tracker(), newparent);


  int rc = 0;

  fuse_id id(req);

  // do a parent check
  cap::shared_cap p1cap = Instance().caps.acquire(req, parent,
                                                  S_IFDIR | R_OK, true);

  cap::shared_cap p2cap = Instance().caps.acquire(req, newparent,
                                                  S_IFDIR | W_OK, true);

  if (p1cap->errc())
  {
    rc = p1cap->errc();
  }
  if (!rc && p2cap->errc())
  {
    rc = p2cap->errc();
  }

  if (!rc)
  {
    metad::shared_md md;
    metad::shared_md p1md;
    metad::shared_md p2md;

    md = Instance().mds.lookup(req, parent, name);
    p1md = Instance().mds.get(req, parent, p1cap->authid());
    p2md = Instance().mds.get(req, newparent, p2cap->authid());

    uint64_t md_ino = 0;
    {
      XrdSysMutexHelper mLock(md->Locker());

      if (md->deleted())
      {
	// we need to wait that this entry is really gone
	Instance().mds.wait_flush(req, md);
      }

      if (!md->id() || md->deleted())
      {
	rc = md->deleted()? ENOENT : md->err();
      }
      else
      {
	md_ino = md->id();
      }
    }

    if (!rc)
    {
      Track::Monitor mone (__func__,Instance().Tracker(), md_ino, true);

      std::string new_name = newname;
      Instance().mds.mv (p1md, p2md, md, newname, p1cap->authid(), p2cap->authid());
    }
  }

  EXEC_TIMING_END(__func__);

  fuse_reply_err(req, rc);

  COMMONTIMING("_stop_", &timing);

  eos_static_notice("t(ms)=%.03f %s target-name=%s", timing.RealTime(),
		    dump(id, parent, 0, rc, name).c_str(), newname);
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

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;

  fuse_id id(req);

  metad::shared_md md = Instance().mds.get(req, ino);
  if (!md->id())
  {
    rc = md->deleted()? ENOENT : md->err();
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
EosFuse::open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("flags=%x", fi->flags);

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  int rc = 0;

  fuse_id id(req);

  int mode = R_OK;

  if (fi->flags & (O_RDWR | O_WRONLY) )
  {
    mode = W_OK;
  }


  {
    metad::shared_md md;
    md = Instance().mds.get(req, ino);

    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      // do a parent check
      cap::shared_cap pcap = Instance().caps.acquire(req, md->pid(),
                                                     S_IFDIR | mode);

      XrdSysMutexHelper mLock(pcap->Locker());
      if (pcap->errc())
      {
        rc = pcap->errc();
      }
      else
      {
        uint64_t pquota=0;

        if (mode == W_OK)
        {
          if (! (pquota = Instance().caps.has_quota(pcap, 1024 * 1024)))
          {
            rc = EDQUOT;
          }
        }
        if (!rc)
        {
          mLock.UnLock();

          struct fuse_entry_param e;
          memset(&e, 0, sizeof (e));
          md->convert(e);

          data::data_fh* io = data::data_fh::Instance(Instance().datas.get(req, md->id(), md), md, (mode == W_OK));
          pcap->Locker().Lock();
          io->set_authid(pcap->authid());

          if (pquota < pcap->max_file_size())
          {
            io->set_maxfilesize(pquota);
          }
          else
          {
            io->set_maxfilesize(pcap->max_file_size());
          }
          pcap->Locker().UnLock();

          // attach a datapool object
          fi->fh = (uint64_t) io;

          std::string cookie=md->Cookie();

          io->ioctx()->set_remote(Instance().Config().hostport,
                                  md->name(),
                                  md->md_ino(),
                                  md->md_pino(),
                                  req,
				  (mode == W_OK) );

          bool outdated = (io->ioctx()->attach(req, cookie, fi->flags ) == EKEYEXPIRED);

          fi->keep_cache = outdated ? 0 : Instance().Config().options.data_kernelcache;
          fi->direct_io = 0;
          eos_static_info("%s", md->dump(e).c_str());
        }
      }
    }
  }

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_open(req, fi);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);

  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, fi, rc).c_str());

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
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

  if (!S_ISREG(mode))
  {
    // we only implement files
    rc = ENOSYS;
  }
  else
    create(req, parent, name, mode, 0);

  if (rc)
    fuse_reply_err(req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);

  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc, name).c_str());

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::create(fuse_req_t req, fuse_ino_t parent, const char *name,
                mode_t mode, struct fuse_file_info * fi)
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
  COMMONTIMING("_start_", &timing);

  Track::Monitor mon (__func__, Instance().Tracker(), parent, true);

  if (fi)
  {
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

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    {
      if (!Instance().caps.has_quota(pcap, 1024 * 1024))
      {
        rc = EDQUOT;
      }
    }

    if (!rc)
    {
      metad::shared_md md;
      metad::shared_md pmd;
      md = Instance().mds.lookup(req, parent, name);
      pmd = Instance().mds.get(req, parent, pcap->authid());

      XrdSysMutexHelper mLock(md->Locker());

      if (md->id() && !md->deleted())
      {
        rc = EEXIST;
      }
      else
      {
	if (md->deleted())
	{
	  // we need to wait that this entry is really gone
	  Instance().mds.wait_flush(req, md);
	}

        md->set_mode(mode | S_IFREG);
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
        md->set_id(Instance().mds.insert(req, md, pcap->authid()));

        XrdSysMutexHelper mLockParent(pmd->Locker());
        pmd->set_mtime(ts.tv_sec);
        pmd->set_mtime_ns(ts.tv_nsec);
        mLockParent.UnLock();

        if ( (Instance().Config().options.create_is_sync) ||
            (fi && fi->flags & O_EXCL) )
        {
          md->set_type(md->EXCL);
          rc = Instance().mds.add_sync(pmd, md, pcap->authid());
        }
        else
        {
          Instance().mds.add(pmd, md, pcap->authid());
        }

        Instance().caps.book_inode(pcap);

        memset(&e, 0, sizeof (e));

        if (!rc)
        {
          md->convert(e);
          md->lookup_inc();

          if (fi)
          {
            // -----------------------------------------------------------------------
            // TODO: for the moment there is no kernel cache used until
            // we add top-level management on it
            // -----------------------------------------------------------------------
            // FUSE caches the file for reads on the same filedescriptor in the buffer
            // cache, but the pages are released once this filedescriptor is released.
            fi->keep_cache = Instance().Config().options.data_kernelcache;

            if ( (fi->flags & O_DIRECT) ||
                (fi->flags & O_SYNC) )
              fi->direct_io = 1;

            else
              fi->direct_io = 0;

            data::data_fh* io = data::data_fh::Instance(Instance().datas.get(req, md->id(), md), md, true);

            io->set_authid(pcap->authid());

            io->set_maxfilesize(pcap->max_file_size());

            // attach a datapool object
            fi->fh = (uint64_t) io;

            std::string cookie=md->Cookie();

            io->ioctx()->set_remote(Instance().Config().hostport,
                                    md->name(),
                                    md->md_ino(),
                                    md->md_pino(),
                                    req,
				    true);

            io->ioctx()->attach(req, cookie, fi->flags);
          }
        }
        eos_static_info("%s", md->dump(e).c_str());
      }
    }
  }

  if (rc)
    fuse_reply_err (req, rc);
  else
  {
    if (fi)
      // create
      fuse_reply_create(req, &e, fi);
    else
      // mknod
      fuse_reply_entry(req, &e);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  eos_static_debug("inode=%llu size=%li off=%llu",
                   (unsigned long long) ino, size, (unsigned long long) off);

  eos_static_debug("");

  fuse_id id(req);

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  data::data_fh* io = (data::data_fh*) fi->fh;
  ssize_t res=0;

  int rc = 0;

  if (io)
  {
    char* buf=0;
    if ( (res = io->ioctx()->peek_pread(req, buf, size, off)) == -1)
    {
      rc = EIO;
    }
    else
    {
      fuse_reply_buf (req, buf, res);
    }
    io->ioctx()->release_pread();
  }
  else
  {
    rc = ENXIO;
  }

  if (rc)
    fuse_reply_err (req, rc);

  eos_static_debug("t(ms)=%.03f %s", timing.RealTime(),
                   dump(id, ino, 0, rc).c_str());

  EXEC_TIMING_END(__func__);

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
               off_t off, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  eos_static_debug("inode=%lld size=%lld off=%lld buf=%lld",
                   (long long) ino, (long long) size,
                   (long long) off, (long long) buf);

  eos_static_debug("");

  fuse_id id(req);

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  data::data_fh* io = (data::data_fh*) fi->fh;

  int rc = 0;

  if (io)
  {
    eos_static_debug("max-file-size=%llu", io->maxfilesize());

    if ( (off + size) > io->maxfilesize() )
    {
      eos_static_err("io-error: maximum file size exceeded inode=%lld size=%lld off=%lld buf=%lld max-size=%llu",
                     ino, size, off, buf, io->maxfilesize());
      rc = EFBIG;
    }
    else
    {
      if (io->ioctx()->pwrite(req, buf, size, off) == -1)
      {
        eos_static_err("io-error: inode=%lld size=%lld off=%lld buf=%lld", ino, size, off, buf);
        rc = EIO;
      }
      else
      {
        {
          XrdSysMutexHelper mLock(io->mdctx()->Locker());
          io->mdctx()->set_size(io->ioctx()->size());
          io->set_update();
        }
        fuse_reply_write (req, size);
      }
    }
  }
  else
  {
    rc = ENXIO;
  }

  if (rc)
    fuse_reply_err (req, rc);

  eos_static_debug("t(ms)=%.03f %s", timing.RealTime(),
                   dump(id, ino, 0, rc).c_str());

  EXEC_TIMING_END(__func__);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  int rc = 0;

  fuse_id id(req);

  if (fi->fh)
  {

    data::data_fh* io = (data::data_fh*) fi->fh;
    std::string cookie="";
    io->ioctx()->detach(req, cookie, io->rw);
    delete io;
    Instance().datas.release(req, ino);
  }
  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);

  fuse_reply_err (req, rc);

  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
               struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("datasync=%d", datasync);

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;

  fuse_id id(req);


  data::data_fh* io = (data::data_fh*) fi->fh;

  if (io)
  {
    if (io->has_update())
    {
      if (Instance().Config().options.global_flush)
      {
        Instance().mds.begin_flush(io->md, io->authid()); // flag an ongoing flush centrally
      }

      struct timespec tsnow;
      eos::common::Timing::GetTimeSpec(tsnow);

      XrdSysMutexHelper mLock(io->md->Locker());
      io->md->set_mtime(tsnow.tv_sec);
      io->md->set_mtime_ns(tsnow.tv_nsec);
      Instance().mds.update(req, io->md, io->authid());

      // step 1 call flush
      rc = io->ioctx()->flush(req); // actually do the flush

      std::string cookie=io->md->Cookie();
      io->ioctx()->store_cookie(cookie);

      if (!rc)
      {
        // step 2 call sync - this currently flushed all open filedescriptors - should be ok
        rc = io->ioctx()->sync(); // actually wait for writes to be acknowledged
        if (rc)
          rc = errno;
      }
      else
      {
        rc = errno;
      }
      if (Instance().Config().options.global_flush)
      {
        //XrdSysTimer sleeper;
        //sleeper.Wait(5000);
        Instance().mds.end_flush(io->md, io->authid()); // unflag an ongoing flush centrally
      }
    }
  }


  fuse_reply_err (req, rc);


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

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  int rc = 0;

  fuse_id id(req);

  rc = Instance().mds.forget(req, ino, nlookup);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s nlookup=%d", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str(), nlookup);

  if (!rc)
    Instance().Tracker().forget(ino);

  fuse_reply_none(req);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  int rc = 0;

  fuse_id id(req);

  data::data_fh* io = (data::data_fh*) fi->fh;

  if (io)
  {
    if (io->has_update())
    {
      cap::shared_cap pcap = Instance().caps.acquire(req, io->md->pid(),
                                                     S_IFDIR | W_OK, true);
      XrdSysMutexHelper pLock(pcap->Locker());
      if (pcap->errc())
      {
        rc = pcap->errc();
      }
      else
      {
        {
          ssize_t size_change = (int64_t) io->md->size() - (int64_t) io->opensize();
          if (size_change > 0)
            Instance().caps.book_volume(pcap, size_change);
          else
            Instance().caps.free_volume(pcap, size_change);

          eos_static_debug("booking %ld bytes on cap ", size_change);
        }
        pLock.UnLock();

        struct timespec tsnow;
        eos::common::Timing::GetTimeSpec(tsnow);

        XrdSysMutexHelper mLock(io->md->Locker());
        io->md->set_mtime(tsnow.tv_sec);
        io->md->set_mtime_ns(tsnow.tv_nsec);
        Instance().mds.update(req, io->md, io->authid());

        // actually do the flush
        if (io->ioctx()->flush(req))
        {
          rc = EIO;
        }

        std::string cookie=io->md->Cookie();
        io->ioctx()->store_cookie(cookie);

        pcap->Locker().Lock();
        if (! Instance().caps.has_quota(pcap, 0) )
        {
          // we signal an error to the client if the quota get's exceeded although
          // we let the file be complete
          rc = EDQUOT;
        }
        pcap->Locker().UnLock();
      }
    }
  }


  fuse_reply_err (req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

#ifdef __APPLE__
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name,
                  size_t size, uint32_t position)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name,
                  size_t size)
/* -------------------------------------------------------------------------- */
#endif
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;

  fuse_id id(req);

  cap::shared_cap pcap;

  std::string key = xattr_name;
  std::string value;

  bool local_getxattr = false;

  // the root user has a bypass to be able to retrieve information in
  // realtime
  if (fuse_req_ctx(req)->uid == 0)
  {
    static std::string s_md = "system.eos.md";
    static std::string s_cap = "system.eos.cap";
    static std::string s_ls_caps= "system.eos.caps";
    static std::string s_ls_vmap= "system.eos.vmap";

    if (key.substr(0, s_md.length()) == s_md)
    {
      local_getxattr = true;
      metad::shared_md md;

      pcap = Instance().caps.get(req, ino);
      md = Instance().mds.get(req, ino, pcap->authid());
      {
        value = Instance().mds.dump_md(md);
      }
    }
    if (key.substr(0, s_cap.length()) == s_cap)
    {
      local_getxattr = true;
      pcap = Instance().caps.get(req, ino);
      {
        value = pcap->dump();
      }
    }
    if (key.substr(0, s_ls_caps.length()) == s_ls_caps)
    {
      local_getxattr = true;
      value = Instance().caps.ls();
    }
    if (key.substr(0, s_ls_vmap.length()) == s_ls_vmap)
    {
      local_getxattr = true;
      value = Instance().mds.vmaps().dump();
    }

    if ( (size) && (value.size() > size))
    {
      value.erase(size - 4);
      value += "...";
    }
  }

  if (!local_getxattr)
  {
    {
      metad::shared_md md;
      metad::shared_md pmd;

      static std::string s_sec = "security.";
      static std::string s_acl_a = "system.posix_acl_access";
      static std::string s_acl_d = "system.posix_acl_default";
      static std::string s_apple = "com.apple";

      // don't return any security attribute
      if (key.substr(0, s_sec.length()) == s_sec)
      {
        rc = ENOATTR;
      }
      else
      {
        // don't return any posix acl attribute
        if ( (key == s_acl_a) || (key == s_acl_d) )
        {
          rc = ENOATTR;
        }
#ifdef __APPLE__
        else
          // don't return any finder attribute
          if (key.substr(0, s_apple.length()) == s_apple)
        {
          rc = ENOATTR;
        }
#endif
      }

      if (!rc)
      {
        md = Instance().mds.get(req, ino);

        XrdSysMutexHelper mLock(md->Locker());

        if (!md->id() || md->deleted())
        {
	  rc = md->deleted()? ENOENT : md->err();
        }
        else
        {
          auto map = md->attr();
          if ( key.substr(0, 4) == "eos.")
          {
            if ( key == "eos.md_ino" )
            {
              std::string md_ino;
              value = eos::common::StringConversion::GetSizeString(md_ino,
                                                                   (unsigned long long) md->md_ino());
            }
            if ( key == "eos.btime" )
            {
              char btime[256];
              snprintf(btime, sizeof (btime), "%lu.%lu", md->btime(), md->btime_ns());
              value = btime;
              ;
            }
            if ( key == "eos.name")
            {
              value = Instance().Config().name;
            }
            if ( key == "eos.hostport")
            {
              value = Instance().Config().hostport;
            }
            if ( key == "eos.mgmurl")
            {
              std::string mgmurl = "root://";
              mgmurl += Instance().Config().hostport;
              value = mgmurl;
            }
            if ( key == "eos.quota")
            {
              pcap = Instance().caps.acquire(req, ino,
                                             R_OK);
              if (pcap->errc())
              {
                rc = pcap->errc();
              }
              else
              {
                cap::shared_quota q = Instance().caps.quota(pcap);
                XrdSysMutexHelper qLock(q->Locker());
                char qline[1024];
                snprintf(qline, sizeof (qline), "instance             uid     gid        vol-avail        ino-avail        max-fsize                         endpoint\n"
                         "%-16s %7u %7u %16lu %16lu %16lu %32s\n",
                         Instance().Config().name.c_str(),
                         pcap->uid(),
                         pcap->gid(),
                         q->volume_quota(),
                         q->inode_quota(),
                         pcap->max_file_size(),
                         Instance().Config().hostport.c_str());
                value = qline;
              }
            }
          }
          else
          {
            if (S_ISDIR(md->mode()))
            {
              // retrieve the appropriate cap of this inode
              pcap = Instance().caps.acquire(req, ino,
                                             R_OK);
            }
            else
            {
              // retrieve the appropriate cap of the parent inode
              pcap = Instance().caps.acquire(req, md->pid(), R_OK);

            }
            if (pcap->errc())
            {
              rc = pcap->errc();
            }
            else
            {
              if (!map.count(key))
              {
                rc = ENOATTR;
              }
              else
              {
                value = map[key];
              }
            }

            if (size != 0 )
            {
              if (value.size() > size)
              {
                rc = ERANGE;
              }
            }
          }
        }
      }
    }
  }


  if (rc)
    fuse_reply_err (req, rc);
  else
  {
    if (size == 0)
    {
      fuse_reply_xattr (req, value.size());
    }
    else
    {
      fuse_reply_buf (req, value.c_str(), value.size());
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
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name,
                  const char *xattr_value, size_t size, int flags,
                  uint32_t position)
/* -------------------------------------------------------------------------- */
#else

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name,
                  const char *xattr_value, size_t size, int flags)
/* -------------------------------------------------------------------------- */
#endif
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("key=%s", xattr_name);

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  int rc = 0;

  fuse_id id(req);

  cap::shared_cap pcap;

  std::string value;

  bool local_setxattr = false;

  std::string key = xattr_name;
  value.assign(xattr_value, size);

  // the root user has a bypass to be able to change th fuse configuration in
  // realtime
  if (fuse_req_ctx(req)->uid == 0)
  {
    static std::string s_debug = "system.eos.debug";
    static std::string s_dropcap = "system.eos.dropcap";
    static std::string s_dropallcap = "system.eos.dropallcap";

    if (key.substr(0, s_debug.length()) == s_debug)
    {
      local_setxattr = true;
      rc = EINVAL;
      // only root can do this configuration changes

#ifdef EOSCITRINE
      if (value == "notice")
      {
        eos::common::Logging::GetInstance().SetLogPriority(LOG_NOTICE);
        rc = 0;
      }
      if (value == "info")
      {
        eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO);
        rc = 0;
      }
      if (value == "debug")
      {
        eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
        rc = 0;
      }
#else
      if (value == "notice")
      {
        eos::common::Logging::SetLogPriority(LOG_NOTICE);
        rc = 0;
      }
      if (value == "info")
      {
        eos::common::Logging::SetLogPriority(LOG_INFO);
        rc = 0;
      }
      if (value == "debug")
      {
        eos::common::Logging::SetLogPriority(LOG_DEBUG);
        rc = 0;
      }
#endif
    }

    if (key.substr(0, s_dropcap.length()) == s_dropcap)
    {
      local_setxattr = true;
      cap::shared_cap pcap = Instance().caps.get(req, ino);
      if (pcap->id())
      {
        Instance().caps.forget(pcap->capid(req, ino));
      }
    }

    if (key.substr(0, s_dropallcap.length()) == s_dropallcap)
    {
      local_setxattr = true;
      Instance().caps.reset();
    }
  }

  if (!local_setxattr)
  {
    metad::shared_md md;

    md = Instance().mds.get(req, ino);

    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      // retrieve the appropriate cap
      if (S_ISDIR(md->mode()))
      {
        pcap = Instance().caps.acquire(req, ino,
                                       SA_OK);
      }
      else
      {
        pcap = Instance().caps.acquire(req, md->pid(),
                                       SA_OK);
      }

      if (pcap->errc())
      {
        rc = pcap->errc();
      }
      else
      {

        static std::string s_sec = "security.";
        static std::string s_acl = "system.posix_acl_access";
        static std::string s_apple = "com.apple";

        // ignore silently any security attribute
        if (key.substr(0, s_sec.length()) == s_sec)
        {
          rc = 0;
        }
        else
          // ignore silently any posix acl attribute
          if (key == s_acl)
        {
          rc = 0;
        }
#ifdef __APPLE__
        else
          // ignore silently any finder attribute
          if (key.substr(0, s_apple.length()) == s_apple)
        {
          rc = 0;
        }
#endif
        else
        {
          auto map = md->mutable_attr();

          bool exists=false;
          if ( (*map).count(key))
          {
            exists = true;
          }

          if (exists && (flags == XATTR_CREATE) )
          {
            rc = EEXIST;
          }
          else
            if ( !exists && (flags == XATTR_REPLACE) )
          {
            rc = ENOATTR;
          }
          else
          {
            (*map)[key] = value;
            Instance().mds.update(req, md, pcap->authid());
          }
        }
      }
    }
  }

  fuse_reply_err (req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());

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

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;

  fuse_id id(req);

  cap::shared_cap pcap;
  std::string attrlist;

  metad::shared_md md;

  md = Instance().mds.get(req, ino);

  // retrieve the appropriate cap
  if (S_ISDIR(md->mode()))
  {
    pcap = Instance().caps.acquire(req, ino,
                                   SA_OK, true);
  }
  else
  {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   SA_OK, true);
  }

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      auto map = md->attr();

      size_t attrlistsize=0;
      attrlist="";

      for ( auto it = map.begin(); it != map.end(); ++it)
      {
        attrlistsize += it->first.length() + 1;
        attrlist += it->first;
        attrlist += '\0';
      }

      if (size == 0 )
      {
        fuse_reply_xattr (req, attrlistsize);
      }
      else
      {
        if (attrlist.size() > size)
        {
          rc = ERANGE;
        }
      }
    }
  }

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_buf (req, attrlist.c_str(), attrlist.length());

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::removexattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name)
/* -------------------------------------------------------------------------- */
{

  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0 ;

  fuse_id id(req);

  cap::shared_cap pcap;


  metad::shared_md md;

  md = Instance().mds.get(req, ino);

  // retrieve the appropriate cap
  if (S_ISDIR(md->mode()))
  {
    pcap = Instance().caps.acquire(req, ino,
                                   SA_OK, true);
  }
  else
  {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   SA_OK, true);
  }

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = md->deleted()? ENOENT : md->err();
    }
    else
    {
      std::string key = xattr_name;

      static std::string s_sec = "security.";
      static std::string s_acl = "system.posix_acl";
      static std::string s_apple = "com.apple";

      // ignore silently any security attribute
      if (key.substr(0, s_sec.length()) == s_sec)
      {
        rc = 0;
      }
      else
        // ignore silently any posix acl attribute
        if (key == s_acl)
      {
        rc = 0;
      }
#ifdef __APPLE__
      else
        // ignore silently any finder attribute
        if (key.substr(0, s_apple.length()) == s_apple)
      {
        rc = 0;
      }
#endif
      else
      {
        auto map = md->mutable_attr();

        bool exists=false;
        if ( (*map).count(key))
        {
          exists = true;
        }

        if ( !exists )
        {
          rc = ENOATTR;
        }
        else
        {
          (*map).erase(key);
          Instance().mds.update(req, md, pcap->authid());
        }
      }
    }
  }

  fuse_reply_err (req, rc);

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

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  int rc = 0;
  std::string target;

  fuse_id id(req);

  cap::shared_cap pcap;

  metad::shared_md md;

  md = Instance().mds.get(req, ino);

  // retrieve the appropriate cap
  if (S_ISDIR(md->mode()))
  {
    pcap = Instance().caps.acquire(req, ino,
                                   SA_OK, true);
  }
  else
  {
    pcap = Instance().caps.acquire(req, md->pid(),
                                   SA_OK, true);
  }

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id() || md->deleted())
    {
      rc = ENOENT;
    }
    else
    {
      if (!(md->mode() & S_IFLNK))
      {
        // no a link
        rc = EINVAL;
      }
      else
      {
        target = md->target();
      }
    }
  }

  if (!rc)
  {
    fuse_reply_readlink (req, target.c_str());
    return;
  }
  else
  {
    fuse_reply_err (req, errno);
    return;
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                 const char *name)
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

  Track::Monitor mon (__func__, Instance().Tracker(), parent);

  int rc = 0;

  fuse_id id(req);

  struct fuse_entry_param e;

  // do a parent check
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                 S_IFDIR | W_OK, true);

  if (pcap->errc())
  {
    rc = pcap->errc();
  }
  else
  {
    metad::shared_md md;
    metad::shared_md pmd;
    md = Instance().mds.lookup(req, parent, name);
    pmd = Instance().mds.get(req, parent, pcap->authid());

    XrdSysMutexHelper mLock(md->Locker());

    if (md->id() && !md->deleted())
    {
      rc = EEXIST;
    }
    else
    {
      if (md->deleted())
      {
	// we need to wait that this entry is really gone
	Instance().mds.wait_flush(req, md);
      }

      md->set_mode( S_IRWXU | S_IRWXG | S_IRWXO | S_IFLNK );
      md->set_target(link);

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
      md->set_id(Instance().mds.insert(req, md, pcap->authid()));
      md->lookup_inc();

      if (Instance().Config().options.mkdir_is_sync)
      {
	md->set_type(md->EXCL);
	rc = Instance().mds.add_sync(pmd, md, pcap->authid());
      }
      else
      {
	Instance().mds.add(pmd, md, pcap->authid());
      }

      memset(&e, 0, sizeof (e));
      md->convert(e);
    }
  }

  if (rc)
  {
    fuse_reply_err (req, rc);
  }
  else
  {
    fuse_reply_entry (req, &e);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, parent, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::getlk(fuse_req_t req, fuse_ino_t ino,
               struct fuse_file_info *fi, struct flock * lock)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino);

  fuse_id id(req);

  int rc = 0;

  if (!Instance().Config().options.global_locking)
  {
    // use default local locking
    rc = EOPNOTSUPP;
  }
  else
  {
    // use global locking
    data::data_fh* io = (data::data_fh*) fi->fh;

    if (io)
    {
      rc = Instance().mds.getlk(req, io->mdctx(), lock);
    }
    else
    {
      rc = ENXIO;
    }
  }

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_lock(req, lock);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::setlk(fuse_req_t req, fuse_ino_t ino,
               struct fuse_file_info *fi,
               struct flock *lock, int sleep)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  ADD_FUSE_STAT(__func__, req);

  EXEC_TIMING_BEGIN(__func__);

  Track::Monitor mon (__func__, Instance().Tracker(), ino, true);

  fuse_id id(req);

  int rc = 0;

  if (!Instance().Config().options.global_locking)
  {
    // use default local locking
    rc = EOPNOTSUPP;
  }
  else
  {
    // use global locking
    data::data_fh* io = (data::data_fh*) fi->fh;

    if (io)
    {
      size_t w_ms=10;
      do
      {
        // we currently implement the polling lock on client side due to the
        // thread-per-link model of XRootD
        rc = Instance().mds.setlk(req, io->mdctx(), lock, sleep);
        if (rc && sleep)
        {
          XrdSysTimer sleeper;
          sleeper.Wait(w_ms);
          // do exponential back-off with a hard limit at 1s
          w_ms *= 2;
          if (w_ms > 1000)
          {
            w_ms = 1000;
          }
          continue;
        }
        break;
      }
      while (rc);
    }
    else
    {
      rc = ENXIO;
    }
  }

  fuse_reply_err (req, rc);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(id, ino, 0, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
EosFuse::getHbStat(eos::fusex::statistics& hbs)
/* -------------------------------------------------------------------------- */
{
  eos::common::LinuxMemConsumption::linux_mem_t mem;
  eos::common::LinuxStat::linux_stat_t osstat;

  if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem))
  {
    eos_static_err("failed to get the MEM usage information");
  }

  if (!eos::common::LinuxStat::GetStat(osstat))
  {
    eos_static_err("failed to get the OS usage information");
  }

  hbs.set_inodes(EosFuse::Instance().getMdStat().inodes());
  hbs.set_inodes_todelete(EosFuse::Instance().getMdStat().inodes_deleted());
  hbs.set_inodes_backlog(EosFuse::Instance().getMdStat().inodes_backlog());
  hbs.set_inodes_ever(EosFuse::Instance().getMdStat().inodes_ever());
  hbs.set_inodes_ever_deleted(EosFuse::Instance().getMdStat().inodes_deleted_ever());
  hbs.set_threads(osstat.threads);
  hbs.set_vsize_mb(osstat.vsize);
  hbs.set_rss_mb(osstat.rss);
}
