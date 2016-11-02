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

#include "eosfuse.hh"
#include "MacOSXHelper.hh"

#include <string>
#include <map>
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


#include <sys/types.h>
#ifdef __APPLE__
#include <sys/xattr.h>
#else
#include <attr/xattr.h>
#endif

#include <json/json.h>

#include "common/Timing.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "md.hh"
#include "kv.hh"

#define _FILE_OFFSET_BITS 64 

EosFuse* EosFuse::sEosFuse = 0;

/* -------------------------------------------------------------------------- */
EosFuse::EosFuse()
{
  sEosFuse = this;
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

  struct fuse_chan* ch;
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
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
                             epos - npos : option.length() - npos);
      break;
    }
  }

  fprintf(stderr, "# fsname='%s'\n", fsname.c_str());

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
              jsonconfig.c_str(), reader.getFormatedErrorMessages().c_str());
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
    config.mdcachehost = root["mdcachehost"].asString();
    config.mdcacheport = root["mdcacheport"].asInt();
    if (!config.statfilesuffix.length())
    {
      config.statfilesuffix="stats";
    }
  }

  int debug;
  if ((fuse_parse_cmdline(&args, &local_mount_dir, NULL, &debug) != -1) &&
      ((ch = fuse_mount(local_mount_dir, &args)) != NULL) &&
      (fuse_daemonize(0) != -1))
  {
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
      if (getenv("EOS_FUSE_LOG_PREFIX"))
      {
        log_path += getenv("EOS_FUSE_LOG_PREFIX");
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
        ::chmod(cPath.GetPath(), S_IRUSR | S_IWUSR);
    }


    setvbuf(fstderr, (char*) NULL, _IONBF, 0);

    eos::common::Logging::Init ();
    eos::common::Logging::SetUnit ("FUSE@eosxd");
    eos::common::Logging::gShortFormat = true;

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

    if (config.mdcachehost.length())
    {
      if (mKV.connect(config.mdcachehost, config.mdcacheport ?
                      config.mdcacheport : 6379))
      {
        fprintf(stderr, "error: failed to connect to md cache - connect-string=%s",
                config.mdcachehost.c_str());
        exit(EINVAL);
      }
    }

    mds.init();

    fusestat.Add("getattr", 0, 0, 0);
    fusestat.Add("setattr", 0, 0, 0);
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

    tDumpStatistic = std::thread(EosFuse::DumpStatistic);
    tDumpStatistic.detach();
    tStatCirculate = std::thread(EosFuse::StatCirculate);
    tStatCirculate.detach();
    tMetaCacheFlush = std::thread(&metad::mdcflush, &mds);
    tMetaCacheFlush.detach();

    eos_static_warning("********************************************************************************");
    eos_static_warning("eosdx started version %s - FUSE protocol version %d", VERSION, FUSE_USE_VERSION);
    eos_static_warning("eos-instance-url       := %s", config.hostport.c_str());

    struct fuse_session* se;
    se = fuse_lowlevel_new(&args,
                           &(get_operations()),
                           sizeof (operations), NULL);

    if ((se != NULL))
    {
      if (fuse_set_signal_handlers(se) != -1)
      {
        fuse_session_add_chan(se, ch);

        if (getenv("EOS_FUSE_NO_MT") &&
            (!strcmp(getenv("EOS_FUSE_NO_MT"), "1")))
        {
          err = fuse_session_loop(se);
        }
        else
        {
          err = fuse_session_loop_mt(se);
        }

        fuse_remove_signal_handlers(se);
        fuse_session_remove_chan(ch);
      }
      fuse_session_destroy(se);
    }

    fuse_unmount(local_mount_dir, ch);
  }
  return err ? 1 : 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::init(void *userdata, struct fuse_conn_info *conn)
/* -------------------------------------------------------------------------- */
{

  eos_static_debug("");
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
  while (1)
  {
    eos_static_debug("dumping statistics");
    XrdOucString out;
    EosFuse::Instance().getFuseStat().PrintOutTotal(out);
    std::string sout = out.c_str();
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

  EXEC_TIMING_BEGIN("getattr");

  int rc = 0;

  metad::shared_md md = Instance().mds.get(req, ino);
  if (!md->id())
  {
    rc = ENOENT;
    fuse_reply_err(req, ENOENT);
  }
  else
  {
    struct fuse_entry_param e;
    md->convert(e);
    eos_static_info("%s", md->dump(e).c_str());
    fuse_reply_attr (req, &e.attr, e.attr_timeout);
  }

  EXEC_TIMING_END("getattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(req, ino, fi, rc).c_str());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set,
                 struct fuse_file_info *fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("setattr");

  EXEC_TIMING_END("setattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  struct fuse_entry_param e;
  memset(&e, 0, sizeof (e));

  EXEC_TIMING_BEGIN("lookup");

  {
    metad::shared_md md;
    md = Instance().mds.lookup(req, parent, name);

    if (md->id())
    {
      md->convert(e);
      eos_static_info("%s", md->dump(e).c_str());
    }
    else
    {
      // negative cache entry
      e.ino = 0;
      e.attr_timeout = 0;
      e.entry_timeout = 0;
    }
  }

  EXEC_TIMING_END("lookup");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());

  fuse_reply_entry(req, &e);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN(__func__);

  ADD_FUSE_STAT(__func__, req);

  // retrieve cap

  // retrieve md
  metad::shared_md md = Instance().mds.get(req, ino);

  if (!md->id())
  {
    fuse_reply_err(req, ENOENT);
  }
  else
  {
    eos_static_info("%s", md->dump().c_str());

    // copy the current state
    eos::fusex::md* fh_md = new eos::fusex::md(*md);
    fi->fh = (unsigned long) fh_md;
    fuse_reply_open (req, fi);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info *fi)
/* -------------------------------------------------------------------------- */
/*
    EBADF  Invalid directory stream descriptor fi->fh
 */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  EXEC_TIMING_BEGIN(__func__);

  ADD_FUSE_STAT(__func__, req);

  if (!fi->fh)
  {
    fuse_reply_err (req, EBADF);
  }
  else
  {
    eos::fusex::md* md = (eos::fusex::md*) fi->fh;
    auto map = md->children();


    struct stat st;

    auto it = map.begin();

    eos_static_info("off=%lu size=%lu", off, map.size());

    if ((size_t) off < map.size())
      std::advance(it, off);
    else
      it = map.end();

    char b[size];

    char* b_ptr = b;
    off_t b_size = 0;

    for ( ; it != map.end(); ++it)
    {
      std::string bname = it->first;
      fuse_ino_t cino = it->second;

      metad::shared_md cmd = Instance().mds.get(req, cino);

      mode_t mode = cmd->mode();

      mode = DT_DIR << 12;

      struct stat stbuf;
      memset (&stbuf, 0, sizeof ( struct stat ));
      stbuf.st_ino = cino;
      stbuf.st_mode = mode;


      stbuf.st_mode = S_IFREG;

      size_t a_size = fuse_add_direntry (req, b_ptr , size - b_size,
                                         bname.c_str(), &st, ++off);

      eos_static_info("name=%s ino=%08lx mode=%08lx bytes=%u/%u",
                      bname.c_str(), cino, mode, a_size, size - b_size);

      if (a_size > (size - b_size))
        break;

      b_ptr += a_size;
      b_size += a_size;
    }

    fuse_reply_buf (req, b_size ? b : 0, b_size);

    eos_static_debug("size=%lu off=%llu reply-size=%lu", size, off, b_size);
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN(__func__);

  ADD_FUSE_STAT(__func__, req);

  eos::fusex::md* md = (eos::fusex::md*) fi->fh;
  if (md)
  {
    delete md;
    fi->fh = 0;
  }

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  int rc=0;
  struct statvfs svfs;
  svfs.f_bsize = 128 * 1024;
  svfs.f_blocks = 1000000000ll;
  svfs.f_bfree = 1000000000ll;
  svfs.f_bavail = 1000000000ll;
  svfs.f_files = 1000000;
  svfs.f_ffree = 1000000;

  if (rc)
    fuse_reply_err (req, rc);
  else
    fuse_reply_statfs (req, &svfs);

  EXEC_TIMING_END(__func__);

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("t(ms)=%.03f %s", timing.RealTime(),
                    dump(req, ino, 0, rc).c_str());

  return;
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

  EXEC_TIMING_BEGIN("mkdir");

  int rc = 0;

  // do a parent check
  cap::shared_cap pcap = Instance().caps.acquire(req, parent,
                                                 S_IFDIR | X_OK | W_OK);


  if (pcap->errc())
  {
    rc = pcap->errc();
    fuse_reply_err (req, rc);
  }
  else
  {
    metad::shared_md md;
    metad::shared_md pmd;
    md = Instance().mds.lookup(req, parent, name);
    pmd = Instance().mds.get(req, parent);


    if (md->id())
    {
      rc = EEXIST;
      fuse_reply_err (req, rc);
    }
    else
    {
      md->set_mode(mode | S_IFDIR);
      struct timespec ts;
      eos::common::Timing::GetTimeSpec(ts);
      md->set_name(name);
      md->set_atime(ts.tv_sec);
      md->set_atime_ns(ts.tv_nsec);
      md->set_mtime(ts.tv_sec);
      md->set_mtime_ns(ts.tv_sec);
      md->set_ctime(ts.tv_sec);
      md->set_ctime_ns(ts.tv_nsec);
      md->set_btime(ts.tv_sec);
      md->set_btime_ns(ts.tv_nsec);
      md->set_uid(pcap->uid());
      md->set_gid(pcap->gid());
      md->set_id(Instance().mds.insert(req, md));

      Instance().mds.add(pmd, md);

      struct fuse_entry_param e;
      md->convert(e);
      fuse_reply_entry(req, &e);
      eos_static_info("%s", md->dump(e).c_str());
    }
  }

  EXEC_TIMING_END("mkdir");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("unlink");

  EXEC_TIMING_END("unlink");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("rmdir");

  EXEC_TIMING_END("rmdir");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("rename");

  EXEC_TIMING_END("rename");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  fuse_reply_err(req, 0);

  EXEC_TIMING_END("access");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("open");

  EXEC_TIMING_END("open");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("mknod");

  EXEC_TIMING_END("mknod");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::create(fuse_req_t req, fuse_ino_t parent, const char *name,
                mode_t mode, struct fuse_file_info *fi)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("create");

  EXEC_TIMING_END("create");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("inode=%llu size=%li off=%llu",
                   (unsigned long long) ino, size, (unsigned long long) off);

  EXEC_TIMING_BEGIN("read");

  EXEC_TIMING_END("read");

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
               off_t off, struct fuse_file_info * fi)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("inode=%lld size=%lld off=%lld buf=%lld",
                   (long long) ino, (long long) size,
                   (long long) off, (long long) buf);

  EXEC_TIMING_BEGIN("write");

  EXEC_TIMING_END("write");
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

  EXEC_TIMING_BEGIN("release");

  EXEC_TIMING_END("release");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  eos_static_debug("");

  EXEC_TIMING_BEGIN("fsync");

  EXEC_TIMING_END("fsync");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("forget");

  EXEC_TIMING_END("forget");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("flush");

  EXEC_TIMING_END("flush");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f errno=%d", __FUNCTION__, timing.RealTime(), errno);
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

  EXEC_TIMING_BEGIN("getxattr");

  fuse_reply_err (req, ENOATTR);

  EXEC_TIMING_END("getxattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

#ifdef __APPLE__
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name,
                  const char *xattr_value, size_t size, int flags, uint32_t position)
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

  eos_static_debug("");

  EXEC_TIMING_BEGIN("setxattr");

  EXEC_TIMING_END("setxattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
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

  EXEC_TIMING_BEGIN("listxattr");

  EXEC_TIMING_END("listxattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::removexattr(fuse_req_t req, fuse_ino_t ino, const char *xattr_name)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("removexattr");

  EXEC_TIMING_END("removexattr");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
/* -------------------------------------------------------------------------- */
EosFuse::readlink(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("readlink");

  EXEC_TIMING_END("readlink");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void/* -------------------------------------------------------------------------- */
EosFuse::symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                 const char *name)
/* -------------------------------------------------------------------------- */
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);

  eos_static_debug("");

  EXEC_TIMING_BEGIN("symlink");

  EXEC_TIMING_END("symlink");

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}
