//------------------------------------------------------------------------------
//! @file eosfuse.hh
//! @author Andreas-Joachim Peters, Geoffray Adde, Elvin Sindrilaru CERN
//! @brief EOS C++ Fuse low-level implementation 
//------------------------------------------------------------------------------

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

#ifndef FUSE_EOSFUSE_HH_
#define FUSE_EOSFUSE_HH_

#include "llfusexx.hh"
#include "filesystem.hh"
#include <string>

class EosFuse : public llfusexx::FuseBase<EosFuse>
{
public:

  static EosFuse&
  instance ()
  {
    static EosFuse i;
    return i;
  }

  EosFuse ();
  virtual ~EosFuse ();

  int run( int argc, char* argv[], void *userdata );
   
  static void init (void *userdata, struct fuse_conn_info *conn);

  static void destroy (void *userdata);

  static void getattr (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void setattr (fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);

  static void
  lookup (fuse_req_t req, fuse_ino_t parent, const char *name);

  static void dirbuf_add (fuse_req_t req, struct dirbuf *b, const char *name, fuse_ino_t ino, const struct stat *s);

  static int reply_buf_limited (fuse_req_t req, const char *buf, size_t bufsize, off_t off, size_t maxsize);

  static void opendir (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void readdir (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);

  static void releasedir (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void statfs (fuse_req_t req, fuse_ino_t ino);

  static void mknod (fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);

  static void mkdir (fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);

  static void rm (fuse_req_t req, fuse_ino_t parent, const char *name);

  static void unlink (fuse_req_t req, fuse_ino_t parent, const char *name);

  static void rmdir (fuse_req_t req, fuse_ino_t parent, const char *name);

#ifdef _FUSE3
  static void rename (fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);
#else
  static void rename (fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
#endif

  static void access (fuse_req_t req, fuse_ino_t ino, int mask);

  static void open (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

  static void create (fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);

  static void read (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info * fi);

  static void write (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info * fi);

  static void release (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

  static void fsync (fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info * fi);

  static void forget (fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);

  static void flush (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

#ifdef __APPLE__
  static void getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position);
#else
  static void getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
#endif

#ifdef __APPLE__
  static void setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position);
#else
  static void setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags);
#endif

  static void listxattr (fuse_req_t req, fuse_ino_t ino, size_t size);

  static void removexattr (fuse_req_t req, fuse_ino_t ino, const char *name);

  static void readlink (fuse_req_t req, fuse_ino_t ino);

  static void
  symlink (fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);

  static inline bool checkpathname(const char* pathname)
  {
    if(instance().config.encode_pathname)
      return true;
    return fuse_filesystem::checkpathname(pathname);
  }
  
  fuse_filesystem&
  fs() { return fsys;}
  
private:

  fuse_filesystem fsys;

  typedef struct cfg
  {
    int isdebug;
    int foreground;
    double entrycachetime;
    double attrcachetime;
    double neg_entrycachetime;
    double readopentime;
    double cap_creator_lifetime;
    bool kernel_cache;
    bool direct_io;
    bool no_access;
    bool encode_pathname;
    bool lazy_open;
    bool is_sync;
    bool inline_repair;
    std::string mount_point;
    std::string mounthostport; ///< mount hostport of the form: hostname:port
    std::string mountprefix; ///< mount prefix of the form: dir1/dir2/dir3
  } cfg_t;

  cfg_t config;
};

#endif /* FUSE_EOSFUSE_HH_ */
