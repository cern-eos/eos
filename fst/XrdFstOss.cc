//------------------------------------------------------------------------------
// File XrdFstOss.cc
// Author Elvin-Alin Sindrilaru - CERN
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

/*----------------------------------------------------------------------------*/
#include <fcntl.h>
#include <strings.h>
#include <utime.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/resource.h>
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdOuc/XrdOuca2x.hh"
#include "XrdSys/XrdSysPlatform.hh"
/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOss.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/

extern XrdSysError OssEroute;

// Set the version information
XrdVERSIONINFO(XrdOssGetStorageSystem, FstOss);

extern "C"
{
  XrdOss*
  XrdOssGetStorageSystem (XrdOss* native_oss,
                          XrdSysLogger* Logger,
                          const char* config_fn,
                          const char* parms)
  {
    OssEroute.SetPrefix("FstOss_");
    OssEroute.logger(Logger);
    eos::fst::XrdFstOss* fstOss = new eos::fst::XrdFstOss();
    return (fstOss->Init(Logger, config_fn) ? 0 : (XrdOss*) fstOss);
  }
}

EOSFSTNAMESPACE_BEGIN

#define XrdFstOssFDMINLIM  64

// pointer to the current OSS implementation to be used by the oss files
XrdFstOss* XrdFstSS = 0;


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFstOss::XrdFstOss () :
eos::common::LogId (),
mFdFence (-1),
mFdLimit (-1),
mPrBytes(0),
mPrActive(0),
mPrDepth(0),
mPrQSize(0)
{
  eos_debug("Calling the constructor of XrdFstOss.");
  mPrPBits = (long long)sysconf(_SC_PAGESIZE);
  mPrPSize = static_cast<int>(mPrPBits);
  mPrPBits--;
  mPrPMask = ~mPrPBits;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOss::~XrdFstOss()
{
  // empty 
}


//------------------------------------------------------------------------------
// Init function
//------------------------------------------------------------------------------
int
XrdFstOss::Init (XrdSysLogger* lp, const char* configfn)
{
  int NoGo = 0;
  XrdFstSS = this;

  // Set logging parameters
  XrdOucString unit = "fstoss@";
  unit += "localhost";
  
  // Setup the circular in-memory log buffer
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority(LOG_DEBUG);
  eos::common::Logging::SetUnit(unit.c_str());
  eos_debug("info=\"oss logging configured\"");

  // Process the configuration file
  OssEroute.logger(lp);
  NoGo = Configure(configfn, OssEroute);
  
  // Establish the FD limit
  struct rlimit rlim;

  if (getrlimit(RLIMIT_NOFILE, &rlim) < 0)
  {
    eos_warning("can not get resource limits, errno=", errno);
    mFdLimit = XrdFstOssFDMINLIM;
  }
  else
  {
    mFdLimit = rlim.rlim_cur;
  }

  if (mFdFence < 0 || mFdFence >= mFdLimit)
  {
    mFdFence = mFdLimit >> 1;
  }

  return NoGo;
}


//------------------------------------------------------------------------------
// Configuration function for the oss plugin
//------------------------------------------------------------------------------
int
XrdFstOss::Configure(const char* configfn, XrdSysError& Eroute)
{
  char *var;
  int cfgFD;
  int NoGo = 0;
  XrdOucEnv myEnv;
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"), &myEnv, "=====> ");

  // If there is no config file, return with the defaults sets
  if( !configfn || !*configfn)
  {
    Eroute.Say("Config warning: config file not specified; defaults assumed.");
    return NoGo;
  }
  
  // Try to open the configuration file
  if ( (cfgFD = open(configfn, O_RDONLY, 0)) < 0)
  {
    Eroute.Emsg("Config", errno, "open config file", configfn);
    return 1;
  }

  Config.Attach(cfgFD);

  // Now start reading records until eof
  while((var = Config.GetMyFirstWord()))
  {
    if (!strncmp(var, "oss.", 4))
    {
      if (!strncmp(var+4, "preread", 7))
      {
        NoGo = xprerd(Config, Eroute);
      }
    }
  }


  eos_info("preread depth=%i, queue_size=%i and bytes=%i",
           mPrDepth, mPrQSize, mPrBytes);
  
  Config.Close();
  return NoGo;
}


//------------------------------------------------------------------------------
// Function xprerd to parse the preread directive
//------------------------------------------------------------------------------
int
XrdFstOss::xprerd(XrdOucStream &Config, XrdSysError &Eroute)
{
  static const long long m16 = 16777216LL;
  char *val;
  long long lim = 1048576;
  int depth, qeq = 0, qsz = 128;
  
  if (!(val = Config.GetWord()))
  {
    Eroute.Emsg("Config", "preread depth not specified");
    return 1;
  }
  
  if (!strcmp(val, "on")) 
  {
    depth = 3;
  }
  else if (XrdOuca2x::a2i(Eroute, "preread depth", val, &depth, 0, 1024))
  {
    return 1;
  }
  
  while((val = Config.GetWord()))
  {
    if (!strcmp(val, "limit"))
    {
      if (!(val = Config.GetWord()))
      {
        Eroute.Emsg("Config", "preread limit not specified");
        return 1;
      }

      if (XrdOuca2x::a2sz(Eroute, "preread limit", val, &lim, 0, m16))
        return 1;
    }
    else if (!strcmp(val, "qsize"))
    {
      if (!(val = Config.GetWord()))
      {
        Eroute.Emsg("Config", "preread qsize not specified");
        return 1;
      }

      if (XrdOuca2x::a2i(Eroute, "preread qsize", val, &qsz, 0, 1024))
        return 1;
      
      if (qsz < depth)
      {
        Eroute.Emsg("Config","preread qsize must be >= depth");
        return 1;
      }
    }
    else
    {
      Eroute.Emsg("Config","invalid preread option -",val);
      return 1;
    }
  }
  
  if (lim < mPrPSize || !qsz)
  {
    depth = 0;
  }
  
  if (!qeq && depth)
  {
    qsz = qsz / (depth / 2 + 1);
    
    if (qsz < depth)
      qsz = depth;
  }
  
  mPrDepth = depth;
  mPrQSize = qsz;
  mPrBytes = lim;
  return 0;
}


//------------------------------------------------------------------------------
// New file
//------------------------------------------------------------------------------
XrdOssDF*
XrdFstOss::newFile (const char* tident)
{
  eos_debug("Calling XrdFstOss::newFile. ");
  return ( XrdOssDF*) new XrdFstOssFile(tident);
}


//------------------------------------------------------------------------------
// New directory
//------------------------------------------------------------------------------
XrdOssDF*
XrdFstOss::newDir (const char* tident)
{
  eos_debug("Calling XrdFstOss::newDir - not used in EOS.");
  return NULL;
}


//------------------------------------------------------------------------------
// Unlink file and its block checksum if needed
//------------------------------------------------------------------------------
int
XrdFstOss::Unlink (const char* path, int opts, XrdOucEnv* ep)
{
  int retc = 0;
  struct stat statinfo;
  //............................................................................
  // Unlink the block checksum files - this is not the 'best' solution,
  // but we don't have any info about block checksums
  //............................................................................
  Adler xs; // the type does not matter here
  const char* xs_path = xs.MakeBlockXSPath(path);

   //............................................................................
  // Delete also any entries in the oss file <-> blockxs map
  //............................................................................
  DropXs(path, true);
  
  if ((Stat(xs_path, &statinfo)))
  {
    eos_err("error=cannot stat closed file - probably already unlinked: %s",
            xs_path);
  }
  else
  {
    if (!xs.UnlinkXSPath())
    {
      eos_debug("info=\"removed block-xs\" path=%s.", path);
    }
  }

  //............................................................................
  // Unlink the file
  //............................................................................
  int i;
  char local_path[MAXPATHLEN + 1 + 8];
  strcpy(local_path, path);

  if (lstat(local_path, &statinfo))
  {
    retc = (errno == ENOENT ? 0 : -errno);
  }
  else if ((statinfo.st_mode & S_IFMT) == S_IFLNK)
  {
    retc = BreakLink(local_path, statinfo);
  }
  else if ((statinfo.st_mode & S_IFMT) == S_IFDIR)
  {
    i = strlen(local_path);

    if (local_path[i - 1] != '/') strcpy(local_path + i, "/");

    if ((retc = rmdir(local_path))) retc = -errno;

    return retc;
  }

  if (!retc)
  {
    if (unlink(local_path)) retc = -errno;
    else retc = XrdOssOK;
  }

  return retc;
}


//------------------------------------------------------------------------------
// Delete a link file
//------------------------------------------------------------------------------
int
XrdFstOss::BreakLink (const char* local_path, struct stat& statbuff)
{
  char lnkbuff[MAXPATHLEN + 64];
  int lnklen, retc = XrdOssOK;

  //............................................................................
  // Read the contents of the link
  //............................................................................
  if ((lnklen = readlink(local_path, lnkbuff, sizeof ( lnkbuff) - 1)) < 0)
    return -errno;

  //............................................................................
  // Return the actual stat information on the target (which might not exist
  //............................................................................
  lnkbuff[lnklen] = '\0';

  if (stat(lnkbuff, &statbuff)) statbuff.st_size = 0;
  else if (unlink(lnkbuff) && errno != ENOENT)
  {
    retc = -errno;
    OssEroute.Emsg("BreakLink", retc, "unlink symlink target", lnkbuff);
  }

  return retc;
}


//--------------------------------------------------------------------------
// Chmod on a file
//--------------------------------------------------------------------------
int
XrdFstOss::Chmod (const char* path, mode_t mode, XrdOucEnv* eP)
{
  return ( chmod(path, mode) ? -errno : XrdOssOK);
}


//--------------------------------------------------------------------------
// Create a file named 'path' with 'mode' access mode bits set
//--------------------------------------------------------------------------
int
XrdFstOss::Create (const char* tident,
                   const char* path,
                   mode_t mode,
                   XrdOucEnv& env,
                   int opts)
{
  int retc = 0;
  int datfd;
  int is_link = 0;
  int missing = 1;
  char local_path[MAXPATHLEN + 1], *p, pc;
  struct stat buf;
  const int AMode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH; // 775

  if (strlen(path) >= MAXPATHLEN) return -ENAMETOOLONG;

  strcpy(local_path, path);

  //............................................................................
  // Determine the state of the file. We will need this information as we go on
  //............................................................................
  if ((missing = lstat(path, &buf)))
  {
    retc = errno;
  }
  else
  {
    if ((is_link = ((buf.st_mode & S_IFMT) == S_IFLNK)))
    {
      if (stat(path, &buf))
      {
        if (errno != ENOENT)
        {
          return -errno;
        }

        OssEroute.Emsg("Create", "removing dangling link", path);

        if (unlink(path))
        {
          retc = errno;
        }

        missing = 1;
        is_link = 0;
      }
    }
  }

  if (retc && (retc != ENOENT)) return -retc;

  //............................................................................
  // The file must not exist if it's declared "new". Otherwise, reuse the space
  //............................................................................
  if (!missing)
  {
    if (opts & XRDOSS_new) return -EEXIST;

    if ((buf.st_mode & S_IFMT) == S_IFDIR) return -EISDIR;

    do
    {
      datfd = open(local_path, opts >> 8, mode);
    }
    while (datfd < 0 && errno == EINTR);

    if (datfd < 0) return -errno;
    else close(datfd);

    if (opts >> 8 & O_TRUNC && buf.st_size && is_link)
    {
      buf.st_mode = (buf.st_mode & ~S_IFMT) | S_IFLNK;
    }

    return XrdOssOK;
  }

  //............................................................................
  // If the path is to be created, make sure the path exists at this point
  //............................................................................
  if ((opts & XRDOSS_mkpath) && (p = rindex(local_path, '/')))
  {
    p++;
    pc = *p;
    *p = '\0';
    XrdOucUtils::makePath(local_path, AMode);
    *p = pc;
  }

  //............................................................................
  // Simply open the file in the local filesystem, creating it if need be
  //............................................................................
  do
  {
    datfd = open(local_path, opts >> 8, mode);
  }
  while (datfd < 0 && errno == EINTR);

  if (datfd < 0) return -errno;
  else close(datfd);

  return XrdOssOK;
}


//------------------------------------------------------------------------------
// Create directory
//------------------------------------------------------------------------------
int
XrdFstOss::Mkdir (const char* path,
                  mode_t mode,
                  int mkpath,
                  XrdOucEnv* eP)
{
  //............................................................................
  // Operation not supported in EOS
  //............................................................................
  return -ENOTSUP;
}


//------------------------------------------------------------------------------
// Delete a directory from the namespace
//------------------------------------------------------------------------------
int
XrdFstOss::Remdir (const char* path,
                   int opts,
                   XrdOucEnv* eP)
{
  //............................................................................
  // Operation not supported in EOS
  //............................................................................
  return -ENOTSUP;
}


//------------------------------------------------------------------------------
// Renames a file with name 'old_name' to 'new_name'
//------------------------------------------------------------------------------
int
XrdFstOss::Rename (const char* oldname,
                   const char* newname,
                   XrdOucEnv* old_env,
                   XrdOucEnv* new_env)
{
  int retc2;
  int retc = XrdOssOK;
  char local_path_old[MAXPATHLEN + 8];
  char local_path_new[MAXPATHLEN + 8];
  char* slash_plus, sPChar;
  struct stat statbuff;
  static const mode_t pMode = S_IRWXU | S_IRWXG;
  strcpy(local_path_old, oldname);
  strcpy(local_path_new, newname);
  //............................................................................
  // Make sure that the target file does not exist
  //............................................................................
  retc2 = lstat(local_path_new, &statbuff);

  if (!retc2) return -EEXIST;

  //............................................................................
  // We need to create the directory path if it does not exist.
  //............................................................................
  if (!(slash_plus = rindex(local_path_new, '/'))) return -EINVAL;

  slash_plus++;
  sPChar = *slash_plus;
  *slash_plus = '\0';
  retc2 = XrdOucUtils::makePath(local_path_new, pMode);
  *slash_plus = sPChar;

  if (retc2) return retc2;

  //............................................................................
  // Check if this path is really a symbolic link elsewhere
  //............................................................................
  if (lstat(local_path_old, &statbuff))
  {
    retc = -errno;
  }
  else if (rename(local_path_old, local_path_new))
  {
    retc = -errno;
  }

  return retc;
}


//------------------------------------------------------------------------------
// Determine if file 'path' actually exists
//------------------------------------------------------------------------------
int
XrdFstOss::Stat (const char* path,
                 struct stat* buff,
                 int opts,
                 XrdOucEnv* EnvP)
{
  int retc;
  char local_path[MAXPATHLEN + 1];
  strcpy(local_path, path);

  //............................................................................
  // Stat the file in the local filesystem and update access time if so requested
  //............................................................................
  if (!stat(local_path, buff))
  {
    if (opts & XRDOSS_updtatm && (buff->st_mode & S_IFMT) == S_IFREG)
    {
      struct utimbuf times;
      times.actime = time(0);
      times.modtime = buff->st_mtime;
      utime(local_path, &times);
    }

    retc = XrdOssOK;
  }
  else
  {
    retc = (errno ? -errno : -ENOMSG);
  }

  return retc;
}


//------------------------------------------------------------------------------
// Truncate a file
//------------------------------------------------------------------------------
int
XrdFstOss::Truncate (const char* path,
                     unsigned long long size,
                     XrdOucEnv* envP)
{
  struct stat statbuff;
  char local_path[MAXPATHLEN + 1];
  strcpy(local_path, path);

  if (lstat(local_path, &statbuff)) return -errno;
  else if ((statbuff.st_mode & S_IFMT) == S_IFDIR) return -EISDIR;
  else if ((statbuff.st_mode & S_IFMT) == S_IFLNK)
  {
    struct stat buff;
    if (stat(local_path, &buff)) return -errno;
  }

  if (truncate(local_path, size)) return -errno;

  return XrdOssOK;
}


//------------------------------------------------------------------------------
// Add new entry to file name <-> blockchecksum map
//------------------------------------------------------------------------------
XrdSysRWLock*
XrdFstOss::AddMapping (const std::string& fileName,
                       CheckSum*& blockXs,
                       bool isRW)
{
  XrdSysRWLockHelper wr_lock(mRWMap, 0); // --> wrlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;
  eos_debug("Initial map size: %i and filename: %s.",
            mMapFileXs.size(), fileName.c_str());

  if (mMapFileXs.count(fileName))
  {
    pair_value = mMapFileXs[fileName];
    XrdSysRWLockHelper wr_xslock(pair_value.first, 0); // --> wrlock xs obj

    //..........................................................................
    // If no. ref 0 then the obj is closed and waiting to be deleted so we can
    // add the new one, else return the old one
    //..........................................................................
    if (pair_value.second->GetTotalRef() == 0)
    {
      delete pair_value.second;
      pair_value = std::make_pair(pair_value.first, blockXs);
      mMapFileXs[fileName] = pair_value;
      eos_debug("Update old entry, map size: %i. ", mMapFileXs.size());
    }
    else
    {
      delete blockXs;
      blockXs = pair_value.second;
    }

    blockXs->IncrementRef(isRW);
    return pair_value.first;
  }
  else
  {
    XrdSysRWLock* mutex_xs = new XrdSysRWLock();
    pair_value = std::make_pair(mutex_xs, blockXs);
    //..........................................................................
    // Can increment without the lock as no one knows about this obj yet
    //..........................................................................
    blockXs->IncrementRef(isRW);
    mMapFileXs[fileName] = pair_value;
    eos_debug("Add completely new obj, map size: %i and filename: %s.",
              mMapFileXs.size(), fileName.c_str());
    return mutex_xs;
  }
}


//------------------------------------------------------------------------------
// Get blockchecksum object for a filname
//------------------------------------------------------------------------------
std::pair<XrdSysRWLock*, CheckSum*>
XrdFstOss::GetXsObj (const std::string& fileName, bool isRW)
{
  XrdSysRWLockHelper rd_lock(mRWMap); // --> rdlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;

  if (mMapFileXs.count(fileName))
  {
    pair_value = mMapFileXs[fileName];
    XrdSysRWLock* mutex_xs = pair_value.first;
    CheckSum* xs_obj = pair_value.second;
    //..........................................................................
    // Lock xs obj as multiple threads can update the value here
    //..........................................................................
    XrdSysRWLockHelper xs_wrlock(mutex_xs, 0); // --> wrlock xs obj
    eos_debug("\nXs obj no ref: %i.\n", xs_obj->GetTotalRef());

    if (xs_obj->GetTotalRef() != 0)
    {
      xs_obj->IncrementRef(isRW);
      return std::make_pair(mutex_xs, xs_obj);
    }
    else
    {
      //........................................................................
      // If no refs., it means the obj was closed and waiting to be deleted
      //........................................................................
      return std::make_pair<XrdSysRWLock*, CheckSum*>(NULL, NULL);
    }
  }

  return std::make_pair<XrdSysRWLock*, CheckSum*>(NULL, NULL);
}


//------------------------------------------------------------------------------
// Drop blockchecksum object for a file name
//------------------------------------------------------------------------------
void
XrdFstOss::DropXs (const std::string& fileName, bool force)
{
  XrdSysRWLockHelper wr_lock(mRWMap, 0); // --> wrlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;
  eos_debug("Oss map size before drop: %i.", mMapFileXs.size());

  if (mMapFileXs.count(fileName))
  {
    pair_value = mMapFileXs[fileName];
    //..........................................................................
    // If no refs to the checksum, we can safely delete it
    //..........................................................................
    pair_value.first->WriteLock(); // --> wrlock xs obj
    eos_debug("Xs obj no ref: %i.", pair_value.second->GetTotalRef());

    if ((pair_value.second->GetTotalRef() == 0) || force)
    {
      pair_value.first->UnLock(); // <-- unlock xs obj
      delete pair_value.first;
      delete pair_value.second;
      mMapFileXs.erase(fileName);
    }
    else
    {
      eos_debug("Do not drop the mapping");
      pair_value.first->UnLock(); // <-- unlock xs obj
    }
  }

  eos_debug("Oss map size after drop: %i.", mMapFileXs.size());
}

EOSFSTNAMESPACE_END

