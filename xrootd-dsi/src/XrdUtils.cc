
/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @file XrdUtils.cc
//! @author Geoffray Adde - CERN
//! @brief Some utility class and functions to help to use Xrd.
//!        Most of the following code has been replicated and slightly modified
//!        from XRootD source code (mainly XrdCl and XrdPosix)
//!        The main goal is to get rid of the dependency on XrdPosix.
//------------------------------------------------------------------------------

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/param.h>

#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdSys/XrdSysHeaders.hh"
#include "XProtocol/XProtocol.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClConstants.hh"
#include "./XrdUtils.hh"

/******************************************************************************/
/*         X r o o t P a t h    C o n s t r u c t o r          */
/******************************************************************************/

XrootPath::XrootPath()
: xplist(0),
  pBase(0)
{
  XrdOucTokenizer thePaths(0);
  char *plist = 0, *colon = 0, *subs = 0, *lp = 0, *tp = 0;
  int aOK = 0;

  cwdPath = 0; cwdPlen = 0;

  if (!(plist = getenv("XROOTD_VMP")) || !*plist) return;
  pBase = strdup(plist);

  thePaths.Attach(pBase);

  if ((lp = thePaths.GetLine())) while((tp = thePaths.GetToken()))
  {aOK = 1;
  if ((colon = rindex(tp, (int)':')) && *(colon+1) == '/')
  {if (!(subs = index(colon, (int)'='))) subs = 0;
  else if (*(subs+1) == '/') {*subs = '\0'; subs++;}
  else if (*(subs+1)) aOK = 0;
  else {*subs = '\0'; subs = (char*)"";}
  } else aOK = 0;

  if (aOK)
  {*colon++ = '\0';
  while(*(colon+1) == '/') colon++;
  xplist = new xpath(xplist, tp, colon, subs);
  } else cerr <<"XrdUtils: Invalid XROOTD_VMP token '" <<tp <<'"' <<endl;
  }
}

/******************************************************************************/
/*          X r o o t P a t h    D e s t r u c t o r           */
/******************************************************************************/

XrootPath::~XrootPath()
{
  struct xpath *xpnow;

  while((xpnow = xplist))
  {xplist = xplist->next; delete xpnow;}
}

/******************************************************************************/
/*                     X r o o t P a t h : : C W D                      */
/******************************************************************************/

void XrootPath::CWD(const char *path)
{
  if (cwdPath) free(cwdPath);
  cwdPlen = strlen(path);
  if (*(path+cwdPlen-1) == '/') cwdPath = strdup(path);
  else if (cwdPlen <= MAXPATHLEN)
  {char buff[MAXPATHLEN+8];
  strcpy(buff, path);
  *(buff+cwdPlen  ) = '/';
  *(buff+cwdPlen+1) = '\0';
  cwdPath = strdup(buff); cwdPlen++;
  }
}

/******************************************************************************/
/*                     X r o o t P a t h : : U R L                      */
/******************************************************************************/

char *XrootPath::BuildURL(const char *path, char *buff, int blen)
{
  const char   *rproto = "root://";
  const int     rprlen = strlen(rproto);
  const char   *xproto = "xroot://";
  const int     xprlen = strlen(xproto);
  struct xpath *xpnow = xplist;
  char tmpbuff[2048];
  int plen, pathlen = 0;

  // If this starts with 'root", then this is our path
  //
  if (!strncmp(rproto, path, rprlen)) return (char *)path;

  // If it starts with xroot, then convert it to be root
  //
  if (!strncmp(xproto, path, xprlen))
  {
    if (!buff) return (char *)1;
    if ((int(strlen(path))) > blen) return 0;
    strcpy(buff, path+1);
    return buff;
  }

  // If a relative path was specified, convert it to an abso9lute path
  //
  if (path[0] == '.' && path[1] == '/' && cwdPath)
  {
    pathlen = (strlen(path) + cwdPlen - 2);
    if (pathlen < (int)sizeof(tmpbuff))
    {
      strcpy(tmpbuff, cwdPath);
      strcpy(tmpbuff+cwdPlen, path+2);
      path = (const char *)tmpbuff;
    }
    else return 0;
  }

  // Check if this path starts with one or our known paths
  //
  while(*(path+1) == '/') path++;
  while(xpnow)
    if (!strncmp(path, xpnow->path, xpnow->plen)) break;
    else xpnow = xpnow->next;

  // If we did not match a path, this is not our path.
  //
  if (!xpnow) return 0;
  if (!buff) return (char *)1;

  // Verify that we won't overflow the buffer
  //
  if (!pathlen) pathlen = strlen(path);
  plen = xprlen + pathlen + xpnow->servln + 2;
  if (xpnow->nath) plen =  plen - xpnow->plen + xpnow->nlen;
  if (plen >= blen) return 0;

  // Build the url
  //
  strcpy(buff, rproto);
  strcat(buff, xpnow->server);
  strcat(buff, "/");
  if (xpnow->nath) {strcat(buff, xpnow->nath); path += xpnow->plen;}
  if (*path != '/') strcat(buff, "/");
  strcat(buff, path);
  return buff;
}

/******************************************************************************/
/*                     X r o o t P a t h : : S p l i t U R L                      */
/******************************************************************************/

int XrootPath::SplitURL(const char *url, char *server, char *path, int blen)
{
  int i,j;
  const char *pathptr;
  i=0; j=-1;
  // url is supposed to be given in the foloowing format xxxx//xxxx/yyyyyyyy
  // xxxx//xxxx is the server and yyyyyyyy is the path

  while(i!=3 && j<(int)strlen(url)) if(url[++j]=='/') i++;
  if(i==3) { // server part successfully parsed
    pathptr=url+j;
  } else { // error parsing path
    return 1;
  }
  if(blen<j+1) return 2; // buffer is too small to write the server part
  strncpy(server,url,j);
  server[j]='\0';

  while ( (strlen(pathptr)>1) && (pathptr[0] == '/' && pathptr[1] == '/')) pathptr++;
  if(blen<(int)strlen(pathptr)+1) return 2; // buffer is too small to write the path part
  strcpy(path,pathptr);

  if(strlen(path)==0) return 3; // the path part is empty (should be at last '/')

  return 0;
}

/******************************************************************************/
/*                              i n i t S t a t                               */
/******************************************************************************/

void XrootStatUtils::initStat(globus_gfs_stat_t *buf)
{
  static int initStat = 0;
  static dev_t st_rdev;
  static dev_t st_dev;
  static uid_t myUID = getuid();
  static gid_t myGID = getgid();

  // Initialize the xdev fields. This cannot be done in the constructor because
  // we may not yet have resolved the C-library symbols.
  //
  if (!initStat) {initStat = 1; initXdev(st_dev, st_rdev);}
  memset(buf, 0, sizeof(globus_gfs_stat_t));

  // Preset common fields
  //
  buf->dev    = st_dev;
  buf->nlink  = 1;
  buf->uid    = myUID;
  buf->gid    = myGID;
}

/******************************************************************************/
/*                              i n i t X d e v                               */
/******************************************************************************/

void XrootStatUtils::initXdev(dev_t &st_dev, dev_t &st_rdev)
{
  struct stat buf;

  // Get the device id for /tmp used by stat()
  //
  if (stat("/tmp", &buf)) {st_dev = 0; st_rdev = 0;}
  else {st_dev = buf.st_dev; st_rdev = buf.st_rdev;}
}

/******************************************************************************/
/*                              m a p F l a g s                               */
/******************************************************************************/

int XrootStatUtils::mapFlagsXrd2Pos(int flags)
{
  int newflags = 0;

  // Map the xroot flags to unix flags
  //
  if (flags & kXR_xset)     newflags |= S_IXUSR;
  if (flags & kXR_readable) newflags |= S_IRUSR;
  if (flags & kXR_writable) newflags |= S_IWUSR;
  if (flags & kXR_other)    newflags |= S_IFBLK;
  else if (flags & kXR_isDir) newflags |= S_IFDIR;
  else newflags |= S_IFREG;
  if (flags & kXR_offline) newflags |= S_ISVTX;
  if (flags & kXR_poscpend)newflags |= S_ISUID;

  return newflags;
}

int XrootStatUtils::mapFlagsPos2Xrd(int flags)
{
  int XOflags;

  XOflags = (flags & (O_WRONLY | O_RDWR) ? kXR_open_updt : kXR_open_read);
  if (flags & O_CREAT) {
      XOflags |= (flags & O_EXCL ? kXR_new : kXR_delete);
      XOflags |= kXR_mkpath;
  }
  else if ( (flags & O_TRUNC) && (XOflags & kXR_open_updt))
             XOflags |= kXR_delete;

  return XOflags;
}

/******************************************************************************/
/*                               m a p M o d e                                */
/******************************************************************************/

int XrootStatUtils::mapModePos2Xrd(mode_t mode)
{
  int XMode = 0;

  // Map the mode
  //
  if (mode & S_IRUSR) XMode |= kXR_ur;
  if (mode & S_IWUSR) XMode |= kXR_uw;
  if (mode & S_IXUSR) XMode |= kXR_ux;
  if (mode & S_IRGRP) XMode |= kXR_gr;
  if (mode & S_IWGRP) XMode |= kXR_gw;
  if (mode & S_IXGRP) XMode |= kXR_gx;
  if (mode & S_IROTH) XMode |= kXR_or;
  if (mode & S_IXOTH) XMode |= kXR_ox;
  return XMode;
}

int XrootStatUtils::mapModeXrd2Pos(mode_t mode)
{
  int XMode = 0;

  // Map the mode
  //
  if (mode & kXR_ur ) XMode |= S_IRUSR;
  if (mode & kXR_uw ) XMode |= S_IWUSR;
  if (mode & kXR_ux ) XMode |= S_IXUSR;
  if (mode & kXR_gr ) XMode |= S_IRGRP;
  if (mode & kXR_gw ) XMode |= S_IWGRP;
  if (mode & kXR_gx ) XMode |= S_IXGRP;
  if (mode & kXR_or ) XMode |= S_IROTH;
  if (mode & kXR_ox ) XMode |= S_IXOTH;
  return XMode;
}


int XrootStatUtils::mapError(int rc)
{
  switch(rc)
  {
  case kXR_NotFound:      return ENOENT;
  case kXR_NotAuthorized: return EACCES;
  case kXR_IOError:       return EIO;
  case kXR_NoMemory:      return ENOMEM;
  case kXR_NoSpace:       return ENOSPC;
  case kXR_ArgTooLong:    return ENAMETOOLONG;
  case kXR_noserver:      return EHOSTUNREACH;
  case kXR_NotFile:       return ENOTBLK;
  case kXR_isDirectory:   return EISDIR;
  case kXR_FSError:       return ENOSYS;
  default:                return ECANCELED;
  }
}

using namespace XrdCl;
XrdCl::XRootDStatus XrdUtils::GetRemoteCheckSum( std::string       &checkSum,
    const std::string &checkSumType,
    const std::string &server,
    const std::string &path )
{
  XrdCl::FileSystem   *fs = new FileSystem( URL( server ) );
  Buffer        arg; arg.FromString( path );
  Buffer       *cksResponse = 0;
  XRootDStatus  st;

  st = fs->Query( QueryCode::Checksum, arg, cksResponse );
  delete fs;

  if( !st.IsOK() )
    return st;

  if( !cksResponse )
    return XRootDStatus( stError, errInternal );

  std::vector<std::string> elems;
  XrdUtils::splitString( elems, cksResponse->ToString(), " " );
  delete cksResponse;

  if( elems.size() != 2 )
    return XRootDStatus( stError, errInvalidResponse );

  if( elems[0] != checkSumType )
    return XRootDStatus( stError, errCheckSumError );

  checkSum = elems[0] + ":";
  checkSum += elems[1];

  return XRootDStatus();
}

