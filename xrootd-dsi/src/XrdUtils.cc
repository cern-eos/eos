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
//!        Part of the following code has been replicated and slightly modified
//!        from XRootD source code (mainly XrdCl and XrdPosix)
//!        The main goal is to get rid of the dependency on XrdPosix.
//------------------------------------------------------------------------------
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <set>
#include <sys/param.h>
#include <sstream>

#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysHeaders.hh"
#include "XProtocol/XProtocol.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClConstants.hh"
#include "./XrdUtils.hh"

/******************************************************************************/
/*         X r o o t P a t h    C o n s t r u c t o r          */
/******************************************************************************/

XrootPath::XrootPath () :
    xplist (0), pBase (0)
{
  XrdOucTokenizer thePaths (0);
  char *plist = 0, *colon = 0, *subs = 0, *lp = 0, *tp = 0;
  int aOK = 0;

  cwdPath = 0;
  cwdPlen = 0;

  if (!(plist = getenv ("XROOTD_VMP")) || !*plist) return;
  pBase = strdup (plist);

  thePaths.Attach (pBase);

  if ((lp = thePaths.GetLine ())) while ((tp = thePaths.GetToken ()))
  {
    aOK = 1;
    if ((colon = rindex (tp, (int) ':')) && *(colon + 1) == '/')
    {
      if (!(subs = index (colon, (int) '=')))
        aOK = 0;
      else if (*(subs + 1) == '/')
      {
        *subs = '\0';
        subs++;
      }
      else if (*(subs + 1))
        aOK = 0;
      else
      {
        *subs = '\0';
        subs = (char*) "";
      }
    }
    else
      aOK = 0;

    if (aOK)
    {
      *colon++ = '\0';
      while (*(colon + 1) == '/')
        colon++;
      xplist = new xpath (xplist, tp, colon, subs);
    }
    else
    {
      std::stringstream ss;
      ss << "XrdUtils: Invalid XROOTD_VMP token '" << tp << '"' << endl;
      pParseErrStr = ss.str ();
      return;
    }
  }
}

/******************************************************************************/
/*         X r o o t P a t h    C h e ck V M P                                */
/******************************************************************************/

bool XrootPath::CheckVMP (char *errbuff, int errbufflen)
{
  struct xpath *xpnow = xplist;
  if (!xpnow) return false;
  // Check all the known paths
  while (xpnow)
  {
    XrdCl::URL url;
    XrdCl::XRootDStatus status;

    url.FromString (xpnow->server);
    url.SetUserName ("XrootPath_CheckVMP");

    XrdCl::StatInfo* xrdstatinfo = 0;
    XrdCl::FileSystem fs (url);
    status = fs.Stat (xpnow->nath, xrdstatinfo);
    if (status.IsError ())
    {
      if (xrdstatinfo) delete xrdstatinfo;
      snprintf (errbuff, errbufflen, "cannot stat Xrootd Virtual Mount Point %s   %s, error is \"%s\"", xpnow->server, xpnow->nath,
                status.ToString ().c_str ());
      return false;
    }
    xpnow = xpnow->next;
  }
  return true;
}

/******************************************************************************/
/*          X r o o t P a t h    D e s t r u c t o r           */
/******************************************************************************/

XrootPath::~XrootPath ()
{
  struct xpath *xpnow;
  if (pBase) free (pBase);
  while ((xpnow = xplist))
  {
    xplist = xplist->next;
    delete xpnow;
  }
}

/******************************************************************************/
/*                     X r o o t P a t h : : C W D                      */
/******************************************************************************/

void XrootPath::CWD (const char *path)
{
  if (cwdPath) free (cwdPath);
  cwdPlen = strlen (path);
  if (*(path + cwdPlen - 1) == '/')
    cwdPath = strdup (path);
  else if (cwdPlen <= MAXPATHLEN)
  {
    char buff[MAXPATHLEN + 8];
    strcpy (buff, path);
    *(buff + cwdPlen) = '/';
    *(buff + cwdPlen + 1) = '\0';
    cwdPath = strdup (buff);
    cwdPlen++;
  }
}

/******************************************************************************/
/*                     X r o o t P a t h : : U R L                      */
/******************************************************************************/

char *
XrootPath::BuildURL (const char *path, char *buff, int blen)
{
  const char *rproto = "root://";
  const int rprlen = strlen (rproto);
  const char *xproto = "xroot://";
  const int xprlen = strlen (xproto);
  struct xpath *xpnow = xplist;
  char tmpbuff[2048];
  int plen, pathlen = 0;

  // If this starts with 'root", then this is our path
  //
  if (!strncmp (rproto, path, rprlen)) return (char *) path;

  // If it starts with xroot, then convert it to be root
  //
  if (!strncmp (xproto, path, xprlen))
  {
    if (!buff) return (char *) 1;
    if ((int (strlen (path))) > blen) return 0;
    strcpy (buff, path + 1);
    return buff;
  }

  // If a relative path was specified, convert it to an abso9lute path
  //
  if (path[0] == '.' && path[1] == '/' && cwdPath)
  {
    pathlen = (strlen (path) + cwdPlen - 2);
    if (pathlen < (int) sizeof(tmpbuff))
    {
      strcpy (tmpbuff, cwdPath);
      strcpy (tmpbuff + cwdPlen, path + 2);
      path = (const char *) tmpbuff;
    }
    else
      return 0;
  }

  // Check if this path starts with one or our known paths
  //
  while (*(path + 1) == '/')
    path++;
  while (xpnow)
    if (!strncmp (path, xpnow->path, xpnow->plen))
      break;
    else
      xpnow = xpnow->next;

  // If we did not match a path, this is not our path.
  //
  if (!xpnow) return 0;
  if (!buff) return (char *) 1;

  // Verify that we won't overflow the buffer
  //
  if (!pathlen) pathlen = strlen (path);
  plen = xprlen + pathlen + xpnow->servln + 2;
  if (xpnow->nath) plen = plen - xpnow->plen + xpnow->nlen;
  if (plen >= blen) return 0;

  // Build the url
  //
  strcpy (buff, rproto);
  strcat (buff, xpnow->server);
  strcat (buff, "/");
  if (xpnow->nath)
  {
    strcat (buff, xpnow->nath);
    path += xpnow->plen;
  }
  if (*path != '/') strcat (buff, "/");
  strcat (buff, path);
  return buff;
}

void XrootPath::GetServerList(std::vector<std::string> *listasvector, std::string *listasstring)
{
  auto it = this->xplist;
  do
  {
    std::string s(it->server);
    auto pos = s.rfind(':');
    if(pos!=std::string::npos) s.resize(pos);

    if (listasvector) listasvector->push_back (s);
    if (listasstring)
    {
      if(listasstring->size()) listasstring->append ("|");
      listasstring->append (s);
    }
  }
  while((it=it->next)!=NULL);
}

/******************************************************************************/
/*                     X r o o t P a t h : : S p l i t U R L                      */
/******************************************************************************/

int XrootPath::SplitURL (const char *url, char *server, char *path, int blen)
{
  int i, j;
  const char *pathptr;
  i = 0;
  j = -1;
  // url is supposed to be given in the foloowing format xxxx//xxxx/yyyyyyyy
  // xxxx//xxxx is the server and yyyyyyyy is the path

  while (i != 3 && j < (int) strlen (url))
    if (url[++j] == '/') i++;
  if (i == 3)
  { // server part successfully parsed
    pathptr = url + j;
  }
  else
  { // error parsing path
    return 1;
  }
  if (blen < j + 1) return 2; // buffer is too small to write the server part
  strncpy (server, url, j);
  server[j] = '\0';

  while ((strlen (pathptr) > 1) && (pathptr[0] == '/' && pathptr[1] == '/'))
    pathptr++;
  if (blen < (int) strlen (pathptr) + 1) return 2; // buffer is too small to write the path part
  strcpy (path, pathptr);

  if (strlen (path) == 0) return 3; // the path part is empty (should be at last '/')

  return 0;
}

/******************************************************************************/
/*                              i n i t S t a t                               */
/******************************************************************************/

void XrootStatUtils::initStat (globus_gfs_stat_t *buf)
{
  static int initStat = 0;
  static dev_t st_rdev;
  static dev_t st_dev;
  static uid_t myUID = getuid ();
  static gid_t myGID = getgid ();

  // Initialize the xdev fields. This cannot be done in the constructor because
  // we may not yet have resolved the C-library symbols.
  //
  if (!initStat)
  {
    initStat = 1;
    initXdev (st_dev, st_rdev);
  }
  memset (buf, 0, sizeof(globus_gfs_stat_t));

  // Preset common fields
  //
  buf->dev = st_dev;
  buf->nlink = 1;
  buf->uid = myUID;
  buf->gid = myGID;
}

/******************************************************************************/
/*                              i n i t X d e v                               */
/******************************************************************************/

void XrootStatUtils::initXdev (dev_t &st_dev, dev_t &st_rdev)
{
  struct stat buf;

  // Get the device id for /tmp used by stat()
  //
  if (stat ("/tmp", &buf))
  {
    st_dev = 0;
    st_rdev = 0;
  }
  else
  {
    st_dev = buf.st_dev;
    st_rdev = buf.st_rdev;
  }
}

/******************************************************************************/
/*                              m a p F l a g s                               */
/******************************************************************************/

int XrootStatUtils::mapFlagsXrd2Pos (int flags)
{
  int newflags = 0;

  // Map the xroot flags to unix flags
  //
  if (flags & kXR_xset) newflags |= S_IXUSR;
  if (flags & kXR_readable) newflags |= S_IRUSR;
  if (flags & kXR_writable) newflags |= S_IWUSR;
  if (flags & kXR_other)
    newflags |= S_IFBLK;
  else if (flags & kXR_isDir)
    newflags |= S_IFDIR;
  else
    newflags |= S_IFREG;
  if (flags & kXR_offline) newflags |= S_ISVTX;
  if (flags & kXR_poscpend) newflags |= S_ISUID;

  return newflags;
}

int XrootStatUtils::mapFlagsPos2Xrd (int flags)
{
  int XOflags;

  XOflags = (flags & (O_WRONLY | O_RDWR) ? kXR_open_updt : kXR_open_read);
  if (flags & O_CREAT)
  {
    XOflags |= (flags & O_EXCL ? kXR_new : kXR_delete);
    XOflags |= kXR_mkpath;
  }
  else if ((flags & O_TRUNC) && (XOflags & kXR_open_updt)) XOflags |= kXR_delete;

  return XOflags;
}

/******************************************************************************/
/*                               m a p M o d e                                */
/******************************************************************************/

int XrootStatUtils::mapModePos2Xrd (mode_t mode)
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

int XrootStatUtils::mapModeXrd2Pos (mode_t mode)
{
  int XMode = 0;

  // Map the mode
  //
  if (mode & kXR_ur) XMode |= S_IRUSR;
  if (mode & kXR_uw) XMode |= S_IWUSR;
  if (mode & kXR_ux) XMode |= S_IXUSR;
  if (mode & kXR_gr) XMode |= S_IRGRP;
  if (mode & kXR_gw) XMode |= S_IWGRP;
  if (mode & kXR_gx) XMode |= S_IXGRP;
  if (mode & kXR_or) XMode |= S_IROTH;
  if (mode & kXR_ox) XMode |= S_IXOTH;
  return XMode;
}

/******************************************************************************/
/*                               m a p E r r o r                              */
/******************************************************************************/

int XrootStatUtils::mapError (int rc)
{
  switch (rc)
  {
    case kXR_NotFound:
      return ENOENT;
    case kXR_NotAuthorized:
      return EACCES;
    case kXR_IOError:
      return EIO;
    case kXR_NoMemory:
      return ENOMEM;
    case kXR_NoSpace:
      return ENOSPC;
    case kXR_ArgTooLong:
      return ENAMETOOLONG;
    case kXR_noserver:
      return EHOSTUNREACH;
    case kXR_NotFile:
      return ENOTBLK;
    case kXR_isDirectory:
      return EISDIR;
    case kXR_FSError:
      return ENOSYS;
    default:
      return ECANCELED;
  }
}

/******************************************************************************/
/*                        G e t R e m o t e C h e c k S u m                   */
/******************************************************************************/

using namespace XrdCl;
XrdCl::XRootDStatus XrdUtils::GetRemoteCheckSum (std::string &checkSum, const std::string &checkSumType, const std::string &server,
                                                 const std::string &path)
{
  XrdCl::FileSystem *fs = new FileSystem (URL (server));
  Buffer arg;
  arg.FromString (path);
  Buffer *cksResponse = 0;
  XRootDStatus st;

  st = fs->Query (QueryCode::Checksum, arg, cksResponse);
  delete fs;

  if (!st.IsOK ()) return st;

  if (!cksResponse) return XRootDStatus (stError, errInternal);

  std::vector<std::string> elems;
  XrdUtils::splitString (elems, cksResponse->ToString (), " ");
  delete cksResponse;

  if (elems.size () != 2) return XRootDStatus (stError, errInvalidResponse);

  if (elems[0] != checkSumType) return XRootDStatus (stError, errCheckSumError);

  checkSum = elems[0] + ":";
  checkSum += elems[1];

  return XRootDStatus ();
}

/******************************************************************************/
/*                               L o c a t e                                  */
/******************************************************************************/

XrdCl::XRootDStatus XrdUtils::LocateFileXrootd (std::vector<std::string> &urls, std::vector<std::string> &servers,
                                                const std::string &server, const std::string &path, globus_l_gfs_xrootd_filemode_t fileMode,
                                                std::vector<std::string>& unFilteredServerList)
{
  XrdCl::FileSystem *fs = new FileSystem (URL (server));
  XRootDStatus st;

  LocationInfo *li;
  st = fs->Locate (path, fileMode == XROOTD_FILEMODE_READING ? (OpenFlags::Read) : (OpenFlags::Write), li, 10);
  if (!st.IsOK ()) return st;

  for (auto it = li->Begin (); it != li->End (); it++)
  {
    urls.push_back (it->GetAddress ());
    XrdCl::URL url;
    url.FromString (it->GetAddress ());
    servers.push_back (url.GetHostName ());
    unFilteredServerList.push_back (url.GetHostName ());
  }

  return XRootDStatus ();
}

XrdCl::XRootDStatus XrdUtils::IssueEosCmd (XrdOucString &rstdout, const XrdOucString &sserver, const XrdOucString &command,
                                           const XrdOucString &opaque, bool admincmd)
{
  XrdOucString out = "";
  XrdOucString path = sserver;
  if(admincmd)
    path += "//proc/admin/";
  else
    path += "//proc/user/";
  path += "?mgm.cmd=";
  path += command;
  if (opaque.length () && (*opaque.c_str () != '&')) path += '&';
  path += opaque;

  if(admincmd)
  {
  XrdCl::URL url;
  url.FromString(path.c_str());
  url.SetUserName("root");
  path = url.GetURL().c_str();
  }

  XrdCl::OpenFlags::Flags flags_xrdcl = XrdCl::OpenFlags::Read;
  XrdCl::File* client = new XrdCl::File ();
  XrdCl::XRootDStatus status = client->Open (path.c_str (), flags_xrdcl);
  //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "IssueEosCmd : try to open %s\n",path.c_str());

  if (status.IsOK ())
  {
    off_t offset = 0;
    uint32_t nbytes = 0;
    char buffer[4096 + 1];
    status = client->Read (offset, 4096, buffer, nbytes);

    while (status.IsOK () && (nbytes > 0))
    {
      buffer[nbytes] = 0;
      out += buffer;
      offset += nbytes;
      status = client->Read (offset, 4096, buffer, nbytes);
    }

    client->Close ();

    delete client;

    XrdOucEnv outEnv (out.c_str ());
    rstdout = outEnv.Get ("mgm.proc.stdout");
    while (rstdout.replace ("#and#", "&"))
    {
    };
     //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "IssueEosCmd : open was sucessful, rstdout is [[ %s ]] \n",rstdout.c_str());
    return XrdCl::XRootDStatus ();
  }
  else
  {
     //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "IssueEosCmd : open failed ! \n",rstdout.c_str());
    return status;
  }
}
struct mystruct
{
  bool operator() (char c1, char c2)
  {
    return c1 == ' ' && c2 == c1;
  }
};

XrdCl::XRootDStatus XrdUtils::LocateFileEos (std::vector<std::string> &urls, std::vector<std::string> &servers, bool &isReplicaLayout,
                                             const std::string &sserver, const std::string &spath, globus_l_gfs_xrootd_filemode_t fileMode,
                                             std::vector<std::string>& unFilteredServerList)
{
  XrdOucString rstdout;
  XrdOucString opaque;
  opaque += "&mgm.path=";
  opaque += spath.c_str ();
  opaque += "&mgm.file.info.option=-m--fullpath";

  XrdCl::XRootDStatus status = IssueEosCmd (rstdout, sserver.c_str (), "fileinfo", opaque);

  if (status.IsOK ())
  {
    XrdOucString token;
    std::list<std::pair<int, XrdOucString> > fsidFullpath;
    int nstripes;

    auto layoutIdx = rstdout.find ("layout=");
    if (layoutIdx != STR_NPOS)
    {
      rstdout.tokenize (token, layoutIdx + 7, ' ');
      isReplicaLayout = (token == "replica");
    }
    else
    {
      // WARNING
      status.SetErrorMessage ("could not parse layout of the file");
      return Status (stError, errInvalidResponse);
    }

    auto nstripesIdx = rstdout.find ("nstripes=");
    if (layoutIdx != STR_NPOS)
    {
      rstdout.tokenize (token, nstripesIdx + 9, ' ');
      nstripes = token.atoi ();
    }
    else
    {
      // WARNING
      status.SetErrorMessage ("could not parse number of stripes in the file");
      return Status (stError, errInvalidResponse);
    }

    int j = nstripesIdx;
    for (int i = 0; i < nstripes; i++)
    {
      j = rstdout.find ("fsid=", j);
      if (j == STR_NPOS)
      {
        // WARNING
        status.SetErrorMessage ("could not parse expected fsid");
        return Status (stError, errInvalidResponse);
      }
      rstdout.tokenize (token, j + 5, ' ');
      int fsid = token.atoi ();

      j = rstdout.find ("fullpath=", j);
      if (j == STR_NPOS)
      {
        // WARNING
        status.SetErrorMessage ("could not parse expected fullpath");
        return Status (stError, errInvalidResponse);
      }
      rstdout.tokenize (token, j + 9, ' ');
      fsidFullpath.push_front (std::make_pair (fsid, token.c_str ()));
    }

    // THIS IS SORT OF A HACK TO RETRIEVE THE HOSTNAMES OF THE FSTS HOSTING THE FSIDS
    rstdout.erase ();
    opaque.erase ();
    token.erase ();
    opaque += "&mgm.path=";
    opaque += spath.c_str ();
    opaque += "&mgm.file.info.option=--fullpath";

    XrdCl::XRootDStatus status2 = IssueEosCmd (rstdout, sserver.c_str (), "fileinfo", opaque);
    if (status2.IsOK ())
    {
      // if a line contains both the fsid and the full path, it should contain the fst's hostname
      XrdOucString line;
      int pos = 0;
      std::list<XrdOucString> lines;
      while ((pos = rstdout.tokenize (line, pos, '\n')) != STR_NPOS)
        lines.push_front (line);

      for (auto itl = lines.begin (); itl != lines.end (); itl++)
      {
        XrdOucString &line = *itl;
        mystruct myop;
        std::unique ((char*) line.c_str (), (char*) line.c_str () + line.length () + 1, myop);
        std::set<std::string> lineTokens;
        pos = 0;
        while ((pos = line.tokenize (token, pos, ' ')) != STR_NPOS)
          lineTokens.insert (token.c_str ());

        for (auto it = fsidFullpath.begin (); it != fsidFullpath.end (); it++)
        {
          token.form ("%d", it->first);
          auto notfound = lineTokens.end ();
          if (lineTokens.find (token.c_str ()) != notfound && lineTokens.find (it->second.c_str ()) != notfound)
          {
            // a we found an fst
            XrdOucString sep = (it->second.find ('?') == STR_NPOS) ? '?' : '&';
            // the hostname is the token after the fsid
            token.form (" %d ", it->first);
            pos = line.find (token);
            XrdOucString hostname;
            pos += token.length ();
            while (line[pos] == ' ')
              pos++;
            line.tokenize (hostname, pos, ' ');
            unFilteredServerList.push_back (hostname.c_str ());

            if (lineTokens.find ("booted") != notfound && lineTokens.find ("online") != notfound
                && (lineTokens.find ("rw") != notfound || (fileMode == XROOTD_FILEMODE_READING && lineTokens.find ("ro") != notfound)))
            {
              // we found an fst matching access creteria
              urls.push_back ((XrdOucString ((sserver + spath + sep.c_str ()).c_str ()) + "eos.force.fsid=" + it->first).c_str ());
              servers.push_back (hostname.c_str ());
//	    globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "LocateFileEos : one replica spotted , path is [[ %s ]] and server is [[ %s ]] \n",
//				    urls.back ().c_str (), servers.back ().c_str ());
              fsidFullpath.erase (it);
              break;
            }
          }
        }
      }
    }
    else
    {
      return status2;
    }

    return XrdCl::XRootDStatus ();
  }
  else
  {
    return status;
  }

  return status;
}

XrdCl::XRootDStatus XrdUtils::ListFstEos (std::vector<std::string> &servers, const std::string &sserver)
{
  // root://localhost//proc/admin/?mgm.cmd=node&mgm.subcmd=ls&mgm.outformat=m
  XrdOucString rstdout;
  XrdOucString opaque;
  opaque += "&mgm.subcmd=ls&mgm.outformat=m&eos.rgid=0&eos.ruid=0";

  XrdCl::XRootDStatus status = IssueEosCmd (rstdout, sserver.c_str (), "node", opaque,true);

  if (status.IsOK ())
  {
    int pos = -1;
    while ((pos = rstdout.find ("hostport=", pos+1)) != (int)std::string::npos)
    {
      size_t pos2 = rstdout.find (":", pos);
      if (pos2 == std::string::npos) pos2 = rstdout.find (" ", pos);

      if (pos2 == std::string::npos)
      {
        globus_gfs_log_message (GLOBUS_GFS_LOG_ERR, "could not parse token %s", rstdout.c_str () + pos);
        continue;
      }
      XrdOucString urlNoPort (rstdout, pos + 9, pos2 - 1);
      //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "adding %s\n", urlNoPort.c_str ());
      servers.push_back (urlNoPort.c_str ());
    }
    return XrdCl::XRootDStatus ();
  }

  return status;
}


/******************************************************************************/
/*                        G e t R e m o t e S e r v e r s                     */
/******************************************************************************/

bool XrdUtils::GetRemoteServers (std::vector<std::string> &selectedServers, std::string &errStr,
                                 std::vector<std::string> &potentialNewServers, XrdGsiBackendMapper *backend, const std::string &fileServer,
                                 std::string filePath, const std::string &TruncationTmpFileSuffix, globus_l_gfs_xrootd_filemode_t fileMode,
                                 bool useEosSpecifics)
{
  std::stringstream ss;
  std::vector<std::string> locatedUrls, locatedServers;

  // for plain XrootD servers we assume replicas
  // so the file can be striped read efficiently
  bool isReplicaLayout = true;

  // if there is no filename just selected one random server
  if (fileServer.size () && filePath.size () && fileMode != XROOTD_FILEMODE_NONE)
  {
    if (fileMode == XROOTD_FILEMODE_TRUNCATE) filePath.append (TruncationTmpFileSuffix);
    XrdCl::XRootDStatus status;
    if (useEosSpecifics)
      status = LocateFileEos (locatedUrls, locatedServers, isReplicaLayout, fileServer, filePath, fileMode, potentialNewServers);
    else
      status = LocateFileXrootd (locatedUrls, locatedServers, fileServer, filePath, fileMode, potentialNewServers);
    if (!status.IsOK ())
    {
      ss << "could not locate host for server " << fileServer << " and path " << filePath << " : " << status.GetErrorMessage ();
      errStr = ss.str ();
      return false;
    }

    ss.str ("");
    ss << "All the XROOTD servers available for the file " << fileServer << "\\\\" << filePath << " := ";
    for (auto it = locatedServers.begin (); it != locatedServers.end (); it++)
      ss << "|" << *it << "|  ";
    globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "GetRemoteServers : %s \n", ss.str ().c_str ());

    // sort the file servers
    SortAlongFirstVect (locatedServers, locatedUrls);

    ss.str ("");
    ss << "All the unfilteredServers XROOTD servers available for the file " << fileServer << "\\\\" << filePath << " := ";
    for (auto it = potentialNewServers.begin (); it != potentialNewServers.end (); it++)
      ss << "|" << *it << "|  ";
    globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "GetRemoteServers : %s \n", ss.str ().c_str ());
    ss.str ("");
    ss << "All the servers available for the file " << fileServer << "\\\\" << filePath << " := ";

    // among the xrootd file (FST's in eos case) servers keep only those which are also backend gridftp servers
    GetAvailableGsiInList (selectedServers, locatedServers, backend);

    ss.str ("");
    ss << "XROOTD/GRIDFTP servers for the file " << fileServer << "\\\\" << filePath << " := ";
    for (auto it = selectedServers.begin (); it != selectedServers.end (); it++)
      ss << "|" << *it << "|  ";
    globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "GetRemoteServers : %s \n", ss.str ().c_str ());
  }

  // if there is no server hosting a replica, just pick one randomly that will do the gateway
  if (selectedServers.empty ())
  {
    int oldstate;
    pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, &oldstate);
    backend->LockBackendServers ();
    selectedServers.push_back ((*(backend->GetActiveBackEnd ()))[rand () % (backend->GetActiveBackEnd ())->size ()].c_str ());
    backend->UnLockBackendServers ();
    pthread_setcancelstate (oldstate,NULL);

  }

  // if we are not doing a stripe read, keep only one server to do the gateway
  if (!(fileMode == XROOTD_FILEMODE_READING && isReplicaLayout) && (selectedServers.size () > 1))
  {
    auto randserv = selectedServers[rand () % selectedServers.size ()];
    selectedServers.clear ();
    selectedServers.push_back (randserv);
  }

  if (fileServer.size () && filePath.size ())
  {
    ss.str ("");
    ss << "Final servers for the file " << fileServer << "\\\\" << filePath << " := ";
  }
  else
    ss << "Final servers for the request := ";
  for (auto it = selectedServers.begin (); it != selectedServers.end (); it++)
    ss << "|" << *it << "|  ";
  globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "GetRemoteServers : %s \n", ss.str ().c_str ());

  return true;
}

void XrdUtils::HostId2Host (char * host_id, char * host)
{
  char * p;
  char * tmp = strdup (host_id);

  p = strrchr (tmp, ':');
  if (p) *p = '\0';

  host = strncpy (host, tmp, HOST_NAME_MAX);
  free (tmp);
}

/******************************************************************************/
/*                        R e n a m e T m p T o F i n a l                     */
/******************************************************************************/

XrdCl::XRootDStatus XrdUtils::RenameTmpToFinal (const std::string &temp_url, size_t suffix_size, bool useEosSpecifics)
{
  XrdCl::XRootDStatus ret;
  ret.status = XrdCl::stError;
  if (temp_url.size () > 0)
  {
    std::stringstream ss;
    XrdCl::URL url (temp_url.c_str ());
    XrdCl::FileSystem fs (url.GetProtocol () + std::string ("://") + url.GetHostId ());
    auto tmpname = url.GetPath ();
    auto finalname = tmpname.substr (0, tmpname.size () - suffix_size);
    globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "xrootd: moving temp file %s to final file %s on filesystem %s\n", tmpname.c_str (),
                            finalname.c_str (), url.GetHostName ().c_str ());
    XrdCl::StatInfo *si = NULL;
    XrdCl::XRootDStatus st;
    if (fs.Stat (finalname, si).IsOK ()) st = fs.Rm (finalname);
    if (si) delete si;
    if (!st.IsOK () && st.code != XrdCl::errNotFound)
    {
      ret.errNo = st.errNo;
      ss << "Error removing destination file" << finalname.c_str () << "for truncation : " << st.ToStr ().c_str ();
      XrdCl::XRootDStatus st = fs.Rm (tmpname);
      if (!st.IsOK ()) ss << "  AND  Error removing temporary file" << tmpname.c_str () << "for cleanup : " << st.ToStr ().c_str ();
      ret.SetErrorMessage (ss.str ().c_str ());
    }
    else
    {
      if (useEosSpecifics)
      {
        XrdOucString surl = (url.GetProtocol () + std::string ("://") + url.GetHostId ()).c_str ();
        surl += '/';
        surl += "/proc/user/?mgm.cmd=file&mgm.path=";
        surl += tmpname.c_str ();
        surl += "&mgm.subcmd=rename&mgm.file.source=";
        surl += tmpname.c_str ();
        surl += "&mgm.file.target=";
        surl += finalname.c_str ();
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "xrootd: eos command to rename is %s\n", surl.c_str ());
        XrdCl::OpenFlags::Flags flags_xrdcl = XrdCl::OpenFlags::Read;
        XrdCl::File* client = new XrdCl::File ();
        st = client->Open (surl.c_str (), flags_xrdcl);
      }
      else
      {
        XrdCl::XRootDStatus st = fs.Mv (tmpname, finalname);
      }
      if (!st.IsOK ())
      {
        ret.errNo = st.errNo;
        ss << "Error renaming temporary file" << tmpname.c_str () << " to its final name " << finalname.c_str () << " : "
            << st.ToStr ().c_str ();
        XrdCl::XRootDStatus st = fs.Rm (tmpname);
        if (!st.IsOK ()) ss << "  AND  Error removing temporary file" << tmpname.c_str () << "for cleanup : " << st.ToStr ().c_str ();
        ret.SetErrorMessage (ss.str ().c_str ());
      }
      else
        ret = XrdCl::XRootDStatus ();
    }
  }
  return ret;
}
