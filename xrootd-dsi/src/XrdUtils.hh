#ifndef __XRDUTILS_HH__
#define __XRDUTILS_HH__
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
//! @file XrdUtils.hh
//! @author Geoffray Adde - CERN
//! @brief Some utility class and functions to help to use Xrd.
//!        Most of the following code has been replicated and slightly modified
//!        from XRootD source code (mainly XrdCl and XrdPosix)
//!        The main goal is to get rid of the dependency on XrdPosix.
//------------------------------------------------------------------------------

extern "C" {
#include "globus_gridftp_server.h"
}
#include "XrdCl/XrdClStatus.hh"

class XrootPath
{
public:

  void  CWD(const char *path);
  char *BuildURL(const char *path, char *buff, int blen);

  static int   SplitURL(const char *url, char *server, char *path, int blen);

  XrootPath();
  bool CheckVMP(char *errbuff, int errbufflen);
  ~XrootPath();

private:

  struct xpath
  {
    struct xpath *next;
    const  char  *server;
    int    servln;
    const  char  *path;
    int    plen;
    const  char  *nath;
    int    nlen;

    xpath(struct xpath *cur,
        const   char *pServ,
        const   char *pPath,
        const   char *pNath) : next(cur),
        server(pServ),
        servln(strlen(pServ)),
        path(pPath),
        plen(strlen(pPath)),
        nath(pNath),
        nlen(pNath ? strlen(pNath) : 0) {}
    ~xpath() {}
  };

  struct xpath *xplist;
  char         *pBase;
  char         *cwdPath;
  int           cwdPlen;
};

namespace XrootStatUtils {

void initStat(globus_gfs_stat_t *buf);
void initXdev(dev_t &st_dev, dev_t &st_rdev);
int mapFlagsXrd2Pos(int flags);
int mapFlagsPos2Xrd(int flags);
int mapModeXrd2Pos(mode_t mode);
int mapModePos2Xrd(mode_t mode);
int mapError(int rc);

};


namespace XrdUtils{

template<class Container>
static void splitString( Container         &result,
    const std::string &input,
    const std::string &delimiter )
{
  size_t start  = 0;
  size_t end    = 0;
  size_t length = 0;

  do
  {
    end = input.find( delimiter, start );

    if( end != std::string::npos )
      length = end - start;
    else
      length = input.length() - start;

    if( length )
      result.push_back( input.substr( start, length ) );

    start = end + delimiter.size();
  }
  while( end != std::string::npos );
}

XrdCl::XRootDStatus GetRemoteCheckSum( std::string       &checkSum,
    const std::string &checkSumType,
    const std::string &server,
    const std::string &path );

}
#endif
