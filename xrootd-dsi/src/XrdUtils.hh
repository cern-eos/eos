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
//!        Part of the following code has been replicated and slightly modified
//!        from XRootD source code (mainly XrdCl and XrdPosix)
//!        The main goal is to get rid of the dependency on XrdPosix.
//------------------------------------------------------------------------------

extern "C" {
#include "globus_gridftp_server.h"
  typedef enum {
  	XROOTD_FILEMODE_NONE = 0,
	XROOTD_FILEMODE_READING,
	XROOTD_FILEMODE_WRITING,
	XROOTD_FILEMODE_TRUNCATE
  } globus_l_gfs_xrootd_filemode_t;
}
#include <vector>
#include <algorithm>
#include "XrdGsiBackendMapper.hh"
#include "XrdCl/XrdClStatus.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdOuc/XrdOucString.hh"

class XrootPath
{
public:

  void  CWD(const char *path);
  char *BuildURL(const char *path, char *buff, int blen);
  void GetServerList(std::vector<std::string> *listascextor, std::string *listasstring);

  static int   SplitURL(const char *url, char *server, char *path, int blen);
  const std::string & getParseErrStr() const { return pParseErrStr; } 

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

  std::string pParseErrStr;

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

namespace XrdUtils
{

  template<class Container>
    static void splitString (Container &result, const std::string &input, const std::string &delimiter)
    {
      size_t start = 0;
      size_t end = 0;
      size_t length = 0;

      do
      {
        end = input.find (delimiter, start);

        if (end != std::string::npos)
          length = end - start;
        else
          length = input.length () - start;

        if (length) result.push_back (input.substr (start, length));

        start = end + delimiter.size ();
      }
      while (end != std::string::npos);
    }

  XrdCl::XRootDStatus GetRemoteCheckSum (std::string &checkSum, const std::string &checkSumType, const std::string &server,
                                         const std::string &path);

  void HostId2Host (char * host_id, char * host);

  // locate a file using plain XRoot
  XrdCl::XRootDStatus LocateFileXrootd (std::vector<std::string> &urls, std::vector<std::string> &servers, const std::string &server,
                                        const std::string &path, globus_l_gfs_xrootd_filemode_t fileMode,
                                        std::vector<std::string>& unFilteredServerList);

  // issue an EOS command to the head node and get the output and the status
  XrdCl::XRootDStatus IssueEosCmd (XrdOucString &rstdout, const XrdOucString &sserver, const XrdOucString &command,
                                   const XrdOucString &opaque, bool admincmd = false);

  // locate a file using eos specifics (WARNING, EOS does not support plain XRoot location of a file!! This is the only supported method for EOS)
  XrdCl::XRootDStatus LocateFileEos (std::vector<std::string> &urls, std::vector<std::string> &servers, bool &isReplicaLayout,
                                     const std::string &sserver, const std::string &spath, globus_l_gfs_xrootd_filemode_t fileMode,
                                     std::vector<std::string>& unFilteredServerList);

  // get the list of Fst nodes (storage nodes). It requires the host of the current running gridftp server to be registered as a gateway.
  XrdCl::XRootDStatus ListFstEos (std::vector<std::string> &urls, const std::string &sserver);

  // sort a vector vec1 and apply the same sorting permutation to vec2 as well
  template<typename T>
    bool SortAlongFirstVect (std::vector<T>&vec1, std::vector<T>&vec2)
    {
      std::vector<std::pair<T, T> > vecp;
      assert(vec1.size () == vec2.size ());
      vecp.reserve (vec1.size ());

      for (size_t t = 0; t < vec1.size (); t++)
        vecp.push_back (std::make_pair (vec1[t], vec2[t]));

      std::sort (vecp.begin (), vecp.end ());

      for (size_t t = 0; t < vec1.size (); t++)
      {
        vec1[t] = vecp[t].first;
        vec2[t] = vecp[t].second;
      }
      return true;
    }

  // this expect sorted vectors as  inputs
  template<typename T>
    bool GetSortedIntersectIdx (std::vector<size_t>&indicesInV1, const std::vector<T> &v1, const std::vector<T>&v2)
    {
      //std::set_intersection(v1.begin(),v1.end(),v2.begin(),v2.end(),std::back_inserter(intersectVect));
      auto __first1 = v1.begin ();
      auto __last1 = v1.end ();
      size_t __idx1 = 0;
      auto __first2 = v2.begin ();
      auto __last2 = v2.end ();
      auto __result = std::back_inserter (indicesInV1);
      while (__first1 != __last1 && __first2 != __last2)
      {
        if (*__first1 < *__first2)
        {
          ++__first1;
          ++__idx1;
        }
        else if (*__first2 < *__first1)
          ++__first2;
        else
        {
          *__result = __idx1;
          {
            ++__first1;
            ++__idx1;
          }
          ++__first2;
          ++__result;
        }
      }
      return true;
    }

  inline bool GetAvailableGsiInList (std::vector<std::string>&availableGsiServers, const std::vector<std::string> &list,
                                     XrdGsiBackendMapper *backend)
  {
    backend->LockBackendServers ();
    auto *backendMap = backend->GetBackEndMap ();
    for (auto it = list.begin (); it != list.end (); ++it)
    {
      auto k = backend->Key (it->c_str ());
      auto itm = backendMap->lower_bound (k);
      if (itm != backendMap->end () && itm->first.compare (0, k.size (), k) == 0 && itm->second.gsiFtpAvailable)
        availableGsiServers.push_back (itm->first.c_str ());
    }
    backend->UnLockBackendServers ();

    return true;
  }

  bool GetRemoteServers (std::vector<std::string> &selectedServers, std::string &errStr, std::vector<std::string> &potentialNewServers,
                         XrdGsiBackendMapper * backend, const std::string &fileServer, std::string filePath,
                         const std::string &TruncationTmpFileSuffix, globus_l_gfs_xrootd_filemode_t accessType, bool useEosSpecifics);

  XrdCl::XRootDStatus RenameTmpToFinal (const std::string &temp_url, size_t suffix_size, bool useEosSpecifics);

}
#endif
