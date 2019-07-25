// ----------------------------------------------------------------------
// File: Find.cc
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

// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------



//------------------------------------------------------------------------------
// Low-level recursive namespace clone handling
//------------------------------------------------------------------------------
/* cFlag (cloneId defaults to 0):
 * '>' - list files modified after <cloneId>
 * '=' - list files with clone-id <cloneId>
 * '-' - clean up clone-id <cloneId>,
 * '+' - clone if modified after after <cloneId>,
 * '?' - list all files/directories with cloneId/stime detail
 * */
#include "common/FileId.hh"
#include <string.h>
#include <curl/curl.h>

class _cloneFoundItem {
  public:
    id_t id;
    int depth;
    bool isContainer;

    _cloneFoundItem(id_t i, int d, bool cont) : id(i), depth(d), isContainer(cont) { };
};

/* prefix-less, stripped-down version of eos::common::StringConversion::curl_escaped */
static std::string
_clone_escape(std::string s) {

  if (strpbrk(s.c_str(), " %") == NULL) return s;           /* only use escape sequences when needed */

  static CURL *curlAnchor = NULL;
  if (curlAnchor == NULL) curlAnchor = curl_easy_init();

  char *esc = curl_easy_escape(curlAnchor, s.c_str(), s.size());
  std::string t(esc);
  curl_free(esc);

#ifdef notNeededThereAintNoSlashesInFilenames
  size_t pos = 0;
  while (pos = t.find("%2F", pos)) {
      t.replace(pos, 3, "/");
      pos += 1;
  }
#endif

  return(t);
}

static void
_cloneResp(std::string p, XrdOucErrInfo& out_error, XrdOucString& stdErr, eos::common::VirtualIdentity& vid,
            std::list<_cloneFoundItem>& _found, std::map<std::string, std::set<std::string> >& found) {
  std::stack<std::string> pp;
  std::shared_ptr<eos::IContainerMD> cmd;
  int depth = 0;
  char sbuff[4096];
  eos::IContainerMD::tmtime_t stime;

  pp.push(p.substr(0,p.rfind('/', p.length()-2)+1));  /* "parent" path: /eos/a1/a2/ -> /eos/a1/ */
  pp.push(std::string("/eos/a1/dummy/"));           /* expect 1st element container @ depth 0, here's a dummy */

  for (auto i: _found) {
    if (i.isContainer) {
      try {
        cmd = gOFS->eosDirectoryService->getContainerMD(i.id);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
        return;
      }

      while (i.depth <= depth) {            /* pop previous container(s) */
          pp.pop();
          depth--;
      };

      while (i.depth > depth) {
        pp.push(pp.top() + _clone_escape(cmd->getName()) + "/");
        depth++;
      }


      cmd->getTMTime(stime);
      snprintf(sbuff, sizeof(sbuff), " %ld:%ld:%s", stime.tv_sec, cmd->getCloneId(), cmd->getCloneFST().c_str());

      found[pp.top()].insert(sbuff);
    } else {
      std::shared_ptr<eos::IFileMD> fmd;

      try {
        fmd = gOFS->eosFileService->getFileMD(i.id);
      } catch (eos::MDException& e) {
        eos_static_err("exception ec=%d emsg=\"%s\" dir %s id %#lx\n", e.getErrno(), e.getMessage().str().c_str(), p.c_str());
        return;
      }

      gOFS->FuseXCastFile(fmd->getIdentifier());
      fmd->getSyncTime(stime);
      snprintf(sbuff, sizeof(sbuff)-1, " %ld:%ld/%lx/%lx:%s",
               stime.tv_sec, fmd->getCloneId(), cmd->getId(), fmd->getId(), fmd->getCloneFST().c_str());

      found[pp.top() + _clone_escape(fmd->getName())].insert(sbuff);
    }
  }
};

static bool
_cloneMD(std::shared_ptr<eos::IContainerMD>& cloneMd, char cFlag, uint64_t cloneId) {

  char buff[1024];

  snprintf(buff, sizeof(buff), "%s/clone/%ld", gOFS->MgmProcPath.c_str(), cloneId);
  std::string clonePath(buff);
    
  try {
    cloneMd = gOFS->eosView->getContainer(clonePath);

    if (cFlag == '+') {
      eos_static_err("clone directory %s already exists!", clonePath.c_str());
      return false;
    }

  } catch (eos::MDException& e) {
    eos_static_debug("exception ec=%d emsg=\"%s\" dir %s\n", e.getErrno(), e.getMessage().str().c_str(), clonePath.c_str());
    if (cFlag == '+') {
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      try {
        cloneMd = gOFS->eosView->createContainer(clonePath, true);
        cloneMd->setMode(S_IFDIR | S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        eos_static_info("%s permissions are %#o", clonePath.c_str(), cloneMd->getMode());
        std::shared_ptr<eos::IContainerMD> pCloneMd = gOFS->eosDirectoryService->getContainerMD(cloneMd->getParentId());
        gOFS->eosDirectoryService->updateStore(cloneMd.get());
        gOFS->eosDirectoryService->updateStore(pCloneMd.get());
        lock.Release();
        gOFS->FuseXCastContainer(cloneMd->getIdentifier());
        gOFS->FuseXCastContainer(pCloneMd->getIdentifier());
        gOFS->FuseXCastRefresh(pCloneMd->getIdentifier(), pCloneMd->getParentIdentifier());
      } catch (eos::MDException& e) {
        eos_static_err("cannot create the %s directory mode 755", clonePath.c_str());
        return false;
      }
    }
  }
  return true;
}

static int
_clone(std::shared_ptr<eos::IContainerMD> cmd, XrdOucErrInfo& out_error,
                 XrdOucString& stdErr, eos::common::VirtualIdentity& vid,
                 std::list<_cloneFoundItem>& _found,
                 char cFlag, uint64_t cloneId, time_t newId, std::shared_ptr<eos::IContainerMD> cloneMd, int depth)
{
  std::shared_ptr<eos::IContainerMD> ccmd;
  int rc = SFS_OK;

  if (EOS_LOGS_DEBUG) eos_static_debug("_found container %#lx depth %d %s cloneId=%d", cmd->getId(), depth, cmd->getName().c_str(), cloneId);
  _found.emplace_back(cmd->getId(), depth, true);


  if (cFlag == '+') {
      /* cloneId <= 9: special, single level markers
       * all others: this is a new clone, make the directory part of it
       */
      uint64_t thisId = newId, saveId;
      if (cloneId < 10) {
          thisId = cloneId;
          saveId = cmd->getCloneId();
          if (saveId >= 10) saveId = 0;                             /* only save an Id serving as marker */

          cmd->setCloneFST(saveId ? std::to_string(saveId) : "");  /* save this for later restore */
      }
      cmd->setCloneId(thisId);
      gOFS->eosDirectoryService->updateStore(cmd.get());
      if (cloneId <= 9) return 0;
  } else if (cFlag == '-' && (uint64_t)cmd->getCloneId() == cloneId) {
      /* clean the directory flag if it is part of this clone */
      std::string prev_marker = cmd->getCloneFST();                 /* reset cloneId to a potential previous marker */
      uint64_t cleanId = 0;
      if (!prev_marker.empty()) {
          cmd->setCloneFST("");
          cleanId = std::stol(prev_marker);
      }
      cmd->setCloneId(cleanId);
      gOFS->eosDirectoryService->updateStore(cmd.get());
  }



  std::shared_ptr<eos::IFileMD> fmd;
  std::string link;
  eos::IContainerMD::tmtime_t stime;

  eos::common::RWMutexWriteLock rwlock;
  
  /* find/create clone anchor directory for operations that require it */
  if (cloneMd == NULL && cFlag != '?' && cFlag != '>') {
    if (! _cloneMD(cloneMd, cFlag, (cFlag == '+') ? newId : cloneId))
        return SFS_ERROR;

    /* The eosViewRWMutex lock is expicitely grabbed (for '+') only at the "root" level of the tree and
     * "quickly" released and re-grabbed at each directory in lower levels. Hence at deeper
     * recursion levels the lock is already held on entry */
    if (cFlag == '+') rwlock.Grab(gOFS->eosViewRWMutex);
  }


  for (auto fit = eos::FileMapIterator(cmd); fit.valid(); fit.next()) {
    if (EOS_LOGS_DEBUG) eos_static_debug("%c depth %d file %s id %#lx", cFlag, depth, fit.key().c_str(), fit.value());

    try {
      fmd = gOFS->eosFileService->getFileMD(fit.value());
    } catch (eos::MDException& e) {
      char sbuff[1024];
      snprintf(sbuff, sizeof(sbuff),"msg=\"exception\" ec=%d fn=%s/%s emsg=\"%s\"\n", e.getErrno(), cmd->getName().c_str(), fit.key().c_str(), e.getMessage().str().c_str());
      eos_static_info(sbuff);
      stdErr += sbuff; stdErr += "\n";
      continue;
    }

    if (fmd->isLink()) {
      link = fmd->getLink();
      //found[cPath].insert(fname+" -> "+link);
      //???
    }

    fmd->getSyncTime(stime);

    switch(cFlag) {
      case '>':
        if ((uint64_t) stime.tv_sec < cloneId) continue;
      case '+':
        if ((uint64_t) stime.tv_sec < cloneId) break;
        fmd->setCloneId((uint64_t) newId);
        fmd->setCloneFST("");       /* clean clone fid */
        gOFS->eosFileService->updateStore(fmd.get());
        break;
      case '=':
      case '-':
        if (fmd->getCloneId() != cloneId) continue;
        if (cFlag == '-' && cloneId > 9) {
          gOFS->eosViewRWMutex.LockWrite();
          std::string hex_fid = fmd->getCloneFST();
          fmd->setCloneId(0);         /* clear cloneId */
          fmd->setCloneFST("");       /* clean up clone fid */
          gOFS->eosFileService->updateStore(fmd.get());
          gOFS->eosViewRWMutex.UnLockWrite();

          if (hex_fid != "") {
            eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
            eos::common::FileId::fileid_t clFid = eos::common::FileId::Hex2Fid(hex_fid.c_str());
            try {
              std::shared_ptr<eos::IFileMD> gmd = gOFS->eosFileService->getFileMD(clFid);
              int rc = gOFS->_rem(gOFS->eosView->getUri(gmd.get()).c_str(), out_error, rootvid, "", false, true, true, true);
            } catch (eos::MDException& e) {
              eos_static_info("msg=\"exception\" ec=%d Fid %#lx emsg=\"%s\"\n", e.getErrno(), clFid, e.getMessage().str().c_str());
            }
          }
          continue;
        }
      case '?':
        break;
      default: /* do something intelligent */
        ;
    }

    /* The output is produced in _cloneResp, outside the big lock */
    _found.emplace_back(fmd->getId(), depth, false);
  }

  for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {

    if (cFlag == '+') gOFS->eosViewRWMutex.UnLockWrite();
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, dit.value());
    if (cFlag == '+') gOFS->eosViewRWMutex.LockWrite();

    try {
      ccmd = gOFS->eosDirectoryService->getContainerMD(dit.value());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      ccmd.reset();
      eos_static_info("msg=\"exception\" ec=%d cid %#lx emsg=\"%s\"\n",
                e.getErrno(), dit.value(), e.getMessage().str().c_str());
      continue;
    }

    ccmd->getTMTime(stime);

    /* if (cFlag == '?' && stime.tv_sec < cloneId) continue;     only if stime reliably percolates down to the root */

    uint64_t ccId = ccmd->getCloneId();

    if (ccId == 0 || cloneId == 0 || cFlag == '+' ||
            ( (cFlag == '=' || cFlag == '-') && ccId == cloneId)  ) {     /* Only descend for matching subdirs */
      int rc2 = _clone(ccmd, out_error, stdErr, vid, _found, cFlag, cloneId, newId, cloneMd, depth+1);
      if (rc2 > rc) rc = rc2;
    }

  }

  if ( cloneMd != NULL && depth == 0 && cFlag == '-') {        /* clean up clone directory */
    std::list<std::string> ctrs2remove;
    std::list<std::string> ctrs2zap;
    for (auto dit = eos::ContainerMapIterator(cloneMd); dit.valid(); dit.next()) {
      try {
        ccmd = gOFS->eosDirectoryService->getContainerMD(dit.value());
        std::list<std::string> files2remove;
        std::list<std::string> files2zap;
        for (auto fit = eos::FileMapIterator(ccmd); fit.valid(); fit.next()) {
          try {
            fmd = gOFS->eosFileService->getFileMD(fit.value());
            files2remove.push_back(gOFS->eosView->getUri(fmd.get()));
          } catch (eos::MDException& e) {
            char sbuff[1024];
            int sblen = snprintf(sbuff, sizeof(sbuff), "exception ec=%d emsg=\"%s\" cid %#lx %s fid %#lx %s\n",
                    e.getErrno(), e.getMessage().str().c_str(), dit.value(), ccmd->getName().c_str(), fit.value(), fit.key().c_str());
            stdErr += sbuff;
            sbuff[sblen-1] = '\0' /* no new-line */;
            eos_static_info(sbuff);
            files2zap.push_back(fit.key());
          }
        }
        for (auto it = files2remove.begin(); it != files2remove.end(); it++) {
          try {
            gOFS->eosView->unlinkFile(*it);
          } catch (eos::MDException& e) {
            eos_static_err("exception ec=%d emsg=\"%s\" cid %#lx uri %s\n", e.getErrno(), e.getMessage().str().c_str(), dit.value(), (*it).c_str());
          }
        }
        for (auto it = files2zap.begin(); it != files2zap.end(); it++) {
          eos_static_info("zapping file %s in %s", it->c_str(), ccmd->getName());
          ccmd->removeFile(*it);
        }
        ctrs2remove.push_back(gOFS->eosView->getUri(ccmd.get()));
      } catch (eos::MDException& e) {
        ccmd.reset();
        eos_static_info("exception ec=%d emsg=\"%s\" cid %#lx name %s\n", e.getErrno(), e.getMessage().str().c_str(), dit.value(), dit.key().c_str());
        ctrs2zap.push_back(dit.key());
        continue;
      }
    }

    for (auto it = ctrs2remove.begin(); it != ctrs2remove.end(); it++) {
      try {
        gOFS->eosView->removeContainer(*it);
      } catch (eos::MDException& e) {
        char sbuff[4096];
        int sblen = snprintf(sbuff, sizeof(sbuff), "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(), e.getMessage().str().c_str(), it->c_str());
        stdErr += sbuff;
        out_error.setErrInfo(e.getErrno(), sbuff);
        sbuff[sblen-1] = '\0' /* no new-line */;
        eos_static_info(sbuff);
        return SFS_ERROR;
      }
    }
    for (auto it = ctrs2zap.begin(); it != ctrs2zap.end(); it++) {
      eos_static_info("zapping %s", (*it).c_str());
      try {
        cloneMd->removeContainer(*it);
        gOFS->eosDirectoryService->updateStore(cloneMd.get());
      } catch (eos::MDException& e) {
        char sbuff[4096];
        int sblen = snprintf(sbuff, sizeof(sbuff), "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(), e.getMessage().str().c_str(), it->c_str());
        eos_static_debug(sbuff);
        out_error.setErrInfo(e.getErrno(), sbuff);
        sbuff[sblen-1] = '\0' /* no new-line */;
        eos_static_info(sbuff);
        return SFS_ERROR;
      }
    }

    try {
      std::string cname = cloneMd->getName();
      eos::ContainerIdentifier cloneDir = cloneMd->getParentIdentifier();
      gOFS->eosView->removeContainer(gOFS->eosView->getUri(cloneMd.get()));
      gOFS->FuseXCastDeletion(cloneDir, cname);
    } catch (eos::MDException& e) {
      char sbuff[4096];
      int sblen = snprintf(sbuff, sizeof(sbuff), "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(), e.getMessage().str().c_str(), cloneMd->getName().c_str());
      out_error.setErrInfo(e.getErrno(), sbuff);
      sbuff[sblen-1] = '\0' /* no new-line */;
      eos_static_info(sbuff);
      return SFS_ERROR;
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Low-level namespace find command
//------------------------------------------------------------------------------
int
XrdMgmOfs::_find(const char* path, XrdOucErrInfo& out_error,
                 XrdOucString& stdErr, eos::common::VirtualIdentity& vid,
                 std::map<std::string, std::set<std::string> >& found,
                 const char* key, const char* val, bool no_files,
                 time_t millisleep, bool nscounter, int maxdepth,
                 const char* filematch, bool take_lock)
{
  std::vector< std::vector<std::string> > found_dirs;
  std::shared_ptr<eos::IContainerMD> cmd;
  std::string Path = path;
  EXEC_TIMING_BEGIN("Find");

  if (nscounter) {
    gOFS->MgmStats.Add("Find", vid.uid, vid.gid, 1);
  }

  if (*Path.rbegin() != '/') {
    Path += '/';
  }

  errno = 0;
  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = Path.c_str();
  int deepness = 0;
  // Users cannot return more than 100k files and 50k dirs with one find,
  // unless there is an access rule allowing deeper queries
  static uint64_t dir_limit = 50000;
  static uint64_t file_limit = 100000;
  Access::GetFindLimits(vid, dir_limit, file_limit);
  uint64_t filesfound = 0;
  uint64_t dirsfound = 0;
  bool limitresult = false;
  bool limited = false;

  if ((vid.uid != 0) && (!vid.hasUid(3)) && (!vid.hasGid(4)) && (!vid.sudoer)) {
    limitresult = true;
  }

  bool isClone = (key != NULL) && (strcmp(key, "sys.clone") == 0);

  if (isClone) {
    /* sys.clone==<flag><id>, flag ~= [>=?-+], id ~= \d+ (a numeric timestamp)
     * flag: '>' list files modified after <id>, '=' list files with clone-id <id>
     * '-' = clean up clone-id <id>, '+' - reclone after <id>,
     * '?' = list all with clone-id and stime data */
    char cFlag = val[0];

    if (strchr(">=?-+", cFlag) == NULL) return SFS_ERROR;   /* invalid argugment */

    time_t clone_id = atol(val+1);                  /* could be 0 */

    if (limitresult) return SFS_ERROR;

    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, Path.c_str());
    try {
      cmd = gOFS->eosView->getContainer(Path.c_str(), false);
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      cmd.reset();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }

    time_t newId = time(NULL);
    std::list<_cloneFoundItem> _found;

    int rc = _clone(cmd, out_error, stdErr, vid, _found, cFlag, clone_id, newId, NULL, 0);  /* clone releases and re-acquires the eosViewRWMutex! */
    if (rc == 0) 
      _cloneResp(Path, out_error, stdErr, vid, _found, found);

    return rc;
  }

  do {
    bool permok = false;
    found_dirs.resize(deepness + 2);

    // Loop over all directories in that deepness
    for (unsigned int i = 0; i < found_dirs[deepness].size(); i++) {
      Path = found_dirs[deepness][i].c_str();
      eos_static_debug("Listing files in directory %s", Path.c_str());

      // Slow down the find command without holding locks
      if (millisleep) {
        std::this_thread::sleep_for(std::chrono::milliseconds(millisleep));
      }

      // Held only for the current loop
      eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
          Path.c_str());
      eos::common::RWMutexReadLock ns_rd_lock;

      if (take_lock) {
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }

      try {
        cmd = gOFS->eosView->getContainer(Path.c_str(), false);
        permok = cmd->access(vid.uid, vid.gid, R_OK | X_OK);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        cmd.reset();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }

      if (!gOFS->allow_public_access(Path.c_str(), vid)) {
        stdErr += "error: public access level restriction - no access in  ";
        stdErr += Path.c_str();
        stdErr += "\n";
        continue;
      }

      if (cmd) {
        if (!permok) {
          // check-out for ACLs
          permok = _access(Path.c_str(), R_OK | X_OK, out_error, vid, "",
                           false) ? false : true;
        }

        if (!permok) {
          stdErr += "error: no permissions to read directory ";
          stdErr += Path.c_str();
          stdErr += "\n";
          continue;
        }

        // Add all children into the 2D vectors
        for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
          std::string fpath = Path.c_str();
          fpath += dit.key();
          fpath += "/";

          // check if we select by tag
          if (key) {
            XrdOucString wkey = key;

            if (wkey.find("*") != STR_NPOS) {
              // this is a search for 'beginswith' match
              eos::IContainerMD::XAttrMap attrmap;

              if (!gOFS->_attr_ls(fpath.c_str(), out_error, vid,
                                  (const char*) 0, attrmap, !take_lock)) {
                for (auto it = attrmap.begin(); it != attrmap.end(); it++) {
                  XrdOucString akey = it->first.c_str();

                  if (akey.matches(wkey.c_str())) {
                    // Trick to add element
                    (void)found[fpath].size();
                  }
                }
              }

              found_dirs[deepness + 1].push_back(fpath.c_str());
            } else {
              // This is a search for a full match or a key search
              std::string sval = val;
              XrdOucString attr = "";

              if (!gOFS->_attr_get(fpath.c_str(), out_error, vid,
                                   (const char*) 0, key, attr, !take_lock)) {
                found_dirs[deepness + 1].push_back(fpath.c_str());

                if ((val == std::string("*")) || (attr == val)) {
                  (void)found[fpath].size();
                }
              }
            }
          } else {
            if (limitresult) {
              // Apply  user limits for non root/admin/sudoers
              if (dirsfound >= dir_limit) {
                stdErr += "warning: find results are limited for you to ndirs=";
                stdErr += (int) dir_limit;
                stdErr += " -  result is truncated!\n";
                limited = true;
                break;
              }
            }

            found_dirs[deepness + 1].push_back(fpath.c_str());
            (void)found[fpath].size();
            dirsfound++;
          }
        }

        if (!no_files) {
          std::string link;
          std::string fname;
          std::shared_ptr<eos::IFileMD> fmd;

          for (auto fit = eos::FileMapIterator(cmd); fit.valid(); fit.next()) {
            fname = fit.key();

            try {
              fmd = cmd->findFile(fname);
            } catch (eos::MDException& e) {
              fmd = 0;
            }

            if (fmd) {
              // Skip symbolic links
              if (fmd->isLink()) {
                link = fmd->getLink();
              } else {
                link.clear();
              }

              if (limitresult) {
                // Apply user limits for non root/admin/sudoers
                if (filesfound >= file_limit) {
                  stdErr += "warning: find results are limited for you to nfiles=";
                  stdErr += (int) file_limit;
                  stdErr += " -  result is truncated!\n";
                  limited = true;
                  break;
                }
              }

              if (!filematch) {
                if (link.length()) {
                  std::string ip = fname;
                  ip += " -> ";
                  ip += link;
                  found[Path].insert(ip);
                } else {
                  found[Path].insert(fname);
                }

                filesfound++;
              } else {
                XrdOucString name = fname.c_str();

                if (name.matches(filematch)) {
                  found[Path].insert(fname);
                  filesfound++;
                }
              }
            }
          }
        }
      }

      if (limited) {
        break;
      }
    }

    deepness++;

    if (limited) {
      break;
    }
  } while (found_dirs[deepness].size() && ((!maxdepth) || (deepness < maxdepth)));

  if (!no_files) {
    // If the result is empty, maybe this was a find by file
    if (!found.size()) {
      XrdSfsFileExistence file_exists;

      if (((_exists(Path.c_str(), file_exists, out_error, vid,
                    0, take_lock)) == SFS_OK) &&
          (file_exists == XrdSfsFileExistIsFile)) {
        eos::common::Path cPath(Path.c_str());
        found[cPath.GetParentPath()].insert(cPath.GetName());
      }
    }
  }

  // Include also the directory which was specified in the query if it is
  // accessible and a directory since it can evt. be missing if it is empty
  XrdSfsFileExistence dir_exists;

  if (((_exists(found_dirs[0][0].c_str(), dir_exists, out_error, vid,
                0, take_lock)) == SFS_OK)
      && (dir_exists == XrdSfsFileExistIsDirectory)) {
    eos::common::Path cPath(found_dirs[0][0].c_str());
    (void) found[found_dirs[0][0].c_str()].size();
  }

  if (nscounter) {
    EXEC_TIMING_END("Find");
  }

  return SFS_OK;
}
