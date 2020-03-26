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
 * '!' - list all files where with non-zero cloneId different from <cloneid>
 * */
#include "common/FileId.hh"
#include "namespace/interface/IContainerMD.hh"
#include <string.h>

class _cloneFoundItem
{
public:
  eos::IContainerMD::id_t id;
  int depth;
  bool isContainer;

  _cloneFoundItem(eos::IContainerMD::id_t i, int d, bool cont) : id(i), depth(d),
    isContainer(cont) { };
};

/* curl encode string if needed */
static std::string
_clone_escape(std::string s)
{
  if (strpbrk(s.c_str(), " %") == NULL) {
    return s;  /* only use escape sequences when needed */
  }

  std::string t = eos::common::StringConversion::curl_default_escaped(s);
#ifdef notNeededThereAintNoSlashesInFilenames
  size_t pos = 0;

  while (pos = t.find("%2F", pos)) {
    t.replace(pos, 3, "/");
    pos += 1;
  }

#endif
  return (t);
}

static void
_cloneResp(XrdOucErrInfo& out_error, XrdOucString& stdErr,
           eos::common::VirtualIdentity& vid,
           std::list<_cloneFoundItem>& _found, bool json_output, FILE* fstdout)
{
  std::stack<std::string> pp;
  std::shared_ptr<eos::IContainerMD> cmd;
  int depth = 0;
  std::string p;
  eos::IContainerMD::tmtime_t stime;
  Json::Value j;
  Json::FastWriter jfw;

  if (! _found.empty()) {                                   /* first element is root of tree */
    p = gOFS->eosView->getUri(gOFS->eosDirectoryService->getContainerMD(
                                _found.front().id).get());
    pp.push(p.substr(0, p.rfind('/',
                                p.length() - 2) + 1)); /* "parent" path: /eos/a1/a2/ -> /eos/a1/ */
    pp.push(std::string("/eos/a1/dummy/"));               /* expect 1st element container @ depth 0, here's a dummy */
  }

  // typedef std::tuple<mode_t/*st_mode*/, id_t/*st_ino*/, int/*st_dev*/, int/*st_nlink*/, uid_t/*st_uid*/, gid_t/*st_gid*/,
  //        size_t/*st_size*/, double/*st_atime*/, double/*st_mtime*/, double/*st_ctime*/> s_tuple;
  char sts[2048];
  const char* sts_format = "(%d," /*st_mode*/
                           "%ld," /*st_ino*/ "%d," /*st_dev*/ "%d," /*st_nlink*/
                           "%ld," /*st_uid*/ "%ld,"/*st_gid*/ "%ld," /*st_size*/
                           "%9.7f," /*st_atime*/
                           "%9.7f," /*st_mtime*/
                           "%9.7f)" /*st_ctime*/;

  for (auto i : _found) {
    eos::IContainerMD::XAttrMap attrmap;

    if (i.isContainer) {
      try {
        cmd = gOFS->eosDirectoryService->getContainerMD(i.id);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
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

      if (json_output) {
        struct timespec ts;
        j.clear();
        j["n"] = pp.top();
        j["t"] = (Json::Value::UInt64) stime.tv_sec;
        j["c"] = (Json::Value::UInt64) cmd->getCloneId();
        j["T"] = cmd->getCloneFST();
        cmd->getMTime(ts);
        j["mt"] = (Json::Value::UInt64) ts.tv_sec;
        cmd->getCTime(ts);
        j["ct"] = (Json::Value::UInt64) ts.tv_sec;
        eos::listAttributes(gOFS->eosView, cmd.get(), attrmap, false);
        eos::IContainerMD::ctime_t ctime, mtime;
        cmd->getCTime(ctime);
        cmd->getMTime(mtime);
        snprintf(sts, sizeof(sts), sts_format,
                 cmd->getMode() | S_IFDIR, cmd->getId(), 42,
                 cmd->getNumFiles(), /*st_mode,st_ino,st_dev,st_nlink*/
                 cmd->getCUid(), cmd->getCGid(),
                 cmd->getNumContainers(),        /*st_uid,st_gid,st_size*/
                 0.0,                                                            /*st_atime*/
                 mtime.tv_sec + mtime.tv_nsec * 10E-9,
                 ctime.tv_sec + ctime.tv_nsec * 10E-9);
      } else {
        fprintf(fstdout, "%s %ld:%ld:%s\n", pp.top().c_str(), stime.tv_sec,
                cmd->getCloneId(), cmd->getCloneFST().c_str());
      }
    } else {    /* a file */
      std::shared_ptr<eos::IFileMD> fmd, gmd;
      uint64_t mdino = 0, hardlinkTgt = 0;

      try {
        gmd = gOFS->eosFileService->getFileMD(i.id);

        if (gmd->getName().substr(0, 13) == "...eos.ino...") {
          /* This is a zombie hard link target, kept around simply because another file points to it;
           * drop it from the dump - if that other file is backed up it'll get picked up again.
           */
          continue;
        }

        if (!gmd->hasAttribute(XrdMgmOfsFile::k_mdino)) {
          fmd = gmd;

          if (fmd->hasAttribute(XrdMgmOfsFile::k_nlink)) {
            /* a (no-zombie) target for hard link(s), goes into the log */
            hardlinkTgt = eos::common::FileId::FidToInode(fmd->getId());
          }
        } else {                                /* this is a hard link to another file */
          /*
           * for hard links:
           *    name is filled from the named file,
           *    time stamps, clone id, clone path, attributes from the hard link larget;
           *
           * on restore they could be fiddled back together over the clone_path;
           * from above: we do not report the zombie targets themselves
           */
          mdino = std::stoll(gmd->getAttribute(XrdMgmOfsFile::k_mdino));
          fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(mdino));
          eos_static_debug("hlnk switched from %s to file %s (%#llx)",
                           gmd->getName().c_str(), fmd->getName().c_str(), mdino);
        }
      } catch (eos::MDException& e) {
        eos_static_err("exception ec=%d emsg=\"%s\" dir %s id %#lx\n", e.getErrno(),
                       e.getMessage().str().c_str(), p.c_str());
        return;
      }

      gOFS->FuseXCastFile(fmd->getIdentifier());
      fmd->getSyncTime(stime);

      if (json_output) {
        char sbuff[256];
        struct timespec ts;
        j.clear();
        sprintf(sbuff, "%lx/%lx", cmd->getId(), fmd->getId());
        j["n"] = pp.top() + gmd->getName();                 // Name
        j["t"] = (Json::Value::UInt64) stime.tv_sec;        // time stamp
        j["c"] = (Json::Value::UInt64) fmd->getCloneId();   // cloneId
        j["T"] = fmd->getCloneFST();                        // tag
        j["p"] = sbuff;                                     // clone path

        if (mdino) {
          j["H"] = (Json::Value::UInt64)
                   mdino;  // a hard link alias: the mdino can be used to find the peers on restore
        }

        if (hardlinkTgt) {
          j["L"] = (Json::Value::UInt64)
                   hardlinkTgt;  // a hard link target: the inum can be used to find the peers on restore
        }

        if (fmd->isLink()) {
          j["S"] = fmd->getLink();                          // the target of the symlink
        }

        fmd->getMTime(ts);
        j["mt"] = (Json::Value::UInt64) ts.tv_sec;
        fmd->getCTime(ts);
        j["ct"] = (Json::Value::UInt64) ts.tv_sec;
        eos::listAttributes(gOFS->eosView, fmd.get(), attrmap, false);
        eos::IContainerMD::ctime_t ctime, mtime;
        cmd->getCTime(ctime);
        cmd->getMTime(mtime);
        size_t nlink = (attrmap.count("sys.eos.nlink") > 0) ? std::stol(
                         attrmap["sys.eos.nlink"]) : 1;
        snprintf(sts, sizeof(sts), sts_format,
                 fmd->getFlags() | S_IFREG, fmd->getId(), 42,
                 nlink,             /*st_mode,st_ino,st_dev,st_nlink*/
                 fmd->getCUid(), fmd->getCGid(),
                 fmd->getSize(),                 /*st_uid,st_gid,st_size*/
                 0.0,                                                            /*st_atime*/
                 mtime.tv_sec + mtime.tv_nsec * 10E-9,
                 ctime.tv_sec + ctime.tv_nsec * 10E-9);
      } else
        fprintf(fstdout, "%s%s %ld:%ld/%lx/%lx:%s\n", pp.top().c_str(),
                _clone_escape(gmd->getName()).c_str(),
                stime.tv_sec, fmd->getCloneId(), cmd->getId(), fmd->getId(),
                fmd->getCloneFST().c_str());
    }

    if (json_output) {
      Json::Value attr;

      for (auto it = attrmap.begin(); it != attrmap.end(); it++) {
        if (it->first == "sys.vtrace" ||
            it->first == XrdMgmOfsFile::k_mdino || it->first == XrdMgmOfsFile::k_nlink) {
          continue;
        }

        attr[it->first] = it->second;
      }

      j["attr"] = attr;
      j["st"] = sts;
      fprintf(fstdout, "%s", jfw.write(j).c_str());
    }
  }
};

static bool
_cloneMD(std::shared_ptr<eos::IContainerMD>& cloneMd, char cFlag,
         uint64_t cloneId, std::shared_ptr<eos::IContainerMD>& cmd)
{
  char buff[1024];
  snprintf(buff, sizeof(buff), "%s/clone/%ld", gOFS->MgmProcPath.c_str(),
           cloneId);
  std::string clonePath(buff);

  try {
    cloneMd = gOFS->eosView->getContainer(clonePath);

    if (cFlag == '+') {
      eos_static_err("clone directory %s already exists!", clonePath.c_str());
      return false;
    }
  } catch (eos::MDException& e) {
    eos_static_debug("clonePath %s exception ec=%d emsg=\"%s\" cFlag '%c'", buff,
                     e.getErrno(), e.getMessage().str().c_str(), cFlag);

    if (cFlag == '+' || cFlag == '-') {
      /* for '-': the clone directory may have been incorrectly removed, this should 
       * not prevent a cleanup */
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      eos::common::Path mdPath(buff);

      try {
        std::shared_ptr<eos::IContainerMD> pCloneMd = gOFS->eosView->getContainer(
              mdPath.GetParentPath());
        cloneMd = gOFS->eosView->createContainer(clonePath);
        cloneMd->setMode(S_IFDIR | S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        eos_static_info("%s permissions are %#o", clonePath.c_str(),
                        cloneMd->getMode());
        cloneMd->setAttribute("sys.clone.root",  gOFS->eosView->getUri(cmd.get()));
        gOFS->eosDirectoryService->updateStore(cloneMd.get());
        gOFS->eosDirectoryService->updateStore(pCloneMd.get());
        eos::ContainerIdentifier md_id = cloneMd->getIdentifier(); /* see "mkdir" */
        eos::ContainerIdentifier d_id = pCloneMd->getIdentifier();
        eos::ContainerIdentifier d_pid = pCloneMd->getParentIdentifier();
        lock.Release();
        gOFS->FuseXCastContainer(md_id);
        gOFS->FuseXCastContainer(d_id);
        gOFS->FuseXCastRefresh(d_id, d_pid);
      } catch (eos::MDException& e) {
        eos_static_err("cannot create the %s directory mode 755", clonePath.c_str());
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}

static int
_clone(std::shared_ptr<eos::IContainerMD>& cmd,
       XrdOucErrInfo& out_error, XrdOucString& stdErr,
       eos::common::VirtualIdentity& vid,
       std::list<_cloneFoundItem>& _found,
       char cFlag, uint64_t cloneId, time_t newId,
       std::shared_ptr<eos::IContainerMD> cloneMd, int depth)
{
  // cmd could almost be passed "by value", except for the "-" (purge) case; hence the cmd_ref passed by reference
  std::shared_ptr<eos::IContainerMD>
  ccmd;                          /* container pointer for recursion */
  int rc = SFS_OK;
  std::shared_ptr<eos::IFileMD> fmd;
  std::string link;
  eos::IContainerMD::tmtime_t stime;
  eos::common::RWMutexWriteLock rwlock;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  /* Only at depth 0: find/create clone anchor directory for operations that require it */
  if (cloneMd == NULL && cFlag != '?' && cFlag != '>') {
    if (! _cloneMD(cloneMd, cFlag, (cFlag == '+') ? newId : cloneId, cmd)) {
      return SFS_ERROR;
    }

    /* The eosViewRWMutex lock is explicitly grabbed (for '+') only at the "root" level of the tree and
     * "quickly" released and re-grabbed at each directory in lower levels. Hence at deeper
     * recursion levels the lock is already held on entry */
    if (cFlag == '+') {
      rwlock.Grab(gOFS->eosViewRWMutex);
    } else if (cFlag == '-' &&
               cloneMd->hasAttribute("sys.clone.root")) { /* reset start of purge */
      std::string rootDir = cloneMd->getAttribute("sys.clone.root");

      try {
        cmd = gOFS->eosView->getContainer(
                rootDir);             /* this only happens @ depth 0! */
        eos_static_info("clone %ld purge hint %s", cloneId, rootDir.c_str());
      } catch (eos::MDException& e) {
        eos_static_info("clone %ld root hint %s ignored ec=%d emsg='%s'",
                        cloneId, rootDir.c_str(), e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  if (cFlag == '+') {
    /* cloneId <= 9: special, single level markers
     * all others: this is a new clone, make the directory part of it
     */
    uint64_t thisId = newId, saveId;

    if (cloneId < 10) {
      thisId = cloneId;
      saveId = cmd->getCloneId();

      if (saveId >= 10) {
        saveId = 0;  /* only save an Id serving as marker */
      }

      cmd->setCloneFST(saveId ? std::to_string(saveId) :
                       "");  /* save this for later restore */
    }

    cmd->setCloneId(thisId);
    gOFS->eosDirectoryService->updateStore(cmd.get());

    if (cloneId <= 9) {
      return 0;
    }
  } else if (cFlag == '-' && (uint64_t)cmd->getCloneId() == cloneId) {
    /* clean the directory flag if it is part of this clone */
    std::string prev_marker =
      cmd->getCloneFST();                 /* reset cloneId to a potential previous marker */
    uint64_t cleanId = 0;

    if (!prev_marker.empty()) {
      cmd->setCloneFST("");
      cleanId = std::stol(prev_marker);
    }

    cmd->setCloneId(cleanId);
    gOFS->eosDirectoryService->updateStore(cmd.get());
  }

  if (cFlag != '!' or (cmd->getCloneId() != 0 and (uint64_t)cmd->getCloneId() != cloneId))
      _found.emplace_back(cmd->getId(), depth, true);   /* log this directory */

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("_found container %#lx depth %d %s cloneId=%d", cmd->getId(),
                     depth, cmd->getName().c_str(), cloneId);
  }

  for (auto fit = eos::FileMapIterator(cmd); fit.valid(); fit.next()) {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("%c depth %d file %s id %#lx", cFlag, depth, fit.key().c_str(),
                       fit.value());
    }

    try {
      fmd = gOFS->eosFileService->getFileMD(fit.value());
    } catch (eos::MDException& e) {
      char sbuff[1024];
      snprintf(sbuff, sizeof(sbuff), "msg=\"exception\" ec=%d fn=%s/%s emsg=\"%s\"\n",
               e.getErrno(), cmd->getName().c_str(), fit.key().c_str(),
               e.getMessage().str().c_str());
      eos_static_info(sbuff);
      stdErr += sbuff;
      stdErr += "\n";
      continue;
    }

    if (fmd->isLink()) {
      link = fmd->getLink();
    }

    fmd->getSyncTime(stime);

    switch (cFlag) {
    case '>':
      if ((uint64_t) stime.tv_sec < cloneId) {
        continue;
      }

    case '+':
      if ((uint64_t) stime.tv_sec < cloneId) {
        break;
      }

      fmd->setCloneId((uint64_t) newId);
      fmd->setCloneFST("");       /* clean clone fid */
      gOFS->eosFileService->updateStore(fmd.get());
      break;

    case '=':
    case '-':
      if (fmd->getCloneId() != cloneId) {
        continue;
      }

      if (cFlag == '-' && cloneId > 9) {
        gOFS->eosViewRWMutex.LockWrite();
        std::string hex_fid = fmd->getCloneFST();
        fmd->setCloneId(0);         /* clear cloneId */
        fmd->setCloneFST("");       /* clean up clone fid */
        gOFS->eosFileService->updateStore(fmd.get());
        gOFS->eosViewRWMutex.UnLockWrite();

        if (hex_fid != "") {
          eos::common::FileId::fileid_t clFid = eos::common::FileId::Hex2Fid(
                                                  hex_fid.c_str());

          try {
            std::shared_ptr<eos::IFileMD> gmd = gOFS->eosFileService->getFileMD(clFid);
            gOFS->_rem(gOFS->eosView->getUri(gmd.get()).c_str(), out_error, rootvid, "",
                       false, true, true, true);
          } catch (eos::MDException& e) {
            eos_static_info("msg=\"exception\" ec=%d fid=%#lx emsg=\"%s\"\n", e.getErrno(),
                            clFid, e.getMessage().str().c_str());
          }
        }

        continue;
      }
      break;

    case '!':
      if (fmd->getCloneId() == 0 || (uint64_t)fmd->getCloneId() == cloneId) continue;
      break;
       
    case '?':
      break;

    default: /* do something intelligent */
      ;
    }

    /* The output is produced in _cloneResp, outside the big lock */
    _found.emplace_back(fmd->getId(), depth, false);
  }

  for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
    if (cFlag == '+') {
      gOFS->eosViewRWMutex.UnLockWrite();
    }

    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
        dit.value());

    if (cFlag == '+') {
      gOFS->eosViewRWMutex.LockWrite();
    }

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
    uint64_t ccId = ccmd->getCloneId();         /* current container's cloneId */

    if (ccId == 0 or cloneId == 0 or cFlag == '+' or cFlag == '!' or
            ( (cFlag == '-' or cFlag == '=') && ccId == cloneId)
       ) {        /* Only descend for matching subdirs */
      int rc2 = _clone(ccmd, out_error, stdErr, vid, _found, cFlag, cloneId, newId,
                       cloneMd, depth + 1);

      if (rc2 > rc) {
        rc = rc2;
      }
    } else eos_static_debug("Not descending into did:%lld ccId %lld cFlag '%c'",
            ccmd->getId(), ccId, cFlag);
  }

  if (cloneMd != NULL && depth == 0 &&
      cFlag == '-') {         /* clean up clone directory */
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
            int sblen = snprintf(sbuff, sizeof(sbuff),
                                 "exception ec=%d emsg=\"%s\" cid %#lx %s fid %#lx %s\n",
                                 e.getErrno(), e.getMessage().str().c_str(), dit.value(),
                                 ccmd->getName().c_str(), fit.value(), fit.key().c_str());
            stdErr += sbuff;
            sbuff[sblen - 1] = '\0' /* no new-line */;
            eos_static_info(sbuff);
            files2zap.push_back(fit.key());
          }
        }

        for (auto it = files2remove.begin(); it != files2remove.end(); it++) {
          try {
            gOFS->_rem((*it).c_str(), out_error, rootvid, "",
                       false, true, true, true);
          } catch (eos::MDException& e) {
            eos_static_err("exception ec=%d emsg=\"%s\" cid %#lx uri %s\n", e.getErrno(),
                           e.getMessage().str().c_str(), dit.value(), (*it).c_str());
          }
        }

        for (auto it = files2zap.begin(); it != files2zap.end(); it++) {
          eos_static_info("zapping file %s in %s", it->c_str(), ccmd->getName().c_str());
          ccmd->removeFile(*it);
        }

        ctrs2remove.push_back(gOFS->eosView->getUri(ccmd.get()));
      } catch (eos::MDException& e) {
        ccmd.reset();
        eos_static_info("exception ec=%d emsg=\"%s\" cid %#lx name %s\n", e.getErrno(),
                        e.getMessage().str().c_str(), dit.value(), dit.key().c_str());
        ctrs2zap.push_back(dit.key());
        continue;
      }
    }

    for (auto it = ctrs2remove.begin(); it != ctrs2remove.end(); it++) {
      try {
        gOFS->eosView->removeContainer(*it);
      } catch (eos::MDException& e) {
        char sbuff[4096];
        int sblen = snprintf(sbuff, sizeof(sbuff),
                             "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(),
                             e.getMessage().str().c_str(), it->c_str());
        stdErr += sbuff;
        out_error.setErrInfo(e.getErrno(), sbuff);
        sbuff[sblen - 1] = '\0' /* no new-line */;
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
        int sblen = snprintf(sbuff, sizeof(sbuff),
                             "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(),
                             e.getMessage().str().c_str(), it->c_str());
        eos_static_debug(sbuff);
        out_error.setErrInfo(e.getErrno(), sbuff);
        sbuff[sblen - 1] = '\0' /* no new-line */;
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
      int sblen = snprintf(sbuff, sizeof(sbuff),
                           "exception ec=%d emsg=\"%s\" name %s\n", e.getErrno(),
                           e.getMessage().str().c_str(), cloneMd->getName().c_str());
      out_error.setErrInfo(e.getErrno(), sbuff);
      sbuff[sblen - 1] = '\0' /* no new-line */;
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
                 const char* filematch, bool take_lock, bool json_output, FILE* fstdout)
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
  bool sub_cmd_take_lock = false;

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

    if (strchr(">=?-+!", cFlag) == NULL) {
      return SFS_ERROR;  /* invalid argugment */
    }

    time_t clone_id = atol(val + 1);                /* could be 0 */

    if (limitresult) {
      return SFS_ERROR;
    }

    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
        Path.c_str());

    try {
      cmd = gOFS->eosView->getContainer(Path.c_str(), false);
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      cmd.reset();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }

    time_t newId = time(NULL);
    eos_static_info("sys.clone=%c%lld %s >%lld",
            cFlag, clone_id, Path.c_str(), newId);
    std::list<_cloneFoundItem> _found;
    int rc = _clone(cmd, out_error, stdErr, vid, _found, cFlag, clone_id, newId,
                    NULL, 0);  /* clone releases and re-acquires the eosViewRWMutex! */

    if (rc == 0) {
      _cloneResp(out_error, stdErr, vid, _found, json_output, fstdout);
    }

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

      if (take_lock) {
        ns_rd_lock.Release();
        sub_cmd_take_lock = true;
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
                                  (const char*) 0, attrmap, sub_cmd_take_lock)) {
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
                                   (const char*) 0, key, attr, sub_cmd_take_lock)) {
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
                    0, sub_cmd_take_lock)) == SFS_OK) &&
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
                0, sub_cmd_take_lock)) == SFS_OK)
      && (dir_exists == XrdSfsFileExistIsDirectory)) {
    eos::common::Path cPath(found_dirs[0][0].c_str());
    (void) found[found_dirs[0][0].c_str()].size();
  }

  if (nscounter) {
    EXEC_TIMING_END("Find");
  }

  return SFS_OK;
}
