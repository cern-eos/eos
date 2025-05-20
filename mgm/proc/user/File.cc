// ----------------------------------------------------------------------
// File: proc/user/File.cc
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"
#include "mgm/Macros.hh"
#include "mgm/Policy.hh"
#include "mgm/Stat.hh"
#include "mgm/convert/ConverterEngine.hh"
#include "mgm/convert/ConversionTag.hh"
#include "mgm/XattrLock.hh"
#include "mgm/Constants.hh"
#include "common/Utils.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
#include "common/SecEntity.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/Resolver.hh"
#include <XrdCl/XrdClCopyProcess.hh>
#include <math.h>
#include <memory>

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::File()
{
  XrdOucString spath = "";
  XrdOucString spathid = pOpaque->Get("mgm.file.id");
  eos::IFileMD::id_t fid = 0ull;

  if (spathid.length()) {
    try {
      fid = std::strtoull(spathid.c_str(), nullptr, 10);
    } catch (...) {
      stdErr = "error: given file identifier is not numeric";
      retc = EINVAL;
      return SFS_OK;
    }

    std::string err_msg;
    std::string lpath;

    if (GetPathFromFid(lpath, fid, err_msg)) {
      stdErr = err_msg.c_str();
    }

    spath = lpath.c_str();
  } else {
    spath = pOpaque->Get("mgm.path");
  }

  const char* inpath = spath.c_str();

  if (!inpath) {
    inpath = "";
  }

  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  spath = path;
  bool cmdok = false;

  if (!spath.length() && !fid && (mSubCmd != "drop")) {
    stdErr = "error: you have to give a path name to call 'file'";
    retc = EINVAL;
    return SFS_OK;
  } else {
    // --------------------------------------------------------------------------
    // drop a replica referenced by filesystem id
    // --------------------------------------------------------------------------
    if (mSubCmd == "drop") {
      cmdok = true;
      XrdOucString sfsid = pOpaque->Get("mgm.file.fsid");
      XrdOucString sforce = pOpaque->Get("mgm.file.force");
      bool forceRemove = false;

      if (sforce.length() && (sforce == "1")) {
        forceRemove = true;
      }

      unsigned long fsid = (sfsid.length()) ? strtoul(sfsid.c_str(), 0, 10) : 0;
      unsigned long fid = (spathid.length()) ? strtoul(spathid.c_str(), 0, 10) : 0;

      if (gOFS->_dropstripe(spath.c_str(), fid, *mError, *pVid, fsid, forceRemove)) {
        stdErr += "error: unable to drop stripe";
        retc = errno;
      } else {
        stdOut += "success: dropped stripe on fs=";
        stdOut += (int) fsid;
      }
    }

    // -------------------------------------------------------------------------
    // change the number of stripes for files with replica layout
    // -------------------------------------------------------------------------
    if (mSubCmd == "layout") {
      cmdok = true;
      XrdOucString stripes = pOpaque->Get("mgm.file.layout.stripes");
      XrdOucString cksum = pOpaque->Get("mgm.file.layout.checksum");
      XrdOucString layout = pOpaque->Get("mgm.file.layout.type");
      int checksum_type = eos::common::LayoutId::kNone;
      XrdOucString ne = "eos.layout.checksum=";
      ne += cksum;
      XrdOucEnv env(ne.c_str());
      int newstripenumber = 0;
      std::string newlayoutstring;

      if (stripes.length()) {
        newstripenumber = atoi(stripes.c_str());
      }

      if (layout.length()) {
        newlayoutstring = layout.c_str();
      }

      if (!stripes.length() && !cksum.length() && !newlayoutstring.length()) {
        stdErr = "error: you have to give a valid number of stripes"
                 " as an argument to call 'file layout' or a valid checksum or a layout id";
        retc = EINVAL;
      } else if (stripes.length() &&
                 ((newstripenumber < 1) ||
                  (newstripenumber > 255))) {
        stdErr = "error: you have to give a valid number of stripes"
                 " as an argument to call 'file layout'";
        retc = EINVAL;
      } else if (cksum.length() &&
                 ((checksum_type = eos::common::LayoutId::GetChecksumFromEnv(
                                     env)) == eos::common::LayoutId::kNone)) {
        stdErr = "error: you have to give a valid checksum typ0e"
                 " as an argument to call 'file layout'";
        retc = EINVAL;
      } else {
        // only root can do that
        if (pVid->uid == 0) {
          std::shared_ptr<eos::IFileMD> fmd;
          eos::common::RWMutexWriteLock viewWriteLock;

          if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
            WAIT_BOOT;
            unsigned long long fid = Resolver::retrieveFileIdentifier(
                                       spath).getUnderlyingUInt64();
            // reference by fid+fsid
            //-------------------------------------------
            viewWriteLock.Grab(gOFS->eosViewRWMutex);

            try {
              fmd = gOFS->eosFileService->getFileMD(fid);
            } catch (eos::MDException& e) {
              errno = e.getErrno();
              stdErr = "error: cannot retrieve file meta data - ";
              stdErr += e.getMessage().str().c_str();
              eos_debug("caught exception %d %s\n",
                        e.getErrno(),
                        e.getMessage().str().c_str());
            }
          } else {
            // -----------------------------------------------------------------
            // reference by path
            // ----------------------------------------------------------------
            viewWriteLock.Grab(gOFS->eosViewRWMutex);

            try {
              fmd = gOFS->eosView->getFile(spath.c_str());
            } catch (eos::MDException& e) {
              errno = e.getErrno();
              stdErr = "error: cannot retrieve file meta data - ";
              stdErr += e.getMessage().str().c_str();
              eos_debug("caught exception %d %s\n",
                        e.getErrno(),
                        e.getMessage().str().c_str());
            }
          }

          if (fmd) {
            bool only_replica = false;
            bool only_tape = false;
            bool any_layout = false;

            if (fmd->getNumLocation() > 0) {
              only_replica = true;
            } else {
              any_layout = true;
            }

            if (fmd->getNumLocation() == 1) {
              if (fmd->hasLocation(EOS_TAPE_FSID)) {
                only_tape = true;
              }
            }

            if (!cksum.length()) {
              checksum_type = eos::common::LayoutId::GetChecksum(fmd->getLayoutId());
            }

            if (!newstripenumber) {
              newstripenumber = eos::common::LayoutId::GetStripeNumber(
                                  fmd->getLayoutId()) + 1;
            }

            int lid = eos::common::LayoutId::kReplica;
            unsigned long newlayout =
              eos::common::LayoutId::GetId(lid,
                                           checksum_type,
                                           newstripenumber,
                                           eos::common::LayoutId::GetBlocksizeType(fmd->getLayoutId())
                                          );

            if (newlayoutstring.length()) {
              newlayout = strtol(newlayoutstring.c_str(), 0, 16);
            }

            if ((only_replica &&
                 (((eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                    eos::common::LayoutId::kReplica) ||
                   (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                    eos::common::LayoutId::kPlain)) &&
                  (eos::common::LayoutId::GetLayoutType(newlayout) ==
                   eos::common::LayoutId::kReplica))) || only_tape || any_layout) {
              fmd->setLayoutId(newlayout);
              stdOut += "success: setting layout to ";
              stdOut += eos::common::LayoutId::PrintLayoutString(newlayout).c_str();
              stdOut += " for path=";
              stdOut += spath;
              // commit new layout
              gOFS->eosView->updateFileStore(fmd.get());
            } else {
              retc = EPERM;
              stdErr = "error: you can only change the number of "
                       "stripes for files with replica layout or files without locations";
            }
          } else {
            retc = errno;
            stdErr += "error: no such file";
          }

          viewWriteLock.Release();
          //-------------------------------------------
        } else {
          retc = EPERM;
          stdErr = "error: you have to take role 'root' to execute this command";
        }
      }
    }

    // -------------------------------------------------------------------------
    // verify checksum, size for files issuing an asynchronous verification req
    // -------------------------------------------------------------------------
    if (mSubCmd == "verify") {
      cmdok = true;
      XrdOucString option = "";
      XrdOucString computechecksum = pOpaque->Get("mgm.file.compute.checksum");
      XrdOucString commitchecksum = pOpaque->Get("mgm.file.commit.checksum");
      XrdOucString commitsize = pOpaque->Get("mgm.file.commit.size");
      XrdOucString commitfmd = pOpaque->Get("mgm.file.commit.fmd");
      XrdOucString verifyrate = pOpaque->Get("mgm.file.verify.rate");
      XrdOucString sendresync = pOpaque->Get("mgm.file.resync");
      bool doresync = false;

      if (computechecksum == "1") {
        option += "&mgm.verify.compute.checksum=1";
      }

      if (commitchecksum == "1") {
        option += "&mgm.verify.commit.checksum=1";
      }

      if (commitsize == "1") {
        option += "&mgm.verify.commit.size=1";
      }

      if (commitfmd == "1") {
        option += "&mgm.verify.commit.fmd=1";
      }

      if (verifyrate.length()) {
        option += "&mgm.verify.rate=";
        option += verifyrate;
      }

      if (sendresync == "1") {
        doresync = true;
      }

      XrdOucString fsidfilter = pOpaque->Get("mgm.file.verify.filterid");
      int acceptfsid = 0;

      if (fsidfilter.length()) {
        acceptfsid = atoi(pOpaque->Get("mgm.file.verify.filterid"));
      }

      // only root can do that
      if (pVid->uid == 0) {
        eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IFileMD> fmd;

        if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
          WAIT_BOOT;
          unsigned long long fid = Resolver::retrieveFileIdentifier(
                                     spath).getUnderlyingUInt64();

          // -------------------------------------------------------------------
          // reference by fid+fsid
          // -------------------------------------------------------------------

          try {
            fmd = gOFS->eosFileService->getFileMD(fid);
            std::string fullpath = gOFS->eosView->getUri(fmd.get());
            spath = fullpath.c_str();
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            stdErr = "error: cannot retrieve file meta data - ";
            stdErr += e.getMessage().str().c_str();
            eos_debug("caught exception %d %s\n",
                      e.getErrno(),
                      e.getMessage().str().c_str());
          }
        } else {
          // -------------------------------------------------------------------
          // reference by path
          // -------------------------------------------------------------------
          try {
            fmd = gOFS->eosView->getFile(spath.c_str());
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            stdErr = "error: cannot retrieve file meta data - ";
            stdErr += e.getMessage().str().c_str();
            eos_debug("caught exception %d %s\n",
                      e.getErrno(),
                      e.getMessage().str().c_str());
          }
        }

        if (fmd) {
          // -------------------------------------------------------------------
          // copy out the locations vector
          // -------------------------------------------------------------------
          eos::IFileMD::LocationVector::const_iterator it;
          bool isRAIN = false;
          eos::IFileMD::LocationVector locations = fmd->getLocations();
          eos::common::LayoutId::layoutid_t fmdlid = fmd->getLayoutId();
          eos::common::FileId::fileid_t fileid = fmd->getId();

          if ((eos::common::LayoutId::GetLayoutType(fmdlid) ==
               eos::common::LayoutId::kRaidDP) ||
              (eos::common::LayoutId::GetLayoutType(fmdlid) ==
               eos::common::LayoutId::kArchive) ||
              (eos::common::LayoutId::GetLayoutType(fmdlid) ==
               eos::common::LayoutId::kRaid6)
             ) {
            isRAIN = true;
          }

          if (computechecksum == "1" && commitchecksum == "1") {
            auto dmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
            eos::IContainerMD::XAttrMap attrmap;
            eos::listAttributes(gOFS->eosView, dmd.get(), attrmap, false);

            if (attrmap.count(SYS_ALTCHECKSUMS)) {
              option += "&mgm.verify.compute.altchecksum=";
              option += attrmap[SYS_ALTCHECKSUMS].c_str();
            }
          }

          viewReadLock.Release();
          retc = 0;
          bool acceptfound = false;

          for (it = locations.begin(); it != locations.end(); ++it) {
            if (acceptfsid && (acceptfsid != (int) *it)) {
              continue;
            }

            if (acceptfsid) {
              acceptfound = true;
            }

            if (doresync) {
              int lretc = gOFS->QueryResync(fileid, (int) * it, true);

              if (!lretc) {
                stdOut += "success: sending FMD resync to fsid=";
                stdOut += (int) * it;
                stdOut += " for path=";
                stdOut += spath;
                stdOut += "\n";
              } else {
                stdErr = "error: failed to send FMD resync to fsid=";
                stdErr += (int) * it;
                stdErr += "\n";
                retc = errno;
              }
            } else {
              if (isRAIN) {
                int lretc = gOFS->QueryResync(fileid, (int) * it);

                if (!lretc) {
                  stdOut += "success: sending resync for RAIN layout to fsid=";
                  stdOut += (int) * it;
                  stdOut += " for path=";
                  stdOut += spath;
                  stdOut += "\n";
                } else {
                  retc = errno;
                }
              } else {
                // rain layouts only resync meta data records
                int lretc = gOFS->_verifystripe(spath.c_str(), *mError, vid,
                                                (unsigned long) * it, option.c_str());

                if (!lretc) {
                  stdOut += "success: sending verify to fsid=";
                  stdOut += (int) * it;
                  stdOut += " for path=";
                  stdOut += spath;
                  stdOut += "\n";
                } else {
                  retc = errno;
                }
              }
            }

            // -------------------------------------------------------------------
            // we want to be able to force the registration and verification of a
            // not registered replica
            // -------------------------------------------------------------------
            if (acceptfsid && (!acceptfound)) {
              int lretc = gOFS->_verifystripe(spath.c_str(), *mError, vid,
                                              (unsigned long) acceptfsid, option.c_str());

              if (!lretc) {
                stdOut += "success: sending forced verify to fsid=";
                stdOut += acceptfsid;
                stdOut += " for path=";
                stdOut += spath;
                stdOut += "\n";
              } else {
                retc = errno;
              }
            }
          }
        }

        //-------------------------------------------
      } else {
        retc = EPERM;
        stdErr = "error: you have to take role 'root' to execute this command";
      }
    }

    // -------------------------------------------------------------------------
    // move a replica/stripe from source fs to target fs
    // -------------------------------------------------------------------------
    if (mSubCmd == "move") {
      cmdok = true;
      XrdOucString sfsidsource = pOpaque->Get("mgm.file.sourcefsid");
      unsigned long sourcefsid = (sfsidsource.length()) ?
                                 strtoul(sfsidsource.c_str(), 0, 10) : 0;
      XrdOucString sfsidtarget = pOpaque->Get("mgm.file.targetfsid");
      unsigned long targetfsid = (sfsidsource.length()) ?
                                 strtoul(sfsidtarget.c_str(), 0, 10) : 0;

      if (gOFS->_movestripe(spath.c_str(),
                            *mError,
                            *pVid,
                            sourcefsid,
                            targetfsid)
         ) {
        stdErr += "error: unable to move stripe";
        retc = errno;
      } else {
        stdOut += "success: scheduled move from source fs=";
        stdOut += sfsidsource;
        stdOut += " => target fs=";
        stdOut += sfsidtarget;
      }
    }

    // -------------------------------------------------------------------------
    // replicate a replica/stripe from source fs to target fs
    // -------------------------------------------------------------------------
    if (mSubCmd == "replicate") {
      cmdok = true;
      XrdOucString sfsidsource = pOpaque->Get("mgm.file.sourcefsid");
      unsigned long sourcefsid = (sfsidsource.length()) ?
                                 strtoul(sfsidsource.c_str(), 0, 10) : 0;
      XrdOucString sfsidtarget = pOpaque->Get("mgm.file.targetfsid");
      unsigned long targetfsid = (sfsidtarget.length()) ?
                                 strtoul(sfsidtarget.c_str(), 0, 10) : 0;

      if (gOFS->_copystripe(spath.c_str(), *mError, *pVid, sourcefsid, targetfsid)) {
        stdErr += "error: unable to replicate stripe";
        retc = errno;
      } else {
        stdOut += "success: scheduled replication from source fs=";
        stdOut += sfsidsource;
        stdOut += " => target fs=";
        stdOut += sfsidtarget;
      }
    }

    // -------------------------------------------------------------------------
    // create URLs to share a file without authentication
    // -------------------------------------------------------------------------
    if (mSubCmd == "share") {
      cmdok = true;
      XrdOucString sexpires = pOpaque->Get("mgm.file.expires");
      time_t expires = (sexpires.length()) ?
                       (time_t) strtoul(sexpires.c_str(), 0, 10) : 0;

      if (!expires) {
        // default is 30 days
        expires = (time_t)(time(NULL) + (30 * 86400));
      }

      std::string sharepath;
      sharepath = gOFS->CreateSharePath(spath.c_str(), "", expires, *mError, *pVid);

      if (vid.uid != 0) {
        // non-root users cannot create shared URLs with validity > 90 days
        if ((expires - time(NULL)) > (90 * 86400)) {
          stdErr += "error: you cannot request shared URLs with a validity longer than 90 days!\n";
          errno = EINVAL;
          retc = EINVAL;
          sharepath = "";
        }
      }

      if (!sharepath.length()) {
        stdErr += "error: unable to create URLs for file sharing";
        retc = errno;
      } else {
        XrdOucString httppath = "http://";
        httppath += gOFS->HostName;
        httppath += ":";
        httppath += gOFS->mHttpdPort;
        httppath += "/";
        size_t qpos = sharepath.find("?");
        std::string httpunenc = sharepath;
        httpunenc.erase(qpos);
        std::string httpenc = eos::common::StringConversion::curl_escaped(httpunenc);
        // remove /#curl#
        httpenc.erase(0, 7);
        httppath += httpenc.c_str();
        httppath += httpenc.c_str();
        XrdOucString cgi = sharepath.c_str();
        cgi.erase(0, qpos);

        while (cgi.replace("+", "%2B", qpos)) {
        }

        httppath += cgi.c_str();
        XrdOucString rootUrl = "root://";
        rootUrl += gOFS->ManagerId;
        rootUrl += "/";
        rootUrl += sharepath.c_str();

        if (mHttpFormat) {
          stdOut += "<h4 id=\"sharevalidity\" >File Sharing Links: [ valid until  ";
          struct tm* newtime;
          newtime = localtime(&expires);
          stdOut += asctime(newtime);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " ]</h4>\n";
          stdOut += path;
          stdOut += "<table border=\"0\"><tr><td>";
          stdOut += "<img alt=\"\" src=\"data:image/gif;base64,R0lGODlhEAANAJEAAAJ6xv///wAAAAAAACH5BAkAAAEALAAAAAAQAA0AAAg0AAMIHEiwoMGDCBMqFAigIYCFDBsadPgwAMWJBB1axBix4kGPEhN6HDgyI8eTJBFSvEgwIAA7\">";
          stdOut += "<a id=\"httpshare\" href=\"";
          stdOut += httppath.c_str();
          stdOut += "\">HTTP</a></td>";
          stdOut += "</tr><tr><td>";
          stdOut += "<img alt=\"\" src=\"data:image/gif;base64,R0lGODlhEAANAJEAAAJ6xv///wAAAAAAACH5BAkAAAEALAAAAAAQAA0AAAg0AAMIHEiwoMGDCBMqFAigIYCFDBsadPgwAMWJBB1axBix4kGPEhN6HDgyI8eTJBFSvEgwIAA7\">";
          stdOut += "<a id=\"rootshare\" href=\"";
          stdOut += rootUrl.c_str();
          stdOut += "\">ROOT</a></td>";
          stdOut += "</tr></table>\n";
        } else {
          stdOut += "[ root ]: ";
          stdOut += rootUrl;
          stdOut += "\n";
          stdOut += "[ http ]: ";
          stdOut += httppath;
          stdOut += "\n";
        }
      }
    }

    // -------------------------------------------------------------------------
    // rename a file or directory from source to target path
    // -------------------------------------------------------------------------
    if (mSubCmd == "rename") {
      cmdok = true;
      XrdOucString source = spath;
      XrdOucString target = pOpaque->Get("mgm.file.target");
      PROC_MOVE_TOKENSCOPE(source.c_str(), target.c_str());

      if (gOFS->rename(source.c_str(), target.c_str(), *mError, *pVid, 0, 0, true)) {
        stdErr += "error: ";
        stdErr += mError->getErrText();
        retc = errno;
      } else {
        stdOut += "success: renamed '";
        stdOut += source.c_str();
        stdOut += "' to '";
        stdOut += target.c_str();
        stdOut += "'";
      }
    }

    // -------------------------------------------------------------------------
    // link a file or directory from source to target path
    // -------------------------------------------------------------------------
    if (mSubCmd == "symlink") {
      cmdok = true;
      XrdOucString source = pOpaque->Get("mgm.file.source");
      XrdOucString target = pOpaque->Get("mgm.file.target");
      XrdOucString forceS = pOpaque->Get("mgm.file.force");
      bool force = (forceS == "1");

      if (gOFS->symlink(source.c_str(), target.c_str(), *mError, *pVid, 0, 0,
                        force)) {
        stdErr += "error: unable to link";
        retc = errno;
      } else {
        stdOut += "success: linked '";
        stdOut += source.c_str();
        stdOut += "' to '";
        stdOut += target.c_str();
        stdOut += "'";
      }
    }

    // -------------------------------------------------------------------------
    // trigger a workflow on a given file
    // -------------------------------------------------------------------------
    if (mSubCmd == "workflow") {
      cmdok = true;
      XrdOucString event = pOpaque->Get("mgm.event");
      XrdOucString workflow = pOpaque->Get("mgm.workflow");
      unsigned long long fid = 0;

      if (!event.length() || !workflow.length()) {
        stdErr = "error: you have to specify a workflow and an event!\n";
        retc = EINVAL;
        return SFS_OK;
      }

      if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
        //-------------------------------------------
        // reference by fid+fsid
        //-------------------------------------------
        unsigned long long fid = Resolver::retrieveFileIdentifier(
                                   spath).getUnderlyingUInt64();
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IFileMD> fmd;

        try {
          fmd = gOFS->eosFileService->getFileMD(fid);
          spath = gOFS->eosView->getUri(fmd.get()).c_str();
        } catch (eos::MDException& e) {
          eos_debug("caught exception %d %s\n",
                    e.getErrno(),
                    e.getMessage().str().c_str());
          stdErr += "error: ";
          stdErr += mError->getErrText();
          retc = errno;
          return SFS_OK;
        }
      } else {
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IFileMD> fmd;

        try {
          fmd = gOFS->eosView->getFile(spath.c_str());
          fid = fmd->getId();
        } catch (eos::MDException& e) {
          eos_debug("caught exception %d %s\n",
                    e.getErrno(),
                    e.getMessage().str().c_str());
          stdErr += "error: ";
          stdErr += mError->getErrText();
          retc = errno;
          return SFS_OK;
        }
      }

      XrdSfsFSctl args;
      XrdOucString opaque = "mgm.pcmd=event&mgm.fid=";
      XrdOucString lSec;
      opaque += eos::common::FileId::Fid2Hex(fid).c_str();
      opaque += "&mgm.logid=";
      opaque += logId;
      opaque += "&mgm.event=";
      opaque += event.c_str();
      opaque += "&mgm.workflow=";
      opaque += workflow.c_str();
      opaque += "&mgm.path=";
      opaque += spath.c_str();
      opaque += "&mgm.ruid=";
      opaque += (int) vid.uid;
      opaque += "&mgm.rgid=";
      opaque += (int) vid.gid;
      XrdSecEntity lClient(pVid->prot.c_str());
      lClient.name = (char*) pVid->name.c_str();
      lClient.tident = (char*) pVid->tident.c_str();
      lClient.host = (char*) pVid->host.c_str();
      lSec = "&mgm.sec=";
      lSec += eos::common::SecEntity::ToKey(&lClient, "eos").c_str();
      opaque += lSec;
      args.Arg1 = spath.c_str();
      args.Arg1Len = spath.length();
      args.Arg2 = opaque.c_str();
      args.Arg2Len = opaque.length();

      if (gOFS->FSctl(SFS_FSCTL_PLUGIN,
                      args,
                      *mError,
                      &lClient) != SFS_DATA) {
        stdErr += "error: unable to run workflow '";
        stdErr += event.c_str();
        stdErr += "' : ";
        stdErr += mError->getErrText();
        retc = errno;
      } else {
        stdOut += "success: triggered workflow  '";
        stdOut += event.c_str();
        stdOut += "' on '";
        stdOut += spath.c_str();
        stdOut += "'";
      }
    }

    // -------------------------------------------------------------------------
    // tag/untag a file to be located on a certain file system
    // -------------------------------------------------------------------------
    if (mSubCmd == "tag") {
      cmdok = true;

      if (!((vid.prot == "sss") && vid.hasUid(DAEMONUID)) && (vid.uid != 0)) {
        stdErr = "error: permission denied - you have to be root to "
                 "run the 'tag' command";
        retc = EPERM;
        return SFS_OK;
      }

      bool do_add = false;
      bool do_rm = false;
      bool do_unlink = false;
      XrdOucString sfsid = pOpaque->Get("mgm.file.tag.fsid");

      if (sfsid.beginswith("+")) {
        do_add = true;
      }

      if (sfsid.beginswith("-")) {
        do_rm = true;
      }

      if (sfsid.beginswith("~")) {
        do_unlink = true;
      }

      sfsid.erase(0, 1);
      errno = 0;
      int fsid = (sfsid.c_str()) ? (int) strtol(sfsid.c_str(), 0, 10) : 0;

      if (errno || (fsid == 0) || (!do_add && !do_rm && !do_unlink)) {
        stdErr = "error: no valid filesystem id and/or operation (+/-/~) "
                 "provided e.g. 'file tag /myfile +1000'\n";
        stdErr += sfsid;
        retc = EINVAL;
        return SFS_OK;
      } else {
        std::shared_ptr<eos::IFileMD> fmd = nullptr;

        try {
          if (fid) {
            fmd = gOFS->eosFileService->getFileMD(fid);
          } else {
            fmd = gOFS->eosView->getFile(spath.c_str());
          }

          eos::MDLocking::FileWriteLock fwLock(fmd.get());

          if (do_add && fmd->hasLocation(fsid)) {
            stdErr += "error: file '";
            stdErr += spath.c_str();
            stdErr += "' is already located on fs=";
            stdErr += (int) fsid;
            retc = EINVAL;
            return SFS_OK;
          } else if ((do_rm || do_unlink) &&
                     (!fmd->hasLocation(fsid) &&
                      !fmd->hasUnlinkedLocation(fsid))) {
            stdErr += "error: file '";
            stdErr += spath.c_str();
            stdErr += "' is not located on fs=";
            stdErr += (int) fsid;
            retc = EINVAL;
            return SFS_OK;
          } else {
            if (do_add) {
              fmd->addLocation(fsid);
              stdOut += "success: added location to file '";
            }

            if (do_rm || do_unlink) {
              fmd->unlinkLocation(fsid);

              if (do_rm) {
                stdOut += "success: removed location from file '";
                fmd->removeLocation(fsid);
              } else {
                stdOut += "success: unlinked location from file '";
              }
            }

            gOFS->eosView->updateFileStore(fmd.get());
            stdOut += spath.c_str();
            stdOut += "' on fs=";
            stdOut += (int) fsid;
          }
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                    e.getMessage().str().c_str());
        }

        if (!fmd) {
          stdErr += "error: unable to get file meta data of file '";
          stdErr += spath.c_str();
          stdErr += "'";
          retc = errno;
          return SFS_OK;
        }
      }
    }

    // Third-party copy files/directories
    if (mSubCmd == "copy") {
      cmdok = true;
      XrdOucString src = spath;
      XrdOucString dst = pOpaque->Get("mgm.file.target");

      if (!dst.length()) {
        stdErr += "error: missing destination argument";
        retc = EINVAL;
      } else {
        struct stat srcbuf;
        struct stat dstbuf;

        // check that we can access source and destination
        if (gOFS->_stat(src.c_str(), &srcbuf, *mError, *pVid, "")) {
          stdErr += "error: ";
          stdErr += mError->getErrText();
          retc = errno;
        } else {
          XrdOucString option = pOpaque->Get("mgm.file.option");
          bool silent = false;

          if ((option.find("s")) != STR_NPOS) {
            silent = true;
          } else {
            if ((option.find("c")) != STR_NPOS) {
              stdOut += "info: cloning '";
            } else {
              stdOut += "info: copying '";
            }

            stdOut += spath;
            stdOut += "' => '";
            stdOut += dst;
            stdOut += "' ...\n";
          }

          int dstat = gOFS->_stat(dst.c_str(), &dstbuf, *mError, *pVid, "");

          if ((option.find("f") == STR_NPOS) && !dstat) {
            // there is no force flag and the target exists
            stdErr += "error: the target file exists - use '-f' to force the copy";
            retc = EEXIST;
          } else {
            // check source and destination access
            if (gOFS->_access(src.c_str(), R_OK, *mError, *pVid, "") ||
                gOFS->_access(dst.c_str(), W_OK, *mError, *pVid, "")) {
              stdErr += "error: ";
              stdErr += mError->getErrText();
              retc = errno;
            } else {
              std::vector<std::string> lCopySourceList;
              std::vector<std::string> lCopyTargetList;
              // If this is a directory create a list of files to copy
              std::map < std::string, std::set < std::string >> found;

              if (S_ISDIR(srcbuf.st_mode) && S_ISDIR(dstbuf.st_mode)) {
                if (!gOFS->_find(src.c_str(), *mError, stdErr, *pVid, found)) {
                  // Add all to the copy source,target list ...
                  for (auto dirit = found.begin(); dirit != found.end(); dirit++) {
                    // Loop over dirs and add all the files
                    for (auto fileit = dirit->second.begin(); fileit != dirit->second.end();
                         fileit++) {
                      std::string src_path = dirit->first;
                      std::string end_path = src_path;
                      end_path.erase(0, src.length());
                      src_path += *fileit;
                      std::string dst_path = dst.c_str();
                      dst_path += end_path;
                      dst_path += *fileit;
                      lCopySourceList.push_back(src_path.c_str());
                      lCopyTargetList.push_back(dst_path.c_str());
                      stdOut += "info: copying '";
                      stdOut += src_path.c_str();
                      stdOut += "' => '";
                      stdOut += dst_path.c_str();
                      stdOut += "' ... \n";
                    }
                  }
                } else {
                  stdErr += "error: find failed";
                }
              } else {
                // Add a single file to the copy list
                lCopySourceList.push_back(src.c_str());
                lCopyTargetList.push_back(dst.c_str());
              }

              for (size_t i = 0; i < lCopySourceList.size(); i++) {
                // Setup a TPC job
                XrdCl::PropertyList properties;
                XrdCl::PropertyList result;

                if (srcbuf.st_size) {
                  // TPC for non-empty files
                  properties.Set("thirdParty", "only");
                }

                properties.Set("force", true);
                properties.Set("posc", false);
                properties.Set("coerce", false);
                std::string source = lCopySourceList[i];
                std::string target = lCopyTargetList[i];
                std::string sizestring;
                std::string cgi = "eos.ruid=";
                cgi += eos::common::StringConversion::GetSizeString(sizestring,
                       (unsigned long long) pVid->uid);
                cgi += "&eos.rgid=";
                cgi += eos::common::StringConversion::GetSizeString(sizestring,
                       (unsigned long long) pVid->gid);
                cgi += "&eos.app=filecopy";

                if ((option.find("c")) != STR_NPOS) {
                  char clonetime[256];
                  snprintf(clonetime, sizeof(clonetime) - 1, "&eos.ctime=%lu&eos.mtime=%lu",
                           srcbuf.st_ctime, srcbuf.st_mtime);
                  cgi += clonetime;
                }

                XrdCl::URL url_src;
                url_src.SetProtocol("root");
                url_src.SetHostName("localhost");
                url_src.SetUserName("root");
                url_src.SetParams(cgi);
                url_src.SetPath(source);
                XrdCl::URL url_trg;
                url_trg.SetProtocol("root");
                url_trg.SetHostName("localhost");
                url_trg.SetUserName("root");
                url_trg.SetParams(cgi);
                url_trg.SetPath(target);
                properties.Set("source", url_src);
                properties.Set("target", url_trg);
                properties.Set("sourceLimit", (uint16_t) 1);
                properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
                properties.Set("parallelChunks", (uint8_t) 1);
                XrdCl::CopyProcess lCopyProcess;
                lCopyProcess.AddJob(properties, &result);
                XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
                eos_static_info("[tpc]: %s=>%s %s",
                                url_src.GetURL().c_str(),
                                url_trg.GetURL().c_str(),
                                lTpcPrepareStatus.ToStr().c_str());

                if (lTpcPrepareStatus.IsOK()) {
                  XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
                  eos_static_info("[tpc]: %s %d",
                                  lTpcStatus.ToStr().c_str(),
                                  lTpcStatus.IsOK());

                  if (lTpcStatus.IsOK()) {
                    if (!silent) {
                      stdOut += "success: copy done '";
                      stdOut += source.c_str();
                      stdOut += "'\n";
                    }
                  } else {
                    stdErr += "error: copy failed ' ";
                    stdErr += source.c_str();
                    stdErr += "' - ";
                    stdErr += lTpcStatus.ToStr().c_str();
                    retc = EIO;
                  }
                } else {
                  stdErr += "error: copy failed - ";
                  stdErr += lTpcPrepareStatus.ToStr().c_str();
                  retc = EIO;
                }
              }
            }
          }
        }
      }
    }

    if (mSubCmd == "convert") {
      cmdok = true;

      // -----------------------------------------------------------------------
      // check access permissions on source
      // -----------------------------------------------------------------------
      if ((gOFS->_access(spath.c_str(), W_OK, *mError, *pVid, "") != SFS_OK)) {
        stdErr += "error: you have no write permission on '";
        stdErr += spath.c_str();
        stdErr += "'";
        retc = EPERM;
      } else {
        while (1) {
          using eos::common::LayoutId;
          LayoutId::eChecksum echecksum{LayoutId::eChecksum::kNone};
          XrdOucString layout = pOpaque->Get("mgm.convert.layout");
          XrdOucString space = pOpaque->Get("mgm.convert.space");
          XrdOucString plctplcy = pOpaque->Get("mgm.convert.placementpolicy");
          XrdOucString checksum = pOpaque->Get("mgm.convert.checksum");
          XrdOucString option = pOpaque->Get("mgm.option");

          //stdOut += ("Placement Policy is: " + plctplcy);
          if (plctplcy.length()) {
            // -------------------------------------------------------------------
            // check that the placement policy is valid
            // i.e. scattered, hybrid:<geotag> or gathered:<geotag>
            // -------------------------------------------------------------------
            if (plctplcy != "scattered" &&
                !plctplcy.beginswith("hybrid:") &&
                !plctplcy.beginswith("gathered:")) {
              stdErr += "error: placement policy is invalid";
              retc = EINVAL;
              return SFS_OK;
            }

            // Check geotag in case of hybrid or gathered policy
            if (plctplcy != "scattered") {
              std::string policy = plctplcy.c_str();
              std::string targetgeotag = policy.substr(policy.find(':') + 1);
              std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);

              if (tmp_geotag != targetgeotag) {
                stdErr += tmp_geotag.c_str();
                retc = EINVAL;
                return SFS_OK;
              }
            }

            plctplcy = "~" + plctplcy;
          } else {
            plctplcy = "";
          }

          if (checksum.length()) {
            int xs = LayoutId::GetChecksumFromString(checksum.c_str());

            if (xs != -1) {
              echecksum = static_cast<LayoutId::eChecksum>(xs);
            }
          }

          if (!space.length()) {
            // Get target space from the layout settings
            eos::common::Path cPath(spath.c_str());
            eos::IContainerMD::XAttrMap map;
            int rc = gOFS->_attr_ls(cPath.GetParentPath(), *mError, *pVid, (const char*) 0,
                                    map);

            if (rc || (!map.count("sys.forced.space") && !map.count("user.forced.space"))) {
              stdErr += "error: cannot get default space settings from parent "
                        "directory attributes";
              retc = EINVAL;
            } else {
              if (map.count("sys.forced.space")) {
                space = map["sys.forced.space"].c_str();
              } else {
                space = map["user.forced.space"].c_str();
              }
            }
          }

          if (space.length()) {
            if (!layout.length() && (option != "rewrite")) {
              stdErr += "error: conversion layout has to be defined";
              retc = EINVAL;
            } else {
              // get the file meta data
              std::shared_ptr<eos::IFileMD> fmd;
              int fsid = 0;
              eos::common::LayoutId::layoutid_t layoutid = 0;
              eos::common::FileId::fileid_t fileid = 0;
              {
                eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

                try {
                  fmd = gOFS->eosView->getFile(spath.c_str());
                  layoutid = fmd->getLayoutId();
                  fileid = fmd->getId();

                  if (fmd->getNumLocation()) {
                    eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                    fsid = *(loc_vect.begin());
                  }
                } catch (eos::MDException& e) {
                  errno = e.getErrno();
                  eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                            e.getMessage().str().c_str());
                }
              }

              if (!fmd) {
                stdErr += "error: unable to get file meta data of file ";
                stdErr += spath.c_str();
                retc = errno;
              } else {
                std::string conversiontag;

                if (option == "rewrite") {
                  if (layout.length() == 0) {
                    stdOut += "info: rewriting file with identical layout id\n";
                    char hexlayout[17];
                    snprintf(hexlayout, sizeof(hexlayout) - 1, "%08llx",
                             (long long) layoutid);
                    layout = hexlayout;
                  }

                  // get the space this file is currently hosted
                  if (!fsid) {
                    // bummer, this file has not even a single replica
                    stdErr += "error: file has no replica attached\n";
                    retc = ENODEV;
                    break;
                  }

                  // figure out which space this fsid is in ...
                  {
                    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                    FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);

                    if (!filesystem) {
                      stdErr += "error: couldn't find filesystem in view\n";
                      retc = EINVAL;
                      break;
                    }

                    // get the space of that filesystem
                    space = filesystem->GetString("schedgroup").c_str();
                    space.erase(space.find("."));
                    stdOut += "info:: rewriting into space '";
                    stdOut += space;
                    stdOut += "'\n";
                  }
                }

                if (eos::common::StringConversion::IsHexNumber(layout.c_str(), "%08x")) {
                  conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                                     std::string(""), false);
                  stdOut += "info: conversion based on hexadecimal layout id\n";
                } else {
                  unsigned long layout_type = 0;
                  unsigned long layout_stripes = 0;
                  // check if it was provided as <layout>:<stripes>
                  std::string lLayout = layout.c_str();
                  std::string lLayoutName;
                  std::string lLayoutStripes;

                  if (eos::common::StringConversion::SplitKeyValue(lLayout,
                      lLayoutName,
                      lLayoutStripes)) {
                    XrdOucString lLayoutString = "eos.layout.type=";
                    lLayoutString += lLayoutName.c_str();
                    lLayoutString += "&eos.layout.nstripes=";
                    lLayoutString += lLayoutStripes.c_str();
                    // ---------------------------------------------------------------
                    // add block checksumming and the default blocksize of 4 M
                    // ---------------------------------------------------------------

                    // ---------------------------------------------------------------
                    // unless explicitely stated, use the layout checksum
                    // ---------------------------------------------------------------
                    if (echecksum == eos::common::LayoutId::eChecksum::kNone) {
                      echecksum = static_cast<eos::common::LayoutId::eChecksum>(
                                    eos::common::LayoutId::GetChecksum(layoutid));
                    }

                    XrdOucEnv lLayoutEnv(lLayoutString.c_str());
                    layout_type =
                      eos::common::LayoutId::GetLayoutFromEnv(lLayoutEnv);
                    layout_stripes =
                      eos::common::LayoutId::GetStripeNumberFromEnv(lLayoutEnv);
                    // ---------------------------------------------------------------
                    // re-create layout id by merging in the layout stripes, type & checksum
                    // ---------------------------------------------------------------
                    layoutid =
                      eos::common::LayoutId::GetId(layout_type,
                                                   echecksum,
                                                   layout_stripes,
                                                   eos::common::LayoutId::k4M,
                                                   eos::common::LayoutId::kCRC32C,
                                                   eos::common::LayoutId::GetRedundancyStripeNumber(layoutid));
                    conversiontag = ConversionTag::Get(fileid, space.c_str(), layoutid,
                                                       plctplcy.c_str(), false);
                    stdOut += "info: conversion based layout+stripe arguments\n";
                  } else {
                    // assume this is the name of an attribute
                    conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                                       plctplcy.c_str(), false);
                    stdOut += "info: conversion based conversion attribute name\n";
                  }
                }

                std::string err_msg;

                // Push conversion job to QuarkDB
                if (gOFS->mConverterEngine->ScheduleJob(fmd->getId(),
                                                        conversiontag, err_msg)) {
                  stdOut += "success: pushed conversion job '";
                  stdOut += conversiontag.c_str();
                  stdOut += "' to QuarkDB";
                } else {
                  stdErr += "error: failed to schedule conversion '";
                  stdErr += conversiontag.c_str();

                  if (err_msg.empty()) {
                    stdErr += " msg=\"";
                    stdErr += err_msg.c_str();
                    stdErr += "\"";
                  }

                  retc = EINVAL;
                  break;
                }
              }
            }
          }

          break; // while 1
        }
      }
    }

    // -------------------------------------------------------------------------
    // touch a file
    // -------------------------------------------------------------------------
    if (mSubCmd == "touch") {
      cmdok = true;
      bool useLayout = true;
      bool truncate = false;
      bool absorb = false;
      bool lock = false;
      bool unlock = false;
      time_t lifetime = 86400;
      size_t size = 0;
      const char* hardlinkpath = 0;
      const char* checksuminfo = 0;
      std::string errmsg;

      if (pOpaque->Get("mgm.file.touch.nolayout")) {
        useLayout = false;
      }

      if (pOpaque->Get("mgm.file.touch.truncate")) {
        truncate = true;
      }

      if (pOpaque->Get("mgm.file.touch.size")) {
        size = strtoull(pOpaque->Get("mgm.file.touch.size"), 0, 10);
      }

      if (pOpaque->Get("mgm.file.touch.absorb")) {
        absorb = true;
      }

      char* lockop = 0;
      char* wildcard = pOpaque->Get("mgm.file.touch.wildcard");
      bool userwildcard = false;
      bool appwildcard = false;

      if (wildcard && (std::string(wildcard) != "user") &&
          (std::string(wildcard) != "app")) {
        stdErr = "error: invalid wildcard type specified, can be only 'user' or 'app'\n";
        retc = EINVAL;
        return SFS_OK;
      } else {
        if (wildcard) {
          if (std::string(wildcard) == "user") {
            userwildcard = true;
          } else {
            appwildcard = true;
          }
        }
      }

      if ((lockop = pOpaque->Get("mgm.file.touch.lockop"))) {
        if (std::string(lockop) == "lock") {
          lock = true;
          unlock = false;
        } else if (std::string(lockop) == "unlock") {
          unlock = true;
          lock = false;
        } else {
          stdErr = "error: invalid lock operation specified - can be either 'lock' or 'unlock' '";
          stdErr += lockop;
          stdErr += "'";
          retc = EINVAL;
          return SFS_OK;
        }
      }

      char* lock_lifetime = 0;

      if ((lock_lifetime = pOpaque->Get("mgm.file.touch.lockop.lifetime"))) {
        lifetime = atoi(lock_lifetime);
      }

      hardlinkpath = pOpaque->Get("mgm.file.touch.hardlinkpath");
      checksuminfo = pOpaque->Get("mgm.file.touch.checksuminfo");

      if (!spath.length()) {
        stdErr = "error: There is no file with given id! '";
        stdErr += spathid;
        stdErr += "'";
        retc = ENOENT;
      } else {
        if (gOFS->_touch(spath.c_str(), *mError, *pVid, 0, true, useLayout, truncate,
                         size, absorb, hardlinkpath, checksuminfo, &errmsg)) {
          stdErr = "error: unable to touch '";
          stdErr += spath.c_str();
          stdErr += "'";

          if (errmsg.length()) {
            stdErr += "\n";
            stdErr += errmsg.c_str();
          }

          retc = errno;
        } else {
          if (lock) {
            // try to set a xattr lock
            XattrLock applock;
            errno = 0;

            if (applock.Lock(spath.c_str(), false, lifetime, *pVid, userwildcard,
                             appwildcard)) {
              stdOut += "success: created exclusive lock for '";
              stdOut += spath.c_str();
              stdOut += "'\n";
              stdOut += applock.Dump().c_str();
            } else {
              stdErr += "error: cannot get exclusive lock for '";
              stdErr += spath.c_str();
              stdErr += "'\n";
              stdErr += applock.Dump().c_str();
              retc = errno;
              return SFS_OK;
            }
          }

          if (unlock) {
            // try to unlock an xattr lock
            XattrLock applock;

            if (applock.Unlock(spath.c_str(), *pVid)) {
              stdOut += "success: removed exclusive lock for '";
              stdOut += spath.c_str();
              stdOut += "'\n";
              stdOut += applock.Dump().c_str();
            } else {
              if (errno == ENODATA) {
                stdOut += "info: there was no exclusive lock for '";
                stdOut += spath.c_str();
                stdOut += "'\n";
              } else {
                stdErr += "error: failed to remove exclusive lock for '";
                stdErr += spath.c_str();
                stdErr += "'\n";
                stdErr += applock.Dump().c_str();
                retc = errno;
                return SFS_OK;
              }
            }
          }

          stdOut += "success: touched '";
          stdOut += spath.c_str();
          stdOut += "'";

          if (errmsg.length()) {
            stdOut += "\n";
            stdOut += errmsg.c_str();
          }
        }
      }
    }

    //--------------------------------------------------------------------------
    // Fix file by removing/repairing or adding replicas/stripes
    // -------------------------------------------------------------------------
    if (mSubCmd == "adjustreplica") {
      // Only root can do that
      cmdok = true;

      if (pVid->uid) {
        retc = EPERM;
        stdErr = "error: you have to take role 'root' to execute this command";
        return SFS_OK;
      }

      uint32_t lid = 0ul;
      uint64_t size = 0ull;
      unsigned long long fid = 0ull;
      std::shared_ptr<eos::IFileMD> fmd {nullptr};
      eos::IFileMD::LocationVector loc_vect;
      XrdOucString file_option = pOpaque->Get("mgm.file.option");
      bool nodrop = false;

      if (file_option == "nodrop") {
        nodrop = true;
      }

      int icreationsubgroup = -1;
      std::string creationspace = (pOpaque->Get("mgm.file.desiredspace") ?
                                   pOpaque->Get("mgm.file.desiredspace") : "");

      if (pOpaque->Get("mgm.file.desiredsubgroup")) {
        icreationsubgroup = atoi(pOpaque->Get("mgm.file.desiredsubgroup"));
      }

      {
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

        // Reference by fid+fsid
        if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
          WAIT_BOOT;
          unsigned long long fid = Resolver::retrieveFileIdentifier(
                                     spath).getUnderlyingUInt64();

          try {
            fmd = gOFS->eosFileService->getFileMD(fid);
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            stdErr = "error: cannot retrieve file meta data - ";
            stdErr += e.getMessage().str().c_str();
            eos_debug("caught exception %d %s\n",
                      e.getErrno(),
                      e.getMessage().str().c_str());
          }
        } else {
          // Reference by path
          try {
            fmd = gOFS->eosView->getFile(spath.c_str());
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            stdErr = "error: cannot retrieve file meta data - ";
            stdErr += e.getMessage().str().c_str();
            eos_debug("caught exception %d %s\n",
                      e.getErrno(),
                      e.getMessage().str().c_str());
          }
        }

        if (fmd) {
          fid = fmd->getId();
          lid = fmd->getLayoutId();
          loc_vect = fmd->getLocations();
          size = fmd->getSize();
        } else {
          retc = errno ? errno : EINVAL;
          return SFS_OK;
        }
      }

      std::string refspace = "";
      std::string space = "default";
      unsigned int forcedsubgroup = 0;

      if (eos::common::LayoutId::GetLayoutType(lid) ==
          eos::common::LayoutId::kReplica) {
        // Check the configured and available replicas
        unsigned int nrep_online = 0;
        unsigned int nrep = loc_vect.size();
        unsigned int nrep_layout = eos::common::LayoutId::GetStripeNumber(lid) + 1;
        // Give priority to healthy file systems during scheduling
        std::vector<unsigned int> src_fs;
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

        for (auto loc_it = loc_vect.begin();
             loc_it != loc_vect.end(); ++loc_it) {
          if (*loc_it == 0) {
            eos_err("msg=\"skip file system with id 0\" fxid=%08llx", fid);
            continue;
          }

          FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(*loc_it);

          if (filesystem) {
            eos::common::FileSystem::fs_snapshot_t snapshot;
            filesystem->SnapShotFileSystem(snapshot, true);
            // Remember the spacename
            space = snapshot.mSpace;

            if (!refspace.length()) {
              refspace = space;
            } else {
              if (space != refspace) {
                eos_warning("msg=\"replicas are in different spaces\" "
                            "fxid=%08llx space=%s req_space=%s", fid,
                            space.c_str(), refspace.c_str());
                continue;
              }
            }

            forcedsubgroup = snapshot.mGroupIndex;

            if ((snapshot.mConfigStatus > eos::common::ConfigStatus::kDrain) &&
                (snapshot.mStatus == eos::common::BootStatus::kBooted)) {
              // This is an accessible replica
              ++nrep_online;
              src_fs.insert(src_fs.begin(), *loc_it);
            } else {
              // Give less priority to unhealthy file systems
              src_fs.push_back(*loc_it);
            }
          } else {
            eos_err("msg=\"skip unknown file system\" fsid=%lu fxid=%08llx",
                    *loc_it, fid);
          }
        }

        eos_debug("path=%s nrep=%lu nrep-layout=%lu nrep-online=%lu",
                  spath.c_str(), nrep, nrep_layout, nrep_online);

        if (nrep_layout > nrep_online) {
          // Set the desired space & subgroup if provided
          if (creationspace.length()) {
            space = creationspace;
          }

          if (icreationsubgroup != -1) {
            forcedsubgroup = icreationsubgroup;
          }

          // If space explicitly set, don't force a particular subgroup
          if (creationspace.length()) {
            forcedsubgroup = -1;
          }

          // Trigger async replication if not enough replicas online
          int nrep_new = nrep_layout - nrep_online;
          // Get the location where we can read that file
          eos_debug("msg=\"creating %d new replicas\" fxid=%08llx space=%s "
                    "forcedsubgroup=%d icreationsubgroup=%d", nrep_new,
                    fid, space.c_str(), forcedsubgroup, icreationsubgroup);
          // This defines the fs to use in the selectedfs vector
          unsigned long fs_indx;
          std::vector<unsigned int> selectedfs;
          std::vector<unsigned int> unavailfs;
          std::vector<unsigned int> excludefs;
          std::string tried_cgi;
          // Now we just need to ask for <n> targets
          int layoutId = eos::common::LayoutId::GetId(eos::common::LayoutId::kReplica,
                         eos::common::LayoutId::kNone, nrep_new);
          eos::common::Path cPath(spath.c_str());
          eos::IContainerMD::XAttrMap attrmap;
          gOFS->_attr_ls(cPath.GetParentPath(), *mError, *pVid, (const char*) 0, attrmap);
          eos::mgm::Scheduler::tPlctPolicy plctplcy;
          std::string targetgeotag;
          // Get placement policy
          Policy::GetPlctPolicy(spath.c_str(), attrmap, *pVid, *pOpaque,
                                plctplcy, targetgeotag);
          // We don't know the container tag here, but we don't really
          // care since we are scheduled as root
          Scheduler::PlacementArguments plctargs;
          plctargs.alreadyused_filesystems = &src_fs;
          plctargs.bookingsize = size;
          plctargs.forced_scheduling_group_index = forcedsubgroup;
          plctargs.lid = layoutId;
          plctargs.inode = (ino64_t) fid;
          plctargs.path = spath.c_str();
          plctargs.plctTrgGeotag = &targetgeotag;
          plctargs.plctpolicy = plctplcy;
          plctargs.exclude_filesystems = &excludefs;
          plctargs.selected_filesystems = &selectedfs;
          plctargs.spacename = &space;
          plctargs.truncate = true;
          plctargs.vid = pVid;

          if (!plctargs.isValid()) {
            stdErr += "error: invalid argument for file placement";
            retc = EINVAL;
          } else {
            {
              errno = retc = Quota::FilePlacement(&plctargs);
            }

            if (!errno) {
              Scheduler::AccessArguments acsargs;
              acsargs.bookingsize = 0;
              acsargs.forcedspace = space.c_str();
              acsargs.fsindex = &fs_indx;
              acsargs.isRW = false;
              acsargs.lid = (unsigned long) lid;
              acsargs.inode = (ino64_t) fid;
              acsargs.locationsfs = &src_fs;
              acsargs.tried_cgi = &tried_cgi;
              acsargs.unavailfs = &unavailfs;
              acsargs.vid = pVid;

              if (!acsargs.isValid()) {
                stdErr += "error: invalid argument for file access";
                retc = EINVAL;
              } else {
                // We got a new replication vector
                for (unsigned int i = 0; i < selectedfs.size(); ++i) {
                  errno = Scheduler::FileAccess(&acsargs);

                  if (!errno) {
                    // This is now our source filesystem
                    unsigned int src_fsid = src_fs[fs_indx];

                    if (gOFS->_replicatestripe(fmd.get(), spath.c_str(),
                                               *mError, *pVid, src_fsid,
                                               selectedfs[i], false)) {
                      retc = mError->getErrInfo();
                      stdErr = SSTR("error: unable to replicate stripe "
                                    << src_fsid << " => " << selectedfs[i]
                                    << " msg=" << mError->getErrText()
                                    << std::endl).c_str();

                      // Add message from previous successful replication job
                      if (stdOut.length()) {
                        stdErr = stdOut + stdErr;
                      }
                    } else {
                      stdOut = SSTR("success: scheduled replication from source fs="
                                    << src_fsid << " => target fs="
                                    << selectedfs[i] << std::endl).c_str();
                    }
                  } else {
                    retc = ENOSPC;
                    stdErr = "error: create new replicas => no source available: ";
                    stdErr += spath;
                    stdErr += "\n";
                  }
                }
              }
            } else {
              stdErr = "error: create new replicas => cannot place replicas: ";
              stdErr += spath;
              stdErr += "\n";
            }
          }
        } else {
          // Run this in case of over-replication
          if ((nrep_layout < nrep) && (nodrop == false)) {
            unsigned int n2delete = nrep - nrep_layout;
            std::multimap <common::ConfigStatus, int /*fsid*/> statemap;
            std::multimap <std::string /*schedgroup*/, int /*fsid*/> groupmap;
            // We have too many replica's online, we drop (nrep_online - nrep_layout)
            // replicas starting with the lowest configuration state
            eos_debug("msg=\"drop %d replicas\" space=%s group=%d fxid=%08llx",
                      n2delete, creationspace.c_str(), icreationsubgroup, fid);
            {
              for (auto loc_it = loc_vect.begin();
                   loc_it != loc_vect.end(); ++loc_it) {
                if (!(*loc_it)) {
                  eos_err("msg=\"skip file system with id 0\" fxid=%08llx", fid);
                  continue;
                }

                eos::common::FileSystem::fsid_t fsid = *loc_it;
                FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);
                eos::common::FileSystem::fs_snapshot_t fs;

                if (filesystem && filesystem->SnapShotFileSystem(fs, true)) {
                  statemap.insert(std::pair<common::ConfigStatus, int>(fs.mConfigStatus, fsid));
                  groupmap.insert(std::pair<std::string, int>(fs.mGroup, fsid));
                }
              }
            }
            std::string cspace = creationspace.c_str();

            if (!cspace.empty() && (icreationsubgroup > 0)) {
              cspace += SSTR("." << icreationsubgroup);
            }

            std::multimap <common::ConfigStatus, int> limitedstatemap;

            for (auto sit = groupmap.begin(); sit != groupmap.end(); ++sit) {
              // Use fsid only if they match the space and/or group req
              if (sit->first.find(cspace) != 0) {
                continue;
              }

              // Default to the highest state for safety reasons
              common::ConfigStatus state = eos::common::ConfigStatus::kRW;

              // get the state for each fsid matching
              for (auto state_it = statemap.begin();
                   state_it != statemap.end(); ++state_it) {
                if (state_it->second == sit->second) {
                  state = state_it->first;
                  break;
                }
              }

              // fill the map containing only the candidates
              limitedstatemap.insert(std::pair<common::ConfigStatus, int>
                                     (state, sit->second));
            }

            std::vector<unsigned long> fsid2delete;

            for (auto lit = limitedstatemap.begin(); lit != limitedstatemap.end(); ++lit) {
              fsid2delete.push_back(lit->second);

              if (fsid2delete.size() == n2delete) {
                break;
              }
            }

            if (fsid2delete.size() != n2delete) {
              // add a warning that something does not work as requested ....
              stdErr = SSTR("warning: cannot adjust replicas according to "
                            "your requirement:"
                            << " space=" << creationspace
                            << " subgroup=" << icreationsubgroup
                            << std::endl).c_str();
            }

            eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

            try { // Get again the original file meta data
              auto fmd = gOFS->eosFileService->getFileMD(fid);

              for (unsigned int i = 0; i < fsid2delete.size(); i++) {
                if (fmd->hasLocation(fsid2delete[i])) {
                  fmd->unlinkLocation(fsid2delete[i]);
                  eos_debug("msg=\"removing location\" fsid=%lu fxid=%08llx",
                            fsid2delete[i], fid);
                  stdOut += "success: dropping replica on fsid=";
                  stdOut += (int) fsid2delete[i];
                  stdOut += "\n";
                }
              }

              gOFS->eosView->updateFileStore(fmd.get());
            } catch (eos::MDException& e) {
              errno = e.getErrno();
              eos_debug("msg=\"caught exception\" errno=%d msg=\"%s\"",
                        e.getErrno(), e.getMessage().str().c_str());
              stdErr = SSTR("error: drop excess replicas => cannot unlink "
                            "location - " << e.getMessage().str().c_str()
                            << std::endl).c_str();
            }
          }
        }
      } else {
        // This is a rain layout, we try to rewrite the file using the converter
        if (eos::common::LayoutId::IsRain(lid)) {
          ProcCommand Cmd;
          // rewrite the file asynchronous using the converter
          XrdOucString option = pOpaque->Get("mgm.option");
          XrdOucString info;
          info += "&mgm.cmd=file&mgm.subcmd=convert&mgm.option=rewrite&mgm.path=";
          info += spath.c_str();
          retc = Cmd.open("/proc/user", info.c_str(), *pVid, mError);
          Cmd.AddOutput(stdOut, stdErr);
          Cmd.close();
          retc = Cmd.GetRetc();
        } else {
          retc = EINVAL;
          stdOut += "warning: no action for this layout type (neither replica nor rain)\n";
        }
      }
    }

    // -------------------------------------------------------------------------
    // return meta data for a particular file
    // -------------------------------------------------------------------------
    if (mSubCmd == "getmdlocation") {
      cmdok = true;
      gOFS->MgmStats.Add("GetMdLocation", pVid->uid, pVid->gid, 1);
      // this returns the access urls to query local metadata information
      XrdOucString spath = pOpaque->Get("mgm.path");
      const char* inpath = spath.c_str();
      NAMESPACEMAP;
      PROC_BOUNCE_ILLEGAL_NAMES;
      PROC_BOUNCE_NOT_ALLOWED;
      spath = path;

      if (!spath.length()) {
        stdErr = "error: you have to give a path name to call 'fileinfo'";
        retc = EINVAL;
      } else {
        std::shared_ptr<eos::IFileMD> fmd;
        //-------------------------------------------
        std::string ns_path {};
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

        try {
          if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
            WAIT_BOOT;
            unsigned long long fid = Resolver::retrieveFileIdentifier(
                                       spath).getUnderlyingUInt64();
            // reference by fid+fsid
            //-------------------------------------------
            fmd = gOFS->eosFileService->getFileMD(fid);
          } else {
            fmd = gOFS->eosView->getFile(spath.c_str());
            ns_path = spath.c_str();
          }
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          stdErr = "error: cannot retrieve file meta data - ";
          stdErr += e.getMessage().str().c_str();
          eos_debug("caught exception %d %s\n",
                    e.getErrno(),
                    e.getMessage().str().c_str());
        }

        if (!fmd) {
          retc = errno;
        } else {
          if (ns_path.empty()) {
            try {
              ns_path = gOFS->eosView->getUri(fmd.get());
            } catch (const eos::MDException& e) {
              // File is no longer attached to a cointainer put only the name
              ns_path = fmd->getName();
            }
          }

          XrdOucString sizestring;
          int i = 0;
          stdOut += "&";
          stdOut += "mgm.nrep=";
          stdOut += (int) fmd->getNumLocation();
          stdOut += "&";
          stdOut += "mgm.checksumtype=";
          stdOut += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
          stdOut += "&";
          stdOut += "mgm.size=";
          stdOut +=
            eos::common::StringConversion::GetSizeString(sizestring,
                (unsigned long long) fmd->getSize());
          stdOut += "&";
          stdOut += "mgm.checksum=";
          eos::appendChecksumOnStringAsHex(fmd.get(), stdOut, 0x00, SHA256_DIGEST_LENGTH);
          stdOut += "&";
          stdOut += "mgm.stripes=";
          stdOut += (int)(eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);
          stdOut += "&";
          eos::IFileMD::LocationVector loc_vect = fmd->getLocations();

          for (auto loc_it = loc_vect.begin();
               loc_it != loc_vect.end(); ++loc_it) {
            // ignore filesystem id 0
            if (!(*loc_it)) {
              eos_err("fsid 0 found fxid=%08llx", fmd->getId());
              continue;
            }

            eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                                    *loc_it);

            if (filesystem) {
              XrdOucString host;
              std::string hostport = filesystem->GetString("hostport");
              stdOut += "mgm.replica.url";
              stdOut += i;
              stdOut += "=";
              stdOut += hostport.c_str();
              stdOut += "&";
              const std::string hex_fid = eos::common::FileId::Fid2Hex(fmd->getId());
              stdOut += "mgm.fid";
              stdOut += i;
              stdOut += "=";
              stdOut += hex_fid.c_str();
              stdOut += "&";
              stdOut += "mgm.fsid";
              stdOut += i;
              stdOut += "=";
              stdOut += (int) * loc_it;
              stdOut += "&";
              stdOut += "mgm.fsbootstat";
              stdOut += i;
              stdOut += "=";
              stdOut += filesystem->GetString("stat.boot").c_str();
              stdOut += "&";
              stdOut += "mgm.fstpath";
              stdOut += i;
              stdOut += "=";
              stdOut +=
                eos::common::FileId::FidPrefix2FullPath(hex_fid.c_str(),
                    filesystem->GetPath().c_str()).c_str();
              stdOut += "&";
              stdOut += "mgm.nspath=";
              stdOut += ns_path.c_str();
              stdOut += "&";
            } else {
              stdOut += "NA&";
            }

            i++;
          }
        }
      }
    }

    // Purge versions of a file
    if (mSubCmd == "purge") {
      cmdok = true;
      int max_versions = 0;
      const char* ptr = pOpaque->Get("mgm.purge.version");
      std::string smax_versions = (ptr ? ptr : "");
      ProcCommand Cmd;
      XrdOucString info;

      if (smax_versions.empty()) {
        max_versions = -1; // read the max version from the parent xattr
      } else {
        try {
          max_versions = std::stoi(smax_versions);
        } catch (...) {
          stdErr = "error: illegal version count specified";
          retc = EINVAL;
          return SFS_OK;
        }
      }

      // Check that the requests file exists
      struct stat buf;

      if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, "")) {
        stdErr = "error: unable to stat path=";
        stdErr += spath.c_str();
        retc = errno;
        return SFS_OK;
      }

      XrdOucString version_dir;
      eos::common::Path cPath(spath.c_str());
      version_dir += cPath.GetParentPath();
      version_dir += "/.sys.v#.";
      version_dir += cPath.GetName();
      version_dir += "/";

      if (gOFS->PurgeVersion(version_dir.c_str(), *mError, max_versions)) {
        if (mError->getErrInfo()) {
          retc = mError->getErrInfo();
          stdErr += SSTR("error: unable to purge versions for path=" << spath
                         << "\nerror: " << mError->getErrText()).c_str();
        } else {
          stdErr += SSTR("info: no versions to purge for path=" << spath).c_str();
        }

        return SFS_OK;
      }
    }

    // Create a new version of a file
    if (mSubCmd == "version") {
      cmdok = true;
      XrdOucString max_count = pOpaque->Get("mgm.purge.version");
      int maxversion = 0;

      if (!max_count.length()) {
        maxversion = -1;
      } else {
        maxversion = atoi(max_count.c_str());

        if (!maxversion) {
          stdErr = "error: illegal version count specified version-cnt=";
          stdErr += max_count.c_str();
          retc = EINVAL;
          return SFS_OK;
        }
      }

      struct stat buf;

      if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, "")) {
        stdErr = "error; unable to stat path=";
        stdErr += spath.c_str();
        retc = errno;
        return SFS_OK;
      }

      // Third party copy the file to a temporary name
      ProcCommand Cmd;
      eos::common::Path atomicPath(spath.c_str());
      XrdOucString info;
      info += "&mgm.cmd=file&mgm.subcmd=copy&mgm.file.target=";
      info += atomicPath.GetAtomicPath(true);
      info += "&mgm.path=";
      info += spath.c_str();
      retc = Cmd.open("/proc/user", info.c_str(), *pVid, mError);
      Cmd.AddOutput(stdOut, stdErr);
      Cmd.close();

      if ((!Cmd.GetRetc())) {
        if (maxversion > 0) {
          XrdOucString versiondir;
          eos::common::Path cPath(spath.c_str());
          versiondir += cPath.GetParentPath();
          versiondir += "/.sys.v#.";
          versiondir += cPath.GetName();
          versiondir += "/";

          if (gOFS->PurgeVersion(versiondir.c_str(), *mError, maxversion)) {
            stdErr += "error: unable to purge versions of path=";
            stdErr += spath.c_str();
            stdErr += "\n";
            stdErr += "error: ";
            stdErr += mError->getErrText();
            retc = mError->getErrInfo();
            return SFS_OK;
          }
        }

        // Everything worked well
        stdOut = "info: created new version of '";
        stdOut += spath.c_str();
        stdOut += "'";

        if (maxversion > 0) {
          stdOut += " keeping ";
          stdOut += (int) maxversion;
          stdOut += " versions!";
        }
      }
    }

    // List or grab version(s) of a file
    if (mSubCmd == "versions") {
      cmdok = true;
      XrdOucString grab = pOpaque->Get("mgm.grab.version");

      if (grab == "-1") {
        ProcCommand Cmd;
        // list versions
        eos::common::Path vpath(spath.c_str());
        XrdOucString info;
        info += "&mgm.cmd=ls&mgm.option=-l";
        info += "&mgm.path=";
        info += vpath.GetVersionDirectory();
        Cmd.open("/proc/user", info.c_str(), *pVid, mError);
        Cmd.AddOutput(stdOut, stdErr);
        Cmd.close();
        retc = Cmd.GetRetc();

        if (retc && (retc == ENOENT)) {
          stdOut = "";
          stdErr = "error: no version exists for '";
          stdErr += spath.c_str();
          stdErr += "'";
          return SFS_OK;
        }
      } else {
        eos::common::Path vpath(spath.c_str());
        struct stat buf;
        struct stat vbuf;

        if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, "")) {
          stdErr = "error; unable to stat path=";
          stdErr += spath.c_str();
          retc = errno;
          return SFS_OK;
        }

        // grab version
        XrdOucString versionname = pOpaque->Get("mgm.grab.version");

        if (!versionname.length()) {
          stdErr = "error: you have to provide the version you want to stage!";
          retc = EINVAL;
          return SFS_OK;
        }

        XrdOucString versionpath = vpath.GetVersionDirectory();
        versionpath += versionname;

        if (gOFS->_stat(versionpath.c_str(), &vbuf, *mError, *pVid, "")) {
          stdErr = "error: failed to stat your provided version path='";
          stdErr += versionpath;
          stdErr += "'";
          retc = errno;
          return SFS_OK;
        }

        // now stage a new version of the existing file
        XrdOucString versionedpath;

        if (gOFS->Version(eos::common::FileId::InodeToFid(buf.st_ino), *mError,
                          *pVid, -1, &versionedpath)) {
          stdErr += "error: unable to create a version of path=";
          stdErr += spath.c_str();
          stdErr += "\n";
          stdErr += "error: ";
          stdErr += mError->getErrText();
          retc = mError->getErrInfo();
          return SFS_OK;
        }

        // and stage back the desired version
        if (gOFS->rename(versionpath.c_str(), spath.c_str(), *mError, *pVid)) {
          stdErr += "error: unable to stage";
          stdErr += " '";
          stdErr += versionpath.c_str();
          stdErr += "' back to '";
          stdErr += spath.c_str();
          stdErr += "'";
          retc = errno;
          return SFS_OK;
        } else {
          {
            // Copy the xattrs of the current file to the newly restored one
            std::set<std::string> exclude_xattrs {"sys.utrace", "sys.vtrace"};
            eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
            auto versioned_fmd = gOFS->eosView->getFile(versionedpath.c_str());
            auto restored_fmd = gOFS->eosView->getFile(spath.c_str());

            if (!versioned_fmd || !restored_fmd) {
              stdErr = "error: failed to copy xattrs";
              retc = EINVAL;
              return SFS_OK;
            }

            eos::IFileMD::XAttrMap map_xattrs = versioned_fmd->getAttributes();

            for (const auto& xattr : map_xattrs) {
              if (exclude_xattrs.find(xattr.first) == exclude_xattrs.end()) {
                restored_fmd->setAttribute(xattr.first, xattr.second);
              }
            }

            gOFS->eosView->updateFileStore(restored_fmd.get());
          }
          stdOut += "success: staged '";
          stdOut += versionpath;
          stdOut += "' back to '";
          stdOut += spath.c_str();
          stdOut += "'";
          stdOut += " - the previous file is now '";
          stdOut += versionedpath;
          stdOut += ";";
        }
      }
    }
  }

  if (!cmdok) {
    stdErr = "error: don't know subcmd=";
    stdErr += mSubCmd;
    retc = EINVAL;
  }

  return SFS_OK;
}
EOSMGMNAMESPACE_END
