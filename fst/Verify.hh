// ----------------------------------------------------------------------
// File: Verify.hh
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

#ifndef __XRDFSTOFS_VERIFY_HH__
#define __XRDFSTOFS_VERIFY_HH__
#include "fst/Namespace.hh"
#include "common/FileId.hh"
#include <XrdOuc/XrdOucString.hh>
#include <vector>

class XrdOucEnv;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class Verify
{
public:

  unsigned long long fId;
  unsigned long fsId;
  unsigned long cId;
  unsigned long lId;

  XrdOucString localPrefix;
  XrdOucString managerId;
  XrdOucString opaque;
  XrdOucString container;
  XrdOucString path;

  bool computeChecksum;
  bool commitChecksum;
  bool commitSize;
  bool commitFmd;

  unsigned int verifyRate;

  Verify(unsigned long long fid, unsigned long fsid, const char* localprefix,
         const char* managerid, const char* inopaque, const char* incontainer,
         unsigned long incid, unsigned long inlid, const char* inpath,
         bool inComputeChecksum, bool inCommitChecksum, bool inCommitSize,
         bool inCommitFmd, unsigned int inVerifyRate)
  {
    fId = fid;
    fsId = fsid;
    localPrefix = localprefix;
    managerId = managerid;
    opaque = inopaque;
    container = incontainer;
    cId = incid;
    path = inpath;
    lId = inlid;
    computeChecksum = inComputeChecksum;
    commitChecksum = inCommitChecksum;
    commitSize = inCommitSize;
    verifyRate = inVerifyRate;
    commitFmd = inCommitFmd;
  }

  static Verify*
  Create(XrdOucEnv* capOpaque)
  {
    // decode the opaque tags
    const char* localprefix = 0;
    XrdOucString hexfids = "";
    XrdOucString hexfid = "";
    XrdOucString access = "";
    const char* container = 0;
    const char* scid = 0;
    const char* layout = 0;
    const char* path = 0;
    bool computeChecksum = false;
    bool commitChecksum = false;
    bool commitSize = false;
    bool commitFmd = false;
    const char* sfsid = 0;
    const char* smanager = 0;
    unsigned long long fid = 0;
    unsigned long fsid = 0;
    unsigned long cid = 0;
    unsigned long lid = 0;
    unsigned int verifyRate = 0;

    if (!capOpaque) {
      return 0;
    }

    localprefix = capOpaque->Get("mgm.localprefix");
    hexfid = capOpaque->Get("mgm.fid");
    sfsid = capOpaque->Get("mgm.fsid");
    smanager = capOpaque->Get("mgm.manager");
    access = capOpaque->Get("mgm.access");
    container = capOpaque->Get("mgm.container");
    scid = capOpaque->Get("mgm.cid");
    path = capOpaque->Get("mgm.path");
    layout = capOpaque->Get("mgm.lid");

    if (capOpaque->Get("mgm.verify.compute.checksum")) {
      computeChecksum = atoi(capOpaque->Get("mgm.verify.compute.checksum"));
    }

    if (capOpaque->Get("mgm.verify.commit.checksum")) {
      commitChecksum = atoi(capOpaque->Get("mgm.verify.commit.checksum"));
    }

    if (capOpaque->Get("mgm.verify.commit.size")) {
      commitSize = atoi(capOpaque->Get("mgm.verify.commit.size"));
    }

    if (capOpaque->Get("mgm.verify.commit.fmd")) {
      commitFmd = atoi(capOpaque->Get("mgm.verify.commit.fmd"));
    }

    if (capOpaque->Get("mgm.verify.rate")) {
      verifyRate = atoi(capOpaque->Get("mgm.verify.rate"));
    }

    // permission check
    if (access != "verify") {
      return 0;
    }

    if (!localprefix || !hexfid.length() || !sfsid || !smanager || !layout ||
        !scid) {
      return 0;
    }

    cid = strtoul(scid, 0, 10);
    lid = strtoul(layout, 0, 10);
    int envlen = 0;
    fid = eos::common::FileId::Hex2Fid(hexfid.c_str());
    fsid = atoi(sfsid);
    return new Verify(fid, fsid, localprefix, smanager, capOpaque->Env(envlen),
                      container, cid, lid, path, computeChecksum, commitChecksum, commitSize,
                      commitFmd, verifyRate);
  };

  ~Verify() { };

  //----------------------------------------------------------------------------
  //! Display information about current verification job
  //----------------------------------------------------------------------------
  void
  Show(const char* show = "")
  {
    eos_static_info("Verify fxid=%08llx on fs=%u path=%s compute_checksum=%d "
                    "commit_checksum=%d commit_size=%d commit_fmd=%d "
                    "verify_rate=%d %s", fId, fsId, path.c_str(),
                    computeChecksum, commitChecksum, commitSize, commitFmd,
                    verifyRate, show);
  }
};

EOSFSTNAMESPACE_END

#endif
