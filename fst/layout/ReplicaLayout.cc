// ----------------------------------------------------------------------
// File: ReplicaLayout.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/layout/ReplicaLayout.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ReplicaLayout::ReplicaLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : Layout(thisFile, "replica", lid, outerror)
{
  nReplica = eos::common::LayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)
  replicaClient = 0;
  replicaUrl = "";
  ioLocal = true;
  replicaIndex = 0;
}

/*----------------------------------------------------------------------------*/
int
ReplicaLayout::open(const char                *path,
                    XrdSfsFileOpenMode   open_mode,
                    mode_t               create_mode,
                    const XrdSecEntity        *client,
                    const char                *opaque)
{
  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");

  
  if (index) {
    replicaIndex = atoi(index);
    if ( (replicaIndex<0) || (replicaIndex>eos::common::LayoutId::kSixteenStripe) ) {
      eos_err("illegal replica index %d", replicaIndex);
      return gOFS.Emsg("ReplicaOpen",*error, EINVAL, "open replica - illegal replica index found", index);
    }
    ioLocal = true;
  } else {
    replicaIndex = -1;
    ioLocal = false;
  }

  replicaIndex++;

  // this points to the next URL in the chain
  XrdOucString reptag = "mgm.url"; reptag += replicaIndex;

  const char* rep = ofsFile->capOpaque->Get(reptag.c_str());

  // if we are not the last replica in the chain there must be a url for the next replica
  if (replicaIndex < nReplica) {
    if (!rep) {
      eos_err("Failed to open replica - missing url for replica %s", reptag.c_str());
      return gOFS.Emsg("ReplicaOpen",*error, EINVAL, "open replica - missing url for replica ", reptag.c_str());
    }
  }

  replicaUrl = rep;

  const char* val;
  int envlen;
  XrdOucString remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);

  // create the opaque information for the next replica in the chain
  if ( (val = ofsFile->openOpaque->Get("mgm.replicaindex"))) {
    XrdOucString oldindex = "mgm.replicaindex=";
    XrdOucString newindex = "mgm.replicaindex=";
    oldindex += val;
    newindex += (replicaIndex);
    remoteOpenOpaque.replace(oldindex.c_str(),newindex.c_str());
  } else {
    remoteOpenOpaque += "&mgm.replicaindex=";
    remoteOpenOpaque += (replicaIndex);
  }


  if (!ofsFile->isRW) {
    // read case
    if (ioLocal) {
      // read from this box
      return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
    } else {
      // read from remote box
      // create remote url
      replicaUrl += "?"; replicaUrl += remoteOpenOpaque;
      // open next replicas 
      replicaClient = new XrdClient(replicaUrl.c_str());
      // open remote file
      if (!replicaClient->Open(0,0, false)) {
        // open failed
        eos_err("Failed to open replica - remote open failed on ", replicaUrl.c_str());
        return gOFS.Emsg("ReplicaOpen",*error, EREMOTEIO, "open replica - remote open failed ", replicaUrl.c_str());
      }
    }
    return SFS_OK;
  } else {
    // write case
    // check if we are the last one in the chain?
    eos_static_debug("replicaindex=%u nreplica=%u url=%s?%s",replicaIndex, nReplica, replicaUrl.c_str(),remoteOpenOpaque.c_str());

    if ( (replicaIndex) < nReplica) {
      // create remote url
      replicaUrl += "?"; replicaUrl += remoteOpenOpaque;
      // open next replicas 
      replicaClient = new XrdClient(replicaUrl.c_str());
      // open remote file
      if (!replicaClient->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt | kXR_new, false)) {
        // open failed
        eos_err("Failed to open replica - remote open failed on ", replicaUrl.c_str());
        return gOFS.Emsg("ReplicaOpen",*error, EREMOTEIO, "open replica - remote open failed ", replicaUrl.c_str());
      }
    } 
    if (ioLocal) 
      return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
    return SFS_OK;
  }
}

/*----------------------------------------------------------------------------*/
ReplicaLayout::~ReplicaLayout() 
{
  if (replicaClient) {
    delete replicaClient;
  }
}


/*----------------------------------------------------------------------------*/
int
ReplicaLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  bool rc;
  if (ioLocal) 
    return ofsFile->readofs(offset, buffer,length);
  else {
    rc = replicaClient->Read(buffer, offset, length);
    if (!rc) {
      eos_err("Failed to read remote replica - read failed - %llu %llu %s", offset, length, replicaUrl.c_str());
      return gOFS.Emsg("ReplicaRead",*error, EREMOTEIO, "read remote replica - read failed", replicaUrl.c_str());
    }
    return length;
  }
}

/*----------------------------------------------------------------------------*/
int 
ReplicaLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int rc1 = SFS_OK;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->writeofs(offset, buffer, length);
  }
  if (replicaClient) {
    rc2 = replicaClient->Write(buffer, offset, length);
  }

  if (rc1 <0) {
    eos_err("Failed to write local replica - write failed - %llu %llu %s", offset, length, replicaUrl.c_str());
    return gOFS.Emsg("ReplicaWrite",*error, errno, "write local replica - write failed", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to write remote replica - write failed - %llu %llu %s", offset, length, replicaUrl.c_str());
    return gOFS.Emsg("ReplicaWrite",*error, EREMOTEIO, "write remote replica - write failed", replicaUrl.c_str());
  }
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
ReplicaLayout::truncate(XrdSfsFileOffset offset)
{
  int rc1 = SFS_OK;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->truncateofs(offset);
  }

  if (replicaClient) {
    rc2 = replicaClient->Truncate(offset);
  }

  if (rc1 <0) {
    eos_err("Failed to truncate local replica - %llu %s", offset, replicaUrl.c_str());
    return gOFS.Emsg("ReplicaTruncate",*error, errno, "truncate local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to truncate local replica - %llu %s", offset, replicaUrl.c_str());
    return gOFS.Emsg("Repl}icaTruncate",*error, EREMOTEIO, "truncate remote replica", replicaUrl.c_str());
  }
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
ReplicaLayout::sync() 
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    rc1 = ofsFile->syncofs();
  }

  if (replicaClient) {
    rc2 = replicaClient->Sync();
  }  
  
  if (rc1 <0) {
    eos_err("Failed to sync local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaSync",*error, errno, "sync local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to sync local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("Repl}icaSync",*error, EREMOTEIO, "sync remote replica", replicaUrl.c_str());
  }  
  return rc1;
}


/*----------------------------------------------------------------------------*/
int
ReplicaLayout::close()
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    rc1 = ofsFile->closeofs();
  }

  if (replicaClient) {
    rc2 = replicaClient->Close();
  }  
  
  if (rc1 <0) {
    eos_err("Failed to close local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaClose",*error, errno, "close local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to close local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaClose",*error, EREMOTEIO, "close remote replica", replicaUrl.c_str());
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
