// ----------------------------------------------------------------------
// File: Chksum.cc
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::chksum(XrdSfsFileSystem::csFunc Func,
                  const char* csName,
                  const char* inpath,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief retrieve a checksum
 *
 * @param func function to be performed 'csCalc','csGet' or 'csSize'
 * @param csName name of the checksum
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * We support only checksum type 'eos' which has the maximum length of 20 bytes
 * and returns a checksum based on the defined directory policy (can be adler,
 * md5,sha1 ...). The EOS directory based checksum configuration does not map
 * 1:1 to the XRootD model where a storage system supports only one flavour.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "chksum";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Nobody();
  char buff[MAXPATHLEN + 8];
  int rc;
  XrdOucString CheckSumName = csName;
  // ---------------------------------------------------------------------------
  // retrieve meta data for <path>
  // ---------------------------------------------------------------------------
  // A csSize request is issued usually once to verify everything is working. We
  // take this opportunity to also verify the checksum name.
  // ---------------------------------------------------------------------------
  rc = 0;

  if (Func == XrdSfsFileSystem::csSize) {
    if (1) {
      // just return the length
      error.setErrCode(20);
      return SFS_OK;
    } else {
      eos_static_info("not supported");
      strcpy(buff, csName);
      strcat(buff, " checksum not supported.");
      error.setErrInfo(ENOTSUP, buff);
      return SFS_ERROR;
    }
  }

  NAMESPACEMAP;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  gOFS->MgmStats.Add("Checksum", vid.uid, vid.gid, 1);
  XrdOucEnv Open_Env(ininfo);
  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", inpath, error);
  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  eos_info("path=%s", inpath);
  // ---------------------------------------------------------------------------
  errno = 0;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::Path cPath(path);

  // ---------------------------------------------------------------------------
  // Everything else requires a path
  // ---------------------------------------------------------------------------

  if (!path) {
    strcpy(buff, csName);
    strcat(buff, " checksum path not specified.");
    error.setErrInfo(EINVAL, buff);
    return SFS_ERROR;
  }

  // ---------------------------------------------------------------------------
  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, cPath.GetPath());
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  bool enonet = false;

  try {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
    enonet = !fmd->getNumLocation();
  } catch (eos::MDException& e) {
    errno = e.getErrno();
  }

  if (!fmd) {
    // file does not exist
    *buff = 0;
    rc = ENOENT;
    MAYREDIRECT_ENOENT;
    MAYSTALL_ENOENT;
    error.setErrInfo(rc, "no such file or directory");
    return SFS_ERROR;
  }

  if (enonet) {
    // File has no committed replicas, we might bounce to an alive remote master
    if (!gOFS->mMaster->IsMaster() && gOFS->mMaster->IsRemoteMasterOk()) {
      lock.Release();
      // redirect ENONET to an alive remote master
      int port {0};
      std::string hostname;
      std::string master_id = gOFS->mMaster->GetMasterId();

      if (!eos::common::ParseHostNamePort(master_id, hostname, port)) {
        eos_err("msg=\"failed parsing remote master info\", id=%s",
                master_id.c_str());
        return Emsg(epname, error, ENOENT, "get checksum - failed parsing "
                    "remote master info", path);
      }

      error.setErrInfo(port, hostname.c_str());
      gOFS->MgmStats.Add("RedirectENONET", vid.uid, vid.gid, 1);
      return SFS_REDIRECT;
    }
  }

  // ---------------------------------------------------------------------------
  // Now determine what to do
  // ---------------------------------------------------------------------------
  if ((Func == XrdSfsFileSystem::csCalc) ||
      (Func == XrdSfsFileSystem::csGet)) {
  } else {
    error.setErrInfo(EINVAL, "Invalid checksum function.");
    return SFS_ERROR;
  }

  // Set the checksum type
  std::string cksum_type = eos::common::LayoutId::GetChecksumStringReal(
                             fmd->getLayoutId());
  sprintf(buff, "!%s ", cksum_type.c_str());
  // copy the checksum buffer
  const char* hv = "0123456789abcdef";
  size_t j = strlen(buff);

  for (size_t i = 0;
       i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++) {
    buff[j++] = hv[(fmd->getChecksum().getDataPadded(i) >> 4) & 0x0f];
    buff[j++] = hv[ fmd->getChecksum().getDataPadded(i) & 0x0f];
  }

  if (j == 0) {
    sprintf(buff, "NONE");
  } else {
    buff[j] = '\0';
  }

  eos_info("checksum=\"%s\"", buff);
  error.setErrInfo(0, buff);
  return SFS_OK;
}
