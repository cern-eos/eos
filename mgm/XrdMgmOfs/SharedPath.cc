// ----------------------------------------------------------------------
// File: SharedPath.cc
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
std::string
XrdMgmOfs::CreateSharePath (const char* inpath,
                            const char* ininfo,
                            time_t expires,
                            XrdOucErrInfo &error,
                            eos::common::Mapping::VirtualIdentity & vid)
/*----------------------------------------------------------------------------*/
/*
 * @brief create a file sharing path with given liftime
 *
 * @param path file path to share
 * @param info opaque information
 * @param expires unixtimestamp when signature has to expire
 * @param error error object
 * @param vid virtual ID of the caller
 *
 * @return signed path <path>?<signature>
 */
{
  NAMESPACEMAP;
  errno = 0;

  if (info)
  {
  }

  if (_access(path, R_OK, error, vid, ""))
  {
    errno = EPERM;
    return std::string("");
  }

  XrdSfsFileExistence file_exists;
  if ((_exists(path, file_exists, error, vid, 0)))
  {
    errno = ENOENT;
    return std::string("");
  }

  if (file_exists != XrdSfsFileExistIsFile)
  {
    errno = EISDIR;
    return std::string("");
  }

  struct stat buf;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  if (_stat(path, &buf, error, rootvid))
  {
    return std::string("");
  }

  std::string signit = path;
  signit += "?";

  char sexpires[256];
  snprintf(sexpires, sizeof (sexpires) - 1, "%u", (unsigned int) expires);
  signit += "eos.share.expires=";
  signit += sexpires;
  signit += "&eos.share.fxid=";
  XrdOucString hexstring = "";
  eos::common::FileId::Fid2Hex(buf.st_ino, hexstring);
  signit += hexstring.c_str();

  signit += "&eos.share.signature=";

  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  if (!symkey)
  {
    errno = ENOKEY;
    return std::string("");
  }

  XrdOucString ouc_sign = sexpires;
  ouc_sign += path;
  ouc_sign += sexpires;
  ouc_sign += gOFS->MgmOfsInstanceName;
  ouc_sign += hexstring;
  XrdOucString ouc_signed;

  if (!XrdMqMessage::SymmetricStringEncrypt(ouc_sign,
                                            ouc_signed,
                                            (char*) symkey->GetKey()))
  {
    errno = EKEYREJECTED;
    return std::string("");
  }

  while (ouc_signed.replace("\n", ""))
  {
  }

  signit += ouc_signed.c_str();
  return signit;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::VerifySharePath (const char* path,
                            XrdOucEnv * opaque)
/*----------------------------------------------------------------------------*/
/*
 * @brief verify a file sharing path
 *
 * @param path file path to share
 * @param opaque information containing a file share signature
 *
 * @return true if valid otherwise false
 */
{
  // check if this is a signed path
  if (!opaque->Get("eos.share.signature"))
    return false;

  // check if this has a valid expiration date
  XrdOucString expires = opaque->Get("eos.share.expires");
  if (!expires.length() || expires == "0")
    return false;

  // check if this has fid
  XrdOucString fxid = opaque->Get("eos.share.fxid");
  if (!fxid.length())
    return false;

  // get the fid
  struct stat buf;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;
  if (_stat(path, &buf, error, rootvid))
  {
    return false;
  }

  XrdOucString hexstring = "";
  eos::common::FileId::Fid2Hex(buf.st_ino, hexstring);

  if (fxid != hexstring)
  {
    eos_warning("msg=\"shared file has changed file id - share URL not valid anymore\"");
    return false;
  }

  // check that it is not yet expired
  time_t expired = strtoul(expires.c_str(), 0, 10);
  time_t now = time(NULL);
  if (!expired || (expired < now))
  {
    int envlen;
    eos_static_err("msg=\"shared link expired\" path=%s info=%s\n", path, opaque->Env(envlen));
    return false;
  }

  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  if (!symkey)
  {
    eos_static_err("msg=\"failed to retrieve symmetric key to verify shared link");
    return false;
  }


  // verify the signature
  XrdOucString ouc_sign = expires;
  ouc_sign += path;
  ouc_sign += expires;
  ouc_sign += gOFS->MgmOfsInstanceName;
  ouc_sign += hexstring;
  XrdOucString ouc_signed;

  if (!XrdMqMessage::SymmetricStringEncrypt(ouc_sign,
                                            ouc_signed,
                                            (char*) symkey->GetKey()))
  {
    eos_static_err("msg=\"failed to encrypt to verify shared link");
    return false;
  }

  while (ouc_signed.replace("\n", ""))
  {
  }

  XrdOucString ouc_signature = opaque->Get("eos.share.signature");
  if (ouc_signature == ouc_signed)
  {
    return true;
  }
  else
  {

    int envlen;
    eos_static_err("msg=\"shared link with invalid signature\" path=%s info=%s len=%d len=%d\n", path, opaque->Env(envlen), ouc_signature.length(), ouc_signed.length());
    return false;
  }
}
