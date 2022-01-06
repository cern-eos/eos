//------------------------------------------------------------------------------
// File: TokenCmd.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "TokenCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/Access.hh"
#include "common/Path.hh"
#include "common/token/EosTok.hh"


EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::TokenCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream outStream;
  std::ostringstream errStream;
  XrdOucString m_err {""};
  int ret_c = 0;
  eos::console::TokenProto token = mReqProto.token();

  // ----------------------------------------------------------------------------------------------
  // security barrier for token issuing
  // ----------------------------------------------------------------------------------------------
  // a regular user can only issue tokens for files/paths he owns
  // - if a token asks for a directory path, the user has to own that directory
  // - if a oktne assk for a file path, the user has to own that file or if there is no file he has
  //   to own the parent directory
  // a sudoer or root can ask for any token
  // ----------------------------------------------------------------------------------------------

  if (!eos::common::EosTok::sTokenGeneration) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: change the generation value != 0 e.g. using eos space config default space.token.generation=1 to enable token creation");
    return reply;
  }

  eos_static_info("root=%d sudoer=%d uid=%u gid=%u", mVid.hasUid(0), mVid.sudoer,
                  mVid.uid, mVid.gid);

  if (token.vtoken().empty()) {
    eos_static_info("%s\n", token.vtoken().c_str());

    // check who asks for a token
    if ((mVid.hasUid(0))) {
      // we issue all the token in the world for them
    } else {
      struct stat buf;
      XrdOucErrInfo error;
      // we restrict only on-behalf of the requestor tokens
      token.set_owner(mVid.uid_string);
      token.set_group(mVid.gid_string);

      // we verify that mVid owns the path in the token
      if (token.path().back() == '/') {
        // tree/directory path
        if (gOFS->_stat(token.path().c_str(), &buf, error, mVid, "", 0, false, 0) ||
            (buf.st_uid != mVid.uid)) {
          if (error.getErrInfo()) {
            // stat error
            reply.set_retc(error.getErrInfo());
            reply.set_std_err(error.getErrText());
            return reply;
          } else {
            // owner error
            reply.set_retc(EACCES);
            reply.set_std_err("error: you are not the owner of the path given in your request and you are not a sudoer or root!");
            return reply;
          }
        }
      } else {
        // file path
        eos::common::Path cPath(token.path().c_str());

        if (gOFS->_stat(token.path().c_str(), &buf, error, mVid, "", 0, false, 0)) {
          // file does not exist
          if (gOFS->_stat(cPath.GetParentPath(), &buf, error, mVid, "", 0, false, 0)) {
            // parent does not exist
            reply.set_retc(ENOENT);
            reply.set_std_err("error: neither the given path nor the parent path exists!");
            return reply;
          } else {
            if (buf.st_uid != mVid.uid) {
              // owner error
              reply.set_retc(EACCES);
              reply.set_std_err("error: you are not the owner of the parent path given in your request and you are not a sudoer or root!");
              return reply;
            }
          }
        } else {
          if (buf.st_uid != mVid.uid) {
            // owner error
            reply.set_retc(EACCES);
            reply.set_std_err("error: you are not the owner of the path given in your request and you are not a sudoer or root!");
            return reply;
          }
        }
      }
    }
  }

  eos::common::EosTok eostoken;
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  std::string key = symkey ? symkey->GetKey64() : "0123456789defaultkey";

  if (token.vtoken().empty()) {
    if (token.permission().find(":") != std::string::npos) {
      // someone could try to inject more acl entries here
      reply.set_retc(-EPERM);
      reply.set_std_err("error: illegal permission requested");
      return reply;
    }

    // create a token
    eostoken.SetPath(token.path(), token.allowtree());
    eostoken.SetPermission(token.permission());
    eostoken.SetExpires(token.expires());
    eostoken.SetOwner(token.owner());
    eostoken.SetGroup(token.group());
    eostoken.SetGeneration(eos::common::EosTok::sTokenGeneration);
    eostoken.SetRequester(mVid.getTrace());

    for (int i = 0; i < token.origins_size(); ++i) {
      const eos::console::TokenAuth& auth = token.origins(i);
      eostoken.AddOrigin(auth.host(), auth.name(), auth.prot());
    }

    if (eostoken.VerifyOrigin(vid.host, vid.uid_string,
                              std::string(vid.prot.c_str())) == -EBADE) {
      errStream << "error: one or several origin regexp's are invalid" << std::endl;
      ret_c = -EBADE;
    } else {
      outStream << eostoken.Write(key) ;
    }
  } else {
    if (!(ret_c = eostoken.Read(token.vtoken(), key,
                                eos::common::EosTok::sTokenGeneration.load(), true))) {
      std::string dump;
      eostoken.Dump(dump);
      outStream << dump;
    } else {
      errStream << "error: cannot read token" << std::endl;
    }

    if (eostoken.VerifyOrigin(vid.host, vid.uid_string,
                              std::string(vid.prot.c_str())) == -EBADE) {
      errStream << "error: one or several origin regexp's are invalid" << std::endl;
      ret_c = -EBADE;
    }
  }

  reply.set_retc(ret_c);
  reply.set_std_out(outStream.str());
  reply.set_std_err(errStream.str());
  return reply;
}

EOSMGMNAMESPACE_END
