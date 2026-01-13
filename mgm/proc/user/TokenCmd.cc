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
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/ofs/XrdMgmOfsDirectory.hh"
#include "mgm/quota/Quota.hh"
#include "mgm/recycle/Recycle.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/access/Access.hh"
#include "common/Path.hh"
#include "common/Definitions.hh"
#include "common/token/EosTok.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"

EOSMGMNAMESPACE_BEGIN

int
eos::mgm::TokenCmd::StoreToken(const std::string& token,
                               const std::string& voucherid, std::string& tokenpath, uid_t uid, gid_t gid)
{
  XrdOucErrInfo info;
  std::shared_ptr<eos::IFileMD> fmd;

  if (!GetTokenPrefix(info, uid, gid, tokenpath)) {
    tokenpath += voucherid;

    // create file with voucherid name
    try {
      fmd.reset();
      fmd = gOFS->eosView->getFile(tokenpath.c_str(), 0, 0);
      return EEXIST;
    } catch (eos::MDException& e) {
      fmd = gOFS->eosView->createFile(tokenpath, 0, 0);
      fmd->setSize(0);
      fmd->setCUid(uid);
      fmd->setCGid(gid);
      // store token as extended attribute
      fmd->setAttribute("sys.token", token);
      gOFS->eosView->updateFileStore(fmd.get());
    }

    return 0;
  }

  return EIO;
}

/*----------------------------------------------------------------------------*/
int
eos::mgm::TokenCmd::GetTokenPrefix(XrdOucErrInfo& error, uid_t uid, gid_t gid,
                                   std::string& tokenpath)
/*----------------------------------------------------------------------------*/
{
  const char* epname = "GetTokenPrefix";
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  char stokenuser[4096];
  time_t now = time(NULL);
  struct tm nowtm;
  localtime_r(&now, &nowtm);

  do {
    snprintf(stokenuser, sizeof(stokenuser) - 1, "%s/uid:%u/%04u/%02u/%02u/",
             gOFS->MgmProcTokenPath.c_str(),
             uid,
             1900 + nowtm.tm_year,
             nowtm.tm_mon + 1,
             nowtm.tm_mday);
    struct stat buf;

    if (!gOFS->_stat(stokenuser, &buf, error, rootvid, "")) {
      tokenpath = stokenuser;
      return SFS_OK;
    }

    // Verify/create group/user directory
    if (gOFS->_mkdir(stokenuser, S_IRUSR | S_IXUSR | SFS_O_MKPTH, error, rootvid,
                     "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - the "
                        "token user directory couldn't be created");
    }

    // Check the user token directory

    if (gOFS->_stat(stokenuser, &buf, error, rootvid, "")) {
      return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                        "determine ownership of the token user directory",
                        stokenuser);
    }

    // Check the ownership of the user directory
    if ((buf.st_uid != uid) || (buf.st_gid != gid)) {
      // Set the correct ownership
      if (gOFS->_chown(stokenuser, uid, gid, error, rootvid, "")) {
        return gOFS->Emsg(epname, error, EIO, "remove existing file - could not "
                          "change ownership of the token user directory",
                          stokenuser);
      }
    }

    tokenpath = stokenuser;
    return SFS_OK;
  } while (1);
}




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

  if (mVid.token) {
    // a token authenticated user cannot issue another token
    reply.set_retc(EPERM);
    reply.set_std_err("error: a token authorized user cannot issue another token");
    return reply;
  }

  eos_static_info("root=%d sudoer=%d uid=%u gid=%u", mVid.hasUid(0), mVid.sudoer,
                  mVid.uid, mVid.gid);
  int mode = R_OK | T_OK;

  if (token.permission().find("x") != std::string::npos) {
    mode |= X_OK;
  }

  if (token.permission().find("w") != std::string::npos) {
    mode |= W_OK;
  }

  if (token.vtoken().empty()) {
    eos_static_info("%s\n", token.vtoken().c_str());

    // check who asks for a token
    if ((mVid.hasUid(0))) {
      // we issue all the token in the world for them
    } else {
      // for user token, we only allow rwxd, nothing else;
      for (char const& c : token.permission()) {
        if ((c != 'r') &&
            (c != 'x') &&
            (c != 'w') &&
            (c != 'd') &&
            (c != '!') &&
            (c != '+')) {
          reply.set_retc(EINVAL);
          reply.set_std_err("error: you can only use rwx[+1]d in your permission set!");
          return reply;
        }
      }

      if (token.expires() > ((uint64_t)time(NULL) + (365 * 86400))) {
        reply.set_retc(EINVAL);
        reply.set_std_err("error: the maximum lifetime for a user token is one year!");
        return reply;
      }

      XrdOucErrInfo error;
      // we restrict only on-behalf of the requestor tokens
      token.set_owner(mVid.uid_string);
      token.set_group(mVid.gid_string);

      // deal with multiple paths
      std::vector<std::string> paths;
      eos::common::StringConversion::MulticharTokenize(token.path(),
                                              paths, "://:");

      for ( auto p: paths) {
	// we verify that mVid owns the path in the token
	if (p.back() == '/') {
	  if (token.allowtree()) {
	    if (gOFS->_access(p.c_str(), mode, error, mVid, "")) {
	      if (error.getErrInfo()) {
		// stat error
              reply.set_retc(error.getErrInfo());
              reply.set_std_err(error.getErrText());
              return reply;
	      }
	    }
	  } else {
	    // directory token
	    if (gOFS->_access(p.c_str(), mode, error, mVid, "")) {
	      if (errno) {
		// return errno
		reply.set_retc(errno);
		if (errno == ENOENT) {
		  reply.set_std_err("error: path does not exist!");
		} else {
		  reply.set_std_err("error: no permission!");
		}
		return reply;
	      }
	    }
	  }
	} else {
	  // file path
	  mode |= F_OK;
	  // now tree permission for files
	  token.set_allowtree(false);
	  eos::common::Path cPath(p.c_str());
	  errno = 0;
	  if (gOFS->_access(p.c_str(), mode, error, mVid, "")) {
	    if (errno) {
	      // return errno
	      reply.set_retc(errno);
	      if (errno == ENOENT) {
		reply.set_std_err("error: path does not exist!");
	      } else {
		reply.set_std_err("error: no permission!");
	      }
	      return reply;
	    }
	  }
        }
      }
    }
  }

  eos::common::EosTok eostoken;
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  std::string key = symkey ? symkey->GetKey64() : "0123456789defaultkey";

  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    struct stat buf;

    if (::stat(getenv("EOS_MGM_TOKEN_KEYFILE"), &buf)) {
      reply.set_retc(-ENOKEY);
      reply.set_std_err("error: unable to load token keyfile");
      return reply;
    } else {
      if ((buf.st_uid != DAEMONUID) ||
          (buf.st_mode != 0100400)) {
        reply.set_retc(-ENOKEY);
        eos_static_err("mode bit is %o", buf.st_mode);
        reply.set_std_err("error: unable to load token keyfile - wrong ownership (must be daemon:400)");
        return reply;
      }
    }

    key = eos::common::StringConversion::LoadFileIntoString(
            getenv("EOS_MGM_TOKEN_KEYFILE"), key);
  }

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
      std::string token = eostoken.Write(key) ;
      outStream << token;
      std::string dump;
      eostoken.Dump(dump, true, true);
      std::string voucherid = eostoken.Voucher();
      {
        eos::common::RWMutexReadLock lock(Access::gAccessMutex);

        if (Access::gAllowedTokens.size()) {
          outStream << std::endl;
          outStream <<
                    "warning: the token will not be usuable without approval of an administrator!"
                    << std::endl;
          outStream << "         ask for token approval of voucher:id=" << voucherid <<
                    std::endl;
        }
      }
      std::string token_path;

      if ((ret_c = StoreToken(dump, voucherid, token_path, vid.uid, vid.gid))) {
        errStream << "error: could not store the token: " << ret_c << std::endl;
      } else {
        eos_warning("creating voucher=%s path=%s owner=%s group=%s perm=%s expires=%lu store=%s token:'%s'\n"
                    ,
                    eostoken.Voucher().c_str(),
                    eostoken.Path().c_str(),
                    eostoken.Owner().c_str(),
                    eostoken.Group().c_str(),
                    eostoken.Permission().c_str(),
                    eostoken.Expires(),
                    token_path.c_str(),
                    dump.c_str());
      }
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
