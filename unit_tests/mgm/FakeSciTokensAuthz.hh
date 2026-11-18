//------------------------------------------------------------------------------
// File: FakeSciTokensAuthz.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#ifndef EOS_UNIT_TESTS_MGM_FAKESCITOKENSAUTHZ_HH
#define EOS_UNIT_TESTS_MGM_FAKESCITOKENSAUTHZ_HH

#include "mgm/authz/XrdMgmAuthz.hh"
#include <XrdAcc/XrdAccAuthorize.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <cstring>
#include <string>
#include <utility>

//------------------------------------------------------------------------------
//! Stands in for XrdSciTokens in the token authorization chain of the MGM,
//! which ends with XrdMgmAuthz. It accepts a single token: its scope is the
//! paths under a prefix, and it maps to a given user. It behaves like the
//! Access method of XrdSciTokens (XrdSciTokensAccess.cc):
//! - the token comes from the "authz" CGI, without its "Bearer%20" prefix, or
//!   else from the credentials of a ztn client;
//! - on a path in its scope, the token writes its user in the "request.name"
//!   attribute of the caller's entity, and is allowed;
//! - on another path, the token goes to XrdMgmAuthz with a new entity, which
//!   has no name;
//! - no token, or a token that does not validate, goes to XrdMgmAuthz with the
//!   caller's entity.
//------------------------------------------------------------------------------
class FakeSciTokensAuthz : public XrdAccAuthorize {
public:
  using XrdAccAuthorize::Access;

  FakeSciTokensAuthz(std::string token, std::string scope, std::string user)
      : mToken(std::move(token))
      , mScope(std::move(scope))
      , mUser(std::move(user))
  {
  }

  XrdAccPrivs
  Access(const XrdSecEntity* entity, const char* path, const Access_Operation oper,
         XrdOucEnv* env) override
  {
    const char* authz = env ? env->Get("authz") : nullptr;

    if (authz && !strncmp(authz, "Bearer%20", 9)) {
      authz += 9;
    }

    if (!authz && entity && !strcmp("ztn", entity->prot) && entity->creds &&
        (entity->credslen > 0) && (entity->creds[entity->credslen] == '\0')) {
      authz = entity->creds;
    }

    if (!authz || (mToken != authz)) {
      return mNext.Access(entity, path, oper, env);
    }

    if (std::string(path).rfind(mScope, 0) != 0) {
      XrdSecEntity fresh;
      return mNext.Access(&fresh, path, oper, env);
    }

    entity->eaAPI->Add("request.name", mUser, true);
    return XrdAccPriv_All;
  }

  int
  Audit(const int, const XrdSecEntity*, const char*, const Access_Operation,
        XrdOucEnv*) override
  {
    return 0;
  }

  int
  Test(const XrdAccPrivs, const Access_Operation) override
  {
    return 0;
  }

private:
  std::string mToken;
  std::string mScope;
  std::string mUser;
  XrdMgmAuthz mNext;
};

#endif
