//------------------------------------------------------------------------------
// File: test-utils.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef AUTH_TEST_UTILS_HH
#define AUTH_TEST_UTILS_HH

#include <gtest/gtest.h>
#include "auth/ProcessCache.hh"
#include "auth/AuthenticationGroup.hh"

// class ProcessCacheFixture : public ::testing::Test {
// public:

//   ProcessCacheFixture()
//     : boundIdentityProvider(processCache.getBoundIdentityProvider()),
//       securityChecker(boundIdentityProvider.getSecurityChecker()),
//       processInfoProvider(processCache.getProcessInfoProvider()),
//       environmentReader(boundIdentityProvider.getEnvironmentReader())
//   {
//   }

//   BoundIdentityProvider boundIdentityProvider;
//   ProcessCache processCache;
//   SecurityChecker& securityChecker;
//   ProcessInfoProvider& processInfoProvider;
//   EnvironmentReader& environmentReader;


//   void configureUnixAuth()
//   {
//     CredentialConfig config;
//     processCache.setCredentialConfig(config);
//     boundIdentityProvider.setCredentialConfig(config);
//   }

//   void configureKerberosAuth()
//   {
//     CredentialConfig config;
//     config.use_user_krb5cc = true;
//     config.fuse_shared = true;
//     processCache.setCredentialConfig(config);
//     boundIdentityProvider.setCredentialConfig(config);
//   }

//   void injectProcess(pid_t pid, pid_t ppid, pid_t pgrp, pid_t sid,
//                      Jiffies startup, unsigned flags)
//   {
//     ProcessInfo info;
//     info.fillStat(pid, ppid, pgrp, sid, startup, flags);
//     processInfoProvider.inject(pid, info);
//   }

//   Environment createEnv(const std::string& kerberosPath,
//                         const std::string& x509Path)
//   {
//     std::string env;

//     if (!kerberosPath.empty()) {
//       env = "KRB5CCNAME=FILE:" + kerberosPath;
//       env.push_back('\0');
//     }

//     if (!x509Path.empty()) {
//       env += "X509_USER_PROXY=" + x509Path;
//       env.push_back('\0');
//     }

//     Environment ret;
//     ret.fromString(env);
//     return ret;
//   }

// };

//------------------------------------------------------------------------------
// Helper class for instantiating authentication groups. Inherit your test
// fixture from here.
//------------------------------------------------------------------------------
class AuthenticationFixture {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  AuthenticationFixture();

  //----------------------------------------------------------------------------
  // Lazy-initialize AuthenticationGroup.
  //----------------------------------------------------------------------------
  AuthenticationGroup* group();

  //----------------------------------------------------------------------------
  // Lazy-initialize ProcessCache.
  //----------------------------------------------------------------------------
  ProcessCache* processCache();

  //----------------------------------------------------------------------------
  // Lazy-initialize BoundIdentityProvider.
  //----------------------------------------------------------------------------
  BoundIdentityProvider* boundIdentityProvider();

private:
  CredentialConfig config;
  std::unique_ptr<AuthenticationGroup> groupPtr;
};

#endif
