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

//------------------------------------------------------------------------------
// Helper class to instantiate and use an AuthenticationGroup.
//------------------------------------------------------------------------------
class AuthenticationFixture : public AuthenticationGroup {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  AuthenticationFixture(const CredentialConfig &config)
  : AuthenticationGroup(config) {
    myLocalJail.sameJailAsThisPid = true;
  }

  //----------------------------------------------------------------------------
  // Get CAS path - maybe make configurable in the future, or something
  //----------------------------------------------------------------------------
  static std::string getCASPath() {
    return "/tmp/eos-fusex-unit-tests/cas";
  }

  //----------------------------------------------------------------------------
  // Initialize CAS
  //----------------------------------------------------------------------------
  static void InitializeCAS() {
    system(SSTR("rm -rf " << getCASPath()).c_str());
    system(SSTR("mkdir -p " << getCASPath()).c_str());
  }

  //----------------------------------------------------------------------------
  // Make unix-only configuration
  //----------------------------------------------------------------------------
  static CredentialConfig makeUnixConfig() {
    InitializeCAS(); // This slows the tests down, maybe fix later

    CredentialConfig config;
    config.credentialStore = getCASPath();
    return config;
  }

  //----------------------------------------------------------------------------
  // Make kerberos-only configuration
  //----------------------------------------------------------------------------
  static CredentialConfig makeKrb5Config() {
    InitializeCAS(); // This slows the tests down, maybe fix later

    CredentialConfig config;
    config.use_user_krb5cc = true;
    config.fuse_shared = true;
    config.credentialStore = getCASPath();
    return config;
  }

  //----------------------------------------------------------------------------
  // Inject fake process with given properties
  //----------------------------------------------------------------------------
  void injectProcess(pid_t pid, pid_t ppid, pid_t pgrp, pid_t sid,
                     Jiffies startup, unsigned flags) {
    ProcessInfo info;
    info.fillStat(pid, ppid, pgrp, sid, startup, flags);
    processInfoProvider()->inject(pid, info);
  }

  //----------------------------------------------------------------------------
  // Create environment with the given variables
  //----------------------------------------------------------------------------
  Environment createEnv(const std::string& kerberosPath,
                        const std::string& x509Path);

  //----------------------------------------------------------------------------
  // Define a standard local jail
  //----------------------------------------------------------------------------
  const JailInformation& localJail() const {
    return myLocalJail;
  }

private:
  JailInformation myLocalJail;
};

//------------------------------------------------------------------------------
// Unix authentication fixture - any tests using this are pre-configured
// to use unix only
//------------------------------------------------------------------------------
class UnixAuthF : public AuthenticationFixture, public ::testing::Test {
public:
  UnixAuthF() : AuthenticationFixture(makeUnixConfig()) {}
};

//------------------------------------------------------------------------------
// krb5 authentication fixture - any tests using this are pre-configured
// to use krb5 only, with fallback to unix
//------------------------------------------------------------------------------
class Krb5AuthF : public AuthenticationFixture, public ::testing::Test {
public:
  Krb5AuthF() : AuthenticationFixture(makeKrb5Config()) {}
};

#endif
