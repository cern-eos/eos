//------------------------------------------------------------------------------
// File: credential-finder.cc
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

#include "gtest/gtest.h"
#include "auth/CredentialFinder.hh"

TEST(Environment, BasicSanity)
{
  Environment env;
  std::string envStr = "KEY1=VALUE";
  envStr.append(1, '\0');
  envStr += "non-key value entry";
  envStr.append(1, '\0');
  envStr += "Key2=SomeValue";
  envStr.append(1, '\0');
  envStr += "KEY1=Duplicate";
  envStr.append(1, '\0');
  env.fromString(envStr);
  std::vector<std::string> expected = {"KEY1=VALUE", "non-key value entry", "Key2=SomeValue", "KEY1=Duplicate"};
  ASSERT_EQ(env.getAll(), expected);
  ASSERT_EQ(env.get("KEY1"), "VALUE");
  ASSERT_EQ(env.get("Key2"), "SomeValue");
  // now try reading from a file
  const std::string filename("/tmp/fuse-testfile");
  FILE* out = fopen(filename.c_str(), "w");
  fwrite(envStr.c_str(), 1, envStr.size(), out);
  fclose(out);
  Environment env2;
  env2.fromFile(filename);
  ASSERT_EQ(env2.getAll(), expected);
  ASSERT_EQ(env2.get("KEY1"), "VALUE");
  ASSERT_EQ(env2.get("Key2"), "SomeValue");
}

TEST(TrustedCredentials, BasicSanity)
{
  TrustedCredentials emptycreds;
  ASSERT_TRUE(emptycreds.empty());
  ASSERT_EQ(emptycreds.toXrdParams(), "xrd.wantprot=unix");
  TrustedCredentials cred1;
  ASSERT_TRUE(cred1.empty());
  cred1.setKrb5("/tmp/some-file", 5, 6, 0);
  ASSERT_FALSE(cred1.empty());
  ASSERT_THROW(cred1.setx509("/tmp/some-other-file", 1, 2, 0), FatalException);
  ASSERT_EQ(cred1.toXrdParams(),
            "xrd.k5ccname=/tmp/some-file&xrd.secgid=6&xrd.secuid=5&xrd.wantprot=krb5,unix");
  TrustedCredentials cred2;
  cred2.setKrk5("keyring-name", 5, 6);
  ASSERT_FALSE(cred2.empty());
  ASSERT_EQ(cred2.toXrdParams(),
            "xrd.k5ccname=keyring-name&xrd.secgid=6&xrd.secuid=5&xrd.wantprot=krb5,unix");
  TrustedCredentials cred3;
  cred3.setx509("/tmp/some-file", 5, 6, 0);
  ASSERT_FALSE(cred3.empty());
  ASSERT_EQ(cred3.toXrdParams(),
            "xrd.gsiusrpxy=/tmp/some-file&xrd.secgid=6&xrd.secuid=5&xrd.wantprot=gsi,unix");
  TrustedCredentials cred4;
  cred4.setx509("/tmp/some-evil&file=", 5, 6, 0);
  ASSERT_FALSE(cred4.empty());
  ASSERT_EQ(cred4.toXrdParams(), "xrd.wantprot=unix");
}
