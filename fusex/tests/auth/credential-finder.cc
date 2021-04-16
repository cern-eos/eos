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

#include "auth/CredentialFinder.hh"
#include "gtest/gtest.h"

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
  std::string key;

  ASSERT_TRUE(emptycreds.empty());
  ASSERT_EQ(emptycreds.toXrdParams(), "xrd.wantprot=unix");
  TrustedCredentials cred0;
  ASSERT_TRUE(cred0.empty());
  TrustedCredentials cred1(
			   UserCredentials::MakeKrb5(JailIdentifier(), "/tmp/some-file", 5, 6, key),
    {0, 0},
    ""
  );
  ASSERT_FALSE(cred1.empty());
  ASSERT_EQ(cred1.toXrdParams(),
            "xrd.k5ccname=/tmp/some-file&xrd.wantprot=krb5,unix&xrdcl.secgid=6&xrdcl.secuid=5");
  TrustedCredentials cred2(UserCredentials::MakeKrk5("keyring-name", 5, 6, key), {0, 0}, "");
  ASSERT_FALSE(cred2.empty());
  ASSERT_EQ(cred2.toXrdParams(),
            "xrd.k5ccname=keyring-name&xrd.wantprot=krb5,unix&xrdcl.secgid=6&xrdcl.secuid=5");
  TrustedCredentials cred3(
			   UserCredentials::MakeX509(JailIdentifier(), "/tmp/some-file", 5, 6, key), {0, 0},
    ""
  );

  ASSERT_FALSE(cred3.empty());
  ASSERT_EQ(cred3.toXrdParams(),
            "xrd.gsiusrpxy=/tmp/some-file&xrd.wantprot=gsi,unix&xrdcl.secgid=6&xrdcl.secuid=5");
  TrustedCredentials cred4(
			   UserCredentials::MakeX509(JailIdentifier(), "/tmp/some-evil&file=", 5, 6, key), {0, 0},
    ""
  );
  ASSERT_FALSE(cred4.empty());
  ASSERT_EQ(cred4.toXrdParams(), "xrd.wantprot=unix");
}
