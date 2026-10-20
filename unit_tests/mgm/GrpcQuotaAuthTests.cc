//------------------------------------------------------------------------------
// File: GrpcQuotaAuthTests.cc
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

#include "mgm/grpc/GrpcAuth.hh"
#include "gtest/gtest.h"

#ifdef EOS_GRPC

#include "common/Constants.hh"
#include "common/SymKeys.hh"
#include "common/token/EosTok.hh"
#include <cstdlib>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

namespace {

using eos::common::EosTok;
using eos::common::VirtualIdentity;
using eos::mgm::GrpcAuth;

struct SignedToken {
  std::string encoded;
  std::shared_ptr<EosTok> mapped;
};

SignedToken
IssueToken(const std::vector<std::string>& scopes, bool restrict_origin = false)
{
  EosTok issued;
  issued.SetPath("/eos/quota-test/", true);
  issued.SetPermission("rx");
  issued.SetOwner("1234");
  issued.SetGroup("4321");
  issued.SetExpires(time(nullptr) + 600);
  const auto generation = EosTok::sTokenGeneration.load();
  issued.SetGeneration(generation);

  for (const auto& scope : scopes) {
    issued.AddScope(scope);
  }

  if (restrict_origin) {
    issued.AddOrigin("allowed.example", "quota-user", "grpc");
  }

  // Match the in-process mapping key so decoding the raw credential would
  // succeed: these tests must detect a fallback which bypasses mapping.
  auto* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  const std::string key = symkey ? symkey->GetKey64() : "0123456789defaultkey";
  SignedToken token{issued.Write(key), std::make_shared<EosTok>()};
  EXPECT_FALSE(token.encoded.empty());
  EXPECT_EQ(0, token.mapped->Read(token.encoded, key, generation));
  EXPECT_TRUE(token.mapped->Valid());
  return token;
}

std::vector<std::string>
Authkeys(const std::string& encoded)
{
  std::vector<std::string> authkeys;

  for (const std::string prefix : {"zteos64:", "zteos64%3A", "zteos64%3a"}) {
    const std::string credential = prefix + encoded.substr(8);
    authkeys.push_back(credential);
    authkeys.push_back("Bearer " + credential);
    authkeys.push_back("Bearer%20" + credential);
  }

  return authkeys;
}

} // namespace

TEST(GrpcQuotaAuth, ExecScopeClassifiesQuotaOperations)
{
  for (const auto op : {eos::rpc::GET, eos::rpc::SET, eos::rpc::RM, eos::rpc::RMNODE}) {
    SCOPED_TRACE(static_cast<int>(op));
    eos::rpc::NSRequest request;
    request.mutable_quota()->set_op(op);
    EXPECT_EQ(op == eos::rpc::GET ? "grpc.quota.read" : "grpc.quota.write",
              GrpcAuth::ExecScope(request));
  }

  eos::rpc::NSRequest request;
  request.mutable_quota()->set_op(static_cast<eos::rpc::QUOTAOP>(100));
  EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
}

TEST(GrpcQuotaAuth, ExecScopeRejectsUnsetAndEveryNonquotaCommand)
{
  eos::rpc::NSRequest request;
  EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
  const auto* command = request.GetDescriptor()->FindOneofByName("command");
  ASSERT_NE(nullptr, command);

  for (int i = 0; i < command->field_count(); ++i) {
    const auto* field = command->field(i);

    if (field->name() == "quota") {
      continue;
    }

    SCOPED_TRACE(field->name());
    request.Clear();
    request.GetReflection()->MutableMessage(&request, field);
    EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
  }
}

TEST(GrpcQuotaAuth, ExecScopeRejectsEvenAnEmptyRoleOverride)
{
  eos::rpc::NSRequest request;
  request.mutable_quota()->set_op(eos::rpc::GET);
  request.mutable_role();
  EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
  request.mutable_role()->set_uid(0);
  EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
  request.mutable_role()->set_app("quota-test");
  EXPECT_TRUE(GrpcAuth::ExecScope(request).empty());
  request.clear_role();
  EXPECT_EQ("grpc.quota.read", GrpcAuth::ExecScope(request));
}

TEST(GrpcQuotaAuth, ReadScopeAllowsOnlyQuotaReads)
{
  VirtualIdentity vid = VirtualIdentity::Nobody();
  vid.token = IssueToken({"grpc.quota.read"}).mapped;
  EXPECT_TRUE(GrpcAuth::Authorize(vid, "", "grpc.quota.read").allowed);

  for (const std::string action : {"grpc.quota.write", "", "grpc.exec.mkdir",
                                   "grpc.exec.quota.get", "grpc.quota.unknown"}) {
    SCOPED_TRACE(action);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, "", action).allowed);
  }
}

TEST(GrpcQuotaAuth, WriteScopeAllowsQuotaReadsAndWritesOnly)
{
  VirtualIdentity vid = VirtualIdentity::Nobody();
  vid.token = IssueToken({"grpc.quota.write"}).mapped;

  for (const std::string action : {"grpc.quota.read", "grpc.quota.write"}) {
    const auto decision = GrpcAuth::Authorize(vid, "", action);
    EXPECT_TRUE(decision.allowed);
    EXPECT_EQ(action, decision.action);
  }

  for (const std::string action : {"", "grpc.exec.mkdir", "grpc.quota.unknown"}) {
    SCOPED_TRACE(action);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, "", action).allowed);
  }
}

TEST(GrpcQuotaAuth, RootAndAdminCannotBypassScopeRestrictions)
{
  auto admin = VirtualIdentity::Nobody();
  admin.allowed_uids.insert(eos::common::ADM_UID);
  admin.allowed_gids.insert(eos::common::ADM_GID);
  admin.sudoer = true;
  const auto token = IssueToken({"grpc.quota.read"});

  for (auto vid : {VirtualIdentity::Root(), admin, VirtualIdentity::Nobody()}) {
    SCOPED_TRACE(vid.uid);
    vid.token = token.mapped;
    EXPECT_TRUE(GrpcAuth::Authorize(vid, "", "grpc.quota.read").allowed);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "grpc.quota.write").allowed);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "").allowed);
  }
}

TEST(GrpcQuotaAuth, UnknownOrWildcardScopesFailClosedEvenWithARecognizedScope)
{
  for (const std::string scope :
       {"", "*", "grpc.*", "grpc.quota.*", "grpc.exec.quota.get", "grpc.quota.Read",
        "grpc.quota.read ", " grpc.quota.write"}) {
    SCOPED_TRACE(scope);

    for (const auto& scopes : {std::vector<std::string>{scope},
                               std::vector<std::string>{"grpc.quota.write", scope}}) {
      VirtualIdentity vid = VirtualIdentity::Root();
      vid.token = IssueToken(scopes).mapped;
      EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "grpc.quota.read").allowed);
      EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "grpc.quota.write").allowed);
      EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "").allowed);
    }
  }
}

TEST(GrpcQuotaAuth, ValidMappedTokenIsAuthoritative)
{
  const auto read = IssueToken({"grpc.quota.read"});
  const auto write = IssueToken({"grpc.quota.write"});
  VirtualIdentity vid = VirtualIdentity::Root();
  vid.token = read.mapped;

  for (const auto& authkey : Authkeys(write.encoded)) {
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.write").allowed);
    EXPECT_TRUE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.read").allowed);
  }

  vid.token = write.mapped;

  for (const auto& authkey : Authkeys(read.encoded)) {
    EXPECT_TRUE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.write").allowed);
  }

  EXPECT_TRUE(GrpcAuth::Authorize(vid, "zteos64:invalid", "grpc.quota.write").allowed);
  vid.token = IssueToken({}).mapped;
  EXPECT_TRUE(GrpcAuth::Authorize(vid, write.encoded, "").allowed);
}

TEST(GrpcQuotaAuth, RawEosCredentialsRequireASuccessfullyMappedToken)
{
  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    GTEST_SKIP() << "requires the default in-process token key";
  }

  for (const auto& scopes :
       {std::vector<std::string>{}, std::vector<std::string>{"grpc.quota.read"},
        std::vector<std::string>{"grpc.quota.write"}}) {
    const auto token = IssueToken(scopes);
    const auto vid = VirtualIdentity::Root();

    for (const auto& authkey : Authkeys(token.encoded)) {
      SCOPED_TRACE(authkey.substr(0, 16));
      EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.read").allowed);
      EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.write").allowed);
      EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "").allowed);
    }
  }
}

TEST(GrpcQuotaAuth, InvalidMappedTokensCannotFallBackToTheRawCredential)
{
  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    GTEST_SKIP() << "requires the default in-process token key";
  }

  auto token = IssueToken({"grpc.quota.write"});
  token.mapped->Reset();
  ASSERT_FALSE(token.mapped->Valid());
  VirtualIdentity vid = VirtualIdentity::Root();
  vid.token = token.mapped;
  EXPECT_FALSE(GrpcAuth::Authorize(vid, "", "grpc.quota.read").allowed);

  for (const auto& authkey : Authkeys(token.encoded)) {
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.read").allowed);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.write").allowed);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "").allowed);
  }
}

TEST(GrpcQuotaAuth, OriginRejectedTokensCannotBeRevivedByDecodingAgain)
{
  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    GTEST_SKIP() << "requires the default in-process token key";
  }

  auto token = IssueToken({"grpc.quota.write"}, true);
  ASSERT_NE(0, token.mapped->VerifyOrigin("denied.example", "quota-user", "grpc"));
  token.mapped->Reset();
  VirtualIdentity vid = VirtualIdentity::Root();
  vid.token = token.mapped;

  for (const auto& authkey : Authkeys(token.encoded)) {
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.read").allowed);
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.write").allowed);
    vid.token.reset();
    EXPECT_FALSE(GrpcAuth::Authorize(vid, authkey, "grpc.quota.read").allowed);
    vid.token = token.mapped;
  }
}

TEST(GrpcQuotaAuth, UnscopedOrAbsentTokensKeepExistingAuthorizationBehavior)
{
  for (const auto& token : {std::shared_ptr<EosTok>{}, IssueToken({}).mapped}) {
    VirtualIdentity vid = VirtualIdentity::Nobody();
    vid.token = token;

    for (const std::string action : {"", "grpc.quota.read", "grpc.quota.write",
                                     "grpc.exec.mkdir", "grpc.quota.unknown"}) {
      SCOPED_TRACE(action);
      EXPECT_TRUE(GrpcAuth::Authorize(vid, "", action).allowed);
      EXPECT_TRUE(GrpcAuth::Authorize(vid, "legacy-auth-key", action).allowed);
    }
  }
}

#endif // EOS_GRPC
