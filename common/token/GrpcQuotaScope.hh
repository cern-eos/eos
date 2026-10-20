//------------------------------------------------------------------------------
// File: GrpcQuotaScope.hh
//------------------------------------------------------------------------------

#pragma once

#include "common/Namespace.hh"
#include <string>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

inline bool
IsGrpcQuotaScope(const std::string& scope)
{
  return scope == "grpc.quota.read" || scope == "grpc.quota.write";
}

inline bool
GrpcQuotaScopeAllows(const std::vector<std::string>& scopes, bool write)
{
  bool allowed = false;

  for (const auto& scope : scopes) {
    if (!IsGrpcQuotaScope(scope)) {
      return false;
    }

    if (scope == "grpc.quota.write" || !write) {
      allowed = true;
    }
  }

  return allowed;
}

EOSCOMMONNAMESPACE_END
