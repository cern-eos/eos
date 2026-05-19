// ----------------------------------------------------------------------
// File: GrpcRedirect.hh
// Author: Gianmaria Del Monte - CERN
// ----------------------------------------------------------------------

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

#pragma once

#if defined(EOS_GRPC) || defined(EOS_GRPC_GATEWAY)

#include "mgm/Namespace.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/ProcInterface.hh"
#include <grpc++/grpc++.h>
#include <string>

EOSMGMNAMESPACE_BEGIN

//! Metadata key carrying the master location (host[:port]) returned alongside
//! a FAILED_PRECONDITION status when a write request hits a slave MGM.
constexpr const char* kEosMasterLocationKey = "x-eos-master-location";

//------------------------------------------------------------------------------
//! Return the master location to redirect a write to, or an empty string when
//! no redirect is needed (we are the master, or no master is known yet).
//------------------------------------------------------------------------------
inline std::string GetSlaveRedirectTarget()
{
  if (gOFS == nullptr || gOFS->mMaster == nullptr) {
    return "";
  }

  if (gOFS->mMaster->IsMaster()) {
    return "";
  }

  return gOFS->mMaster->GetMasterId();
}

//------------------------------------------------------------------------------
//! Build the FAILED_PRECONDITION status and attach the master location as
//! trailing metadata so the client can retry against the master.
//------------------------------------------------------------------------------
inline grpc::Status MakeSlaveRedirectStatus(grpc::ServerContext* context,
                                            const std::string& master_id)
{
  if (context != nullptr) {
    context->AddTrailingMetadata(kEosMasterLocationKey, master_id);
  }

  std::string msg = "MGM is in slave mode; retry against master at ";

  if (master_id.empty()) {
    msg += "<unknown>";
  } else {
    msg += master_id;
  }

  return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, msg);
}

EOSMGMNAMESPACE_END

//------------------------------------------------------------------------------
//! Convenience macro for REST gateway endpoints: wraps the typed sub-request
//! into a console::RequestProto, asks ProcInterface to classify it, and if
//! the operation is a write while this MGM is in slave mode, returns the
//! FAILED_PRECONDITION redirect status carrying the master location. Must be
//! used inside a function whose return type is grpc::Status.
//------------------------------------------------------------------------------
#define EOS_GRPC_REDIRECT_WRITE_IF_SLAVE(context, sub_field, sub_request)    \
  do {                                                                       \
    eos::console::RequestProto _eos_redir_req;                               \
    _eos_redir_req.mutable_##sub_field()->CopyFrom(*(sub_request));          \
    if (eos::mgm::ProcInterface::IsProtoWriteAccess(_eos_redir_req)) {       \
      std::string _eos_master_id = eos::mgm::GetSlaveRedirectTarget();       \
      if (!_eos_master_id.empty()) {                                         \
        return eos::mgm::MakeSlaveRedirectStatus((context), _eos_master_id); \
      }                                                                      \
    }                                                                        \
  } while (0)

#endif // EOS_GRPC || EOS_GRPC_GATEWAY
