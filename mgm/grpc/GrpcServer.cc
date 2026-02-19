// ----------------------------------------------------------------------
// File: GrpcServer.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "GrpcServer.hh"
#include "GrpcNsInterface.hh"
#include <google/protobuf/util/json_util.h>
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "mgm/macros/Macros.hh"
#include <XrdSec/XrdSecEntity.hh>

#ifdef EOS_GRPC
#include "proto/Rpc.grpc.pb.h"
#include <grpc++/security/credentials.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::rpc::Eos;
using eos::rpc::PingRequest;
using eos::rpc::PingReply;
using eos::rpc::FileInsertRequest;
using eos::rpc::ContainerInsertRequest;
using eos::rpc::InsertReply;

#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

struct Rates {
  double r_bps = 0;
  double w_bps = 0;
  double r_iops = 0;
  double w_iops = 0;

  // Helper for sorting/comparison
  double
  total_throughput() const
  {
    return r_bps + w_bps;
  }

  // Accumulate
  void
  add(const Rates& other)
  {
    r_bps += other.r_bps;
    w_bps += other.w_bps;
    r_iops += other.r_iops;
    w_iops += other.w_iops;
  }
};

Rates
ExtractWindowRates(const eos::mgm::RateSnapshot& snap,
                   eos::traffic_shaping::TrafficShapingRateRequest::Estimators estimator)
{
  switch (estimator) {
  case eos::traffic_shaping::TrafficShapingRateRequest::SMA_5_SECONDS:
    return {snap.read_rate_sma_5s, snap.write_rate_sma_5s, snap.read_iops_sma_5s, snap.write_iops_sma_5s};
  case eos::traffic_shaping::TrafficShapingRateRequest::SMA_1_MINUTES:
    return {snap.read_rate_sma_1m, snap.write_rate_sma_1m, snap.read_iops_sma_1m, snap.write_iops_sma_1m};
  case eos::traffic_shaping::TrafficShapingRateRequest::SMA_5_MINUTES:
    return {snap.read_rate_sma_5m, snap.write_rate_sma_5m, snap.read_iops_sma_5m, snap.write_iops_sma_5m};
  case eos::traffic_shaping::TrafficShapingRateRequest::EMA_5_SECONDS:
    return {snap.read_rate_ema_5s, snap.write_rate_ema_5s, snap.read_iops_ema_5s, snap.write_iops_ema_5s};
  case eos::traffic_shaping::TrafficShapingRateRequest::EMA_1_MINUTES:
    return {snap.read_rate_ema_1m, snap.write_rate_ema_1m, snap.read_iops_ema_1m, snap.write_iops_sma_1m};
  case eos::traffic_shaping::TrafficShapingRateRequest::EMA_5_MINUTES:
    return {snap.read_rate_ema_5m, snap.write_rate_ema_5m, snap.read_iops_ema_5m, snap.write_iops_ema_5m};
  default:
    return {snap.read_rate_sma_1m, snap.write_rate_sma_1m, snap.read_iops_sma_1m, snap.write_iops_sma_1m};
  }
}

void
BuildReport(const std::shared_ptr<TrafficShapingManager>& brain,
            const eos::traffic_shaping::TrafficShapingRateRequest* request,
            eos::traffic_shaping::TrafficShapingRateResponse* report)
{
  // Snapshot Global State so we don't have to hold locks while processing/sorting
  auto global_stats = brain->GetGlobalStats();

  const auto [estimator_mean, estimator_min, estimator_max] = brain->GetEstimatorsUpdateLoopMicroSecStats();
  const auto [fst_limits_mean, fst_limits_min, fst_limits_max] = brain->GetFstLimitsUpdateLoopMicroSecStats();

  auto* est_stats = report->mutable_estimators_update_thread_loop_stats();
  est_stats->set_mean_elapsed_time_micro_sec(estimator_mean);
  est_stats->set_min_elapsed_time_micro_sec(estimator_min);
  est_stats->set_max_elapsed_time_micro_sec(estimator_max);

  auto* fst_stats = report->mutable_fst_limits_update_thread_loop_stats();
  fst_stats->set_mean_elapsed_time_micro_sec(fst_limits_mean);
  fst_stats->set_min_elapsed_time_micro_sec(fst_limits_min);
  fst_stats->set_max_elapsed_time_micro_sec(fst_limits_max);

  int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  report->set_timestamp_ms(now_ms);

  bool do_uid = false, do_gid = false, do_app = false;
  if (request->include_types_size() == 0) {
    // Default: Include All if unspecified
    do_uid = do_gid = do_app = true;
  } else {
    for (auto type : request->include_types()) {
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_UID) {
        do_uid = true;
      }
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_GID) {
        do_gid = true;
      }
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_APP) {
        do_app = true;
      }
    }
  }

  // Determine which estimators to calculate (e.g., 5s SMA, 1m EMA, etc.)
  std::vector<eos::traffic_shaping::TrafficShapingRateRequest::Estimators> estimators;
  if (request->estimators_size() == 0) {
    estimators.push_back(eos::traffic_shaping::TrafficShapingRateRequest::SMA_5_SECONDS);
  } else {
    for (auto w : request->estimators()) {
      if (w != eos::traffic_shaping::TrafficShapingRateRequest::UNSPECIFIED) {
        estimators.push_back(static_cast<eos::traffic_shaping::TrafficShapingRateRequest::Estimators>(w));
      }
    }
  }

  // Determine Sorting Window
  // If user asks for [1s, 5m] but wants to sort by 5m trend, they set sort_by_window=5m.
  // Default to the first window in the list.
  eos::traffic_shaping::TrafficShapingRateRequest::Estimators sort_window = estimators[0];
  if (request->has_sort_by_estimator() &&
      request->sort_by_estimator() != eos::traffic_shaping::TrafficShapingRateRequest::UNSPECIFIED) {
    sort_window = request->sort_by_estimator();
  }

  // ---------------------------------------------------------------------------
  // 3. Aggregation Logic
  // ---------------------------------------------------------------------------
  // We need to store rates for ALL requested windows for each entity.
  struct AggregatedEntity {
    uint32_t active_streams = 0;
    std::map<eos::traffic_shaping::TrafficShapingRateRequest::Estimators, Rates> window_rates{};
  };

  std::map<uint32_t, AggregatedEntity> uid_agg;
  std::map<uint32_t, AggregatedEntity> gid_agg;
  std::map<std::string, AggregatedEntity> app_agg;

  for (const auto& [key, snap] : global_stats) {
    // Optimization: Calculate rates only for requested windows
    for (auto win : estimators) {
      Rates r = ExtractWindowRates(snap, win);

      // Skip completely idle streams (micro-optimization)
      // if (r.total_throughput() == 0 && r.r_iops == 0 && r.w_iops == 0) { continue; }

      if (do_uid) {
        auto& agg = uid_agg[key.uid];
        agg.window_rates[win].add(r);
        if (win == estimators[0]) {
          agg.active_streams++; // Count once
        }
      }
      if (do_gid) {
        auto& agg = gid_agg[key.gid];
        agg.window_rates[win].add(r);
        if (win == estimators[0]) {
          agg.active_streams++;
        }
      }
      if (do_app) {
        auto& agg = app_agg[key.app];
        agg.window_rates[win].add(r);
        if (win == estimators[0]) {
          agg.active_streams++;
        }
      }
    }
  }

  // Generic Lambda to process any map (UID, GID, or App)
  auto process_stats = [&](const auto& source_map, auto add_entry_fn, auto set_id_fn) {
    if (source_map.empty()) {
      return;
    }

    // A. Map -> Vector (for sorting)
    using PairType = typename std::decay_t<decltype(source_map)>::value_type;
    std::vector<const PairType*> vec;
    vec.reserve(source_map.size());
    for (const auto& item : source_map) {
      vec.push_back(&item);
    }

    // B. Sorter: Sort by 'sort_window' throughput
    auto sorter = [&](const PairType* a, const PairType* b) {
      double val_a = 0, val_b = 0;

      // Safe lookup (rate might not exist for this specific window)
      if (auto it = a->second.window_rates.find(sort_window); it != a->second.window_rates.end()) {
        val_a = it->second.total_throughput();
      }
      if (auto it = b->second.window_rates.find(sort_window); it != b->second.window_rates.end()) {
        val_b = it->second.total_throughput();
      }
      return val_a > val_b;
    };

    // C. Top N Selection
    size_t n = vec.size();
    if (request->has_top_n() && request->top_n() > 0) {
      n = std::min(static_cast<size_t>(request->top_n()), n);
      // Partial Sort is faster than full sort
      std::partial_sort(vec.begin(), vec.begin() + n, vec.end(), sorter);
    } else {
      std::sort(vec.begin(), vec.end(), sorter);
    }

    // D. Populate Protobuf
    for (size_t i = 0; i < n; ++i) {
      auto* entry = add_entry_fn();    // e.g., report->add_uid_stats()
      set_id_fn(entry, vec[i]->first); // e.g., entry->set_uid(1001)

      // Add stats for ALL requested estimators
      for (const auto& [estimator, rates] : vec[i]->second.window_rates) {
        auto* s = entry->add_stats();
        s->set_window(estimator);
        s->set_bytes_read_per_sec(rates.r_bps);
        s->set_bytes_written_per_sec(rates.w_bps);
        s->set_iops_read(rates.r_iops);
        s->set_iops_write(rates.w_iops);
      }
    }
  };

  // ---------------------------------------------------------------------------
  // 5. Apply Logic to Each Entity Type
  // ---------------------------------------------------------------------------

  if (do_uid) {
    process_stats(uid_agg, [&]() { return report->add_user_stats(); }, [](auto* e, uint32_t id) { e->set_uid(id); });
  }

  if (do_gid) {
    process_stats(gid_agg, [&]() { return report->add_group_stats(); }, [](auto* e, uint32_t id) { e->set_gid(id); });
  }

  if (do_app) {
    process_stats(
        app_agg,
        [&]() { return report->add_app_stats(); },
        [](auto* e, const std::string& id) { e->set_app_name(id); });
  }
}

class RequestServiceImpl final : public Eos::Service {
  Status Ping(ServerContext* context, const eos::rpc::PingRequest* request,
              eos::rpc::PingReply* reply) override
  {
    eos_static_info("grpc::ping from client peer=%s ip=%s DN=%s token=%s len=%lu",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str(),
                    request->message().length());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    reply->set_message(request->message());
    return Status::OK;
  }

  Status FileInsert(ServerContext* context,
                    const eos::rpc::FileInsertRequest* request,
                    eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::fileinsert from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::FileInsert(vid, reply, request);
  }

  Status ContainerInsert(ServerContext* context,
                         const eos::rpc::ContainerInsertRequest* request,
                         eos::rpc::InsertReply* reply) override
  {
    eos_static_info("grpc::containerinsert from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::ContainerInsert(vid, reply, request);
  }

  Status MD(ServerContext* context, const eos::rpc::MDRequest* request,
            ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    eos_static_info("grpc::md from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;

    switch (request->type()) {
    case eos::rpc::FILE:
    case eos::rpc::CONTAINER:
    case eos::rpc::STAT:
      return GrpcNsInterface::Stat(vid, writer, request);
      break;

    case eos::rpc::LISTING:
      return GrpcNsInterface::StreamMD(vid, writer, request);
      break;

    default:
      ;
    }

    return Status(grpc::StatusCode::INVALID_ARGUMENT, "request is not supported");
  }

  Status Find(ServerContext* context, const eos::rpc::FindRequest* request,
              ServerWriter<eos::rpc::MDResponse>* writer) override
  {
    eos_static_info("grpc::find from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Find(vid, writer, request);
  }

  Status NsStat(ServerContext* context,
                const eos::rpc::NsStatRequest* request,
                eos::rpc::NsStatResponse* reply) override
  {
    eos_static_info("grpc::nsstat::request from client peer=%s ip=%s DN=%s token=%s",
                    context->peer().c_str(), GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(), request->authkey().c_str());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::NsStat(vid, reply, request);
  }

  Status Exec(ServerContext* context,
              const eos::rpc::NSRequest* request,
              eos::rpc::NSResponse* reply) override
  {
    eos_static_info("grpc::exec::request from client peer=%s ip=%s DN=%s "
                    "token=%s req_type=%lu", context->peer().c_str(),
                    GrpcServer::IP(context).c_str(),
                    GrpcServer::DN(context).c_str(),
                    request->authkey().c_str(), request->command_case());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->authkey());
    WAIT_BOOT;
    return GrpcNsInterface::Exec(vid, reply, request);
  }

  Status
  TrafficShapingRate(ServerContext* context,
                     const eos::traffic_shaping::TrafficShapingRateRequest* request,
                     ServerWriter<eos::traffic_shaping::TrafficShapingRateResponse>* writer) override
  {
    eos_static_info("msg=\"Monitoring Stream Start\" peer=%s", context->peer().c_str());

    auto brain = gOFS->mTrafficShapingEngine.GetBrain();

    while (!context->IsCancelled()) {
      auto start = std::chrono::steady_clock::now();

      eos::traffic_shaping::TrafficShapingRateResponse report;
      BuildReport(brain, request, &report);

      if (!writer->Write(report)) {
        break;
      }

      std::this_thread::sleep_until(start + std::chrono::milliseconds(100));
    }
    return grpc::Status::OK;
  }
};

/* return client DN*/
std::string
GrpcServer::DN(grpc::ServerContext* context)
{
  /*
    The methods GetPeerIdentityPropertyName() and GetPeerIdentity() from grpc::ServerContext.auth_context
    will prioritize SAN fields (x509_subject_alternative_name) in favor of x509_common_name
  */
  std::string tag = "x509_common_name";
  auto resp = context->auth_context()->FindPropertyValues(tag);

  if (resp.empty()) {
    tag = "x509_subject_alternative_name";
    auto resp = context->auth_context()->FindPropertyValues(tag);

    if (resp.empty()) {
      return "";
    }
  }

  return resp[0].data();
}



/* return client IP */
std::string GrpcServer::IP(grpc::ServerContext* context, std::string* id,
                           std::string* port)
{
  // format is ipv4:<ip>:<..> or ipv6:<ip>:<..> - we just return the IP address
  // butq net and id are populated as well with the prefix and suffix, respectively
  // The context peer information is curl encoded
  const std::string decoded_peer =
    eos::common::StringConversion::curl_default_unescaped(context->peer().c_str());
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(decoded_peer, tokens, "[]");

  if (tokens.size() == 3) {
    if (id) {
      *id = tokens[0].substr(0, tokens[0].size() - 1);
    }

    if (port) {
      *port = tokens[2].substr(1, tokens[2].size() - 1);
    }

    return "[" + tokens[1] + "]";
  } else {
    tokens.clear();
    eos::common::StringConversion::Tokenize(decoded_peer, tokens, ":");

    if (tokens.size() == 3) {
      if (id) {
        *id = tokens[0].substr(0, tokens[0].size());
      }

      if (port) {
        *port = tokens[2].substr(0, tokens[2].size());
      }

      return tokens[1];
    }

    return "";
  }
}

/* return VID for a given call */
void
GrpcServer::Vid(grpc::ServerContext* context,
                eos::common::VirtualIdentity& vid,
                const std::string& authkey)
{
  XrdSecEntity client("grpc");
  std::string dn = DN(context);
  client.name = const_cast<char*>(dn.c_str());
  bool isEosToken = (authkey.substr(0, 8) == "zteos64:");
  std::string tident = dn.length() ? dn.c_str() : (isEosToken ? "eostoken" :
                       authkey.c_str());
  std::string id;
  std::string ip = GrpcServer::IP(context, &id).c_str();
  tident += ".1:";
  tident += id;
  tident += "@";
  tident += ip;
  client.tident = tident.c_str();

  if (authkey.length()) {
    client.endorsements = const_cast<char*>(authkey.c_str());
  }

  eos::common::Mapping::IdMap(&client, "eos.app=grpc", client.tident, vid);
}

#endif

void
GrpcServer::Run(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC

  if (getenv("EOS_MGM_GRPC_SSL_CERT") &&
      getenv("EOS_MGM_GRPC_SSL_KEY") &&
      getenv("EOS_MGM_GRPC_SSL_CA")) {
    mSSL = true;
    mSSLCertFile = getenv("EOS_MGM_GRPC_SSL_CERT");
    mSSLKeyFile = getenv("EOS_MGM_GRPC_SSL_KEY");
    mSSLCaFile = getenv("EOS_MGM_GRPC_SSL_CA");

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
        mSSLCert) && !mSSLCert.length()) {
      eos_static_crit("unable to load ssl certificate file '%s'",
                      mSSLCertFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
        mSSLKey) && !mSSLKey.length()) {
      eos_static_crit("unable to load ssl key file '%s'", mSSLKeyFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
        mSSLCa) && !mSSLCa.length()) {
      eos_static_crit("unable to load ssl ca file '%s'", mSSLCaFile.c_str());
      mSSL = false;
    }
  }

  int selected_port = 0;
  RequestServiceImpl service;
  std::string bind_address = "0.0.0.0:";
  bind_address += std::to_string(mPort);
  grpc::ServerBuilder builder;

  if (mSSL) {
    grpc_ssl_client_certificate_request_type gsccrt =
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    if (getenv("EOS_MGM_GRPC_DONT_REQUEST_CLIENT_CERTIFICATE")) {
      gsccrt = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
    }

    grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
      mSSLKey,
      mSSLCert
    };
    grpc::SslServerCredentialsOptions sslOps(gsccrt);
    sslOps.pem_root_certs = mSSLCa;
    sslOps.pem_key_cert_pairs.push_back(keycert);
    builder.AddListeningPort(bind_address, grpc::SslServerCredentials(sslOps),
                             &selected_port);
  } else {
    builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  }

  builder.RegisterService(&service);
  mServer = builder.BuildAndStart();

  if (mSSL && (selected_port == 0)) {
    eos_static_err("msg=\"server failed to bind to port with SSL, "
                   "port %i is taken or certs not valid\"", mPort);
    return;
  }

  if (mServer) {
    eos_static_info("msg=\"gRPC server for EOS is running\" port=%i.", mPort);
    mServer->Wait();
  }

#else
  // Make the compiler happy
  (void) mPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
