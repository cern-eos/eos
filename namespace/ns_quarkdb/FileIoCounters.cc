//------------------------------------------------------------------------------
//! @author Andreas-Joachim Peters - CERN
//! @brief In-memory file traffic counters flushed to eos-file-io:<fid>
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

#include "namespace/ns_quarkdb/FileIoCounters.hh"
#include "common/Logging.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "qclient/QClient.hh"
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileIoCounters::FileIoCounters(qclient::QClient* qcl, MetadataFlusher* flusher)
  : mQcl(qcl), mFlusher(flusher), mLastFlush(std::chrono::steady_clock::now()),
    mDirty(false) {}

//------------------------------------------------------------------------------
// Saturating uint64 add
//------------------------------------------------------------------------------
uint64_t
FileIoCounters::SaturatingAdd(uint64_t current, uint64_t extra)
{
  const uint64_t room = std::numeric_limits<uint64_t>::max() - current;
  return current + std::min(extra, room);
}

//------------------------------------------------------------------------------
// Add a delta back into the shard. Caller holds the shard mutex.
//------------------------------------------------------------------------------
void
FileIoCounters::MergeLocked(Shard& shard, uint64_t fid, const Delta& delta)
{
  Delta& dest = shard.pending[fid];
  dest.rbytes = SaturatingAdd(dest.rbytes, delta.rbytes);
  dest.rbc = SaturatingAdd(dest.rbc, delta.rbc);
  dest.ubytes = SaturatingAdd(dest.ubytes, delta.ubytes);
  dest.ubc = SaturatingAdd(dest.ubc, delta.ubc);
  mDirty.store(true, std::memory_order_relaxed);
}

//------------------------------------------------------------------------------
// Queue one HINCRBYMULTI. Caller holds the shard mutex, so a concurrent
// drop queues its DEL after this request.
//------------------------------------------------------------------------------
void
FileIoCounters::QueueLocked(Shard& shard, uint64_t fid, const Delta& delta)
{
  std::vector<std::string> req;
  req.emplace_back("HINCRBYMULTI");
  const std::string key = constants::sFileIoPrefix + std::to_string(fid);
  const uint64_t cap = static_cast<uint64_t>
                       (std::numeric_limits<int64_t>::max());
  auto append = [&](const char* field, uint64_t value) {
    if (value == 0) {
      return;
    }

    req.emplace_back(key);
    req.emplace_back(field);
    req.emplace_back(std::to_string(std::min(value, cap)));
  };
  append("rbytes", delta.rbytes);
  append("rbc", delta.rbc);
  append("ubytes", delta.ubytes);
  append("ubc", delta.ubc);

  if (req.size() == 1) {
    return;
  }

  try {
    mFlusher->execute(req);
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to queue file io counters\" fid=%llu "
                   "emsg=\"%s\"",
                   static_cast<unsigned long long>(fid), e.what());
    MergeLocked(shard, fid, delta);
  }
}

//------------------------------------------------------------------------------
// Read eos-file-io:<fid>. Missing fields and a missing hash are zero.
//------------------------------------------------------------------------------
FileIoTotals
FileIoCounters::Load(uint64_t fid)
{
  FileIoTotals stored;

  if ((mQcl == nullptr) || (fid == 0)) {
    return stored;
  }

  try {
    const std::string key = constants::sFileIoPrefix + std::to_string(fid);
    qclient::redisReplyPtr reply = mQcl->exec("HMGET", key, "rbytes", "rbc",
                                   "ubytes", "ubc").get();

    if (!reply || (reply->type != REDIS_REPLY_ARRAY) || (reply->elements < 4)) {
      return stored;
    }

    auto parse = [](const redisReply* field) -> uint64_t {
      if (field == nullptr) {
        return 0;
      }

      if ((field->type == REDIS_REPLY_INTEGER) && (field->integer > 0)) {
        return static_cast<uint64_t>(field->integer);
      }

      if ((field->type == REDIS_REPLY_STRING) && (field->str != nullptr)) {
        char* end = nullptr;
        const unsigned long long value = std::strtoull(field->str, &end, 10);

        if ((end == field->str) || ((end != nullptr) && (*end != '\0'))) {
          return 0;
        }

        return static_cast<uint64_t>(value);
      }

      return 0;
    };
    stored.rbytes = parse(reply->element[0]);
    stored.rbc = parse(reply->element[1]);
    stored.ubytes = parse(reply->element[2]);
    stored.ubc = parse(reply->element[3]);
  } catch (const std::exception& e) {
    eos_static_debug("msg=\"failed to read file io counters\" fid=%llu "
                     "emsg=\"%s\"",
                     static_cast<unsigned long long>(fid), e.what());
  }

  return stored;
}

//------------------------------------------------------------------------------
// Remember a close report until the next flush
//------------------------------------------------------------------------------
void
FileIoCounters::add(uint64_t fid, uint64_t rbytes, uint64_t ubytes)
{
  if ((fid == 0) || ((rbytes == 0) && (ubytes == 0))) {
    return;
  }

  Shard& shard = mShards[fid % kShards];
  std::lock_guard<std::mutex> lock(shard.mutex);
  Delta& dest = shard.pending[fid];
  dest.rbytes = SaturatingAdd(dest.rbytes, rbytes);
  dest.ubytes = SaturatingAdd(dest.ubytes, ubytes);

  if (rbytes != 0) {
    dest.rbc = SaturatingAdd(dest.rbc, 1);
  }

  if (ubytes != 0) {
    dest.ubc = SaturatingAdd(dest.ubc, 1);
  }

  mDirty.store(true, std::memory_order_relaxed);
}

//------------------------------------------------------------------------------
// Stored hash plus the pending delta. The hash is read before the pending
// map so a flush that lands in between is not added twice.
//------------------------------------------------------------------------------
FileIoTotals
FileIoCounters::get(uint64_t fid)
{
  FileIoTotals totals = Load(fid);

  if (fid == 0) {
    return totals;
  }

  Delta pending;
  {
    Shard& shard = mShards[fid % kShards];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto pending_it = shard.pending.find(fid);

    if (pending_it != shard.pending.end()) {
      pending = pending_it->second;
    }

    const auto inflight_it = shard.inflight.find(fid);

    if (inflight_it != shard.inflight.end()) {
      pending.rbytes = SaturatingAdd(pending.rbytes, inflight_it->second.rbytes);
      pending.rbc = SaturatingAdd(pending.rbc, inflight_it->second.rbc);
      pending.ubytes = SaturatingAdd(pending.ubytes, inflight_it->second.ubytes);
      pending.ubc = SaturatingAdd(pending.ubc, inflight_it->second.ubc);
    }
  }
  totals.rbytes = SaturatingAdd(totals.rbytes, pending.rbytes);
  totals.rbc = SaturatingAdd(totals.rbc, pending.rbc);
  totals.ubytes = SaturatingAdd(totals.ubytes, pending.ubytes);
  totals.ubc = SaturatingAdd(totals.ubc, pending.ubc);
  return totals;
}

//------------------------------------------------------------------------------
// Swap every dirty file out of memory and queue HINCRBYMULTI
//------------------------------------------------------------------------------
void
FileIoCounters::flush(bool force)
{
  std::lock_guard<std::mutex> flush_lock(mFlushMutex);

  if (mFlusher == nullptr) {
    return;
  }

  const auto now = std::chrono::steady_clock::now();

  if (!force) {
    if (!mDirty.load(std::memory_order_relaxed)) {
      return;
    }

    if ((now - mLastFlush) < std::chrono::seconds(5)) {
      return;
    }
  }

  mDirty.store(false, std::memory_order_relaxed);
  mLastFlush = now;

  for (auto& shard : mShards) {
    std::vector<uint64_t> batch;
    {
      std::lock_guard<std::mutex> lock(shard.mutex);
      batch.reserve(shard.pending.size());

      for (const auto& item : shard.pending) {
        Delta& dest = shard.inflight[item.first];
        dest.rbytes = SaturatingAdd(dest.rbytes, item.second.rbytes);
        dest.rbc = SaturatingAdd(dest.rbc, item.second.rbc);
        dest.ubytes = SaturatingAdd(dest.ubytes, item.second.ubytes);
        dest.ubc = SaturatingAdd(dest.ubc, item.second.ubc);
        batch.emplace_back(item.first);
      }

      shard.pending.clear();
    }

    for (uint64_t fid : batch) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      const auto it = shard.inflight.find(fid);

      if (it == shard.inflight.end()) {
        shard.dropped.erase(fid);
        continue;
      }

      const Delta delta = it->second;
      shard.inflight.erase(it);

      if (shard.dropped.erase(fid) > 0) {
        continue;
      }

      QueueLocked(shard, fid, delta);
    }
  }
}

//------------------------------------------------------------------------------
// Drop pending bytes and delete the side hash. A delta already taken by
// flush is marked dropped so that flush does not queue HINCRBYMULTI after
// this DEL.
//------------------------------------------------------------------------------
void
FileIoCounters::drop(uint64_t fid)
{
  if (fid == 0) {
    return;
  }

  Shard& shard = mShards[fid % kShards];
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.pending.erase(fid);

  if (shard.inflight.find(fid) != shard.inflight.end()) {
    shard.dropped.insert(fid);
  }

  if (mFlusher == nullptr) {
    return;
  }

  try {
    mFlusher->del(constants::sFileIoPrefix + std::to_string(fid));
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to delete file io counters\" fid=%llu "
                   "emsg=\"%s\"",
                   static_cast<unsigned long long>(fid), e.what());
  }
}

EOSNSNAMESPACE_END
