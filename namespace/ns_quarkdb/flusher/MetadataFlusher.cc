/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include <iostream>
#include <list>
#include <sstream>
#include <memory>
#include <qclient/BackpressuredQueue.hh>
#include <qclient/BackgroundFlusher.hh>
#include <qclient/RocksDBPersistency.hh>
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "common/Logging.hh"
#include <iostream>
#include <chrono>
#include <qclient/AssistedThread.hh>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataFlusher::MetadataFlusher(const std::string& path,
                                 const std::string& host, int port)
  : MetadataFlusher(path, qclient::Members(host, port)) {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataFlusher::MetadataFlusher(const std::string& path,
                                 const qclient::Members& qdb_members):
  notifier(*this),
  qcl(qdb_members, true /* yes to redirects */, false /* no to exceptions */),
  backgroundFlusher(qcl, notifier, 50000 /* size limit */,
                    5000 /* pipeline length */,
                    new qclient::RocksDBPersistency(path)),
  sizePrinter(&MetadataFlusher::queueSizeMonitoring, this)
{
  synchronize();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
MetadataFlusher::~MetadataFlusher()
{
  synchronize();
}

//------------------------------------------------------------------------------
// Regularly print queue statistics
//------------------------------------------------------------------------------
void MetadataFlusher::queueSizeMonitoring(qclient::ThreadAssistant& assistant)
{
  while (!assistant.terminationRequested()) {
    eos_static_info("total-pending=%d enqueued=%d acknowledged=%d",
                    backgroundFlusher.size(),
                    backgroundFlusher.getEnqueuedAndClear(),
                    backgroundFlusher.getAcknowledgedAndClear());
    assistant.wait_for(std::chrono::seconds(10));
  }
}

//------------------------------------------------------------------------------
// Queue an hset command
//------------------------------------------------------------------------------
void MetadataFlusher::hset(const std::string& key, const std::string& field,
                           const std::string& value)
{
  backgroundFlusher.pushRequest({"HSET", key, field, value});
}

//------------------------------------------------------------------------------
// Queue an hincrby command
//------------------------------------------------------------------------------
void MetadataFlusher::hincrby(const std::string& key, const std::string& field,
                              int64_t value)
{
  backgroundFlusher.pushRequest({"HINCRBY", key, field, std::to_string(value)});
}

//------------------------------------------------------------------------------
// Queue a del command
//------------------------------------------------------------------------------
void MetadataFlusher::del(const std::string& key)
{
  backgroundFlusher.pushRequest({"DEL", key});
}

//------------------------------------------------------------------------------
// Queue an hdel command
//------------------------------------------------------------------------------
void MetadataFlusher::hdel(const std::string& key, const std::string& field)
{
  backgroundFlusher.pushRequest({"HDEL", key, field});
}

//------------------------------------------------------------------------------
// Queue a sadd command
//------------------------------------------------------------------------------
void MetadataFlusher::sadd(const std::string& key, const std::string& field)
{
  backgroundFlusher.pushRequest({"SADD", key, field});
}

//------------------------------------------------------------------------------
// Queue an srem command
//------------------------------------------------------------------------------
void MetadataFlusher::srem(const std::string& key, const std::string& field)
{
  backgroundFlusher.pushRequest({"SREM", key, field});
}

//------------------------------------------------------------------------------
// Queue an srem command, use a list as contents
//------------------------------------------------------------------------------
void MetadataFlusher::srem(const std::string& key,
                           const std::list<std::string>& items)
{
  std::vector<std::string> req = {"SREM", key};

  for (auto it = items.begin(); it != items.end(); it++) {
    req.emplace_back(*it);
  }

  backgroundFlusher.pushRequest(req);
}

//------------------------------------------------------------------------------
// Sleep until given index has been flushed to the backend
//------------------------------------------------------------------------------
void MetadataFlusher::synchronize(ItemIndex targetIndex)
{
  if (targetIndex < 0) {
    targetIndex = backgroundFlusher.getEndingIndex() - 1;
  }

  eos_static_info("starting-index=%d ending-index=%d msg=\"waiting until "
                  "queue item %d has been acknowledged..\"",
                  backgroundFlusher.getStartingIndex(),
                  backgroundFlusher.getEndingIndex(), targetIndex);

  while (!backgroundFlusher.waitForIndex(targetIndex, std::chrono::seconds(1))) {
    eos_static_warning("starting-index=%d ending-index=%d msg=\"queue item "
                       "%d has not been acknowledged yet..\"",
                       backgroundFlusher.getStartingIndex(),
                       backgroundFlusher.getEndingIndex(), targetIndex);
  }

  eos_static_info("starting-index=%d ending-index=%d msg=\"queue item %d "
                  "has been acknowledged\"", backgroundFlusher.getStartingIndex(),
                  backgroundFlusher.getEndingIndex(), targetIndex);
}

//------------------------------------------------------------------------------
// Get a metadata flusher instance, keyed by (ID, host, port). The ID is an
// arbitrary string which enables having multiple distinct metadata flushers
// towards the same QuarkBD server.
//
// Be extremely careful when using multiple metadata flushers! The different
// instances should all hit distinct sets of the key space.
// TODO(gbitzes): specify a sharding scheme, based on which flusher hits
// which keys, and enforce it with static checks, if possible.
//------------------------------------------------------------------------------
std::map<MetadataFlusherFactory::InstanceKey, MetadataFlusher*>
MetadataFlusherFactory::instances;
std::mutex MetadataFlusherFactory::mtx;
std::string MetadataFlusherFactory::queuePath = "/var/eos/ns-queue/";

void MetadataFlusherFactory::setQueuePath(const std::string& newpath)
{
  queuePath = newpath;
}

MetadataFlusher*
MetadataFlusherFactory::getInstance(const std::string& id,
  const qclient::Members &members)
{
  std::lock_guard<std::mutex> lock(MetadataFlusherFactory::mtx);

  if(members.empty()) {
    eos_static_crit("MetadataFlusherFactory::getInstance received empty qclient::Members!");
    std::terminate();
  }

  std::tuple<std::string, qclient::Members> key = std::make_tuple(id, members);
  auto it = instances.find(key);

  if (it != instances.end()) {
    return it->second;
  }

  MetadataFlusher* flusher = new MetadataFlusher(queuePath + id, members);
  eos_static_notice("Created new metadata flusher towards %s", members.toString().c_str());
  instances[key] = flusher;
  return flusher;
}

//------------------------------------------------------------------------------
// Class to receive notifications from the BackgroundFlusher
//------------------------------------------------------------------------------
FlusherNotifier::FlusherNotifier(MetadataFlusher& flusher_):
  flusher(flusher_) {}

//------------------------------------------------------------------------------
// Record network events
//------------------------------------------------------------------------------
void FlusherNotifier::eventNetworkIssue(const std::string& err)
{
  eos_static_notice("Network issue when contacting the redis backend: %s",
                    err.c_str());
}

//------------------------------------------------------------------------------
// Record unexpected responses
//------------------------------------------------------------------------------
void FlusherNotifier::eventUnexpectedResponse(const std::string& err)
{
  eos_static_crit("Unexpected response when contacting the redis backend: %s",
                  err.c_str());
  // Maybe we should just std::terminate now?
}

EOSNSNAMESPACE_END
