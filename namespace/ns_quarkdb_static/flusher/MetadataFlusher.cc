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

#include <inttypes.h>
#include <iostream>
#include <list>
#include <sstream>
#include <memory>
#include <qclient/BackgroundFlusher.hh>
#include <qclient/RocksDBPersistency.hh>
#include "namespace/ns_quarkdb_static/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "common/Logging.hh"
#include <iostream>
#include <chrono>
#include <qclient/AssistedThread.hh>

#define __PRI64_PREFIX "l"
#define PRId64         __PRI64_PREFIX "d"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataFlusher::MetadataFlusher(const std::string& path,
                                 const QdbContactDetails& contactDetails) :
  id(basename(path.c_str())),
  notifier(*this),
  backgroundFlusher(contactDetails.members, contactDetails.constructOptions(),
                    notifier, new qclient::RocksDBPersistency(path)),
  sizePrinter(&MetadataFlusher::queueSizeMonitoring, this)
{
  synchronize();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
MetadataFlusher::~MetadataFlusher()
{
  sizePrinter.join();
  synchronize();
}

//------------------------------------------------------------------------------
// Regularly print queue statistics
//------------------------------------------------------------------------------
void MetadataFlusher::queueSizeMonitoring(qclient::ThreadAssistant& assistant)
{
  while (!assistant.terminationRequested()) {
    if (backgroundFlusher.size()) {
      eos_static_info("id=%s total-pending=%" PRId64 " enqueued=%" PRId64
                      " acknowledged=%" PRId64,
                      id.c_str(), backgroundFlusher.size(),
                      backgroundFlusher.getEnqueuedAndClear(),
                      backgroundFlusher.getAcknowledgedAndClear());
    }

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

  eos_static_info("starting-index=%" PRId64 " ending-index=%" PRId64
                  " msg=\"waiting until "
                  "queue item %" PRId64 " has been acknowledged..\"",
                  backgroundFlusher.getStartingIndex(),
                  backgroundFlusher.getEndingIndex(), targetIndex);

  while (!backgroundFlusher.waitForIndex(targetIndex, std::chrono::seconds(1))) {
    eos_static_warning("starting-index=%" PRId64 " ending-index=%" PRId64
                       " msg=\"queue item "
                       "%" PRId64 " has not been acknowledged yet..\"",
                       backgroundFlusher.getStartingIndex(),
                       backgroundFlusher.getEndingIndex(), targetIndex);
  }

  eos_static_info("starting-index=%" PRId64 " ending-index=%" PRId64
                  " msg=\"queue item %" PRId64
                  " has been acknowledged\"", backgroundFlusher.getStartingIndex(),
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
std::map<MetadataFlusherFactory::InstanceKey, std::shared_ptr<MetadataFlusher>>
    MetadataFlusherFactory::instances;
std::mutex MetadataFlusherFactory::mtx;
std::string MetadataFlusherFactory::queuePath = "/var/eos/ns-queue/";

void MetadataFlusherFactory::setQueuePath(const std::string& newpath)
{
  queuePath = newpath;
}

std::shared_ptr<MetadataFlusher>
MetadataFlusherFactory::getInstance(const std::string& id,
                                    const QdbContactDetails& contactDetails)
{
  std::lock_guard<std::mutex> lock(MetadataFlusherFactory::mtx);

  if (contactDetails.empty()) {
    eos_static_crit("MetadataFlusherFactory::getInstance received empty QdbContactDetails!");
    std::terminate();
  }

  std::tuple<std::string, qclient::Members> key = std::make_tuple(id,
      contactDetails.members);
  auto it = instances.find(key);

  if (it != instances.end()) {
    return it->second;
  }

  eos_static_notice("Created new metadata flusher towards %s",
                    contactDetails.members.toString().c_str());
  instances.emplace(key,  new MetadataFlusher(queuePath + id, contactDetails));
  return instances[key];
}

//------------------------------------------------------------------------------
// Class to receive notifications from the BackgroundFlusher
//------------------------------------------------------------------------------
FlusherNotifier::FlusherNotifier(MetadataFlusher& flusher):
  mFlusher(flusher) {}

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
