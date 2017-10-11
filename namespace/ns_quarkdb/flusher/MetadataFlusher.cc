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

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataFlusher::MetadataFlusher(const std::string &host, int port)
: qcl(host, port, true /* yes to redirects */, false /* no to exceptions */),
  backgroundFlusher(qcl, dummyNotifier, 50000 /* size limit */, 5000 /* pipeline length */,
  new qclient::RocksDBPersistency("/var/eos/ns-queue/default-queue")) {

}

//------------------------------------------------------------------------------
// Queue an hset command
//------------------------------------------------------------------------------
void MetadataFlusher::hset(const std::string &key, const std::string &field, const std::string &value) {
  backgroundFlusher.pushRequest({"HSET", key, field, value});
}

//------------------------------------------------------------------------------
// Queue an hdel command
//------------------------------------------------------------------------------
void MetadataFlusher::hdel(const std::string &key, const std::string &field) {
  backgroundFlusher.pushRequest( {"HDEL", key, field});
}

//------------------------------------------------------------------------------
// Queue a sadd command
//------------------------------------------------------------------------------
void MetadataFlusher::sadd(const std::string &key, const std::string &field) {
  backgroundFlusher.pushRequest( {"SADD", key, field});
}

//------------------------------------------------------------------------------
// Queue an srem command
//------------------------------------------------------------------------------
void MetadataFlusher::srem(const std::string &key, const std::string &field) {
  backgroundFlusher.pushRequest( {"SREM", key, field});
}

//------------------------------------------------------------------------------
// Queue an srem command, use a list as contents
//------------------------------------------------------------------------------
void MetadataFlusher::srem(const std::string &key, const std::list<std::string> &items) {
  std::vector<std::string> req = {"SREM", key};
  for(auto it = items.begin(); it != items.end(); it++) {
    req.emplace_back(*it);
  }

  backgroundFlusher.pushRequest(req);
}

//------------------------------------------------------------------------------
// Sleep until given index has been flushed to the backend
//------------------------------------------------------------------------------
// void MetadataFlusher::synchronize() {
//
// }


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
std::map<MetadataFlusherFactory::InstanceKey, MetadataFlusher*> MetadataFlusherFactory::instances;
std::mutex MetadataFlusherFactory::mtx;

MetadataFlusher* MetadataFlusherFactory::getInstance(const std::string &id, std::string host, int port) {
  std::lock_guard<std::mutex> lock(MetadataFlusherFactory::mtx);

  if(host.empty() || port == 0) {
    host = BackendClient::sQdbHost;
    port = BackendClient::sQdbPort;
  }

  std::tuple<std::string, std::string, int> key = std::make_tuple(id, host, port);

  auto it = instances.find(key);
  if(it != instances.end()) {
    return it->second;
  }

  MetadataFlusher *flusher = new MetadataFlusher(host, port);
  eos_static_crit("created new metadata flusher at %s : %d", host.c_str(), port);

  instances[key] = flusher;

  return flusher;
}

EOSNSNAMESPACE_END
