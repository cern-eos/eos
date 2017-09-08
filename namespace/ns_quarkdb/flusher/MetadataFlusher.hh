/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Metadata flushing towards QuarkDB
//------------------------------------------------------------------------------

#ifndef __EOS_NS_METADATA_FLUSHER_HH__
#define __EOS_NS_METADATA_FLUSHER_HH__

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "qclient/BackgroundFlusher.hh"
#include <list>
#include <map>

EOSNSNAMESPACE_BEGIN

class ContainerMD;

//------------------------------------------------------------------------------
//! Class to receive notifications from the BackgroundFlusher
//------------------------------------------------------------------------------
// TODO

//------------------------------------------------------------------------------
//! Metadata flushing towards QuarkDB
//------------------------------------------------------------------------------
class MetadataFlusher
{
public:
  MetadataFlusher(const std::string &host, int port);

  void hdel(const std::string &key, const std::string &field);
  void hset(const std::string &key, const std::string &field, const std::string &value);
  void sadd(const std::string &key, const std::string &field);
  void srem(const std::string &key, const std::string &field);
  void srem(const std::string &key, const std::list<std::string> &items);

private:
  qclient::QClient qcl;
  qclient::BackgroundFlusher backgroundFlusher;
  qclient::Notifier dummyNotifier;
};

class MetadataFlusherFactory {
public:
  static MetadataFlusher* getInstance(const std::string &id, std::string host, int port);
private:
  static std::mutex mtx;

  using InstanceKey = std::tuple<std::string, std::string, int>;
  static std::map<std::tuple<std::string, std::string, int>, MetadataFlusher*> instances;
};

EOSNSNAMESPACE_END

#endif
