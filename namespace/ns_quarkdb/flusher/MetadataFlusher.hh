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
#include "qclient/AssistedThread.hh"
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
using ItemIndex = int64_t;
class MetadataFlusher
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MetadataFlusher(const std::string &path, const std::string &host, int port);

  //----------------------------------------------------------------------------
  //! Methods to stage redis commands for background flushing.
  //----------------------------------------------------------------------------
  void del(const std::string &key);
  void hdel(const std::string &key, const std::string &field);
  void hset(const std::string &key, const std::string &field, const std::string &value);
  void sadd(const std::string &key, const std::string &field);
  void srem(const std::string &key, const std::string &field);
  void srem(const std::string &key, const std::list<std::string> &items);


  //----------------------------------------------------------------------------
  //! Block until the queue has flushed all pendind entries at the time of
  //! calling. Example: synchronize is called when pending items in the queue
  //! are [1500, 2000]. The calling thread sleeps up to the point that entry
  //! #2000 is flushed - of course, at that point other items might have been
  //! added to the queue, but we don't wait.
  //----------------------------------------------------------------------------
  void synchronize(ItemIndex targetIndex = -1);

private:
  void queueSizeMonitoring(qclient::ThreadAssistant &assistant);

  qclient::QClient qcl;
  qclient::BackgroundFlusher backgroundFlusher;
  qclient::Notifier dummyNotifier;
  qclient::AssistedThread sizePrinter;
};

class MetadataFlusherFactory {
public:
  static MetadataFlusher* getInstance(const std::string &id, std::string host, int port);
  static void setQueuePath(const std::string &newpath);
private:
  static std::string queuePath;
  static std::mutex mtx;

  using InstanceKey = std::tuple<std::string, std::string, int>;
  static std::map<std::tuple<std::string, std::string, int>, MetadataFlusher*> instances;
};

EOSNSNAMESPACE_END

#endif
