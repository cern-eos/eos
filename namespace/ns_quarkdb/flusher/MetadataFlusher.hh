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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Metadata flushing towards QuarkDB
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "qclient/BackgroundFlusher.hh"
#include "qclient/AssistedThread.hh"
#include <list>
#include <map>

EOSNSNAMESPACE_BEGIN

class MetadataFlusher;
class QdbContactDetails;

//------------------------------------------------------------------------------
//! Class to receive notifications from the BackgroundFlusher
//------------------------------------------------------------------------------
class FlusherNotifier : public qclient::Notifier
{
public:
  FlusherNotifier(MetadataFlusher& flusher);
  virtual void eventNetworkIssue(const std::string& err) override;
  virtual void eventUnexpectedResponse(const std::string& err) override;
private:
  MetadataFlusher& mFlusher;
};

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
  MetadataFlusher(const std::string& path,
                  const QdbContactDetails& contactDetails);

  MetadataFlusher(const std::string& path,
                  const QdbContactDetails& contactDetails,
                  const std::string& flusher_type,
                  const std::string& rocksdb_options);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~MetadataFlusher();

  //----------------------------------------------------------------------------
  //! Methods to stage redis commands for background flushing.
  //----------------------------------------------------------------------------
  template<typename... Args>
  void exec(const Args... args)
  {
    backgroundFlusher.pushRequest(std::vector<std::string> {args...});
  }

  void del(const std::string& key);
  void hdel(const std::string& key, const std::string& field);
  void hset(const std::string& key, const std::string& field,
            const std::string& value);
  void hincrby(const std::string& kye, const std::string& field,
               int64_t value);
  void sadd(const std::string& key, const std::string& field);
  void srem(const std::string& key, const std::string& field);
  void srem(const std::string& key, const std::list<std::string>& items);

  void execute(const std::vector<std::string>& req)
  {
    backgroundFlusher.pushRequest(req);
  }

  //----------------------------------------------------------------------------
  //! Block until the queue has flushed all pending entries at the time of
  //! calling. Example: synchronize is called when pending items in the queue
  //! are [1500, 2000]. The calling thread sleeps up to the point that entry
  //! #2000 is flushed - of course, at that point other items might have been
  //! added to the queue, but we don't wait.
  //----------------------------------------------------------------------------
  void synchronize(ItemIndex targetIndex = -1);

private:
  void queueSizeMonitoring(qclient::ThreadAssistant& assistant);
  std::string id;

  FlusherNotifier notifier;
  qclient::BackgroundFlusher backgroundFlusher;
  qclient::AssistedThread sizePrinter;
};

EOSNSNAMESPACE_END
