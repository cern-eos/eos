// ----------------------------------------------------------------------
// File: WebNotify.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/ASwitzerland                                  *
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

/**
 * @file   WebNotify.hh
 *
 * @brief  Class handling web notification requests
 *
 *
 */

#pragma once
#include "common/Namespace.hh"
#include "common/VirtualIdentity.hh"
#include "common/RWMutex.hh"
#include <map>
#include <string>

EOSCOMMONNAMESPACE_BEGIN;

class WebNotify {

public:

  WebNotify() { }

  static bool Notify(const std::string& protocol,
		     const std::string& uri,
		     const std::string& port,
		     const std::string& channel,
		     const std::string& message,
		     const std::string& timeout);
  
  virtual ~WebNotify() {}
  static size_t NoOpCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    return size * nmemb;  // Tell libcurl we "handled" the data
  }
  bool sendHttpPostNotification(const std::string& url, const std::string& message, long timeoutMs = 2000);
  bool sendActiveMQNotification(const std::string& brokerURI, const std::string& queueName, const std::string& messageText, int timeoutMs = 2000);
  bool sendGrpcNotification(const std::string& target, const std::string& message, int timeoutMs = 2000);
  bool sendQClientNotification(const std::string& hostname, int port, const std::string& channel, const std::string& message, int timeoutMs = 2000, bool push=false);
  
private:
};

EOSCOMMONNAMESPACE_END;
