//------------------------------------------------------------------------------
// File: WebNotify.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "common/WebNotify.hh"
#include <iostream>
#include <memory>

// curl
#include <curl/curl.h>
#include <curl/easy.h>
#include <json/json.h>

// active MQ
#include <cms/Connection.h>
#include <cms/ConnectionFactory.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/MessageProducer.h>
#include <decaf/lang/Thread.h>
#include <decaf/util/UUID.h>
#include <decaf/internal/util/concurrent/Threading.h>
#include <activemq/library/ActiveMQCPP.h>

// grpc
#ifdef EOS_GRPC
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#endif
#include <grpc++/grpc++.h>
#include "proto/Rpc.grpc.pb.h"
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
using eos::rpc::Eos;
using eos::rpc::NotificationRequest;
using eos::rpc::NotificationResponse;
/*#include <grpc/grpc.h>
#include <grpc/grpc_security.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpc/impl/codegen/grpc_types.h>
*/
#endif

// QClient
#include <qclient/QClient.hh>

using namespace cms;
using namespace decaf::lang;
using namespace std;

EOSCOMMONNAMESPACE_BEGIN;


bool WebNotify::Notify(const std::string& protocol,
                       const std::string& uri,
                       const std::string& sport,
                       const std::string& channel,
                       const std::string& message,
                       const std::string& stimeout)
{
  WebNotify notify;
  try {
    int timeoutMs = stimeout.empty() ? 0 : std::stoi(stimeout);
    int port = sport.empty() ? 0 : std::stoi(sport);
    eos_static_debug("protocol='%s'", protocol.c_str());
    if (protocol == "http")
      return notify.sendHttpPostNotification(uri, message, timeoutMs);
    if (protocol == "grpc")
      return notify.sendGrpcNotification(uri, message, timeoutMs);
    if (protocol == "redis")
      return notify.sendQClientNotification(uri, port, channel, message, timeoutMs, true);
    if (protocol == "qclient")
      return notify.sendQClientNotification(uri, port, channel, message, timeoutMs, false);
    if (protocol == "amq")
      return notify.sendActiveMQNotification(uri, channel, message, timeoutMs);
    eos_static_err("msg=\"unsupported notification protocol specified\" protocol=\"%s\"", protocol.c_str());
  } catch (const std::exception& e) {
    eos_static_err("msg=\"invalid numeric input\" error=\"%s\"", e.what());
  }
  
  return false;
}

bool WebNotify::sendHttpPostNotification(const std::string& url, const std::string& message, long timeoutMs) {
  CURL* curl = curl_easy_init();
  if (!curl) return false;
  
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  
  std::string jsonPayload;
  if (!message.empty() && message.front() == '{') {
    jsonPayload = message;
  } else {
    jsonPayload = "{\"message\": \"" + message + "\"}";
  }
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonPayload.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeoutMs);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NoOpCallback);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  
  return (res == CURLE_OK);
}

bool WebNotify::sendActiveMQNotification(const std::string& brokerURI, const std::string& queueName, const std::string& messageText, int timeoutMs) {
  static std::once_flag initFlag;
  
  std::call_once(initFlag, [] {
			     activemq::library::ActiveMQCPP::initializeLibrary();
			   });
  
  try {
    // Construct broker URI with connection timeout
    std::ostringstream fullBrokerURI;
    fullBrokerURI << brokerURI;
    if (brokerURI.find('?') == std::string::npos) {
      fullBrokerURI << "?";
    } else {
      fullBrokerURI << "&";
    }
    fullBrokerURI
      << "connection.requestTimeout=" << timeoutMs
      << "&wireFormat.maxInactivityDuration=" << timeoutMs
      << "&wireFormat.maxInactivityDurationInitialDelay=" << timeoutMs 
      << "&transport.maxReconnectAttempts=0";
    // Create a ConnectionFactory
    std::unique_ptr<ConnectionFactory> connectionFactory(ConnectionFactory::createCMSConnectionFactory(fullBrokerURI.str()));
    
    // Create a Connection
    std::unique_ptr<Connection> connection(connectionFactory->createConnection());
    connection->start();
    
    // Create a Session
    std::unique_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
    
    // Create the Destination (queue)
    std::unique_ptr<Destination> destination(session->createQueue(queueName));
    
    // Create a MessageProducer from the Session to the Topic or Queue
    std::unique_ptr<MessageProducer> producer(session->createProducer(destination.get()));
    producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);
    
    // Create the message and send it
        std::unique_ptr<TextMessage> message(session->createTextMessage(messageText));
        producer->send(message.get());
        return true;
  } catch (const CMSException& e) {
    std::cerr << "CMSException: ";
    e.printStackTrace();
    return false;
  } catch (const std::exception& e) {
    eos_static_err("exception='%s'", e.what());
    return false;
  } catch (...) {
    eos_static_err("Unknown exception occurred while sending ActiveMQ notification.");
    return false;
  }
}

bool WebNotify::sendGrpcNotification(const std::string& target, const std::string& message, int timeoutMs)
{
#ifdef EOS_GRPC
    grpc::ChannelArguments ch_args;
    // This is the key: set connection timeout (in milliseconds)
    ch_args.SetInt("grpc.client_channel_backup_poll_interval_ms", timeoutMs);
    ch_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1); // Unlimited if needed

    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeoutMs);
    std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
        target,
        grpc::InsecureChannelCredentials(),
        ch_args
    );

    auto stub = eos::rpc::Eos::NewStub(channel);

    NotificationRequest request;
    request.set_message(message);

    grpc::ClientContext context;
    context.set_deadline(deadline);

    NotificationResponse response;
    grpc::Status status = stub->Notify(&context, request, &response);
    if (status.ok()) {
      eos_static_debug("gRPC call succeeded");
      return response.success();
    } else {
      eos_static_err("msg=\"gRPC call failed\" errc=%d errmsg='%s'", status.error_code(), status.error_message().c_str());
      return false;
    }
#else
    return false;
#endif
}

bool WebNotify::sendQClientNotification(const std::string& hostname, int port,
                                        const std::string& channel,
                                        const std::string& message,
                                        int timeoutMs,
					bool push) {
  using namespace qclient;

    try {
        // Connect with socket timeout
        QClient client{hostname, port, {}};
   
        // Send PUBLISH command
	std::string method = push ?"RPUSH":"PUBLISH";
	auto publish = client.exec(method, channel, message);
	qclient::redisReplyPtr reply = publish.get();
	if (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer != 0) {
	  eos_static_debug("msg=\"%s\" %s=%d", push ? "pushed to list" : "published", push ? "length" : "subscribers", reply->integer);
	  return true;
	} else {
	  eos_static_err("msg=\"unexpected or null reply from QuarkDB/REDIS")
	  return false;
	}
    } catch (const std::exception& ex) {
      eos_static_err("msg=\"QuarkDB/REDIS connection or command error\" msg='%s'", ex.what());
      return false;
    }
}

EOSCOMMONNAMESPACE_END;
