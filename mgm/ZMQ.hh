// ----------------------------------------------------------------------
// File: ZMQ.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_ZMQ__HH__
#define __EOSMGM_ZMQ__HH__

#include "mgm/Namespace.hh"
#include "mgm/FuseServer.hh"
#include <thread>
#include <vector>
#include <zmq.hpp>
#include <unistd.h>

EOSMGMNAMESPACE_BEGIN


class ZMQ
{
public:
  ZMQ(const char* URL);

  ~ZMQ() = default;

  class Worker
  {
  public:
    Worker(zmq::context_t& ctx, int sock_type)
      : ctx_(ctx), worker_(ctx_, sock_type) {}

    void work();

  private:
    zmq::context_t& ctx_;
    zmq::socket_t worker_;
  };

  class Task
  {
  public:

    Task(std::string url_)
      : ctx_(1),
        frontend_(ctx_, ZMQ_ROUTER),
        backend_(ctx_, ZMQ_DEALER),
        injector_(ctx_, ZMQ_DEALER)
    {
      bindUrl = url_;
    }

    enum {
      kMaxThread = 16
    } ;

    void run()
    {
      int enable_ipv6 = 1;
      frontend_.setsockopt(ZMQ_IPV6, &enable_ipv6, sizeof(enable_ipv6));
      frontend_.bind(bindUrl.c_str());
      backend_.bind("inproc://backend");
      injector_.connect("inproc://backend");
      Worker* worker;
      std::thread* worker_thread;

      for (int i = 0; i < kMaxThread; ++i) {
        worker = new Worker(ctx_, ZMQ_DEALER);
        worker_thread = new std::thread(&Worker::work, worker);
        worker_thread->detach();
      }

      try {
        zmq::proxy(static_cast<void*>(frontend_), static_cast<void*>(backend_),
                   (void*)nullptr);
      } catch (std::exception& e) {}
    }

    void reply(const std::string& id, const std::string& data)
    {
      static XrdSysMutex sMutex;
      XrdSysMutexHelper lLock(sMutex);
      zmq::message_t id_msg(id.c_str(), id.size());
      zmq::message_t data_msg(data.c_str(), data.size());
      injector_.send(id_msg, ZMQ_SNDMORE);
      injector_.send(data_msg);
    }

  private:
    zmq::context_t ctx_;
    zmq::socket_t frontend_;
    zmq::socket_t backend_;
    zmq::socket_t injector_;

    std::string bindUrl;
  } ;

  void ServeFuse();
  Task* task;
  std::string bindUrl;
  static FuseServer gFuseServer;
};

EOSMGMNAMESPACE_END

#endif
