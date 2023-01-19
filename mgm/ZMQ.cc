// ----------------------------------------------------------------------
// File: ZMQ.cc
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

#include <thread>
#include "common/Logging.hh"
#include <common/StringUtils.hh>
#include "mgm/fusex.pb.h"
#include "mgm/FuseServer/Server.hh"
#include "mgm/ZMQ.hh"

EOSMGMNAMESPACE_BEGIN

//int ZMQ::Task::sMaxThreads = 16;
FuseServer::Server ZMQ::gFuseServer;

//------------------------------------------------------------------------------
// Start thread handling fuse server proxying
//------------------------------------------------------------------------------
void
ZMQ::ServeFuse()
{
  mTask.reset(new Task(mBindUrl));
  std::thread t1(&Task::run, mTask.get());
  t1.detach();
}

//------------------------------------------------------------------------------
// Task destructor
//------------------------------------------------------------------------------
ZMQ::Task::~Task()
{
  // Closing the ZMQ context will cause an execption to be thrown in the worker
  // thread with ETERM as the error number
  mZmqCtx.close();

  for (const auto& th : mWorkerThreads) {
    delete th;
  }

  mWorkerThreads.clear();
}


//------------------------------------------------------------------------------
// Start proxy service
//------------------------------------------------------------------------------
void
ZMQ::Task::run() noexcept
{
  int enable_ipv6 = 1;
#if CPPZMQ_VERSION >= ZMQ_MAKE_VERSION(4, 8, 1)
  mFrontend.set(zmq::sockopt::ipv6, enable_ipv6);
#elif CPPZMQ_VERSION >= ZMQ_MAKE_VERSION(4, 7, 1)
  mFrontend.set(ZMQ_IPV6, enable_ipv6);
#else
  mFrontend.setsockopt(ZMQ_IPV6, &enable_ipv6, sizeof(enable_ipv6));
#endif
  {
    // set keepalive options
    int32_t keep_alive = 1;
    int32_t keep_alive_idle = 30;
    int32_t keep_alive_cnt = 2;
    int32_t keep_alive_intvl = 30;
    mFrontend.setsockopt(ZMQ_TCP_KEEPALIVE, &keep_alive, sizeof(keep_alive));
    mFrontend.setsockopt(ZMQ_TCP_KEEPALIVE_IDLE, &keep_alive_idle,
                         sizeof(keep_alive_idle));
    mFrontend.setsockopt(ZMQ_TCP_KEEPALIVE_CNT, &keep_alive_cnt,
                         sizeof(keep_alive_cnt));
    mFrontend.setsockopt(ZMQ_TCP_KEEPALIVE_INTVL, &keep_alive_intvl,
                         sizeof(keep_alive_intvl));
  }
  mFrontend.bind(mBindUrl.c_str());
  mBackend.bind("inproc://backend");
  mInjector.connect("inproc://backend");

  for (int i = 0; i < sMaxThreads; ++i) {
    mWorkerThreads.push_back(new std::thread(&Worker::work,
                             new Worker(mZmqCtx, ZMQ_DEALER)));
    mWorkerThreads.back()->detach();
  }

  try {
    zmq::proxy(static_cast<void*>(mFrontend), static_cast<void*>(mBackend),
               nullptr);
  } catch (const zmq::error_t& e) {
    if (e.num() == ETERM) {
      // Shutdown
      return;
    }
  }
}

//------------------------------------------------------------------------------
//  Reply to a client identifier with a piece of data
//------------------------------------------------------------------------------
void
ZMQ::Task::reply(const std::string& id, const std::string& data)
{
  static XrdSysMutex sMutex;
  XrdSysMutexHelper lLock(sMutex);
  zmq::message_t id_msg(id.c_str(), id.size());
  zmq::message_t data_msg(data.c_str(), data.size());

  try {
    mInjector.send(id_msg, ZMQ_SNDMORE);
    mInjector.send(data_msg);
  } catch (const zmq::error_t& e) {
    if (e.num() == ETERM) {
      return;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
ZMQ::Worker::work()
{
  worker_.connect("inproc://backend");
  eos::fusex::container hb;

  try {
    while (true) {
      int more = 0;
      size_t size = sizeof(int);
      zmq::message_t identity;
      zmq::message_t msg;
      zmq::message_t copied_id;
      zmq::message_t copied_msg;
      worker_.recv(&identity, 0);
      worker_.getsockopt(ZMQ_RCVMORE, &more, &size);

      if (!more) {
        eos_static_warning("discarding illegal message");
        continue;
      }

      worker_.recv(&msg, 0);
      std::string id(static_cast<const char*>(identity.data()), identity.size());
      std::string s(static_cast<const char*>(msg.data()), msg.size());
      hb.Clear();

      if (hb.ParseFromString(s)) {
        switch (hb.type()) {
        case eos::fusex::container::HEARTBEAT: {
          struct timespec tsnow {};
          eos::common::Timing::GetTimeSpec(tsnow);
          hb.mutable_heartbeat_()->set_delta(tsnow.tv_sec - hb.heartbeat_().clock() +
                                             (((int64_t) tsnow.tv_nsec - (int64_t) hb.heartbeat_().clock_ns()) * 1.0 /
                                              1000000000.0));

          if (gFuseServer.Client().Dispatch(id, *(hb.mutable_heartbeat_()))) {
            if (EOS_LOGS_DEBUG) {
              eos_static_debug("msg=\"received new heartbeat\" identity=%s type=%d",
                               (id.length() < 256) ? id.c_str() : "-illegal-", hb.type());
            }
          } else {
            if (EOS_LOGS_DEBUG) {
              eos_static_debug("msg=\"received heartbeat\" identity=%s type=%d",
                               (id.length() < 256) ? id.c_str() : "-illegal-",
                               hb.type());
            }
          }

          if (hb.statistics_().vsize_mb() != 0.0f) {
            gFuseServer.Client().HandleStatistics(id, hb.statistics_());
          }
        }
        break;

        default:
          eos_static_err("%s", "msg=\"message type unknown");
        }
      } else {
        eos_static_debug("msg=\"unable to parse message\": "
                         "id.c_str()=%s, id.length()=%d, id:hex=%s, s.c_str()=%s, s.length()=%d, s:hex=%s",
                         id.c_str(), id.length(), eos::common::stringToHex(id).c_str(), s.c_str(),
                         s.length(), eos::common::stringToHex(s).c_str());
      }
    }
  } catch (const zmq::error_t& e) {
    // Shutdown
    if (e.num() == ETERM) {
      eos_static_debug("%s", "msg=\"shutdown ZMQ worker ...\"");
      delete this;
    }
  }
}

EOSMGMNAMESPACE_END
