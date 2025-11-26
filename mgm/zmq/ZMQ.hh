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
#include "mgm/FuseServer/Server.hh"
#include <thread>
#include <vector>
#include <zmq.hpp>
#include <unistd.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ZMQ
//------------------------------------------------------------------------------
class ZMQ
{
public:
  class Task;
  static FuseServer::Server gFuseServer; ///< Fuse server object

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  explicit ZMQ(const char* URL): mBindUrl(URL)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ZMQ()
  {
    std::cerr << __FUNCTION__ << ":: end of destructor\n";
  }

  //----------------------------------------------------------------------------
  //! Start thread handling fuse server proxying
  //----------------------------------------------------------------------------
  void ServeFuse();

  std::unique_ptr<Task> mTask; ///< Task associated to the ZMQ object

  //----------------------------------------------------------------------------
  //! Class Worker
  //----------------------------------------------------------------------------
  class Worker
  {
  public:
    Worker(zmq::context_t& ctx, int sock_type)
      : mZmqCtx(ctx), worker_(mZmqCtx, sock_type) {}

    void work();

  private:
    zmq::context_t& mZmqCtx;
    zmq::socket_t worker_;
  };

  //----------------------------------------------------------------------------
  //! Class Task
  //----------------------------------------------------------------------------
  class Task
  {
  public:
    const static int sMaxThreads = 16; ///< Max number of worker threads

    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    explicit Task(std::string& url)
      : mZmqCtx(1), mFrontend(mZmqCtx, ZMQ_ROUTER),
        mBackend(mZmqCtx, ZMQ_DEALER), mInjector(mZmqCtx, ZMQ_DEALER),
        mBindUrl(url)
    {}

    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    ~Task();

    //----------------------------------------------------------------------------
    //! Start proxy service
    //----------------------------------------------------------------------------
    void run() noexcept;

    //----------------------------------------------------------------------------
    //! Reply to a client identifier which a pice of data
    //!
    //! @param id cilent idnetifier
    //! @param data data buffer
    //----------------------------------------------------------------------------
    void reply(const std::string& id, const std::string& data);

  private:
    zmq::context_t mZmqCtx; ///< ZMQ context for task
    zmq::socket_t mFrontend; ///< Frontend socket
    zmq::socket_t mBackend; ///< Backend socket
    zmq::socket_t mInjector; ///< Injector socket connected to the backedn
    std::string mBindUrl; ///< URL
    std::list<std::thread*> mWorkerThreads; ///< List of worker threads
  };

private:
  std::string mBindUrl; ///< URL
};

EOSMGMNAMESPACE_END

#endif
