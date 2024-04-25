// ----------------------------------------------------------------------
// File: FuseServer/Flush.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#pragma once

#include <map>

#include "mgm/Namespace.hh"
#include "common/Timing.hh"
#include "common/Logging.hh"

#include <XrdSys/XrdSysPthread.hh>


EOSFUSESERVERNAMESPACE_BEGIN

  //----------------------------------------------------------------------------
  //! Class Flush
  //----------------------------------------------------------------------------

  class Flush : XrdSysMutex
  {
    // essentially a map containing clients which currently flush a file
  public:

    static constexpr int cFlushWindow = 60;

    Flush() = default;

    virtual ~Flush() = default;

    void beginFlush(uint64_t id, std::string client);

    void endFlush(uint64_t id, std::string client);

    bool hasFlush(uint64_t id);

    bool validateFlush(uint64_t id);

    void expireFlush();

    void Print(std::string& out);

  private:

    typedef struct flush_info {

      flush_info() : client(""), nref(0)
      {
        ftime.tv_sec = 0;
        ftime.tv_nsec = 0;
      }

      flush_info(std::string _client) : client(_client)
      {
        eos::common::Timing::GetTimeSpec(ftime);
        ftime.tv_sec += cFlushWindow;
        ftime.tv_nsec = 0;
        nref = 0;
      }

      void Add(struct flush_info l)
      {
        ftime = l.ftime;
        nref++;
      }

      bool Remove(struct flush_info l)
      {
        nref--;

        if (nref > 0) {
          return false;
        }

        return true;
      }

      std::string client;
      struct timespec ftime;
      ssize_t nref;
    } flush_info_t;

    std::map<uint64_t, std::map<std::string, flush_info_t> > flushmap;
  };


EOSFUSESERVERNAMESPACE_END
