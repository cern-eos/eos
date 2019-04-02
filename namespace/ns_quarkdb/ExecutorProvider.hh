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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Global class to hold ownership of folly::Executor objects.
//------------------------------------------------------------------------------

#ifndef EOS_NS_QDB_EXECUTOR_PROVIDER_HH
#define EOS_NS_QDB_EXECUTOR_PROVIDER_HH

#include "namespace/Namespace.hh"
#include <mutex>
#include <map>
#include <vector>
#include <memory>

namespace folly {
  class Executor;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Global class to hold ownership of folly::Executor objects. This is
//! unfortunately necessary due to the way the namespace objects are
//! constructed by the dynamically loaded plugin. TODO(gbitzes): Get rid
//! of this class once it's possible to do so.
//------------------------------------------------------------------------------
class ExecutorProvider {
public:
  //----------------------------------------------------------------------------
  //! Get an executor object based on the given tag - no ownership
  //----------------------------------------------------------------------------
  static folly::Executor* getIOThreadPool(const std::string &tag);

private:
  static std::map<std::string, std::unique_ptr<folly::Executor>> executorMap;
  static std::mutex mtx;
};

EOSNSNAMESPACE_END

#endif