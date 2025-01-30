// ----------------------------------------------------------------------
// File: SharedHashProvider.hh
// Author: Georgios Bitzes - CERN
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

#ifndef EOS_MQ_SHARED_HASH_PROVIDER_HH
#define EOS_MQ_SHARED_HASH_PROVIDER_HH

#include "mq/Namespace.hh"
#include <memory>
#include <map>
#include <mutex>

namespace qclient
{
class SharedManager;
class SharedHash;
}

namespace eos
{
namespace common
{
class SharedHashLocator;
}
}

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to keep ownership of qclient SharedHashes
//------------------------------------------------------------------------------
class SharedHashProvider
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashProvider(qclient::SharedManager* manager);

  //----------------------------------------------------------------------------
  //! Get shared hash
  //----------------------------------------------------------------------------
  std::shared_ptr<qclient::SharedHash>
  Get(const eos::common::SharedHashLocator& locator);

  //----------------------------------------------------------------------------
  //! Delete shared hash
  //!
  //! @param locator locator object for the given hash
  //! @param delete_from_qdb if true delete the backing SharedHash from QDB
  //----------------------------------------------------------------------------
  void Delete(const eos::common::SharedHashLocator& locator,
              bool delete_from_qdb);

private:
  qclient::SharedManager* mSharedManager;

  std::mutex mMutex;
  std::map<std::string, std::shared_ptr<qclient::SharedHash>> mStore;

};

EOSMQNAMESPACE_END

#endif
