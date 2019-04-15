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
//! @brief Utility class to recompute a quotanode
//------------------------------------------------------------------------------

#pragma once

#include "namespace/interface/IContainerMD.hh"
#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"

namespace qclient {
  class QClient;
}

namespace folly {
  class Executor;
}

EOSNSNAMESPACE_BEGIN

class QuotaNodeCore;
class IView;

//------------------------------------------------------------------------------
//! Utility class to recompute a quotanode
//------------------------------------------------------------------------------
class QuotaRecomputer
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaRecomputer(IView *view, qclient::QClient *qcl, folly::Executor *exec);

  //----------------------------------------------------------------------------
  //! Given a quotanode, re-calculate the quota values,
  //! store into QuotaNodeCore.
  //----------------------------------------------------------------------------
  MDStatus recompute(IContainerMDPtr quotanode, QuotaNodeCore &core);

private:
  IView *mView;
  qclient::QClient *mQcl;
  folly::Executor *mExecutor;
};

EOSNSNAMESPACE_END
