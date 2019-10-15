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
//! @brief Class to listen for notifications about cache-invalidated MD entries
//------------------------------------------------------------------------------

#ifndef EOS_NS_CACHE_REFRESH_LISTENER_HH
#define EOS_NS_CACHE_REFRESH_LISTENER_HH

#pragma once
#include "namespace/Namespace.hh"
#include "QdbContactDetails.hh"
#include <qclient/pubsub/Subscriber.hh>

EOSNSNAMESPACE_BEGIN

class MetadataProvider;

//------------------------------------------------------------------------------
//! Class to listen for notifications (typically issued by eos-ns-inspect)
//! about which metadata entries have been modified outside the MGM.
//------------------------------------------------------------------------------
class CacheRefreshListener {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  CacheRefreshListener(const QdbContactDetails &cd, MetadataProvider *provider);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~CacheRefreshListener();

private:
  //----------------------------------------------------------------------------
  //! Process incoming fid invalidation
  //----------------------------------------------------------------------------
  void processIncomingFidInvalidation(qclient::Message &&msg);

  //----------------------------------------------------------------------------
  //! Process incoming cid invalidation
  //----------------------------------------------------------------------------
  void processIncomingCidInvalidation(qclient::Message &&msg);


  QdbContactDetails mContactDetails;
  MetadataProvider* mMetadataProvider;
  qclient::Subscriber mSubscriber;

  std::unique_ptr<qclient::Subscription> mFidSubscription;
  std::unique_ptr<qclient::Subscription> mCidSubscription;
};

EOSNSNAMESPACE_END

#endif
