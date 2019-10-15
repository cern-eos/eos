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

#include "namespace/ns_quarkdb/CacheRefreshListener.hh"
#include "namespace/interface/Identifiers.hh"
#include "namespace/ns_quarkdb/persistency/MetadataProvider.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "common/ParseUtils.hh"
#include <functional>
#include <qclient/pubsub/Message.hh>

using std::placeholders::_1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
CacheRefreshListener::CacheRefreshListener(const QdbContactDetails &cd, MetadataProvider *provider)
: mContactDetails(cd), mMetadataProvider(provider),
  mSubscriber(cd.members, cd.constructSubscriptionOptions()) {

  mFidSubscription = mSubscriber.subscribe(constants::sCacheInvalidationFidChannel);
  mCidSubscription = mSubscriber.subscribe(constants::sCacheInvalidationCidChannel);

  mFidSubscription->attachCallback(std::bind(&CacheRefreshListener::processIncomingFidInvalidation,
    this, _1));

  mCidSubscription->attachCallback(std::bind(&CacheRefreshListener::processIncomingCidInvalidation,
    this, _1));
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
CacheRefreshListener::~CacheRefreshListener() {}

//------------------------------------------------------------------------------
// Process incoming fid invalidation
//------------------------------------------------------------------------------
void CacheRefreshListener::processIncomingFidInvalidation(qclient::Message &&msg) {
  eos_static_info("Received invalidation message for fid=%s", msg.getPayload());

  uint64_t fid;

  if(common::ParseUInt64(msg.getPayload(), fid)) {
    mMetadataProvider->dropCachedFileID(FileIdentifier(fid));
  }
}

//------------------------------------------------------------------------------
// Process incoming cid invalidation
//------------------------------------------------------------------------------
void CacheRefreshListener::processIncomingCidInvalidation(qclient::Message &&msg) {
  eos_static_info("Received invalidation message for cid=%s", msg.getPayload());

  uint64_t cid;

  if(common::ParseUInt64(msg.getPayload(), cid)) {
    mMetadataProvider->dropCachedContainerID(ContainerIdentifier(cid));
  }
}

EOSNSNAMESPACE_END
