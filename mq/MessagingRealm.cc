// ----------------------------------------------------------------------
// File: MessagingRealm.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "mq/MessagingRealm.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Initialize legacy-MQ-based messaging realm.
//------------------------------------------------------------------------------
MessagingRealm::MessagingRealm(XrdMqSharedObjectManager *som, XrdMqSharedObjectChangeNotifier *notif)
: mSom(som), mNotifier(notif), mQSom(nullptr) {}

//------------------------------------------------------------------------------
// Initialize QDB-based messaging realm.
//------------------------------------------------------------------------------
MessagingRealm::MessagingRealm(qclient::SharedManager *qsom)
: mSom(nullptr), mQSom(qsom) {}


EOSMQNAMESPACE_END
