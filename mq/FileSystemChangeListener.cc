// ----------------------------------------------------------------------
// File: FileSystemChangeListener.cc
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

#include "mq/FileSystemChangeListener.hh"
#include "mq/XrdMqSharedObject.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemChangeListener::FileSystemChangeListener(const std::string &name, XrdMqSharedObjectChangeNotifier &notif)
: mNotifier(notif), mListenerName(name) {}

//------------------------------------------------------------------------------
// Subscribe to the given key, such as "stat.errc" or "stat.geotag"
//------------------------------------------------------------------------------
bool FileSystemChangeListener::subscribe(const std::string &key) {
  return mNotifier.SubscribesToKey(mListenerName.c_str(), key,
    XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
}

//------------------------------------------------------------------------------
// Start listening
//------------------------------------------------------------------------------
bool FileSystemChangeListener::startListening() {
  mNotifier.BindCurrentThread(mListenerName);
  return mNotifier.StartNotifyCurrentThread();
}

//------------------------------------------------------------------------------
// Consume next event, block until there's one
//------------------------------------------------------------------------------
bool FileSystemChangeListener::fetch(Event &out, common::ThreadAssistant &assistant) {
  return false;
}

EOSMQNAMESPACE_END
