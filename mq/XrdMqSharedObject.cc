// ----------------------------------------------------------------------
// File: XrdMqSharedObject.cc
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
 * but WITHOUTA ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mq/XrdMqSharedObject.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqStringConversion.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysAtomics.hh"
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>
/*----------------------------------------------------------------------------*/

bool XrdMqSharedObjectManager::debug = 0;
bool XrdMqSharedObjectManager::broadcast = true;

unsigned long long XrdMqSharedHash::SetCounter = 0;
unsigned long long XrdMqSharedHash::SetNLCounter = 0;
unsigned long long XrdMqSharedHash::GetCounter = 0;

__thread XrdMqSharedObjectChangeNotifier::Subscriber *XrdMqSharedObjectChangeNotifier::tlSubscriber = NULL;

#define _NotifierMapUpdate(map,key,subscriber) \
  { \
  auto entry = map.find(key); \
  if( entry != map.end() ) { \
    entry->second.mSubscribers.erase(subscriber); \
    if(entry->second.mSubscribers.empty()) { \
      if(entry->second.mRegex) { \
        regfree(entry->second.mRegex); \
        delete entry->second.mRegex;  \
      } \
      map.erase(entry); \
    } \
  } \
  }

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubject (const std::string &subscriber, const std::string &subject,
						      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber);
  XrdSysMutexHelper lock (s->WatchMutex);

  eos_static_debug("subscribing to subject %s", subject.c_str ());
  if (s->WatchSubjects[type].count (subject)) return false;
  s->WatchSubjects[type].insert (subject);
  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StartNotifySubject (s, subject, type)) return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectRegex (const std::string &subscriber, const std::string &subject,
							   XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber);
  XrdSysMutexHelper lock (s->WatchMutex);

  eos_static_debug("subscribing to subject regex %s", subject.c_str ());
  if (s->WatchSubjectsRegex[type].count (subject)) return false;
  s->WatchSubjectsRegex[type].insert (subject);
  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StartNotifySubjectRegex (s, subject, type)) return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToKey (const std::string &subscriber, const std::string &key,
						  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber);
  XrdSysMutexHelper lock (s->WatchMutex);

  eos_static_debug("subscribing to key %s", key.c_str ());
  if (s->WatchKeys[type].count (key)) return false;
  s->WatchKeys[type].insert (key);
  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StartNotifyKey (s, key, type)) return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToKeyRegex (const std::string &subscriber, const std::string &key,
						       XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber);
  XrdSysMutexHelper lock (s->WatchMutex);

  eos_static_debug("subscribing to key regex %s", key.c_str ());
  if (s->WatchKeysRegex[type].count (key)) return false;
  s->WatchKeysRegex[type].insert (key);
  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StartNotifyKeyRegex (s, key, type)) return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey (const std::string &subscriber,
							    const std::set<std::string> &subjects,
							    const std::set<std::string> &keys,
							    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber);
  XrdSysMutexHelper lock (s->WatchMutex);

  if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    size_t bufsize = 0;
    for (auto it = subjects.begin (); it != subjects.end (); ++it)
      bufsize += (it->size () + 1);
    for (auto it = keys.begin (); it != keys.end (); ++it)
      bufsize += (it->size () + 1);
    bufsize += 64;
    int sz;

    char *buffer = new char[bufsize];
    char *buf = buffer;
    sz = snprintf (buf, bufsize, "subscribing to subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin (); it != subjects.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf (buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin (); it != keys.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }
    sz = snprintf (buf, bufsize, "]");

    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  // firstly update the thread-local vector
  bool insertIntoExisiting = false;
  {
    for (auto it = s->WatchSubjectsXKeys[type].begin (); it != s->WatchSubjectsXKeys[type].end (); it++)
    {

//      {
//         size_t bufsize=0;
//           for(auto it2 = it->first.begin(); it2 != it->first.end(); ++it2)
//             bufsize+=(it2->size()+1);
//           for(auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
//             bufsize+=(it2->size()+1);
//           bufsize += 64;
//           int sz;
//
//           char *buffer = new char[bufsize];
//           char *buf=buffer;
//           sz = snprintf(buf,bufsize,"WatchSubjectsXKeys item : subjects [ ");
//           buf += sz;
//           bufsize -=sz;
//
//           for(auto it2 = it->first.begin(); it2 != it->first.end(); ++it2) {
//             sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//             buf += sz;
//             bufsize -=sz;
//           }
//
//           sz = snprintf(buf,bufsize,"] times keys [ ");
//           buf += sz;
//           bufsize -=sz;
//
//           for(auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
//             sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//             buf += sz;
//             bufsize -=sz;
//           }
//           sz = snprintf(buf,bufsize,"]");
//
//           eos_static_info("%s", buffer);
//           delete[] buffer;
//       }

      if (subjects == it->first)
      {
	size_t sizeBefore = it->second.size ();
	it->second.insert (keys.begin (), keys.end ());
	if (sizeBefore == it->second.size ())
	  return false; // nothing to insert
	else
	{
	  insertIntoExisiting = true;
	  break;
	}
      }
      else if (keys == it->second)
      {
	size_t sizeBefore = it->first.size ();
	it->first.insert (subjects.begin (), subjects.end ());
	if (sizeBefore == it->first.size ())
	  return false; // nothing to insert
	else
	{
	  insertIntoExisiting = true;
	  break;
	}
      }
    }
    if (!insertIntoExisiting) s->WatchSubjectsXKeys[type].push_back (make_pair (subjects, keys));
  }

  if (s->Notify)
  {
    // update the ongoing notification
    return StartNotifySubjectsAndKeys (s, subjects, keys, type);
  }

  return true;
}

bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey (const std::string &subscriber, const std::string &subject,
							    const std::string &key,
							    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s, k;
  s.insert (subject);
  k.insert (key);
  return SubscribesToSubjectAndKey (subscriber, s, k, type);
}
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey (const std::string &subscriber, const std::string &subject,
							    const std::set<std::string> &keys,
							    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s;
  s.insert (subject);
  return SubscribesToSubjectAndKey (subscriber, s, keys, type);
}
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey (const std::string &subscriber,
							    const std::set<std::string> &subjects,
							    const std::string &key,
							    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> k;
  k.insert (key);
  return SubscribesToSubjectAndKey (subscriber, subjects, k, type);
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubject (const std::string &subscriber, const std::string &subject,
							XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StopNotifySubject (s, subject, type)) return false;
  }
  if (s->empty ())
  {
    delete s;
    s = NULL;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectRegex (const std::string &subscriber, const std::string &subject,
							     XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StopNotifySubjectRegex (s, subject, type)) return false;
  }
  if (s->empty ())
  {
    delete s;
    s = NULL;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToKey (const std::string &subscriber, const std::string &key,
						    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StopNotifyKey (s, key, type)) return false;
  }
  if (s->empty ())
  {
    delete s;
    s = NULL;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToKeyRegex (const std::string &subscriber, const std::string &key,
							 XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  if (s->Notify)
  {
    // if the notification is started for this process, update it
    if (!StopNotifyKeyRegex (s, key, type)) return false;
  }
  if (s->empty ())
  {
    delete s;
    s = NULL;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToEverything (const std::string &subscriber)
{
  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  if (s->Notify) StopNotifyCurrentThread ();
  delete s;
  s = NULL;
  return true;
}

///*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey (const std::string &subscriber,
							      std::set<std::string> subjects,
							      std::set<std::string> keys,
							      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    size_t bufsize = 0;
    for (auto it = subjects.begin (); it != subjects.end (); ++it)
      bufsize += (it->size () + 1);
    for (auto it = keys.begin (); it != keys.end (); ++it)
      bufsize += (it->size () + 1);
    bufsize += 64;
    int sz;

    char *buffer = new char[bufsize];
    char *buf = buffer;
    sz = snprintf (buf, bufsize, "unsubscribing to subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin (); it != subjects.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf (buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin (); it != keys.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }
    sz = snprintf (buf, bufsize, "]");

    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  Subscriber *s = GetSubscriberFromCatalog (subscriber, false);
  if (!s) return false;
  XrdSysMutexHelper lock (s->WatchMutex);

  // firstly update the thread-local vector
  bool removedAll = false;
  {
    for (auto it = s->WatchSubjectsXKeys[type].begin (); it != s->WatchSubjectsXKeys[type].end (); it++)
    {
      if (it->first == subjects && std::includes (it->second.begin (), it->second.end (), keys.begin (), keys.end ()))
      {
	set<string> newKeys;
	set_difference (it->second.begin (), it->second.end (), keys.begin (), keys.end (),
			inserter (newKeys, newKeys.end ()));
	it->second = newKeys;
	//it->second.erase(keys.begin(),keys.end());
	removedAll = true;
	if (it->second.empty ()) s->WatchSubjectsXKeys[type].erase (it);
	break;
      }
      else if (it->second == keys
	  && std::includes (it->first.begin (), it->first.end (), subjects.begin (), subjects.end ()))
      {
	set<string> newSubjects;
	set_difference (it->first.begin (), it->first.end (), subjects.begin (), subjects.end (),
			inserter (newSubjects, newSubjects.end ()));
	it->first = newSubjects;
	//it->first.erase(subjects.begin(),subjects.end());
	removedAll = true;
	if (it->first.empty ()) s->WatchSubjectsXKeys[type].erase (it);
	break;
      }
    }
    if (!removedAll) return false;
  }
  // SYNCHRONIZE AITH GLOBAL MAP

  if (s->Notify)
  {
    return StopNotifySubjectsAndKeys (s, subjects, keys, type);
  }
  return true;
}

bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey (const std::string &subscriber, const std::string &subject,
							      const std::string &key,
							      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s, k;
  s.insert (subject);
  k.insert (key);
  return UnsubscribesToSubjectAndKey (subscriber, s, k, type);
}
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey (const std::string &subscriber, const std::string &subject,
							      const std::set<std::string> &keys,
							      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s;
  s.insert (subject);
  return UnsubscribesToSubjectAndKey (subscriber, s, keys, type);
}
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey (const std::string &subscriber,
							      const std::set<std::string> &subjects,
							      const std::string &key,
							      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> k;
  k.insert (key);
  return UnsubscribesToSubjectAndKey (subscriber, subjects, k, type);
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyKey (Subscriber *subscriber, const std::string &key,
						 XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  return (WatchKeys2Subscribers[type][key].mSubscribers.insert (subscriber)).second;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyKeyRegex (Subscriber *subscriber, const std::string &key,
						      XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  bool res = (WatchKeys2Subscribers[type][key].mSubscribers.insert (subscriber)).second;
  if (WatchKeys2Subscribers[type][key].mRegex == NULL)
  {
    regex_t *r = new regex_t;
    if (regcomp (r, key.c_str (), REG_NOSUB))
    {
      WatchKeys2Subscribers[type].erase (key);
      delete r;
      return false;
    }
    WatchKeys2Subscribers[type][key].mRegex = r;
  }
  return res;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyKey (Subscriber *subscriber, const std::string &key,
						XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  _NotifierMapUpdate(WatchKeys2Subscribers[type], key, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyKeyRegex (Subscriber *subscriber, const std::string &key,
						     XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  _NotifierMapUpdate(WatchKeys2Subscribers[type], key, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubject (Subscriber *subscriber, const std::string &subject,
						     XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  return (WatchSubjects2Subscribers[type][subject].mSubscribers.insert (subscriber)).second;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubjectRegex (Subscriber *subscriber, const std::string &subject,
							  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  bool res = (WatchSubjects2Subscribers[type][subject].mSubscribers.insert (subscriber)).second;
  if (WatchSubjects2Subscribers[type][subject].mRegex)
  {
    regex_t *r = new regex_t;
    if (regcomp (r, subject.c_str (), REG_NOSUB))
    {
      WatchSubjects2Subscribers[type].erase (subject);
      delete r;
      return false;
    }
    WatchSubjects2Subscribers[type][subject].mRegex = r;
  }
  return res;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubject (Subscriber *subscriber, const std::string &subject,
						    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  _NotifierMapUpdate(WatchSubjects2Subscribers[type], subject, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubjectRegex (Subscriber *subscriber, const std::string &subject,
							 XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock (WatchMutex);
  _NotifierMapUpdate(WatchSubjects2Subscribers[type], subject, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubjectsAndKeys (Subscriber *subscriber,
							     const std::set<std::string> &subjects,
							     const std::set<std::string> &keys,
							     XrdMqSharedObjectChangeNotifier::notification_t type)
{

  if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    size_t bufsize = 0;
    for (auto it = subjects.begin (); it != subjects.end (); ++it)
      bufsize += (it->size () + 1);
    for (auto it = keys.begin (); it != keys.end (); ++it)
      bufsize += (it->size () + 1);
    bufsize += 64;
    int sz;

    char *buffer = new char[bufsize];
    char *buf = buffer;
    sz = snprintf (buf, bufsize, "starting notification for subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin (); it != subjects.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf (buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin (); it != keys.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }
    sz = snprintf (buf, bufsize, "]");

    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  bool insertIntoExisiting = false;
  XrdSysMutexHelper lock (WatchMutex);
  for (auto it = WatchSubjectsXKeys2Subscribers[type].begin (); it != WatchSubjectsXKeys2Subscribers[type].end (); it++)
  {

//    {
//    size_t bufsize=0;
//      for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2)
//        bufsize+=(it2->size()+1);
//      for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2)
//        bufsize+=(it2->size()+1);
//      bufsize += 64;
//      int sz;
//
//      char *buffer = new char[bufsize];
//      char *buf=buffer;
//      sz = snprintf(buf,bufsize,"WatchSubjectsXKeys2Subscribers item : subjects [ ");
//      buf += sz;
//      bufsize -=sz;
//
//      for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2) {
//        sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//        buf += sz;
//        bufsize -=sz;
//      }
//
//      sz = snprintf(buf,bufsize,"] times keys [ ");
//      buf += sz;
//      bufsize -=sz;
//
//      for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2) {
//        sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//        buf += sz;
//        bufsize -=sz;
//      }
//      sz = snprintf(buf,bufsize,"]");
//
//      eos_static_info("%s", buffer);
//      delete[] buffer;
//  }

    if (subjects == it->first.first)
    {
      if (it->second.size () == 1 && it->second.count (subscriber))
      { // only one subscriber and it's the same, factor
	size_t sizeBefore = it->first.second.size ();
	it->first.second.insert (keys.begin (), keys.end ());
	if (sizeBefore == it->first.second.size ())
	  return false; // nothing to insert
	else
	{
	  insertIntoExisiting = true;
	  break;
	}
      }
      else if (keys == it->first.second && it->second.count (subscriber) == 0)
      {
	it->second.insert (subscriber); // same SubjectXKey without this subscriber -> we insert it
	break;
      }
    }
    else if (keys == it->first.second)
    {
      if (it->second.size () == 1 && it->second.count (subscriber))
      { // only one subscriber and it's the same, factor
	size_t sizeBefore = it->first.first.size ();
	it->first.first.insert (subjects.begin (), subjects.end ());
	if (sizeBefore == it->first.first.size ())
	  return false; // nothing to insert
	else
	{
	  insertIntoExisiting = true;
	  break;
	}
      }
      else if (subjects == it->first.first && it->second.count (subscriber) == 0)
      {
	it->second.insert (subscriber); // same SubjectXKey without this subscriber -> we insert it
	break;
      }
    }
  }
  if (!insertIntoExisiting)
  {
    std::set<Subscriber*> s;
    s.insert (subscriber);
    WatchSubjectsXKeys2Subscribers[type].push_back (make_pair (make_pair (subjects, keys), s));
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubjectsAndKeys (Subscriber *subscriber,
							    const std::set<std::string> &subjects,
							    const std::set<std::string> &keys,
							    XrdMqSharedObjectChangeNotifier::notification_t type)
{

  if (eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    size_t bufsize = 0;
    for (auto it = subjects.begin (); it != subjects.end (); ++it)
      bufsize += (it->size () + 1);
    for (auto it = keys.begin (); it != keys.end (); ++it)
      bufsize += (it->size () + 1);
    bufsize += 64;
    int sz;

    char *buffer = new char[bufsize];
    char *buf = buffer;
    sz = snprintf (buf, bufsize, "stopping notifications for subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin (); it != subjects.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf (buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin (); it != keys.end (); ++it)
    {
      sz = snprintf (buf, bufsize, "%s ", it->c_str ());
      buf += sz;
      bufsize -= sz;
    }
    sz = snprintf (buf, bufsize, "]");

    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  bool removedAll = false;
  // secondly update the global vector
  XrdSysMutexHelper lock (WatchMutex);
  for (auto it = WatchSubjectsXKeys2Subscribers[type].begin (); it != WatchSubjectsXKeys2Subscribers[type].end (); it++)
  {
//    {
//      size_t bufsize=0;
//        for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2)
//          bufsize+=(it2->size()+1);
//        for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2)
//          bufsize+=(it2->size()+1);
//        bufsize += 64;
//        int sz;
//
//        char *buffer = new char[bufsize];
//        char *buf=buffer;
//        sz = snprintf(buf,bufsize,"WatchSubjectsXKeys2Subscribers item : subjects [ ");
//        buf += sz;
//        bufsize -=sz;
//
//        for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2) {
//          sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//          buf += sz;
//          bufsize -=sz;
//        }
//
//        sz = snprintf(buf,bufsize,"] times keys [ ");
//        buf += sz;
//        bufsize -=sz;
//
//        for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2) {
//          sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//          buf += sz;
//          bufsize -=sz;
//        }
//        sz = snprintf(buf,bufsize,"]");
//
//        eos_static_info("%s", buffer);
//        delete[] buffer;
//    }

    if (it->first.first == subjects
	&& std::includes (it->first.second.begin (), it->first.second.end (), keys.begin (), keys.end ()))
    {
      if (it->second.count (subscriber))
      {    // if the subscriber is there
	if (it->second.size () > 1)
	{             // there's some other subscriber, split before update
	  it->second.erase (subscriber);
	  WatchSubjectsXKeys2Subscribers[type].push_back (
	      std::make_pair (it->first, std::set<Subscriber*> (&subscriber, &subscriber + 1)));
	  it = WatchSubjectsXKeys2Subscribers[type].end () - 1;
	}
	if (it->second.size () == 1)
	{            // if the subscriber is the only guy there
	  for (auto itk = keys.begin (); itk != keys.end (); itk++)
	    it->first.second.erase (*itk);
	  if (it->first.second.empty ()) // if this entry is now empty, remove it
	    WatchSubjectsXKeys2Subscribers[type].erase (it);
	}
	removedAll = true;
	break;
      }
    }
    else if (it->first.second == keys
	&& std::includes (it->first.first.begin (), it->first.first.end (), subjects.begin (), subjects.end ()))
    {
      if (it->second.count (subscriber))
      {    // if the subscriber is there
	//eos_static_debug("1 element size is %d vector size is %d",it->second.size(),WatchSubjectsXKeys2Subscribers[type].size());
	if (it->second.size () > 1)
	{             // there's some other subscriber, split before update
	  it->second.erase (subscriber);
	  WatchSubjectsXKeys2Subscribers[type].push_back (
	      std::make_pair (it->first, std::set<Subscriber*> (&subscriber, &subscriber + 1)));
	  it = WatchSubjectsXKeys2Subscribers[type].end () - 1;
	}
	//eos_static_debug("2 element size is %d vector size is %d",it->second.size(),WatchSubjectsXKeys2Subscribers[type].size());
	if (it->second.size () == 1)
	{            // if the subscriber is the only guy there
	  for (auto its = subjects.begin (); its != subjects.end (); its++)
	    it->first.first.erase (*its);
	  //eos_static_debug("3 element size is %d vector size is %d",it->second.size(),WatchSubjectsXKeys2Subscribers[type].size());
	  if (it->first.first.empty ()) // if this entry is now empty, remove it
	    WatchSubjectsXKeys2Subscribers[type].erase (it);
	}
	removedAll = true;
	break;
      }
    }
  }
  if (!removedAll) return false;

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyCurrentThread ()
{
  // to start notifying, we just copy the references to the watched words
  // from the thread local to the global map
  if (!tlSubscriber)
  {
    eos_static_err("the current thread is not bound to any subscriber");
    return false;
  }
  if (tlSubscriber->Notify) return false;
  eos_static_info("Starting notification");
  {
    XrdSysMutexHelper lock1 (tlSubscriber->WatchMutex);
    {
      XrdSysMutexHelper lock2 (WatchMutex);
      for (int type = 0; type < 5; type++)
      {
	for (auto it = tlSubscriber->WatchKeys[type].begin (); it != tlSubscriber->WatchKeys[type].end (); it++)
	  WatchKeys2Subscribers[type][*it].mSubscribers.insert (tlSubscriber);
	for (auto it = tlSubscriber->WatchSubjects[type].begin (); it != tlSubscriber->WatchSubjects[type].end (); it++)
	  WatchSubjects2Subscribers[type][*it].mSubscribers.insert (tlSubscriber);
	for (auto it = tlSubscriber->WatchKeysRegex[type].begin (); it != tlSubscriber->WatchKeysRegex[type].end ();
	    it++)
	{
	  WatchKeys2Subscribers[type][*it].mSubscribers.insert (tlSubscriber);
	  if (!WatchKeys2Subscribers[type][*it].mRegex)
	  {
	    regex_t *r = new regex_t;
	    if (regcomp (r, it->c_str (), REG_NOSUB))
	    {
	      WatchKeys2Subscribers[type].erase (*it);
	      delete r;
	      return false;
	    }
	    WatchKeys2Subscribers[type][*it].mRegex = r;
	  }
	}
	for (auto it = tlSubscriber->WatchSubjectsRegex[type].begin ();
	    it != tlSubscriber->WatchSubjectsRegex[type].end (); it++)
	{
	  WatchSubjects2Subscribers[type][*it].mSubscribers.insert (tlSubscriber);
	  if (!WatchSubjects2Subscribers[type][*it].mRegex)
	  {
	    regex_t *r = new regex_t;
	    if (regcomp (r, it->c_str (), REG_NOSUB))
	    {
	      WatchSubjects2Subscribers[type].erase (*it);
	      delete r;
	      return false;
	    }
	    WatchSubjects2Subscribers[type][*it].mRegex = r;
	  }
	}
      }
    }
  }
  for (int type = 0; type < 5; type++)
    for (auto it = tlSubscriber->WatchSubjectsXKeys[type].begin (); it != tlSubscriber->WatchSubjectsXKeys[type].end ();
	it++)
      StartNotifySubjectsAndKeys (tlSubscriber, it->first, it->second,
				  static_cast<XrdMqSharedObjectChangeNotifier::notification_t> (type));
  tlSubscriber->Notify = true;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyCurrentThread ()
{
  if (!tlSubscriber)
  {
    eos_static_err("the current thread is not bound to any subscriber");
    return false;
  }

  // to stop notifying, we just remove the references to the watched words
  // from the thread local to the global map
  if (!tlSubscriber->Notify) return false;
  eos_static_info("Stopping notification");
  {
    XrdSysMutexHelper lock1 (tlSubscriber->WatchMutex);
    {
      XrdSysMutexHelper lock2 (WatchMutex);
      for (int type = 0; type < 5; type++)
      {
	for (auto it = tlSubscriber->WatchKeys[type].begin (); it != tlSubscriber->WatchKeys[type].end (); it++)
	  _NotifierMapUpdate(WatchKeys2Subscribers[type], *it, tlSubscriber);

	for (auto it = tlSubscriber->WatchSubjects[type].begin (); it != tlSubscriber->WatchSubjects[type].end (); it++)
	  _NotifierMapUpdate(WatchSubjects2Subscribers[type], *it, tlSubscriber);

	for (auto it = tlSubscriber->WatchKeysRegex[type].begin (); it != tlSubscriber->WatchKeysRegex[type].end ();
	    it++)
	  _NotifierMapUpdate(WatchKeys2Subscribers[type], *it, tlSubscriber);

	for (auto it = tlSubscriber->WatchSubjectsRegex[type].begin ();
	    it != tlSubscriber->WatchSubjectsRegex[type].end (); it++)
	  _NotifierMapUpdate(WatchSubjects2Subscribers[type], *it, tlSubscriber);

	std::vector<typeof(WatchSubjectsXKeys2Subscribers[type].begin())> toRemove;
	for (auto it = WatchSubjectsXKeys2Subscribers[type].begin (); it != WatchSubjectsXKeys2Subscribers[type].end ();
	    it++)
	{
	  auto entry = it->second.find (tlSubscriber);
	  if (entry != it->second.end ())
	  { // if the current threads is a subscriber of this entry
	    it->second.erase (tlSubscriber);
	    if (it->second.empty ())
	    { // if the set of subscribers is empty, remove this entry
	      toRemove.push_back (it);
	    }
	  }
	}
      }
    }
  }
  for (int type = 0; type < 5; type++)
    for (auto it = tlSubscriber->WatchSubjectsXKeys[type].begin (); it != tlSubscriber->WatchSubjectsXKeys[type].end ();
	it++)
      StopNotifySubjectsAndKeys (tlSubscriber, it->first, it->second,
				 static_cast<XrdMqSharedObjectChangeNotifier::notification_t> (type));

  tlSubscriber->Notify = false;
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectChangeNotifier::SomListener ()
{
  // thread listening on filesystem errors and configuration changes
  eos_static_info("Starting Listener");
  do
  {
    SOM->SubjectsSem.Wait ();

    XrdSysThread::SetCancelOff ();

    // we always take a lock to take something from the queue and then release it
    SOM->SubjectsMutex.Lock ();

    // listens
    WatchMutex.Lock ();
    std::set<Subscriber*> notifiedSubscribers;
    while (SOM->NotificationSubjects.size ())
    {
      //eos_static_debug("LOOP");
      XrdMqSharedObjectManager::Notification event;
      event = SOM->NotificationSubjects.front ();
      SOM->NotificationSubjects.pop_front ();
      SOM->SubjectsMutex.UnLock ();

      std::string newsubject = event.mSubject.c_str ();
      //eos_static_debug("SOM Listener new notification %d on %s",event.mType,event.mSubject.c_str());

      //const XrdMqSharedObjectManager::notification_t &type = event.mType;
      int type = static_cast<int> (event.mType);

      std::set<Subscriber*> notifiedSubscribersForCurrentEvent;
      std::string key = newsubject;
      std::string queue = newsubject;
      size_t dpos = 0;
      if ((dpos = queue.find (";")) != std::string::npos)
      {
	key.erase (0, dpos + 1);
	queue.erase (dpos);
      }

      // these are useful only if type==4
      std::string newVal;
      bool newValAsserted = false;
      bool isNewVal = false;
      do
      {

	// check if there is a matching key
	for (auto it = WatchKeys2Subscribers[type].begin (); it != WatchKeys2Subscribers[type].end (); it++)
	{
	  if ((it->second.mRegex == NULL && key == it->first)
	      || (it->second.mRegex != NULL && !regexec (it->second.mRegex, key.c_str (), 0, NULL, 0)))
	  {

	    if (type == 4)
	    {
	      if (!newValAsserted)
	      {
		auto lvIt = LastValues.find (newsubject);
		SOM->HashMutex.LockRead ();
		XrdMqSharedHash* hash = SOM->GetObject (queue.c_str (), "hash");
		SOM->HashMutex.UnLockRead ();
		newVal = hash->Get (key);
		if (lvIt == LastValues.end () || lvIt->second != newVal) isNewVal = true;
		newValAsserted = true;
	      }
	      if (isNewVal)
	      {
		//eos_static_debug("notification on %s : new value %s IS a strict change",newsubject.c_str(),newVal.c_str());
		LastValues[newsubject] = newVal;
	      }
	      else
	      {
		//eos_static_debug("notification on %s : new value %s IS NOT a strict change",newsubject.c_str(),newVal.c_str());
		continue;
	      }
	    }

	    for (auto it2 = it->second.mSubscribers.begin (); it2 != it->second.mSubscribers.end (); it2++)
	    {
	      //eos_static_debug("match!");
	      (*it2)->SubjectsMutex.Lock ();
	      (*it2)->NotificationSubjects.push_back (event);
	      (*it2)->SubjectsMutex.UnLock ();
	      notifiedSubscribersForCurrentEvent.insert (*it2);
	      notifiedSubscribers.insert (*it2);
	    }
	  }
	}

	// check if there is a matching subject
	for (auto it = WatchSubjects2Subscribers[type].begin (); it != WatchSubjects2Subscribers[type].end (); it++)
	{

	  if ((it->second.mRegex == NULL && queue == it->first)
	      || (it->second.mRegex != NULL && !regexec (it->second.mRegex, queue.c_str (), 0, NULL, 0)))
	  {

	    if (type == 4)
	    {
	      if (!newValAsserted)
	      {
		auto lvIt = LastValues.find (newsubject);
		SOM->HashMutex.LockRead ();
		XrdMqSharedHash* hash = SOM->GetObject (queue.c_str (), "hash");
		SOM->HashMutex.UnLockRead ();
		newVal = hash->Get (key);
		if (lvIt == LastValues.end () || lvIt->second != newVal) isNewVal = true;
		newValAsserted = true;
	      }
	      if (isNewVal)
	      {
		//eos_static_debug("notification on %s : new value %s IS a strict change",newsubject.c_str(),newVal.c_str());
		LastValues[newsubject] = newVal;
	      }
	      else
	      {
		//eos_static_debug("notification on %s : new value %s IS NOT a strict change",newsubject.c_str(),newVal.c_str());
		continue;
	      }
	    }

	    for (auto it2 = it->second.mSubscribers.begin (); it2 != it->second.mSubscribers.end (); it2++)
	    {
	      if (notifiedSubscribersForCurrentEvent.count (*it2) == 0)
	      { // don't notofy twice for the same event
		(*it2)->SubjectsMutex.Lock ();
		(*it2)->NotificationSubjects.push_back (event);
		(*it2)->SubjectsMutex.UnLock ();
		notifiedSubscribersForCurrentEvent.insert (*it2);
		notifiedSubscribers.insert (*it2);
	      }
	    }
	  }
	  else
	  {
	    //if(it->second.mRegex!=NULL)
	    //  eos_static_info("regex %s DID NOT MATCH %s",it->first.c_str(),queue.c_str());
	  }
	}

	// check if there is a matching subjectXkey
	for (auto it = WatchSubjectsXKeys2Subscribers[type].begin (); it != WatchSubjectsXKeys2Subscribers[type].end ();
	    it++)
	{
	  if (it->first.first.count (queue) && it->first.second.count (key))
	  {
	    if (type == 4)
	    {
	      if (!newValAsserted)
	      {
		auto lvIt = LastValues.find (newsubject);
		SOM->HashMutex.LockRead ();
		XrdMqSharedHash* hash = SOM->GetObject (queue.c_str (), "hash");
		SOM->HashMutex.UnLockRead ();
		newVal = hash->Get (key);
		if (lvIt == LastValues.end () || lvIt->second != newVal) isNewVal = true;
		newValAsserted = true;
	      }
	      if (isNewVal)
	      {
//              if(key=="id") {
//                eos_static_warning("WARNING ID CHANGE in queue %s FROM %s to %s",queue.c_str(),LastValues[newsubject].c_str(),newVal.c_str());
//              }
		LastValues[newsubject] = newVal;
	      }
	      else
	      {
		//eos_static_info("notification on %s : new value %s IS NOT a strict change",newsubject.c_str(),newVal.c_str());
		continue;
	      }
	    }

	    for (auto it2 = it->second.begin (); it2 != it->second.end (); it2++)
	    {
	      if (notifiedSubscribersForCurrentEvent.count (*it2) == 0)
	      { // don't notofy twice for the same event
		(*it2)->SubjectsMutex.Lock ();
		(*it2)->NotificationSubjects.push_back (event);
		(*it2)->SubjectsMutex.UnLock ();
		notifiedSubscribersForCurrentEvent.insert (*it2);
		notifiedSubscribers.insert (*it2);
	      }
	    }
	  }
	}

	if (type == 2) // if it's a modification, check also the strict modifications to be notified
	  type = 4;
	else
	  break;

      }
      while (true);

      SOM->SubjectsMutex.Lock ();
    }

    // wake up all subscriber threads
    for (auto it = notifiedSubscribers.begin (); it != notifiedSubscribers.end (); it++)
      (*it)->SubjectsSem.Post ();
    WatchMutex.UnLock ();

    SOM->SubjectsMutex.UnLock ();
    XrdSysThread::SetCancelOff ();
  }
  while (true);
}

//------------------------------------------------------------------------------
// Start the listener thread
//------------------------------------------------------------------------------
void*
XrdMqSharedObjectChangeNotifier::StartSomListener (void *pp)
{
  ((XrdMqSharedObjectChangeNotifier*) pp)->SomListener ();
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::Start ()
{
  int rc;

  if ((rc = XrdSysThread::Run (&tid, XrdMqSharedObjectChangeNotifier::StartSomListener, static_cast<void *> (this),
  XRDSYSTHREAD_HOLD,
			       "XrdMqSharedObject Change Notifier")))
  {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::Stop ()
{
  XrdSysThread::Cancel (tid);
  XrdSysThread::Join (tid, 0);
  return true;
}

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::XrdMqSharedObjectManager ()
{
  broadcast = true;
  EnableQueue = false;
  DumperFile = "";
  AutoReplyQueue = "";
  AutoReplyQueueDerive = false;
  IsMuxTransaction = false;
  {
    XrdSysMutexHelper mLock(MuxTransactionsMutex);
    MuxTransactions.clear();
  }
  dumper_tid = 0;
}

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::~XrdMqSharedObjectManager ()
{
  if (dumper_tid)
  {
    XrdSysThread::Cancel(dumper_tid);
    XrdSysThread::Join(dumper_tid, 0);
  }

  std::map<std::string, XrdMqSharedHash*>::iterator hashit; // hashsubjects;

  for (hashit = hashsubjects.begin(); hashit != hashsubjects.end(); hashit++)
  {
    delete hashit->second;
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::SetAutoReplyQueue (const char* queue)
{
  AutoReplyQueue = queue;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::CreateSharedHash (const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectCreation);

  HashMutex.LockWrite();

  if (hashsubjects.count(ss) > 0)
  {
    hashsubjects[ss]->SetBroadCastQueue(broadcastqueue);
    HashMutex.UnLockWrite();
    return false;
  }
  else
  {
    XrdMqSharedHash* newhash = new XrdMqSharedHash(subject, broadcastqueue, som);

    hashsubjects.insert(std::pair<std::string, XrdMqSharedHash*> (ss, newhash));

    HashMutex.UnLockWrite();

    if (EnableQueue)
    {
      SubjectsMutex.Lock();
      NotificationSubjects.push_back(event);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::CreateSharedQueue (const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectCreation);

  ListMutex.LockWrite();

  if (queuesubjects.count(ss) > 0)
  {
    ListMutex.UnLockWrite();
    return false;
  }
  else
  {
    XrdMqSharedQueue newlist(subject, broadcastqueue, som);

    queuesubjects.insert(std::pair<std::string, XrdMqSharedQueue > (ss, newlist));

    ListMutex.UnLockWrite();

    if (EnableQueue)
    {
      SubjectsMutex.Lock();
      NotificationSubjects.push_back(event);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::DeleteSharedHash (const char* subject, bool broadcast)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectDeletion);

  HashMutex.LockWrite();

  if ((hashsubjects.count(ss) > 0))
  {
    if (XrdMqSharedObjectManager::broadcast && broadcast)
    {
      XrdOucString txmessage = "";
      hashsubjects[ss]->MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message,0, false, false, true);
    }
    delete (hashsubjects[ss]);
    hashsubjects.erase(ss);
    HashMutex.UnLockWrite();

    if (EnableQueue)
    {
      SubjectsMutex.Lock();
      NotificationSubjects.push_back(event);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
  else
  {
    HashMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::DeleteSharedQueue (const char* subject, bool broadcast)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectDeletion);
  ListMutex.LockWrite();

  if ((queuesubjects.count(ss) > 0))
  {
    if (XrdMqSharedObjectManager::broadcast && broadcast)
    {
      XrdOucString txmessage = "";
      hashsubjects[ss]->MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message, 0, false, false, true);
    }

    queuesubjects.erase(ss);
    ListMutex.UnLockWrite();

    if (EnableQueue)
    {
      SubjectsMutex.Lock();
      NotificationSubjects.push_back(event);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
  else
  {
    ListMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::DumpSharedObjects (XrdOucString& out)
{
  out = "";

  XrdMqRWMutexReadLock lock(HashMutex);
  std::map<std::string, XrdMqSharedHash*>::iterator it_hash;
  std::map<std::string, XrdMqSharedQueue>::iterator it_queue;
  for (it_hash = hashsubjects.begin(); it_hash != hashsubjects.end(); it_hash++)
  {
    out += "===================================================\n";
    out += it_hash->first.c_str();
    out += " [ hash=>  ";
    out += it_hash->second->GetBroadCastQueue();
    out += " ]\n";
    out += "---------------------------------------------------\n";
    it_hash->second->Dump(out);
  }

  for (it_queue = queuesubjects.begin(); it_queue != queuesubjects.end(); it_queue++)
  {
    out += "===================================================\n";
    out += it_queue->first.c_str();
    out += " [ queue=> ";
    out += it_queue->second.GetBroadCastQueue();
    out += " ]\n";
    out += "---------------------------------------------------\n";
    it_queue->second.Dump(out);
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::DumpSharedObjectList (XrdOucString& out)
{
  out = "";
  char formatline[1024];

  XrdMqRWMutexReadLock lock(HashMutex);

  std::map<std::string, XrdMqSharedHash*>::iterator it_hash;
  for (it_hash = hashsubjects.begin(); it_hash != hashsubjects.end(); it_hash++)
  {
    snprintf(formatline, sizeof (formatline) - 1, "subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n", it_hash->first.c_str(), it_hash->second->GetBroadCastQueue(), (unsigned int) it_hash->second->GetSize(), it_hash->second->GetChangeId());
    out += formatline;
  }

  //  ListMutex.LockRead();
  //  std::map<std::string , XrdMqSharedQueue>::iterator it_list;
  //  for (it_list=queuesubjects.begin(); it_list!= queuesubjects.end(); it_list++) {
  //    snprintf(formatline,sizeof(formatline)-1,"subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n",it_hash.first.c_str(), it_hash.second.GetBroadCastQueue(),(unsigned int) it_hash.second.GetSize(), it_hash.second.GetChangeId());
  //out += formatline;
  //  }
  //  ListMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::StartDumper (const char* file)
{
  pthread_t dumper_tid;
  int rc = 0;
  DumperFile = file;
  if ((rc = XrdSysThread::Run(&dumper_tid, XrdMqSharedObjectManager::StartHashDumper, static_cast<void *> (this),
                              XRDSYSTHREAD_HOLD, "HashDumper")))
  {
    fprintf(stderr, "XrdMqSharedObjectManager::StartDumper=> failed to run dumper thread\n");
  }
}

/*----------------------------------------------------------------------------*/
void*
XrdMqSharedObjectManager::StartHashDumper (void* pp)
{
  XrdMqSharedObjectManager* man = (XrdMqSharedObjectManager*) pp;
  man->FileDumper();
  // should never return
  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::FileDumper ()
{
  while (1)
  {
    XrdSysThread::SetCancelOff();
    XrdOucString s;
    DumpSharedObjects(s);
    std::string df = DumperFile;
    df += ".tmp";
    FILE* f = fopen(df.c_str(), "w+");
    if (f)
    {
      fprintf(f, "%s\n", s.c_str());
      fclose(f);
    }
    if (chmod(DumperFile.c_str(), S_IRWXU | S_IRGRP | S_IROTH))
    {
      fprintf(stderr, "XrdMqSharedObjectManager::FileDumper=> unable to set 755 permissions on file %s\n", DumperFile.c_str());
    }

    if (rename(df.c_str(), DumperFile.c_str()))
    {
      fprintf(stderr, "XrdMqSharedObjectManager::FileDumper=> unable to write dumper file %s\n", DumperFile.c_str());
    }
    XrdSysThread::SetCancelOn();
    for (size_t i = 0; i < 60; i++)
    {
      XrdSysTimer sleeper;
      sleeper.Wait(1000);
      XrdSysThread::CancelPoint();
    }
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::PostModificationTempSubjects ()
{
  std::deque<std::string>::iterator it;
  if (debug)fprintf(stderr, "XrdMqSharedObjectManager::PostModificationTempSubjects=> posting now\n");
  SubjectsMutex.Lock();
  for (it = ModificationTempSubjects.begin(); it != ModificationTempSubjects.end(); it++)
  {
    if (debug)fprintf(stderr, "XrdMqSharedObjectManager::PostModificationTempSubjects=> %s\n", it->c_str());
    Notification event(*it, XrdMqSharedObjectManager::kMqSubjectModification);
    NotificationSubjects.push_back(event);
    SubjectsSem.Post();
  }
  ModificationTempSubjects.clear();
  SubjectsMutex.UnLock();
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::ParseEnvMessage (XrdMqMessage* message, XrdOucString &error)
{
  error = "";
  std::string subject = "";
  std::string reply = "";
  std::string type = "";

  if (!message)
  {
    error = "no message provided";
    return false;
  }

  XrdOucEnv env(message->GetBody());

  int envlen;
  env.Env(envlen);
  if (debug)
  {
    env.Env(envlen);
    fprintf(stderr, "XrdMqSharedObjectManager::ParseEnvMessage=> size=%d text=%s\n", envlen, env.Env(envlen));
  }

  if (env.Get(XRDMQSHAREDHASH_SUBJECT))
  {
    subject = env.Get(XRDMQSHAREDHASH_SUBJECT);
  }
  else
  {
    error = "no subject in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_REPLY))
  {
    reply = env.Get(XRDMQSHAREDHASH_REPLY);
  }
  else
  {
    reply = "";
  }

  if (env.Get(XRDMQSHAREDHASH_TYPE))
  {
    type = env.Get(XRDMQSHAREDHASH_TYPE);
  }
  else
  {
    error = "no hash type in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_CMD))
  {
    HashMutex.LockRead();
    XrdMqSharedHash* sh = 0;

    std::vector<std::string> subjectlist;

    // support 'wild card' broadcasts with <name>/*
    int wpos = 0;
    if ((wpos = subject.find("/*")) != STR_NPOS)
    {
      XrdOucString wmatch = subject.c_str();
      wmatch.erase(wpos);
      {
        std::map<std::string, XrdMqSharedHash*>::iterator it;
        for (it = hashsubjects.begin(); it != hashsubjects.end(); it++)
        {
          XrdOucString hs = it->first.c_str();
          if (hs.beginswith(wmatch))
          {
            subjectlist.push_back(hs.c_str());
          }
        }
      }
      {
        std::map<std::string, XrdMqSharedQueue>::iterator it;
        for (it = queuesubjects.begin(); it != queuesubjects.end(); it++)
        {
          XrdOucString hs = it->first.c_str();
          if (hs.beginswith(wmatch))
          {
            subjectlist.push_back(hs.c_str());
          }
        }
      }
    }
    else
    {
      // support 'wild card' broadcasts with */<name>
      if ((subject.find("*/")) == 0)
      {
        XrdOucString wmatch = subject.c_str();
        wmatch.erase(0, 2);
        {
          std::map<std::string, XrdMqSharedHash*>::iterator it;
          for (it = hashsubjects.begin(); it != hashsubjects.end(); it++)
          {
            XrdOucString hs = it->first.c_str();
            if (hs.endswith(wmatch))
            {
              subjectlist.push_back(hs.c_str());
            }
          }
        }
        {
          std::map<std::string, XrdMqSharedQueue>::iterator it;
          for (it = queuesubjects.begin(); it != queuesubjects.end(); it++)
          {
            XrdOucString hs = it->first.c_str();
            if (hs.endswith(wmatch))
            {
              subjectlist.push_back(hs.c_str());
            }
          }
        }
      }
      else
      {
        std::string delimiter = "%";
        // we support also multiplexed subject updates and split the list
        XrdMqStringConversion::Tokenize(subject, subjectlist, delimiter);
      }
    }

    XrdOucString ftag = XRDMQSHAREDHASH_CMD;
    ftag += "=";
    ftag += env.Get(XRDMQSHAREDHASH_CMD);

    if (subjectlist.size() > 0)
      sh = GetObject(subjectlist[0].c_str(), type.c_str());

    if ((ftag == XRDMQSHAREDHASH_BCREQUEST) || (ftag == XRDMQSHAREDHASH_DELETE) || (ftag == XRDMQSHAREDHASH_REMOVE))
    {
      // if we don't know the subject, we don't create it with a BCREQUEST
      if ((ftag == XRDMQSHAREDHASH_BCREQUEST) && (reply == ""))
      {
        HashMutex.UnLockRead();
        error = "bcrequest: no reply address present";
        return false;
      }

      if (!sh)
      {
        if (ftag == XRDMQSHAREDHASH_BCREQUEST)
        {
          error = "bcrequest: don't know this subject";
        }
        if (ftag == XRDMQSHAREDHASH_DELETE)
        {
          error = "delete: don't know this subject";
        }
        if (ftag == XRDMQSHAREDHASH_REMOVE)
        {
          error = "remove: don't know this subject";
        }
        HashMutex.UnLockRead();
        return false;
      }
      else
      {
        HashMutex.UnLockRead();
      }
    }
    else
    {
      // automatically create the subject, if it does not exist
      if (!sh)
      {
        HashMutex.UnLockRead();
        if (AutoReplyQueueDerive)
        {
          AutoReplyQueue = subject.c_str();
          int pos = 0;
          for (int i = 0; i < 4; i++)
          {
            pos = subject.find("/", pos);
            if (i < 3)
            {
              if (pos == STR_NPOS)
              {
                AutoReplyQueue = "";
                error = "cannot derive the reply queue from ";
                error += subject.c_str();
                return false;
              }
              else
              {
                pos++;
              }
            }
            else
            {
              AutoReplyQueue.erase(pos);
            }
          }
        }

        // create the list of subjects
        for (size_t i = 0; i < subjectlist.size(); i++)
        {
          if (!CreateSharedObject(subjectlist[i].c_str(), AutoReplyQueue.c_str(), type.c_str()))
          {
            error = "cannot create shared object for ";
            error += subject.c_str();
            error += " and type ";
            error += type.c_str();
            return false;
          }
        }

        {
          XrdMqRWMutexReadLock lock(HashMutex);
          sh = GetObject(subject.c_str(), type.c_str());
        }
      }
      else
      {
        HashMutex.UnLockRead();
      }
    }

    {
      XrdMqRWMutexReadLock lock(HashMutex);
      // from here on we have a read lock on 'sh'

      if ((ftag == XRDMQSHAREDHASH_UPDATE) || (ftag == XRDMQSHAREDHASH_BCREPLY))
      {
        std::string val = (env.Get(XRDMQSHAREDHASH_PAIRS) ? env.Get(XRDMQSHAREDHASH_PAIRS) : "");
        if (val.length() <= 0)
        {
          error = "no pairs in message body";
          return false;
        }

        if (ftag == XRDMQSHAREDHASH_BCREPLY)
        {
          // we don't have to broad cast this clear => it is a broad cast reply
          sh->Clear(false);
        }

        std::string key;
        std::string value;
        std::string cid;
        std::vector<int> keystart;
        std::vector<int> valuestart;
        std::vector<int> cidstart;

        for (unsigned int i = 0; i < val.length(); i++)
        {
          if (val.c_str()[i] == '|')
          {
            keystart.push_back(i);
          }
          if (val.c_str()[i] == '~')
          {
            valuestart.push_back(i);
          }
          if (val.c_str()[i] == '%')
          {
            cidstart.push_back(i);
          }
        }

        if (keystart.size() != valuestart.size())
        {
          error = "update: parsing error in pairs tag";
          return false;
        }

        if (keystart.size() != cidstart.size())
        {
          error = "update: parsing error in pairs tag";
          return false;
        }


        int parseindex = 0;
        for (size_t s = 0; s < subjectlist.size(); s++)
        {
          sh = GetObject(subjectlist[s].c_str(), type.c_str());
          if (!sh)
          {
            error = "update: subject does not exist (FATAL!)";
            return false;
          }


          std::string sstr;

          //          XrdSysMutexHelper(TransactionMutex);

          {
            sh->StoreMutex.LockWrite();
            SubjectsMutex.Lock();
            for (unsigned int i = parseindex; i < keystart.size(); i++)
            {
              key.assign(val, keystart[i] + 1, valuestart[i] - 1 - (keystart[i]));
              value.assign(val, valuestart[i] + 1, cidstart[i] - 1 - (valuestart[i]));
              if (i == (keystart.size() - 1))
              {
                cid.assign(val, cidstart[i] + 1, val.length() - cidstart[i] - 1);
              }
              else
              {
                cid.assign(val, cidstart[i] + 1, keystart[i + 1] - 1 - (cidstart[i]));
              }
              if (debug)fprintf(stderr, "XrdMqSharedObjectManager::ParseEnvMessage=>Setting [%s] %s=> %s\n", subject.c_str(), key.c_str(), value.c_str());
              if (subjectlist.size() > 1)
              {
                // this is a multiplexed update, where we have to remove the subject from the key if there is a match with the current subject
                // MUX transactions have the #<subject-index># as key prefix
                XrdOucString skey = "#";
                skey += (int) s;
                skey += "#";
                if (!key.compare(0, skey.length(), skey.c_str()))
                {
                  // this is the right key for the subject we are dealing with
                  key.erase(0, skey.length());
                }
                else
                {
                  parseindex = i;
                  break;
                }
              }
              else
              {
                // this can be the case for a single multiplexed message, so we have also to remove the prefix in that case
                XrdOucString skey = "#";
                skey += (int) s;
                skey += "#";
                if (!key.compare(0, skey.length(), skey.c_str()))
                {
                  // this is the right key for the subject we are dealing with
                  key.erase(0, skey.length());
                }
              }

              sh->SetNoLockNoBroadCast(key.c_str(), value.c_str(), true);
              //sh->Set(key, value, false, true);            
            }
            sh->StoreMutex.UnLockWrite();
            SubjectsMutex.UnLock();
          }
          PostModificationTempSubjects();
        }
        return true;
      }

      if (ftag == XRDMQSHAREDHASH_BCREQUEST)
      {
        bool success = true;
        for (unsigned int l = 0; l < subjectlist.size(); l++)
        {
          // try 'queue' and 'hash' to have wildcard broadcasts for both
          sh = GetObject(subjectlist[l].c_str(), "queue");
          if (!sh)
          {
            sh = GetObject(subjectlist[l].c_str(), "hash");
          }

          if (sh)
          {
            success *= sh->BroadCastEnvString(reply.c_str());
          }
        }
        return success;
      }

      if (ftag == XRDMQSHAREDHASH_DELETE)
      {
        std::string val = (env.Get(XRDMQSHAREDHASH_KEYS) ? env.Get(XRDMQSHAREDHASH_KEYS) : "");
        if (val.length() <= 1)
        {
          error = "no keys in message body : ";
          error += env.Env(envlen);
          return false;
        }

        std::string key;
        std::vector<int> keystart;

        for (unsigned int i = 0; i < val.length(); i++)
        {
          if (val.c_str()[i] == '|')
          {
            keystart.push_back(i);
          }
        }

        XrdSysMutexHelper(TransactionMutex);
        std::string sstr;
        for (unsigned int i = 0; i < keystart.size(); i++)
        {
          if (i < (keystart.size() - 1))
            sstr = val.substr(keystart[i] + 1, keystart[i + 1] - 1 - (keystart[i]));
          else
            sstr = val.substr(keystart[i] + 1);

          key = sstr;
          //          message->Print();
          //      fprintf(stderr,"XrdMqSharedObjectManager::ParseEnvMessage=>Deleting [%s] %s\n", subject.c_str(),key.c_str());
          sh->Delete(key.c_str(), false);

        }
      }
    } // end of read mutex on HashMutex

    if (ftag == XRDMQSHAREDHASH_REMOVE)
    {
      for (unsigned int l = 0; l < subjectlist.size(); l++)
      {
        if (!DeleteSharedObject(subjectlist[l].c_str(), type.c_str(), false))
        {
          error = "cannot delete subject ";
          error += subjectlist[l].c_str();
          return false;
        }
      }
    }
    return true;

  }
  error = "unknown message: ";
  error += message->GetBody();
  return false;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::Clear ()
{
  XrdMqRWMutexReadLock lock(HashMutex);
  std::map<std::string, XrdMqSharedHash*>::iterator it_hash;
  for (it_hash = hashsubjects.begin(); it_hash != hashsubjects.end(); it_hash++)
  {
    it_hash->second->Clear();
  }
  std::map<std::string, XrdMqSharedQueue>::iterator it_queue;
  for (it_queue = queuesubjects.begin(); it_queue != queuesubjects.end(); it_queue++)
  {
    it_queue->second.Clear();
  }
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::CloseMuxTransaction ()
{
  // Mux Transactions can only update values with the same broadcastqueue, no deletions of subjects

  {
    XrdSysMutexHelper mLock(MuxTransactionsMutex);
    if (MuxTransactions.size())
    {
      XrdOucString txmessage = "";
      MakeMuxUpdateEnvHeader(txmessage);
      AddMuxTransactionEnvString(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message, MuxTransactionBroadCastQueue.c_str(), false, false, true);
    }

    IsMuxTransaction = false;
    MuxTransactions.clear();
  }
  MuxTransactionMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::MakeMuxUpdateEnvHeader (XrdOucString &out)
{
  std::string subjects = "";
  std::map<std::string, std::set <std::string> >::const_iterator it;
  for (it = MuxTransactions.begin(); it != MuxTransactions.end(); it++)
  {
    subjects += it->first;
    subjects += "%";
  }
  // remove trailing '%'
  if (subjects.length() > 0)
    subjects.erase(subjects.length() - 1, 1);

  out = XRDMQSHAREDHASH_UPDATE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += subjects.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += MuxTransactionType.c_str();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::AddMuxTransactionEnvString (XrdOucString &out)
{
  // encoding works as "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ...."
  out += "&";
  out += XRDMQSHAREDHASH_PAIRS;
  out += "=";
  std::map< std::string, std::set<std::string> >::const_iterator subjectit;

  size_t index = 0;
  for (subjectit = MuxTransactions.begin(); subjectit != MuxTransactions.end(); subjectit++)
  {
    XrdOucString sindex = "";
    sindex += (int) index;
    // loop over subjects
    std::set<std::string>::const_iterator it;

    XrdMqSharedHash* hash = GetObject(subjectit->first.c_str(), MuxTransactionType.c_str());

    if (hash)
    {
      XrdMqRWMutexReadLock lock(hash->StoreMutex);
      for (it = subjectit->second.begin(); it != subjectit->second.end(); it++)
      {
        // loop over variables

        if ((hash->Store.count(it->c_str())))
        {
          out += "|";
          // the subject is a prefix to the key as #<subject-index>#
          out += "#";
          out += sindex.c_str();
          out += "#";
          out += it->c_str();
          out += "~";
          out += hash->Store[it->c_str()].entry.c_str();
          out += "%";
          char cid[1024];
          snprintf(cid, sizeof (cid) - 1, "%llu", hash->Store[it->c_str()].ChangeId);
          out += cid;
        }
      }
    }
    index++;
  }
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash::XrdMqSharedHash (const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som)
{
  BroadCastQueue = broadcastqueue;
  Subject = subject;
  ChangeId = 0;
  IsTransaction = false;
  Type = "hash";
  SOM = som;
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash::~XrdMqSharedHash () { }

/*----------------------------------------------------------------------------*/
std::string
XrdMqSharedHash::StoreAsString (const char* notprefix)
{
  std::string s = "";
  StoreMutex.LockRead();
  std::map<std::string, XrdMqSharedHashEntry>::iterator it;
  for (it = Store.begin(); it != Store.end(); it++)
  {
    XrdOucString key = it->first.c_str();
    if ((!notprefix) || (notprefix && (!strlen(notprefix))) || (!key.beginswith(notprefix)))
    {
      s += it->first.c_str();
      s += "=";
      s += it->second.GetEntry();
      s += " ";
    }
  }
  StoreMutex.UnLockRead();
  return s;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectManager::OpenMuxTransaction (const char* type, const char* broadcastqueue)
{
  MuxTransactionType = type;
  if (MuxTransactionType != "hash")
  {
    return false;
  }
  if (!broadcastqueue)
  {
    if (AutoReplyQueue.length())
      MuxTransactionBroadCastQueue = AutoReplyQueue;
    else
      return false;
  }
  else
  {
    MuxTransactionBroadCastQueue = broadcastqueue;
  }
  MuxTransactionMutex.Lock();
  MuxTransactions.clear();

  IsMuxTransaction = true;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::CloseTransaction ()
{
  bool retval = true;
  if (XrdMqSharedObjectManager::broadcast && Transactions.size())
  {
    XrdOucString txmessage = "";
    MakeUpdateEnvHeader(txmessage);
    AddTransactionEnvString(txmessage, false);

    if (txmessage.length() > (2 * 1000 * 1000))
    {
      // we set the message size limit to 2M, if the message is bigger, we just send transaction item by item
      std::set<std::string>::const_iterator transit;
      for (transit = Transactions.begin(); transit != Transactions.end(); transit++)
      {
        txmessage = "";
        MakeUpdateEnvHeader(txmessage);

        // encoding works as "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ...."
        txmessage += "&";
        txmessage += XRDMQSHAREDHASH_PAIRS;
        txmessage += "=";

        XrdMqRWMutexReadLock lock(StoreMutex);
        if ((Store.count(transit->c_str())))
        {
          txmessage += "|";
          txmessage += transit->c_str();
          txmessage += "~";
          txmessage += Store[transit->c_str()].entry.c_str();
          txmessage += "%";
          char cid[1024];
          snprintf(cid, sizeof (cid) - 1, "%llu", Store[transit->c_str()].ChangeId);
          txmessage += cid;
        }
        XrdMqMessage message("XrdMqSharedHashMessage");
        message.SetBody(txmessage.c_str());
        message.MarkAsMonitor();
        retval &= XrdMqMessaging::gMessageClient.SendMessage(message, BroadCastQueue.c_str(), false, false, true);
      }
      Transactions.clear();
    }
    else
    {
      Transactions.clear();
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      retval &= XrdMqMessaging::gMessageClient.SendMessage(message, BroadCastQueue.c_str(), false, false, true);
    }
  }

  if (XrdMqSharedObjectManager::broadcast && Deletions.size())
  {
    XrdOucString txmessage = "";
    MakeDeletionEnvHeader(txmessage);
    AddDeletionEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    retval &= XrdMqMessaging::gMessageClient.SendMessage(message, BroadCastQueue.c_str(), false, false, true);
  }

  IsTransaction = false;
  TransactionMutex.UnLock();
  return retval;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeBroadCastEnvHeader (XrdOucString &out)
{
  out = XRDMQSHAREDHASH_BCREPLY;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += Subject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeUpdateEnvHeader (XrdOucString &out)
{
  out = XRDMQSHAREDHASH_UPDATE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += Subject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeDeletionEnvHeader (XrdOucString &out)
{
  out = XRDMQSHAREDHASH_DELETE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += Subject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeRemoveEnvHeader (XrdOucString &out)
{
  out = XRDMQSHAREDHASH_REMOVE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += Subject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::BroadCastEnvString (const char* receiver)
{
  TransactionMutex.Lock();
  Transactions.clear();
  IsTransaction = true;

  std::map<std::string, XrdMqSharedHashEntry>::iterator it;

  StoreMutex.LockRead();
  for (it = Store.begin(); it != Store.end(); it++)
  {
    Transactions.insert(it->first);
  }
  StoreMutex.UnLockRead();

  XrdOucString txmessage = "";
  MakeBroadCastEnvHeader(txmessage);
  AddTransactionEnvString(txmessage);
  IsTransaction = false;
  TransactionMutex.UnLock();

  if (XrdMqSharedObjectManager::broadcast)
  {
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    if (XrdMqSharedObjectManager::debug)fprintf(stderr, "XrdMqSharedObjectManager::BroadCastEnvString=>[%s]=>%s \n", Subject.c_str(), receiver);
    return XrdMqMessaging::gMessageClient.SendMessage(message, receiver, false, false, true);
  }
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::AddTransactionEnvString (XrdOucString &out, bool clearafter)
{
  // encoding works as "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ...."
  out += "&";
  out += XRDMQSHAREDHASH_PAIRS;
  out += "=";
  std::set<std::string>::const_iterator transit;

  XrdMqRWMutexReadLock lock(StoreMutex);

  for (transit = Transactions.begin(); transit != Transactions.end(); transit++)
  {
    if ((Store.count(transit->c_str())))
    {
      out += "|";
      out += transit->c_str();
      out += "~";
      out += Store[transit->c_str()].entry.c_str();
      out += "%";
      char cid[1024];
      snprintf(cid, sizeof (cid) - 1, "%llu", Store[transit->c_str()].ChangeId);
      out += cid;
    }
  }
  if (clearafter)
    Transactions.clear();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::AddDeletionEnvString (XrdOucString &out)
{
  // encoding works as "mysh.keys=|<key1>|<key2> ...."
  out += "&";
  out += XRDMQSHAREDHASH_KEYS;
  out += "=";

  std::set<std::string>::const_iterator delit;

  for (delit = Deletions.begin(); delit != Deletions.end(); delit++)
  {
    out += "|";
    out += delit->c_str();
  }
  Deletions.clear();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::Dump (XrdOucString &out)
{
  std::map<std::string, XrdMqSharedHashEntry>::iterator it;
  char keyprint[64];
  StoreMutex.LockRead();
  for (it = Store.begin(); it != Store.end(); it++)
  {
    snprintf(keyprint, sizeof (keyprint) - 1, "key=%-24s", it->first.c_str());
    out += keyprint;
    out += " ";
    it->second.Dump(out);
    out += "\n";
  }
  StoreMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/

bool
XrdMqSharedHash::BroadCastRequest (const char* requesttarget)
{
  XrdOucString out;
  XrdMqMessage message("XrdMqSharedHashMessage");
  out += XRDMQSHAREDHASH_BCREQUEST;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += Subject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_REPLY;
  out += "=";
  out += XrdMqMessaging::gMessageClient.GetClientId();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += Type.c_str();
  message.SetBody(out.c_str());
  message.MarkAsMonitor();
  return XrdMqMessaging::gMessageClient.SendMessage(message, requesttarget, false, false, true);
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::Set (const char* key, const char* value, bool broadcast, bool tempmodsubjects, bool notify)
{
  AtomicInc(SetCounter);

  if (!value)
    return false;

  std::string skey = key;

  {

    bool callback = false;

    {
      XrdMqRWMutexWriteLock lock(StoreMutex);

      if (!Store.count(skey))
      {
        callback = true;
      }

      Store[skey].Set(value, key);
      if (callback)
      {
        CallBackInsert(&Store[skey], skey.c_str());
      }
    }


    if (XrdMqSharedObjectManager::broadcast && broadcast)
    {
      if (SOM->IsMuxTransaction)
      {
        SOM->MuxTransactions[Subject].insert(skey);
      }
      else
      {
        // we emulate a transaction for a single Set
        if (!IsTransaction)
        {
          TransactionMutex.Lock();
          Transactions.clear();
        }

        Transactions.insert(skey);
      }
    }

    // check if we have to do posts for this subject
    if (SOM && notify)
    {
      SOM->SubjectsMutex.Lock();
        std::string fkey = Subject.c_str();
        fkey += ";";
        fkey += skey;
        if (XrdMqSharedObjectManager::debug)fprintf(stderr, "XrdMqSharedObjectManager::Set=>[%s:%s]=>%s notified\n", Subject.c_str(), skey.c_str(), value);
        if (tempmodsubjects)
          SOM->ModificationTempSubjects.push_back(fkey);
        else
        {
          XrdMqSharedObjectManager::Notification event(fkey, XrdMqSharedObjectManager::kMqSubjectModification);
          SOM->NotificationSubjects.push_back(event);
          SOM->SubjectsSem.Post();
      }
      SOM->SubjectsMutex.UnLock();
    }
  }

  if (XrdMqSharedObjectManager::broadcast && broadcast)
  {
    if (!SOM->IsMuxTransaction)
      if (!IsTransaction)
        CloseTransaction();
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::SetNoLockNoBroadCast (const char* key, const char* value, bool tempmodsubjects, bool notify)
{
  // do outside XrdMqRWMutexWriteLock lock(StoreMutex);
  // do outside SOM->SubjectsMutex.Lock();

  AtomicInc(SetNLCounter);

  if (!value)
    return false;

  std::string skey = key;

  {
    bool callback = false;

    if (!Store.count(skey))
    {
      callback = true;
    }

    Store[skey].Set(value, key);
    if (callback)
    {
      CallBackInsert(&Store[skey], skey.c_str());
    }

    // check if we have to do posts for this subject
    if (SOM && notify)
      {
        std::string fkey = Subject.c_str();
        fkey += ";";
        fkey += skey;
        if (XrdMqSharedObjectManager::debug)fprintf(stderr, "XrdMqSharedObjectManager::Set=>[%s:%s]=>%s notified\n", Subject.c_str(), skey.c_str(), value);
        if (tempmodsubjects)
          SOM->ModificationTempSubjects.push_back(fkey);
        else
        {
          XrdMqSharedObjectManager::Notification event(fkey, XrdMqSharedObjectManager::kMqSubjectModification);
          SOM->NotificationSubjects.push_back(event);
          SOM->SubjectsSem.Post();
        }
      }
    }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::Delete (const char* key, bool broadcast, bool notify)
{
  bool deleted = false;

  std::string skey = key;

  XrdMqRWMutexWriteLock lock(StoreMutex);
  if (Store.count(key))
  {
    CallBackDelete(&Store[key]);
    Store.erase(key);
    deleted = true;

    if (broadcast && XrdMqSharedObjectManager::broadcast)
    {
      if (!IsTransaction)
      {
        // emulate a transaction for single shot deletions
        TransactionMutex.Lock();
        Transactions.clear();
      }
      Deletions.insert(key);
      Transactions.erase(key);
      if (!IsTransaction)
      {
        // emulate a transaction for single shot deletions
        CloseTransaction();
      }
    }

    // check if we have to do posts for this subject
    if (SOM && notify)
    {
      SOM->SubjectsMutex.Lock();
        std::string fkey = Subject.c_str();
        fkey += ";";
        fkey += skey;

        if (XrdMqSharedObjectManager::debug)fprintf(stderr, "XrdMqSharedObjectManager::Delete=>[%s:%s] notified\n", Subject.c_str(), skey.c_str());
        XrdMqSharedObjectManager::Notification event(fkey, XrdMqSharedObjectManager::kMqSubjectKeyDeletion);
        SOM->NotificationSubjects.push_back(event);
        SOM->SubjectsSem.Post();
      SOM->SubjectsMutex.UnLock();
    }
  }
  return deleted;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::Print (std::string &out, std::string format)
{
  //-------------------------------------------------------------------------------
  // listformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>:tag=<tag>:condition=<key>=<val>" -> to print a key of the attached children
  // "sep=<seperator>"                                          -> to put a seperator
  // "header=1"                                                 -> to put a header with description on top! This must be the first format tag!
  // "indent=<n>"                                               -> indent the output
  // "headeronly=1"                                             -> only prints the header and nothnig else
  // the formats are:
  // 's' : print as string
  // 'l' : print as long long
  // 'f' : print as double
  // 'o' : print as <key>=<val>
  // '-' : left align the printout
  // '+' : convert numbers into k,M,G,T,P ranges
  // the unit is appended to every number:
  // e.g. 1500 with unit=B would end up as '1.5 kB'
  // the command only appends to <out> and DOES NOT initialize it
  // "tag=<tag>"                                                  -> use <tag> instead of the variable name to print the header

  std::vector<std::string> formattoken;
  bool buildheader = false;
  std::string indent = "";

  std::string header = "";
  std::string body = "";

  std::string conditionkey = "";
  std::string conditionval = "";
  bool headeronly = false;

  XrdMqStringConversion::Tokenize(format, formattoken, "|");

  for (unsigned int i = 0; i < formattoken.size(); i++)
  {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;

    XrdMqStringConversion::Tokenize(formattoken[i], tagtoken, ":");
    for (unsigned int j = 0; j < tagtoken.size(); j++)
    {
      std::vector<std::string> keyval;
      XrdMqStringConversion::Tokenize(tagtoken[j], keyval, "=");
      if (keyval.size() == 3)
      {
        conditionkey = keyval[1];
        conditionval = keyval[2];
      }
      else
      {
        formattags[keyval[0]] = keyval[1];
      }
    }

    //---------------------------------------------------------------------------------------
    // "key=<key>:width=<width>:format=[slfo]:uniq=<unit>"

    bool alignleft = false;
    if ((formattags["format"].find("-") != std::string::npos))
    {
      alignleft = true;
    }

    if (formattags.count("header"))
    {
      // add the desired seperator
      if (formattags.count("header") == 1)
      {
        buildheader = true;
      }
    }

    if (formattags.count("headeronly"))
    {
      headeronly = true;
    }

    if (formattags.count("indent"))
    {
      for (int i = 0; i < atoi(formattags["indent"].c_str()); i++)
      {
        indent += " ";
      }
    }

    if (formattags.count("width") && formattags.count("format"))
    {
      unsigned int width = atoi(formattags["width"].c_str());
      // string
      char line[1024];
      char tmpline[1024];
      char lformat[1024];
      char lenformat[1024];
      line[0] = 0;
      snprintf(lformat, sizeof (lformat) - 1, "%%s");

      if ((formattags["format"].find("s")) != std::string::npos)
        snprintf(lformat, sizeof (lformat) - 1, "%%s");

      if ((formattags["format"].find("l")) != std::string::npos)
      {
        snprintf(lformat, sizeof (lformat) - 1, "%%lld");
      }

      if ((formattags["format"].find("f")) != std::string::npos)
        snprintf(lformat, sizeof (lformat) - 1, "%%.02f");

      if (width == 0)
      {
        if (alignleft)
        {
          snprintf(lenformat, sizeof (lenformat) - 1, "%%-s");
        }
        else
        {
          snprintf(lenformat, sizeof (lenformat) - 1, "%%s");
        }
      }
      else
      {
        if (alignleft)
        {
          snprintf(lenformat, sizeof (lenformat) - 1, "%%-%ds", width);
        }
        else
        {
          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width);
        }
      }

      // normal member printout
      if (formattags.count("key"))
      {
        if ((formattags["format"].find("s")) != std::string::npos)
          snprintf(tmpline, sizeof (tmpline) - 1, lformat, Get(formattags["key"].c_str()).c_str());
        if ((formattags["format"].find("l")) != std::string::npos)
        {
          if (((formattags["format"].find("+")) != std::string::npos))
          {
            std::string ssize;
            XrdMqStringConversion::GetReadableSizeString(ssize, (unsigned long long) GetLongLong(formattags["key"].c_str()), formattags["unit"].c_str());
            snprintf(tmpline, sizeof (tmpline) - 1, "%s", ssize.c_str());
          }
          else
          {
            snprintf(tmpline, sizeof (tmpline) - 1, lformat, GetLongLong(formattags["key"].c_str()));
          }
        }
        if ((formattags["format"].find("f")) != std::string::npos)
          snprintf(tmpline, sizeof (tmpline) - 1, lformat, GetDouble(formattags["key"].c_str()));

        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 1);
          XrdOucString name = formattags["key"].c_str();
          name.replace("stat.", "");
          name.replace("stat.statfs.", "");
          if (formattags.count("tag"))
          {
            name = formattags["tag"].c_str();
          }

          snprintf(headline, sizeof (headline) - 1, lenformat, name.c_str());
          std::string sline = headline;
          if (sline.length() > (width - 1))
          {
            sline.erase(0, ((sline.length() - width + 1 + 3) > 0) ? (sline.length() - width + 1 + 3) : 0);
            sline.insert(0, "...");
          }
          header += "#";
          header += sline;
        }

        snprintf(line, sizeof (line) - 1, lenformat, tmpline);
      }
      body += indent;
      if ((formattags["format"].find("o") != std::string::npos))
      {
        char keyval[4096];
        buildheader = false; // auto disable header
        if (formattags.count("key"))
        {
          // we are encoding spaces here in the URI way
          XrdOucString noblankline=line;

          {
            // replace all inner blanks with %20
            std::string snoblankline=line;
            size_t pos=snoblankline.find_last_not_of(" ");
            if (noblankline.length()>1)
              while (noblankline.replace(" ", "%20", 0, (pos==std::string::npos)?-1:pos)) {}
          }
       
          snprintf(keyval, sizeof (keyval) - 1, "%s=%s", formattags["key"].c_str(), noblankline.c_str());
        }
        body += keyval;
      }
      else
      {
        std::string sline = line;
        if (width)
        {
          if (sline.length() > width)
          {
            sline.erase(0, ((sline.length() - width + 3) > 0) ? (sline.length() - width + 3) : 0);
            sline.insert(0, "...");
          }
        }
        body += sline;
      }
    }

    if (formattags.count("sep"))
    {
      // add the desired seperator
      body += formattags["sep"];
      if (buildheader)
      {
        header += formattags["sep"];
      }
    }
  }

  body += "\n";

  bool accepted = true;

  // here we can match an EXACT condition or the beginning if '*' is 
  if (conditionkey != "")
  {
    XrdOucString condval = conditionval.c_str();
    XrdOucString condisval = "";
    if (condval.endswith("*"))
    {
      condval.erase(condval.length() - 1);
      condisval = Get(conditionkey.c_str()).c_str();
      if (!(condisval.beginswith(condval)))
        accepted = false;
    }
    else
    {
      if (condval.beginswith("!"))
      {
        condisval = Get(conditionkey.c_str()).c_str();
        condval.erase(0, 1);
        if (!condisval.length())
          accepted = false;
        if ((condisval == condval))
        {
          accepted = false;
        }
      }
      else
      {
        if (Get(conditionkey.c_str()) != conditionval)
        {
          accepted = false;
        }
      }
    }
  }


  if (buildheader)
  {
    std::string line = "";
    line += "#";
    for (unsigned int i = 0; i < (header.length() - 1); i++)
    {
      line += ".";
    }
    line += "\n";
    out += line;
    out += indent;
    out += header;
    out += "\n";
    out += indent;
    out += line;
    if (!headeronly && accepted)
    {
      out += body;
    }
  }
  else
  {
    if (accepted)
    {
      out += body;
    }
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::Clear (bool broadcast)
{
  XrdMqRWMutexWriteLock lock(StoreMutex);
  std::map<std::string, XrdMqSharedHashEntry>::iterator storeit;
  for (storeit = Store.begin(); storeit != Store.end(); storeit++)
  {
    CallBackDelete(&storeit->second);
    if (IsTransaction)
    {
      if (XrdMqSharedObjectManager::broadcast && broadcast)
        Deletions.insert(storeit->first);
      Transactions.erase(storeit->first);
    }
  }
  Store.clear();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedQueue::CallBackInsert (XrdMqSharedHashEntry *entry, const char* key)
{
  entry->SetKey(key);
  QueueMutex.Lock();
  Queue.push_back(entry);
  QueueMutex.UnLock();
  LastObjectId++;
  //  fprintf(stderr,"XrdMqSharedObjectManager::CallBackInsert=> on %s => LOID=%llu\n", key, LastObjectId);
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedQueue::CallBackDelete (XrdMqSharedHashEntry *entry)
{
  std::deque<XrdMqSharedHashEntry*>::iterator it;
  QueueMutex.Lock();
  for (it = Queue.begin(); it != Queue.end(); it++)
  {
    if (*it == entry)
    {
      Queue.erase(it);
      break;
    }
  }
  QueueMutex.UnLock();
  //fprintf(stderr,"XrdMqSharedObjectManager::CallBackDelete=> on %s \n", entry->GetKey());
}

/*----------------------------------------------------------------------------*/
