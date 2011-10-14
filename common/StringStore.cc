// ----------------------------------------------------------------------
// File: StringStore.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringStore.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

XrdSysMutex StringStore::StringMutex;

XrdOucHash<XrdOucString> StringStore::theStore; 

char* 
StringStore::Store(const char* charstring , int lifetime) {
  XrdOucString* yourstring;

  if (!charstring) return (char*)"";

  StringMutex.Lock();
  if ((yourstring = theStore.Find(charstring))) {
    StringMutex.UnLock();
    return (char*)yourstring->c_str();
  } else {
    XrdOucString* newstring = new XrdOucString(charstring);
    theStore.Add(charstring,newstring, lifetime);
    StringMutex.UnLock();
    return (char*)newstring->c_str();
  } 
}

EOSCOMMONNAMESPACE_END








