//------------------------------------------------------------------------------
// File: JailIdentifier.hh
// Author: Georgios Bitzes, CERN
//------------------------------------------------------------------------------

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

#ifndef EOS_FUSE_JAIL_IDENTIFIER_HH
#define EOS_FUSE_JAIL_IDENTIFIER_HH

#include <string>

#ifdef __APPLE__
typedef uint64_t ino_t;
#endif

//------------------------------------------------------------------------------
// Uniquely identifies a jail - also contains room for an error message, in
// case jail resolution was not successful.
//------------------------------------------------------------------------------
class JailIdentifier
{
public:
  //----------------------------------------------------------------------------
  // Constructor: Empty object.
  //----------------------------------------------------------------------------
  JailIdentifier() : errc(0), st_dev(0), st_ino(0) {}

  //----------------------------------------------------------------------------
  // Constructor: Indicate an error message - jail resolution failed.
  //----------------------------------------------------------------------------
  static JailIdentifier MakeError(int errc, const std::string& msg)
  {
    JailIdentifier id;
    id.errc = errc;
    id.error = msg;
    return id;
  }

  //----------------------------------------------------------------------------
  // Constructor: Identification succeeded.
  //----------------------------------------------------------------------------
  static JailIdentifier Make(dev_t dev, ino_t ino)
  {
    JailIdentifier id;
    id.st_dev = dev;
    id.st_ino = ino;
    return id;
  }

  //----------------------------------------------------------------------------
  // Describe this object.
  //----------------------------------------------------------------------------
  std::string describe() const;

  //----------------------------------------------------------------------------
  // Simple hash for this jail
  //----------------------------------------------------------------------------
  uint64_t hash() const;

  //----------------------------------------------------------------------------
  // Check if the object contains an error
  //----------------------------------------------------------------------------
  bool ok() const;

  //----------------------------------------------------------------------------
  // Equality operator
  //----------------------------------------------------------------------------
  bool operator==(const JailIdentifier& other) const;

private:
  //----------------------------------------------------------------------------
  // error filled only to indicate jail resolution failed.
  //----------------------------------------------------------------------------
  int errc;
  std::string error;

  //----------------------------------------------------------------------------
  // Identification
  //----------------------------------------------------------------------------
  dev_t st_dev;
  ino_t st_ino;
};

//------------------------------------------------------------------------------
// JailInformation: JailIdentifier + pid_t
//
// We can't store pid in JailIdentifier, it's used as a cache key. Many pids
// will resolve to the same JailIdentifier, adding pid there breaks caching.
//
// But we need the pid to actually do path lookups in such jail, st_dev and
// st_ino can't be used in such case... Hence the distinction between
// JailIdentifier and JailInformation.
//------------------------------------------------------------------------------
struct JailInformation {
  JailIdentifier id;
  pid_t pid;
  bool sameJailAsThisPid;

  std::string describe() const;
};

//------------------------------------------------------------------------------
// Use this class to uniquely resolve jails.
//------------------------------------------------------------------------------
class JailResolver
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  JailResolver();

  //----------------------------------------------------------------------------
  // Resolve a given pid_t to JailIdentifier
  //----------------------------------------------------------------------------
  JailIdentifier resolveIdentifier(pid_t pid);

  //----------------------------------------------------------------------------
  // Resolve a given pid_t to JailInformation
  //----------------------------------------------------------------------------
  JailInformation resolve(pid_t pid);

  //----------------------------------------------------------------------------
  // Resolve a given pid_t to JailInformation - if an error is encountered,
  // return _my_ jail.
  //----------------------------------------------------------------------------
  JailInformation resolveOrReturnMyJail(pid_t pid);

private:
  JailInformation myJail;
};

#endif
