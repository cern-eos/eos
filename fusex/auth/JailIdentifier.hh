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

//------------------------------------------------------------------------------
// Uniquely identifies a jail - also contains room for an error message, in
// case jail resolution was not successful.
//------------------------------------------------------------------------------
class JailIdentifier {
public:
  //----------------------------------------------------------------------------
  // Constructor: Indicate an error message - jail resolution failed.
  //----------------------------------------------------------------------------
  static JailIdentifier MakeError(int errc, const std::string &msg) {
  	JailIdentifier id;
  	id.errc = errc;
  	id.error = msg;
  	return id;
  }

  //----------------------------------------------------------------------------
  // Constructor: Identification succeeded.
  //----------------------------------------------------------------------------
  static JailIdentifier Make(dev_t dev, ino_t ino) {
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
  // Check if the object contains an error
  //----------------------------------------------------------------------------
  bool ok() const;

  //----------------------------------------------------------------------------
  // Equality operator
  //----------------------------------------------------------------------------
  bool operator==(const JailIdentifier &other) const;

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

  //----------------------------------------------------------------------------
  // Private constructor - use methods above to create such an object.
  //----------------------------------------------------------------------------
  JailIdentifier() : errc(0), st_dev(0), st_ino(0) {}
};

//------------------------------------------------------------------------------
// Use this class to uniquely resolve jails.
//------------------------------------------------------------------------------
class JailResolver {
public:
  //----------------------------------------------------------------------------
  // Resolve a given pid_t
  //----------------------------------------------------------------------------
  JailIdentifier resolve(pid_t pid);
};



#endif
