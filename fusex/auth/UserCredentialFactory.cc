// ----------------------------------------------------------------------
// File: UserCredentialFactory.cc
// Author: Georgios Bitzes - CERN
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

#include "UserCredentialFactory.hh"
#include "Utils.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
UserCredentialFactory::UserCredentialFactory(const CredentialConfig &conf) :
  config(conf) {}

//------------------------------------------------------------------------------
// Parse a string, convert into SearchOrder
//------------------------------------------------------------------------------
SearchOrder UserCredentialFactory::parse(LogbookScope &scope,
  const std::string &str, const JailIdentifier &jail) {

  THROW("NYI");
}

//------------------------------------------------------------------------------
// Append defaults into given SearchOrder
//------------------------------------------------------------------------------
void UserCredentialFactory::addDefaults(const JailIdentifier &id,
  const Environment& env, uid_t uid, gid_t gid, SearchOrder &out)
{

}

//------------------------------------------------------------------------------
// Given a single entry of the search path, append any entries
// into the given SearchOrder object
//------------------------------------------------------------------------------
bool UserCredentialFactory::parseSingle(LogbookScope &scope, const std::string &str,
    const JailIdentifier &id, const Environment& env, uid_t uid, gid_t gid,
    SearchOrder &out)
{
  //----------------------------------------------------------------------------
  // Defaults?
  //----------------------------------------------------------------------------
  if(str == "defaults") {
    addDefaults(id, env, uid, gid, out);
    return true;
  }

  //----------------------------------------------------------------------------
  // Cannot parse given string
  //----------------------------------------------------------------------------
  return false;
}
