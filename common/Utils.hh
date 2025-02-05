//------------------------------------------------------------------------------
// File: Utils.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#pragma once
#include "common/Namespace.hh"
#include <cstdint>
#include <string>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Open random temporary file
//!
//! @param pattern location pattern to by used by mkstemp
//!
//! @return string temporary file path, if open failed return empty string
//------------------------------------------------------------------------------
std::string MakeTemporaryFile(std::string& pattern);

//-----------------------------------------------------------------------------
//! Make sure that geotag contains only alphanumeric segments which
//! are no longer than 8 characters, in <tag1>::<tag2>::...::<tagN> format.
//!
//! @param geotag input value
//!
//! @return error message if geotag is not valid, otherwise geotag
//-----------------------------------------------------------------------------
std::string SanitizeGeoTag(const std::string& geotag);

//------------------------------------------------------------------------------
//! Get (keytab) file adler checksum
//!
//! @param adler_xs output computed checksum
//! @param fn ketab file to use as input
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool GetFileAdlerXs(std::string& adler_xs,
                    const std::string& fn = "/etc/eos.keytab");

//------------------------------------------------------------------------------
//! Get binary SHA1 of given (keytab) file
//!
//! @param bin_sha1 binary sha1 result
//! @param fn ketab file to use as input
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool GetFileBinarySha1(std::string& bin_sha1,
                       const std::string& fn = "/etc/eos.keytab");

//------------------------------------------------------------------------------
//! Get SHA-1 hex digest of given (keytab) file
//!
//! @param hex_sha1 SHA-1 hex digest
//! @param fn ketab file to use as input
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool GetFileHexSha1(std::string& hex_sha1,
                    const std::string& fn = "/etc/eos.keytab");


void ComputeSize(uint64_t & size, int64_t delta);

//------------------------------------------------------------------------------
//! Adds eos.app=protocol opaque info to the path or opaque infos provided by pathOrOpaque
//! if eos.app is already present in the pathOrOpaque, the value will be prepended
//! by protocol/. If the value is equal to protocol, then nothing will be done
//! If the value is equal to protocol/xyz, then nothing will be done
//! e.g: pathOrOpaque=/eos/test/fic.txt?eos.app=hello --> pathOrOpaque will be equal to
//! /eos/test/fic.txt?eos.app=protocol/hello
//! e.g2: pathOrOpaque= test=test1&eos.app=protocol --> pathOpaque will be left untouched
//! e.g3: pathOrOpaque=/eos/test/fic.txt?eos.app=http/s3 --> pathOpaque will be left untouched
//! if eos.app is not already present in the pathOrOpaque, the value of eos.app
//! will be eos.app=protocol
//! if eos.app is provided twice in the pathOrOpaque, only the last occurence of eos.app will be
//! considered. Indeed, usually the opaque will be transformed into a XrdOucEnv and it
//! is implemented using a hash --> the second occurence will overwrite the first one!
//!
//! @param pathOrOpaque the pathOrOpaque to add the eos.app opaque info
//! @param protocol the protocol of the eos.app opaque parameter
//------------------------------------------------------------------------------
void AddEosApp(std::string & pathOrOpaque, const std::string & protocol);

EOSCOMMONNAMESPACE_END
