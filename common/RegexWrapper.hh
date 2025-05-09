//------------------------------------------------------------------------------
// File: RegexWrapper.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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
//! Wrapper around std::regex to make sure all the std::regex_ calls are
//! thread safe
//------------------------------------------------------------------------------
#pragma once
#include "common/Namespace.hh"
#include <regex>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Wrapper for std::regex_match taking regular expression as std::string
//!
//! @param input input string
//! @param regex regular expression given as std::string
//!
//! @return true if there is a full match, otherwise false
//------------------------------------------------------------------------------
bool eos_regex_match(const std::string& input, const std::string& regex);

//------------------------------------------------------------------------------
//! Wrapper for std::regex_search taking regular expression as std::string
//!
//! @param input input string
//! @param regex regular expression given as std::string
//!
//! @return true if there is any match inside input and any of its substrings,
//!         otherwise false
//------------------------------------------------------------------------------
bool eos_regex_search(const std::string& input, const std::string& regex);

//------------------------------------------------------------------------------
//! Check if given input is a valid regex
//!
//! @param regex input that should represent a regex
//!
//! @return true if valid, otherwise false
//------------------------------------------------------------------------------
bool eos_regex_valid(const std::string& regex);

EOSCOMMONNAMESPACE_END
