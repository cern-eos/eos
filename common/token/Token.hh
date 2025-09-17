// ----------------------------------------------------------------------
// File: Token.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/ASwitzerland                                  *
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

/**
 * @file   Token.hh
 *
 * @brief  Base class for token support
 *
 */

#pragma once

#include <cstdint>
#include <string>

class Token {
public:
  Token() {};
  virtual ~Token() {};

  virtual std::string Write(const std::string& key) = 0;
  virtual int Read(const std::string& input, const std::string& key, uint64_t generation, bool ignoreexpired) = 0;


  virtual int Reset() = 0;
  virtual int Serialize() = 0;
  virtual int Deserialize() = 0;
  virtual int Sign(const std::string& key) = 0;
  virtual int Verify(const std::string&key) = 0;
  virtual int Dump(std::string& dump, bool filtersec, bool oneline) = 0;
  virtual int SetPath(const std::string& path, bool subtree) = 0;
  virtual int SetPermission(const std::string& perm) = 0;
  virtual int SetOwner(const std::string& owner) = 0;
  virtual int SetGroup(const std::string& group) = 0;
  virtual int SetExpires(time_t expires) = 0;
  virtual int SetGeneration(uint64_t generation) = 0;
  virtual int SetRequester(const std::string& requesteor) = 0;
  virtual int AddOrigin(const std::string& host, const std::string& name, const std::string& prot) = 0;
  virtual int VerifyOrigin(const std::string& host, const std::string& name, const std::string& prot) = 0;
  virtual int ValidatePath(const std::string& path) const  = 0;
  virtual bool Valid() const = 0;
  virtual bool TreeToken() const = 0;
  virtual std::string Owner() const = 0;
  virtual std::string Group() const = 0;
  virtual std::string Permission() const = 0;
  virtual std::string Path() const = 0;
  virtual std::string Voucher() const = 0;
  virtual std::string Requester() const = 0;
  virtual int Generation() const = 0;
};
