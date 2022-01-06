// ----------------------------------------------------------------------
// File: EosToke.hh
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
 * @file   EosTok.hh
 *
 * @brief  Class providing EOS token support
 *
 */

#pragma once

#include "Token.hh"
#include "common/Namespace.hh"
#include <memory>
#include <atomic>
namespace  eos
{
namespace console
{
class TokenEnclosure;
}
}

EOSCOMMONNAMESPACE_BEGIN



class EosTok : public Token
{
public:

  EosTok();
  EosTok(eos::console::TokenEnclosure& token);

  virtual ~EosTok();

  virtual std::string Write(const std::string& key);
  virtual int Read(const std::string& input, const std::string& key,
                   uint64_t generation, bool ignoreerror = false);

  virtual int Reset();
  virtual int Serialize();
  virtual int Deserialize();
  virtual int Sign(const std::string& key);
  virtual int Verify(const std::string& key);
  virtual int Dump(std::string& dump, bool filtersec = false,
                   bool oneline = false);
  virtual int SetPath(const std::string& path, bool subtree);
  virtual int SetPermission(const std::string& perm);
  virtual int SetOwner(const std::string& owner);
  virtual int SetGroup(const std::string& group);
  virtual int SetExpires(time_t expires);
  virtual int SetGeneration(uint64_t generation);
  virtual int SetRequester(const std::string& requester);
  virtual int AddOrigin(const std::string& host, const std::string& name,
                        const std::string& prot);
  virtual int VerifyOrigin(const std::string& host, const std::string& name,
                           const std::string& prot);
  virtual int ValidatePath(const std::string& path) const;
  virtual bool Valid() const;
  virtual std::string Owner() const;
  virtual std::string Group() const;
  virtual std::string Permission() const;
  virtual std::string Path() const;
  virtual std::string Voucher() const;
  virtual std::string Requester() const;
  virtual int Generation() const;

  static std::atomic<uint64_t>
  sTokenGeneration; ///< generation value for token issuing/verification

  static bool isEosToken(const char* pathcgi);
private:

  int Match(const std::string& input, const std::string& match);
  std::shared_ptr<eos::console::TokenEnclosure> share;
  bool valid;
};

EOSCOMMONNAMESPACE_END
