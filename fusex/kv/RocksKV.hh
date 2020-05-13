//------------------------------------------------------------------------------
//! @file RocksKV.hh
//! @author Georgios Bitzes CERN
//! @brief kv persistency class based on rocksdb
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifdef HAVE_ROCKSDB
#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "kv/kv.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <event.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>

//------------------------------------------------------------------------------
// Implementation of the key value store interface based on redis
//------------------------------------------------------------------------------

class RocksKV : public kv
{
public:
  RocksKV();
  virtual ~RocksKV();

  int connect(const std::string& prefix, const std::string& path);

  int get(const std::string& key, std::string& value) override;
  int get(const std::string& key, uint64_t& value) override;
  int put(const std::string& key, const std::string& value) override;
  int put(const std::string& key, uint64_t value) override;
  int inc(const std::string& key, uint64_t& value) override;

  int erase(const std::string& key) override;

  int get(uint64_t key, std::string& value,
          const std::string& name_space = "i") override;
  int put(uint64_t key, const std::string& value,
          const std::string& name_space = "i") override;

  int get(uint64_t key, uint64_t& value,
          const std::string& name_space = "i") override;
  int put(uint64_t key, uint64_t value,
          const std::string& name_space = "i") override;

  int erase(uint64_t key, const std::string& name_space = "i") override;

  int clean_stores(const std::string& storedir,
                   const std::string& newdb) override;

  std::string prefix(const std::string& key)
  {
    return mPrefix + key;
  }

  std::string statistics() override
  {
    return options.statistics->ToString();
    //return std::string("##### cache size is ") + std::to_string(table_options.block_cache->GetUsage());
  }

  using TransactionPtr = std::unique_ptr<rocksdb::Transaction>;
private:
  std::unique_ptr<rocksdb::TransactionDB> transactionDB;
  rocksdb::DB* db; // owned by transactionDB
  std::string mPrefix;

  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;

  TransactionPtr startTransaction()
  {
    rocksdb::WriteOptions opts;
    return TransactionPtr(transactionDB->BeginTransaction(opts));
  }

};

#endif // HAVE_ROCKSDB
