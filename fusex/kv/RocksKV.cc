//------------------------------------------------------------------------------
//! @file RocksKV.cc
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

#ifdef HAVE_ROCKSDB
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include "kv/kv.hh"
#include "kv/RocksKV.hh"
#include "eosfuse.hh"
#include "misc/MacOSXHelper.hh"
#include "common/Logging.hh"
#include "misc/longstring.hh"
#include <sys/types.h>
#include <dirent.h>
#include <regex>
#include <fstream>

/* -------------------------------------------------------------------------- */
static bool
/* -------------------------------------------------------------------------- */
safe_strtoll(const std::string& str, uint64_t& ret)
/* -------------------------------------------------------------------------- */
{
  char* endptr = NULL;
  ret = strtoull(str.c_str(), &endptr, 10);

  if (endptr != str.c_str() + str.size() || ret == ULONG_LONG_MAX) {
    return false;
  }

  return true;
}

/* -------------------------------------------------------------------------- */
RocksKV::RocksKV()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
RocksKV::~RocksKV()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::connect(const std::string& prefix, const std::string& path)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("Opening RocksKV store at local path %s", path.c_str());
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 1024;
  //  table_options.block_cache = rocksdb::NewLRUCache(4*1024);
  //  table_options.cache_index_and_filter_blocks = false;
  options.optimize_filters_for_hits = true;
  options.statistics = rocksdb::CreateDBStatistics();
  //  options.compression = rocksdb::kZSTD;
  //  options.bottommost_compression = rocksdb::kZSTD;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;
  options.row_cache = rocksdb::NewLRUCache(4 * 1024 * 1024);
  options.level_compaction_dynamic_level_bytes = true;
  options.max_subcompactions = 4;
  options.disable_auto_compactions = false;
  options.write_buffer_size = 1 * 1024 * 1024;
  rocksdb::TransactionDBOptions txopts;
  txopts.transaction_lock_timeout = -1;
  txopts.default_lock_timeout = -1;
  rocksdb::TransactionDB* mydb;
  rocksdb::Status st = rocksdb::TransactionDB::Open(options, txopts, path, &mydb);

  if (!st.ok()) {
    eos_static_crit("Could not open RocksKV store, error: %s",
                    st.ToString().c_str());
    return -1;
  }

  mPrefix = prefix;
  transactionDB.reset(mydb);
  db = transactionDB->GetBaseDB();
  return 0;
}

static int badStatus(const rocksdb::Status& st)
{
  eos_static_crit("Unexpected rocksdb status: %s", st.ToString().c_str());
  return -1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(const std::string& key, std::string& value)
/* -------------------------------------------------------------------------- */
{
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), prefix(key), &value);

  if (st.IsNotFound()) {
    return 1;
  } else if (!st.ok()) {
    return badStatus(st);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(const std::string& key, uint64_t& value)
/* -------------------------------------------------------------------------- */
{
  std::string tmp;
  int ret = this->get(key, tmp);

  if (ret != 0) {
    return ret;
  }

  if (!safe_strtoll(tmp.c_str(), value)) {
    eos_static_crit("Expected to find an integer on key %s, instead found %s",
                    key.c_str(), tmp.c_str());
    return -1;
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(const std::string& key, const std::string& value)
/* -------------------------------------------------------------------------- */
{
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), prefix(key), value);

  if (!st.ok()) {
    return badStatus(st);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(const std::string& key, uint64_t value)
/* -------------------------------------------------------------------------- */
{
  return this->put(key, std::to_string(value));
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::inc(const std::string& key, uint64_t& value)
/* -------------------------------------------------------------------------- */
{
  TransactionPtr tx = startTransaction();
  std::string tmp;
  uint64_t initialValue = 0;
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), prefix(key),
                                        &tmp);

  if (!st.ok() && !st.IsNotFound()) {
    return badStatus(st);
  }

  if (!safe_strtoll(tmp, initialValue)) {
    eos_static_crit("Attemted to increase a non-numeric value on key %s: %s",
                    key.c_str(), tmp.c_str());
    return -1;
  }

  st = tx->Put(prefix(key), std::to_string(initialValue + value));

  if (!st.ok()) {
    return badStatus(st);
  }

  st = tx->Commit();

  if (!st.ok()) {
    return badStatus(st);
  }

  value += initialValue;
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::erase(const std::string& key)
/* -------------------------------------------------------------------------- */
{
  rocksdb::Status st = db->Delete(rocksdb::WriteOptions(), prefix(key));

  if (!st.ok()) {
    // deleting a non-existent key is not an error!
    return badStatus(st);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(uint64_t key, std::string& value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  return this->get(buildKey(key, name_space), value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(uint64_t key, const std::string& value,
             const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  return this->put(buildKey(key, name_space), value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(uint64_t key, uint64_t& value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  return this->get(buildKey(key, name_space), value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(uint64_t key, uint64_t value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  return this->put(buildKey(key, name_space), value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::erase(uint64_t key, const std::string& name_space)
{
  return this->erase(buildKey(key, name_space));
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::clean_stores(const std::string& storedir, const std::string& newdb)
{
  DIR* dir;
  struct dirent* ent;

  if ((dir = ::opendir(storedir.c_str())) != NULL) {
    while ((ent = ::readdir(dir)) != NULL) {
      std::string entry = ent->d_name;

      if ((entry == ".") || (entry == "..")) {
        continue;
      }

      if (entry == newdb) {
        continue;
      }

      if (!entry.length()) {
        continue;
      }

      struct stat buf;

      std::string dbdir = storedir;

      dbdir += "/";

      dbdir += entry;

      if (!stat(dbdir.c_str(), &buf)) {
        // check if this is a directory
        if (S_ISDIR(buf.st_mode)) {
          // cleanup this old directory
          std::string rmline = "rm -rf ";
          rmline += dbdir;
          system(rmline.c_str());
          fprintf(stderr, "###### cleaning stale cache directory '%s'\n",
                  dbdir.c_str());
        }
      }
    }

    ::closedir(dir);
  }

  return 0;
}

#endif // HAVE_ROCKSDB
