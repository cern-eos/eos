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

#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include "kv/kv.hh"
#include "kv/RocksKV.hh"
#include "eosfuse.hh"
#include "misc/MacOSXHelper.hh"
#include "common/Logging.hh"
#include "misc/longstring.hh"


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
RocksKV::connect(const std::string &path) // , const std::string &prefix)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("Opening RocksKV store at local path %s", path.c_str());

  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 16 * 1024;

  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;

  rocksdb::DB *mydb;

  rocksdb::Status st = rocksdb::DB::Open(options, path, &mydb);
  if(!st.ok()) {
    eos_static_crit("Could not open RocksKV store, error: %s", st.ToString().c_str());
    return -1;
  }

  db.reset(mydb);
  return 0;
}

static int badStatus(const rocksdb::Status &st) {
  eos_static_crit("Unexpected rocksdb status: %s", st.ToString().c_str());
  return -1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(std::string &key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), key, &value);

  if(st.IsNotFound()) {
    return 1;
  }
  else if(!st.ok()) {
    return badStatus(st);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(std::string &key, uint64_t &value)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(std::string &key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), key, value);
  if(!st.ok()) {
    return badStatus(st);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(std::string &key, uint64_t &value)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::inc(std::string &key, uint64_t &value)
/* -------------------------------------------------------------------------- */
{
  return 1;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::erase(std::string &key)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(uint64_t key, std::string &value, std::string name_space)
/* -------------------------------------------------------------------------- */
{
  return 1;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(uint64_t key, std::string &value, std::string name_space)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::get(uint64_t key, uint64_t &value, std::string name_space)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::put(uint64_t key, uint64_t &value, std::string name_space)
/* -------------------------------------------------------------------------- */
{
  return 1;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RocksKV::erase(uint64_t key, std::string name_space)
/* -------------------------------------------------------------------------- */
{
  return 1;
}
