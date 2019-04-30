/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/DbMap.hh"
#include "namespace/interface/IFileMD.hh"
#include "fst/Fmd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <iostream>
#include <algorithm>
#include <stdlib.h>
#include <getopt.h>

//------------------------------------------------------------------------------
// Display help message
//------------------------------------------------------------------------------
void print_usage(const char* prg_name)
{
  std::cerr << "Usage: : " << prg_name << " --dbpath <full_path> "
            << "[--count] [--dump_ids] [--fid <fid_dec>]"
            << std::endl
            << "   --count         :"
            << " diplay number of entries in the DB" << std::endl
            << "   --dump_ids      :"
            << "dumpd the decimal file ids stored in the DB" << std::endl
            << "   --fid <fid_dec> : "
            << " display stored metadata info about given file id"
            << std::endl;
}

//------------------------------------------------------------------------------
// Dump the all the file ids stored in the local dabase in decimal format
//------------------------------------------------------------------------------
void DumpAllFids(eos::common::DbMap& db)
{
  const eos::common::DbMapTypes::Tkey* k;
  const eos::common::DbMapTypes::Tval* v;
  eos::common::DbMapTypes::Tval val;

  if (db.size() == 0) {
    std::cout << "info: db is empty!" << std::endl;
    return;
  }

  int max_per_row = 10;
  uint64_t count {0ull};
  std::cout << "fid(dec) : " << std::endl;

  for (db.beginIter(false); db.iterate(&k, &v, false); /*no progress*/) {
    eos::fst::Fmd fmd;
    fmd.ParseFromString(v->value);
    std::cout << std::setw(10) << fmd.fid() << " ";
    ++count;

    if (count % max_per_row == 0) {
      std::cout << std::endl;
    }
  }

  std::cout << std::endl;
}

//------------------------------------------------------------------------------
// Dump all the metadata stored in the local database corresponding to a
// particular file id.
//------------------------------------------------------------------------------
bool DumpFileInfo(eos::common::DbMap& db, const std::string& sfid)
{
  eos::IFileMD::id_t fid = std::stoull(sfid);
  eos::common::DbMap::Tval val;
  bool found = db.get(eos::common::Slice((const char*)&fid, sizeof(fid)), &val);

  if (!found) {
    std::cerr << "error: fid " << sfid << " not found in the DB" << std::endl;
    return false;
  }

  eos::fst::Fmd fmd;
  fmd.ParseFromString(val.value);
  eos::fst::FmdHelper fmd_helper;
  fmd_helper.Replicate(fmd);
  auto opaque = fmd_helper.FullFmdToEnv();
  int envlen;
  std::string data {opaque->Env(envlen)};
  std::replace(data.begin(), data.end(), '&', ' ');
  std::cout << "fxid=" << std::hex << fid << std::dec
            << data << std::endl;
  return true;
}

int main(int argc, char* argv[])
{
  int retc = 0;
  int c;
  int long_index = 0;
  bool count_entries = false;
  bool dump_entry_ids = false;
  std::string dbpath, sfid;
  extern char* optarg;
  static struct option long_options[] = {
    {"dbpath", required_argument, 0,  0 },
    {"fid",    required_argument, 0,  0 },
    {"count",  no_argument,       0, 'c'},
    {"dump_ids",  no_argument,       0, 'e'},
    {0,        0,                 0,  0 }
  };

  while ((c = getopt_long(argc, argv, "", long_options, &long_index)) != -1) {
    switch (c) {
    case 0:
      if (strcmp(long_options[long_index].name, "dbpath") == 0) {
        dbpath = optarg;
      } else if (strcmp(long_options[long_index].name, "fid") == 0) {
        sfid = optarg;
      }

      break;

    case 'c':
      count_entries = true;
      break;

    case 'e':
      dump_entry_ids = true;
      break;

    default:
      print_usage(argv[0]);
      return -1;
    }
  }

  eos::common::DbMap db;
  eos::common::LvDbDbMapInterface::Option options;
  options.CacheSizeMb = 0;
  options.BloomFilterNbits = 0;

  if (!db.attachDb(dbpath, false, 0, &options)) {
    std::cerr << "error: failed to attach db: " << dbpath << std::endl;
    return -1;
  } else {
    db.outOfCore(true);
  }

  // Display the number of entries in the DB
  if (count_entries) {
    std::cout << "info: " << db.size() << " entries in the DB" << std::endl;
    db.detachDb();
    return 0;
  }

  // Dump the list of all fids
  if (dump_entry_ids) {
    DumpAllFids(db);
  }

  // Dispaly file info stored in the local database
  if (!sfid.empty()) {
    if (DumpFileInfo(db, sfid) == false) {
      retc = -1;
    }
  }

  db.detachDb();
  return retc;
}
