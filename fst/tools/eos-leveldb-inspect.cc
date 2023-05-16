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
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "common/Fmd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <iostream>
#include <algorithm>
#include <stdlib.h>
#include <getopt.h>
#include <stdio.h>

class LeveldbReadOnlyHack
{
public:
  //------------------------------------------------------------------------------
  //! Constructor - create a symlink to the original leveldb directory and remove
  //! the LOCK file.
  //------------------------------------------------------------------------------
  LeveldbReadOnlyHack(const std::string& dbpath, std::string& symlink_path)
  {
    std::ostringstream oss;
    oss << "/tmp/.eos_inspect_" << getpid() << "/";
    mSymLinkPath = oss.str();
    oss << "LOCK";
    std::string lock_file = oss.str();
    mode_t mode = 0755;

    if (mkdir(mSymLinkPath.c_str(), mode)) {
      std::cerr << "fatal: failed to create symlink directory" << std::endl;
      std::abort();
    }

    oss.str("");
    oss << "ln -s " << dbpath << "* " << mSymLinkPath;

    // Execute symlink command
    if (system(oss.str().c_str()) == -1) {
      std::cerr << "fatal: failed to execte symlink command" << std::endl;
      std::abort();
    }

    if (remove(lock_file.c_str())) {
      std::cerr << "fatal: failed to remove LOCK file" << std::endl;
      std::abort();
    }

    symlink_path = mSymLinkPath;
  }

  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  ~LeveldbReadOnlyHack()
  {
    std::string cmd = "rm -rf ";
    cmd += mSymLinkPath;
    (void) system(cmd.c_str());
  }

private:
  std::string mSymLinkPath;
};

//------------------------------------------------------------------------------
// Display help message
//------------------------------------------------------------------------------
void print_usage(const char* prg_name)
{
  std::cerr << "Usage: : " << prg_name << " --dbpath <full_path> "
            << "[--dump_ids] [--fid <fid> | --fxid <fxid>] [--fsck] [--verbose_fsck]\n"
            << "   --dump_ids      :"
            << "dumpd the decimal file ids stored in the DB\n"
            << "   --fid <fid> | --fxid <fxid> : "
            << " display stored metadata info about given file id decimal/hex\n"
            << "   --fsck          :"
            << " display fsck inconsistencies counters\n"
            << "   --verbose_fsck  : "
            << " display fsck counters together with the hex file ids\n"
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
    eos::common::FmdHelper fmd;
    fmd.mProtoFmd.ParseFromString(v->value);
    std::cout << std::setw(10) << fmd.mProtoFmd.fid() << " ";
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

  eos::common::FmdHelper fmd;
  fmd.mProtoFmd.ParseFromString(val.value);
  auto opaque = fmd.FmdToEnv();
  int envlen;
  std::string data {opaque->Env(envlen)};
  std::replace(data.begin(), data.end(), '&', ' ');
  std::cout << "fxid=" << std::hex << fid << std::dec
            << data << std::endl;
  return true;
}

//------------------------------------------------------------------------------
// Dump fsck statistics
//------------------------------------------------------------------------------
void DumpFsckStats(eos::common::DbMap& db, bool verbose = false)
{
  using namespace eos::common;
  std::map<std::string, std::set < eos::common::FileId::fileid_t>> fid_set;
  std::map<std::string, size_t> statistics {
    {"mem_n",            0}, // no. of files in db
    {"d_sync_n",         0}, // no. of synced files from disk
    {"m_sync_n",         0}, // no. of synced files from MGM
    {FSCK_D_MEM_SZ_DIFF, 0}, // no. files with disk and reference size mismatch
    {FSCK_M_MEM_SZ_DIFF, 0}, // no. files with MGM and reference size mismatch
    {FSCK_D_CX_DIFF,     0}, // no. files with disk and reference checksum mismatch
    {FSCK_M_CX_DIFF,     0}, // no. files with MGM and reference checksum mismatch
    {FSCK_UNREG_N,       0}, // no. of unregistered replicas
    {FSCK_REP_DIFF_N,    0}, // no. of files with replicas number mismatch
    {FSCK_REP_MISSING_N, 0}, // no. of files with replicas missing on disk
    {FSCK_BLOCKXS_ERR,   0}, // no. of replicas with blockxs error
    {FSCK_ORPHANS_N,     0}  // no. of orphaned replicas
  };
  const eos::common::DbMapTypes::Tkey* k;
  const eos::common::DbMapTypes::Tval* v;
  eos::common::DbMapTypes::Tval val;

  for (db.beginIter(false); db.iterate(&k, &v, false);) {
    eos::common::FmdHelper f;
    f.mProtoFmd.ParseFromString(v->value);
    CollectInconsistencies(f, statistics, fid_set);
  }

  // Display summary
  if (verbose) {
    // Helper function for printing
    auto print_fids = [](const std::set<eos::common::FileId::fileid_t>& set)
    -> std::string {
      std::ostringstream oss;
      int count = 0;
      int max_per_line = 10;
      oss << std::setw(8) << std::setfill('0') << std::hex;

      for (const auto& elem : set)
      {
        oss << elem << " ";
        ++count;

        if (count % max_per_line == 0) {
          oss << std::endl;
        }
      }

      oss << std::endl;
      return oss.str();
    };
    std::cout << "Num entries in DB[mem_n]:                      "
              << statistics["mem_n"] << std::endl
              << "Num. files synced from disk[d_sync_n]:         "
              << statistics["d_sync_n"] << std::endl
              << "Num, files synced from MGM[m_sync_n]:          "
              << statistics["m_sync_n"] << std::endl
              << "Disk/referece size missmatch[d_mem_sz_diff]:   "
              << statistics["d_mem_sz_diff"] << std::endl
              << print_fids(fid_set["d_mem_sz_diff"])
              << "MGM/reference size missmatch[m_mem_sz_diff]:   "
              << statistics["m_mem_sz_diff"] << std::endl
              << print_fids(fid_set["m_mem_sz_diff"])
              << "Disk/reference checksum missmatch[d_cx_diff]:  "
              << statistics["d_cx_diff"] << std::endl
              << print_fids(fid_set["d_cx_diff"])
              << "MGM/reference checksum missmatch[m_cx_diff]:   "
              << statistics["m_cx_diff"] << std::endl
              << print_fids(fid_set["m_cx_diff"])
              << "Num. of orphans[orphans_n]:                    "
              << statistics["orphans_n"] << std::endl
              << print_fids(fid_set["orphans_n"])
              << "Num. of unregistered replicas[unreg_n]:        "
              << statistics["unreg_n"] << std::endl
              << print_fids(fid_set["unreg_n"])
              << "Files with num. replica missmatch[rep_diff_n]: "
              << statistics["rep_diff_n"] << std::endl
              << print_fids(fid_set["rep_diff_n"])
              << "Files missing on disk[rep_missing_n]:          "
              << statistics["rep_missing_n"] << std::endl
              << print_fids(fid_set["rep_missing_n"]) << std::endl;
  } else {
    std::cout << "Num. entries in DB[mem_n]:                     "
              << statistics["mem_n"] << std::endl
              << "Num. files synced from disk[d_sync_n]:         "
              << statistics["d_sync_n"] << std::endl
              << "Num, files synced from MGM[m_sync_n]:          "
              << statistics["m_sync_n"] << std::endl
              << "Disk/referece size missmatch[d_mem_sz_diff]:   "
              << statistics["d_mem_sz_diff"] << std::endl
              << "MGM/reference size missmatch[m_mem_sz_diff]:   "
              << statistics["m_mem_sz_diff"] << std::endl
              << "Disk/reference checksum missmatch[d_cx_diff]:  "
              << statistics["d_cx_diff"] << std::endl
              << "MGM/reference checksum missmatch[m_cx_diff]:   "
              << statistics["m_cx_diff"] << std::endl
              << "Num. of orphans[orphans_n]:                    "
              << statistics["orphans_n"] << std::endl
              << "Num. of unregistered replicas[unreg_n]:        "
              << statistics["unreg_n"] << std::endl
              << "Files with num. replica missmatch[rep_diff_n]: "
              << statistics["rep_diff_n"] << std::endl
              << "Files missing on disk[rep_missing_n]:          "
              << statistics["rep_missing_n"] << std::endl;
  }
}

int main(int argc, char* argv[])
{
  int retc = 0;
  int c;
  int long_index = 0;
  bool dump_entry_ids = false;
  bool dump_fsck = false;
  bool verbose_fsck = false;
  std::string dbpath, sfid;
  extern char* optarg;
  static struct option long_options[] = {
    {"dbpath",           required_argument, 0,   0   },
    {"fid",              required_argument, 0,   0   },
    {"fxid",             required_argument, 0,   0   },
    {"dump_ids",         no_argument,       0,   'e' },
    {"fsck",             no_argument,       0,   'f' },
    {"verbose_fsck",     no_argument,       0,   'v' },
    {0,                  0,                 0,   0   }
  };

  while ((c = getopt_long(argc, argv, "", long_options, &long_index)) != -1) {
    switch (c) {
    case 0:
      if (strcmp(long_options[long_index].name, "dbpath") == 0) {
        dbpath = optarg;

        // Make sure the path is / terminated
        if (*dbpath.rbegin() != '/') {
          dbpath += "/";
        }
      } else if (strncmp(long_options[long_index].name, "fid", 3) == 0) {
        sfid = optarg;
      } else if (strncmp(long_options[long_index].name, "fxid", 4) == 0) {
        const std::string sarg = optarg;
        size_t pos = 0;
        uint64_t fid {0ull};

        try {
          fid = std::stoull(sarg, &pos, 16);

          if (pos != sarg.size()) {
            throw std::invalid_argument("failed fxid conversion");
          }
        } catch (...) {
          std::cerr << "error: failed to convert fxid" << std::endl;
          return -1;
        }

        sfid = std::to_string(fid);
      }

      break;

    case 'e':
      dump_entry_ids = true;
      break;

    case 'f':
      dump_fsck = true;
      break;

    case 'v':
      dump_fsck = true;
      verbose_fsck = true;
      break;

    default:
      print_usage(argv[0]);
      return -1;
    }
  }

  // Check that the LevelDB already exists
  struct stat buf;

  if (stat(dbpath.c_str(), &buf)) {
    std::cerr << "error: LevelDB does not exist" << std::endl;
    return -1;
  }

  std::string symlink_dbpath;
  LeveldbReadOnlyHack dummy(dbpath, symlink_dbpath);
  eos::common::DbMap db;
  eos::common::LvDbDbMapInterface::Option options;
  options.CacheSizeMb = 0;
  options.BloomFilterNbits = 0;

  if (!db.attachDb(symlink_dbpath, false, 0, &options)) {
    std::cerr << "error: failed to attach db: " << symlink_dbpath
              << std::endl;
    return -1;
  } else {
    db.outOfCore(true);
  }

  // Dump the list of all fids
  if (dump_entry_ids) {
    DumpAllFids(db);
  }

  // Display file info stored in the local database
  if (!sfid.empty()) {
    if (DumpFileInfo(db, sfid) == false) {
      retc = -1;
    }
  }

  // Dispaly fsck inconsistencies from the local database
  if (dump_fsck) {
    DumpFsckStats(db, verbose_fsck);
  }

  db.detachDb();
  return retc;
}