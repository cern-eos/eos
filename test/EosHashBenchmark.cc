//------------------------------------------------------------------------------
// Copyright (c) 2012 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
#include <iostream>
#include "common/Timing.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/StringConversion.hh"
#include "common/RWMutex.hh"
#include "common/ulib/hash_align.h"

//------------------------------------------------------------------------------
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <google/dense_hash_map>
#include <string>
#include <map>
#include <unordered_map>
#include <random>

eos::common::RWMutex nslock;
XrdSysMutex nsmutex;

using KeyType = long long;
using ValueType = long long;

std::map<KeyType, ValueType> stdmap;
google::dense_hash_map<KeyType, ValueType> googlemap;
ulib::align_hash_map<KeyType, ValueType> ulibmap;
std::unordered_map<KeyType, ValueType> stdumap;

std::map<std::string, double, std::less<>> results; // Allow transparent compare so as to use string_view/strings
std::map<std::string, long long, std::less<>> results_mem;

enum class MapType {
  std_map = 0,
  google_dense = 1,
  ulib = 2,
  std_umap = 3,
  UNKNOWN
};

constexpr uint8_t TOTAL_MAP_COUNT=static_cast<uint8_t>(MapType::UNKNOWN);

// Here we have a factory that'll return the given global map given a type,
// since we can't template based on enum types, do an index and use a fn to the
// index conversion. We'll have a compile time failure if you supply a unknown map_index
template <std::size_t MapIdx>
constexpr auto* getMap(){
  static_assert(MapIdx >= 0 && MapIdx <= TOTAL_MAP_COUNT,
                "Unknown Map Type!!!");

  if constexpr (MapIdx == static_cast<size_t>(MapType::std_map)) {
    return &stdmap;
  } else if constexpr(MapIdx == static_cast<size_t>(MapType::google_dense)) {
    return &googlemap;
  } else if constexpr(MapIdx == static_cast<size_t>(MapType::ulib)) {
    return &ulibmap;
  } else if constexpr(MapIdx == static_cast<size_t>(MapType::std_umap)) {
    return &stdumap;
  }

}
// Get the MapType given an id!
template <typename T>
constexpr size_t MapId(T t){
  return static_cast<size_t>(t);
}

// Apply processing function F on the given set of maps! Note that this function
// will run from the given MapIndex downwards until 0 applying MapType and a
// pointer to the hashmap as the first and second argument.
template <size_t idx=TOTAL_MAP_COUNT-1, class F, class... Args>
void ProcessOp(F&& f, Args&&... args)
{
  // in an ideal world with constexpr function with a loop with fixed compile
  // time arguments, the compiler should be able to unroll the loop, here we do
  // it manually since getMap needs to know the index at compile time
  // accomplish, so we should be able to give it a tuple/list of map_types and
  // we should have this done.
  f(static_cast<MapType>(idx), getMap<idx>(), std::forward<Args>(args)...);
  if constexpr (idx > 0)
    ProcessOp<idx-1>(f,args...);
}

std::string MapName(MapType t)
{
  std::string map_name;
  switch(t) {
  case MapType::std_map:
    map_name = "STL Hash";
    break;
  case MapType::google_dense:
    map_name = "Google Dense Hash";
    break;
  case MapType::ulib:
    map_name = "ULib Hash";
    break;
  case MapType::std_umap:
    map_name = "STL Unordered Hash";
    break;
  case MapType::UNKNOWN:
  default:
    map_name = "UNKNOWN";
  }
  return map_name;
}

//------------------------------------------------------------------------------
// Print current status
//------------------------------------------------------------------------------
void PrintStatus(eos::common::LinuxStat::linux_stat_t& st1,
                 eos::common::LinuxStat::linux_stat_t& st2,
                 eos::common::LinuxMemConsumption::linux_mem_t& mem1,
                 eos::common::LinuxMemConsumption::linux_mem_t& mem2, double& rate)
{
  XrdOucString stdOut;
  XrdOucString sizestring;
  stdOut += "# ------------------------------------------------------------------------------------\n";
  stdOut += "# ------------------------------------------------------------------------------------\n";
  stdOut += "ALL      memory virtual                   ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring,
            (unsigned long long)mem2.vmsize, "B");
  stdOut += "\n";
  stdOut += "ALL      memory resident                  ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring,
            (unsigned long long)mem2.resident, "B");
  stdOut += "\n";
  stdOut += "ALL      memory share                     ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring,
            (unsigned long long)mem2.share, "B");
  stdOut += "\n";
  stdOut += "ALL      memory growths                   ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring,
            (unsigned long long)(st2.vsize - st1.vsize), "B");
  stdOut += "\n";
  stdOut += "# ------------------------------------------------------------------------------------\n";
  stdOut += "ALL      rate                             ";
  char srate[256];
  snprintf(srate, sizeof(srate) - 1, "%.02f", rate);
  stdOut += srate;
  stdOut += "\n";
  stdOut += "# ------------------------------------------------------------------------------------\n";
  fprintf(stderr, "%s", stdOut.c_str());
}


class RThread
{
public:
  RThread(): i(0), n_files(0), type(MapType::UNKNOWN), threads(0), dolock(false) {}

  RThread(size_t a, size_t b, MapType t, size_t nt, bool lock = false):
    i(a), n_files(b), type(t), threads(nt), dolock(lock) {}

  ~RThread() {};

  size_t i;
  size_t n_files;
  MapType type;
  size_t threads;
  bool dolock;
};


//----------------------------------------------------------------------------
// start hash consumer thread
//----------------------------------------------------------------------------

static void* RunReader(void* tconf)
{
  RThread* r = (RThread*) tconf;
  size_t i = r->i;
  size_t n_files = r->n_files;
  bool dolock = r->dolock;

  for (size_t n = 1 + i; n <= n_files; n += r->threads) {
    // if (dolock)nsmutex.Lock();
    if (dolock) {
      nslock.LockRead();
    }

    if (r->type == MapType::std_map) {
      long long v = stdmap[n];

      if (v) {
        v = 1;
      }
    }

    if (r->type == MapType::google_dense) {
      long long v = googlemap[n];

      if (v) {
        v = 1;
      }
    }

    if (r->type == MapType::ulib) {
      long long v = ulibmap[n];

      if (v) {
        v = 1;
      }
    }

    if (r->type == MapType::std_umap) {
      long long v = stdumap[n];

      if (v) {
        v = 1;
      }
    }

    // if (dolock)nsmutex.UnLock();
    if (dolock) {
      nslock.UnLockRead();
    }
  }

  return 0;
}



template <typename KeyType>
std::vector<KeyType> generate_keys(size_t sz, KeyType init = {}, bool randomize=false)
{
  std::vector<KeyType> keys(sz);
  std::iota(keys.begin(), keys.end(), init);

  if (randomize) {
    std::shuffle(keys.begin(), keys.end(),
                 std::mt19937{std::random_device{}()});
  }
  return keys;
}

template <typename HT, typename C>
void InitSingleThreadWrite(MapType t, HT* ht, int& counter, const C& keys)
{
    std::cerr <<
              "# **********************************************************************************"
              << std::endl;
    std::cerr << "[i] Initialize " << MapName(t) << " ..." << std::endl;
    std::cerr <<
              "# **********************************************************************************"
              << std::endl;
    eos::common::LinuxStat::linux_stat_t st[10];;
    eos::common::LinuxMemConsumption::linux_mem_t mem[10];
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");
    COMMONTIMING("hash-start", &tm);
    size_t i = 0;
    for (const auto& key: keys) {
      if (!(i++ % 1000000)) {
        XrdOucString l = "level-";
        l += (int)i;
        COMMONTIMING(l.c_str(), &tm);
      }

      // fill the hash
      // Cross check if insert is defined, this is because ulibmap doesn't follow the same syntax!
      if constexpr(!std::is_same_v<HT,decltype(ulibmap)>)
      {
        ht->insert({key,i});
      } else {
        ht->operator[](key) = i;
      }
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop", &tm);
    tm.Print();
    double rate = (keys.size()) / tm.RealTime() * 1000.0;
    std::stringstream ss;
    ss << std::setw(3) << std::setw(3) << std::setfill('0') << counter << " Fill " << MapName(t);
    std::string title_key = ss.str();
    results.emplace(title_key, rate);
    results_mem.emplace(title_key, st[1].vsize - st[0].vsize);
    PrintStatus(st[0], st[1], mem[0], mem[1], rate);
    counter++;
}

template <typename HT>
void DoReadTests(MapType t, HT *ht, int& counter, size_t n_i, size_t n_files, bool lock=false)
{
  eos::common::LinuxStat::linux_stat_t st[10];;
  eos::common::LinuxMemConsumption::linux_mem_t mem[10];
  std::cerr <<
    "# **********************************************************************************"
            << std::endl;
  std::cerr << "Parallel reader benchmark without locking ";
  std::cerr << MapName(t) << std::endl;
  std::cerr <<
    "# **********************************************************************************"
            << std::endl;
  eos::common::LinuxStat::GetStat(st[0]);
  eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
  eos::common::Timing tm("reading");
  COMMONTIMING("read-start", &tm);
  pthread_t tid[1024];

  // fire threads
  for (size_t i = 0; i < n_i; i++) {
    fprintf(stderr, "# Level %02u\n", (unsigned int)i);
    RThread r(i, n_files, t, n_i, lock);
    XrdSysThread::Run(&tid[i], RunReader, static_cast<void*>(&r), XRDSYSTHREAD_HOLD,
                      "Reader Thread");
  }

  // join them
  for (size_t i = 0; i < n_i; i++) {
    XrdSysThread::Join(tid[i], NULL);
  }

  eos::common::LinuxStat::GetStat(st[1]);
  eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
  COMMONTIMING("read-stop", &tm);

  tm.Print();
  double rate = (n_files) / tm.RealTime() * 1000.0;

  std::stringstream ss;
  std::string lock_str = lock ? "lock " : "no lock ";
  ss << std::setw(3) << std::setw(3) << std::setfill('0') << counter << " Read " << lock_str << MapName(t);
  std::string title_key = ss.str();

  results.emplace(title_key, rate);
  PrintStatus(st[0], st[1], mem[0], mem[1], rate);
  counter++;
}

int main(int argc, char** argv)
{
  googlemap.set_deleted_key(-1);
  googlemap.set_empty_key(0);

  //----------------------------------------------------------------------------
  // Check up the commandline params
  //----------------------------------------------------------------------------
  if (argc != 3) {
    std::cerr << "Usage:"                                << std::endl;
    std::cerr << "  eos-hash-benchmark <entries> <threads>" << std::endl;
    return 1;
  };

  size_t n_files = atoi(argv[1]);

  size_t n_i = atoi(argv[2]);

  if (n_files <= 0) {
    std::cerr << "Error: number of entries has to be > 0" << std::endl;
    return 1;
  }
  int counter = 0;
  auto keys = generate_keys(n_files, 1);

  // Basically process map will process one map type for the given operation So
  // if you want to do entire set of ops on one map type just change the lambda
  // to do this!
  auto write_f = [&counter, &keys](auto&& map_type, auto&& map) {
    InitSingleThreadWrite(map_type, map, counter, keys);
  };
  auto read_no_lock_f = [&counter, &n_i, &n_files](auto&& map_type, auto&& map) {
    DoReadTests(map_type, map, counter, n_i, n_files);
  };

  auto read_lock_f = [&counter, &n_i, &n_files](auto&& map_type, auto&& map) {
    DoReadTests(map_type, map, counter, n_i, n_files, true);
  };

  ProcessOp(write_f);
  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark without locking
  //----------------------------------------------------------------------------
  ProcessOp(read_no_lock_f);
  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark with namespace locking
  //----------------------------------------------------------------------------
  ProcessOp(read_lock_f);

  fprintf(stdout,
          "=====================================================================\n");
  fprintf(stdout,
          "--------------------- SUMMARY ---------------------------------------\n");
  fprintf(stdout,
          "=====================================================================\n");
  int i = 0;

  for (auto it = results.begin(); it != results.end(); it++) {
    if (!(i % TOTAL_MAP_COUNT)) {
      fprintf(stdout, "----------------------------------------------------\n");
    }

    if (i < TOTAL_MAP_COUNT) {
      fprintf(stdout, "%s rate: %.02f MHz mem-overhead: %.02f %%\n",
              it->first.c_str(), it->second / 1000000.0,
              1.0 * results_mem[it->first] / (n_files * 16));
    } else {
      fprintf(stdout, "%s rate: %.02f MHz\n", it->first.c_str(),
              it->second / 1000000.0);
    }

    i++;
  }

  fprintf(stdout, "====================================================\n");
}
