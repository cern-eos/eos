#include "gtest/gtest.h"
#include "common/RWMutex.hh"
#include <map>
#include <thread>
/*
 A simple concurrent map that uses std::map & the various eos rwlocks to
 simulate some concurrent processing issues seen in the MGM, primarily used to
 simulate clients reading and writing caps for eg.
*/
class SimpleConcMap : public eos::common::RWMutex
{
public:
  using auth_id = std::string;
  using expiry_time = uint64_t;

  SimpleConcMap(): eos::common::RWMutex()
  {
    mBlocking=true;
  }

  void add_auth(const std::string& _auth);
  void read_all(int delay);
  void remove_auth(const std::string& _auth);
  size_t get_size();
private:
  std::map<auth_id, expiry_time> auth_map;
};

void SimpleConcMap::add_auth(const std::string& _auth)
{
  eos::common::RWMutexWriteLock wLock(*this);
  auth_map[_auth] = static_cast<uint64_t>(time(NULL));
}

void SimpleConcMap::read_all(int delay)
{
  std::map<auth_id, expiry_time> a_map;
  eos::common::RWMutexReadLock rLock(*this);
  for(const auto& kv: auth_map) {
    a_map.emplace(kv);
    std::this_thread::sleep_for(std::chrono::microseconds(delay));
  }
}

size_t SimpleConcMap::get_size() {
   eos::common::RWMutexReadLock rLock(*this);
   return auth_map.size();
}

TEST(RWMutex, SingleReaderSingleWriterTest)
{
  SimpleConcMap cmap;
  const int map_count = 10000;
  const int delay = 10;
  std::thread writer([&cmap]() {
    for (int i =0; i< map_count; i++) {
      cmap.add_auth(std::to_string(i));
    }
  });
  std::thread reader([&cmap]() {
    size_t sz = 0;
    while(sz < map_count)
    {
      cmap.read_all(delay);
      sz = cmap.get_size();
      std::cout<< "current size: "<< sz << "\n";
    }
  });

  reader.join();
  writer.join();
}
