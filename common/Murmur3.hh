#ifndef __MURMUR3__HH__
#define __MURMUR3__HH__

#include <random>

std::random_device rd;
std::mt19937_64 gen(rd());
std::uniform_int_distribution<uint64_t> dis;

class Murmur3 {
  // simple murmur3 hash
public:

  template<typename T>
  struct MurmurHasher 
  {
    size_t operator()(uint64_t key) const {
      key ^= key >> 33;
      key *= 0xff51afd7ed558ccd;
      key ^= key >> 33;
      key *= 0xc4ceb9fe1a85ec53;
      key ^= key >> 33;
      return key;
    }

    size_t operator()(const std::string& key) const {
      static const uint32_t c1 = 0xcc9e2d51;
      static const uint32_t c2 = 0x1b873593;
      static const uint64_t c3 = 0xff51afd7ed558ccd;

      static const size_t seed = dis(gen);

      size_t hash = seed;
      auto data = key.c_str();
      auto chunk = (const uint32_t*) data;
      auto lengthInBytes = key.size() * sizeof(char);
      auto blocks = lengthInBytes / 4;
      for(int i = 0; i < blocks; i++) {
        uint32_t k = *chunk;
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        hash ^= k;
        hash ^= hash >> 33;
        hash *= c3;

        chunk++;
      }

      auto tail = (const uint8_t*)(data + blocks * 4);
      uint32_t k = 0;
      switch(lengthInBytes & 3)
      {
        case 3: k ^= tail[2] << 16;
        case 2: k ^= tail[1] << 8;
        case 1: k ^= tail[0];
          k *= c1;
          k = (k << 15) | (k >> 17);
          k *= c2;
          hash ^= k;
          hash ^= hash >> 33;
          hash *= c3;
      };

      return hash;
    }
  };
  
  struct eqstr
  {
    bool operator()(const uint64_t s1, const uint64_t s2) const
    {
      return (s1 == s2);
    }

    bool operator()(const std::string& s1, const std::string& s2) const
    {
      return (s1 == s2);
    }
  };
};

#endif

