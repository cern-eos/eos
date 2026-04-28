#pragma once
#include "common/utils/RandUtils.hh"

namespace Murmur3
{
  // Poisoned generic specialization, which cannot be used, where poisoned means
  // the same thing as with std::hash -> https://en.cppreference.com/w/cpp/utility/hash
  template<typename T>
  struct MurmurHasher {
  };

  // uint64_t specialization
  template<>
  struct MurmurHasher<uint64_t> {
    size_t operator()(uint64_t key) const noexcept
    {
      key ^= key >> 33;
      key *= 0xff51afd7ed558ccd;
      key ^= key >> 33;
      key *= 0xc4ceb9fe1a85ec53;
      key ^= key >> 33;
      return key;
    }
  };

  // std::string specialization
  template<>
  struct MurmurHasher<std::string> {
    size_t operator()(const std::string& key) const noexcept
    {
      static const size_t seed = eos::common::getRandom64();
      static const uint32_t c1 = 0xcc9e2d51;
      static const uint32_t c2 = 0x1b873593;
      static const uint64_t c3 = 0xff51afd7ed558ccd;
      size_t hash = seed;
      auto data = key.c_str();
      auto chunk = (const uint32_t*) data;
      auto lengthInBytes = key.size() * sizeof(char);
      auto blocks = lengthInBytes / 4;

      for (size_t i = 0; i < blocks; i++) {
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

      switch (lengthInBytes & 3) {
      case 3:
        k ^= tail[2] << 16;
        /* fallthrough */

      case 2:
        k ^= tail[1] << 8;
        /* fallthrough */

      case 1:
        k ^= tail[0];
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
};
