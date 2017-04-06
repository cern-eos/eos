#ifndef __MURMUR3__HH__
#define __MURMUR3__HH__

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
  };
  
  struct eqstr
  {
    bool operator()(const uint64_t s1, const uint64_t s2) const
    {
      return (s1 == s2);
    }
  };
};

#endif

