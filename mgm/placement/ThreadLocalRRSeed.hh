#ifndef EOS_THREADLOCALRRSEED_HH
#define EOS_THREADLOCALRRSEED_HH

#include <vector>
#include <cstdint>
#include <cstddef>
#include <random>

namespace eos::mgm::placement {

constexpr size_t kDefaultMaxRRSeeds = 1024;


/*A thread local version of RRSeed, in the scheduler context, we don't want
 * the seeds to all start at 0, we initialize with random numbers at first */
struct ThreadLocalRRSeed {
  static uint64_t get(size_t index, size_t n_items);

  static void init(size_t max_items, bool randomize = true);

  static void resize(size_t max_items, bool randomize = true);

  static thread_local std::vector<uint64_t> gRRSeeds;
  static std::mt19937 random_gen;
  static std::random_device rd;
};



} // namespace eos::mgm::placement
#endif // EOS_THREADLOCALRRSEED_HH
