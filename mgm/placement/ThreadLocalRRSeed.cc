
#include "ThreadLocalRRSeed.hh"
#include "common/Logging.hh"
#include "utils/RandUtils.hh"

namespace eos::mgm::placement {

thread_local std::vector<uint64_t> ThreadLocalRRSeed::gRRSeeds(kDefaultMaxRRSeeds,0);

void
ThreadLocalRRSeed::init(size_t max_items, bool randomize)
{
  gRRSeeds.resize(max_items, 0);
  if (randomize) {
    for (size_t i = 0; i < max_items; i++) {
      gRRSeeds[i] = eos::common::getRandom(0ul, max_items);
    }
  }

}

void
ThreadLocalRRSeed::resize(size_t max_items, bool randomize)
{
  auto old_size = gRRSeeds.size();
  gRRSeeds.resize(max_items, 0);
  if (randomize) {
    for (size_t i = old_size; i < max_items; i++) {
      gRRSeeds[i] = eos::common::getRandom(0ul, max_items);
    }
  }
}

uint64_t
ThreadLocalRRSeed::get(size_t index, size_t n_items)
{
  if (index >= gRRSeeds.size()) {
    eos_static_crit("index %lu is out of range %lu", index, gRRSeeds.size());
    return 0;
  }
  uint64_t ret = gRRSeeds[index];
  gRRSeeds[index] += n_items;
  return ret;
}

} // namespace eos::mgm::placement
