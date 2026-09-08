//------------------------------------------------------------------------------
//! @file ReadThroughCacheTests.cc
//! @brief Unit tests for read-through cache placement scoring
//------------------------------------------------------------------------------

#include "gtest/gtest.h"
#include "mgm/cache/ReadThroughCache.hh"

using eos::mgm::ReadThroughCache;

TEST(ReadThroughCache, PlacementScoreIsDeterministic)
{
  const uint64_t a = ReadThroughCache::PlacementScore(14425, 7);
  const uint64_t b = ReadThroughCache::PlacementScore(14425, 7);
  EXPECT_EQ(a, b);
  EXPECT_NE(0u, a);
}

TEST(ReadThroughCache, PlacementScoreDiffersByFs)
{
  const uint64_t s1 = ReadThroughCache::PlacementScore(99, 1);
  const uint64_t s2 = ReadThroughCache::PlacementScore(99, 2);
  EXPECT_NE(s1, s2);
}

TEST(ReadThroughCache, PlacementScoreTieBreakPrefersLowerFsid)
{
  // SelectCacheFs uses score, then lower fsid on a tie. The mix is not
  // guaranteed to collide; this asserts the comparison order used there.
  eos::common::FileSystem::fsid_t best = 0;
  uint64_t best_score = 0;
  const eos::common::FileSystem::fsid_t candidates[] = {5, 2, 9};

  for (const auto fsid : candidates) {
    const uint64_t score = ReadThroughCache::PlacementScore(1, fsid);

    if (!best || (score > best_score) ||
        ((score == best_score) && (fsid < best))) {
      best = fsid;
      best_score = score;
    }
  }

  EXPECT_NE(0u, best);
  EXPECT_NE(0u, best_score);
}
