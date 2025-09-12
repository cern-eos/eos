#include "common/concurrency/ThreadEpochCounter.hh"
#include <gtest/gtest.h>

TEST(ThreadEpochCounter, Basic)
{
  eos::common::ThreadEpochCounter counter;
  ASSERT_FALSE(counter.epochHasReaders(0));
  int epoch=1;
  auto tid = counter.increment(epoch, 1);
  ASSERT_TRUE(counter.epochHasReaders(epoch));
  ASSERT_EQ(counter.getReaders(tid), 1);
  counter.decrement();
  ASSERT_FALSE(counter.epochHasReaders(epoch));
}

TEST(ThreadEpochCounter, HashCollision)
{
  eos::common::ThreadEpochCounter counter;
  std::cout << "My local TID=" << eos::common::tlocalID.get() << "\n";
  ASSERT_FALSE(counter.epochHasReaders(0));
  std::array<std::atomic<int>, 4096> epoch_counter {0};
  std::vector<std::thread> threads;
  for (int i=0; i < 100; ++i) {
    threads.emplace_back([&counter, &epoch_counter, i](){
      int epoch = (i & 1);
      auto tid = counter.increment(epoch, 1);
      // sleep for a bit so that all threads run and we actually get different TIDs,
      // otherwise most of the threads
      // should complete before the other threads start, getting only a TID:1
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      std::cout << "Got TID=" << tid << " local tid= "
                << eos::common::tlocalID.get() << "\n";
      epoch_counter[tid]++;
      ASSERT_EQ(counter.getReaders(tid), epoch_counter[tid]);
    });
  }

  for (auto& t: threads) {
    t.join();
  }

}

TEST(ThreadEpochCounter, HashCollision2)
{
  eos::common::ThreadEpochCounter counter;
  std::cout << "My local TID=" << eos::common::tlocalID.get() << "\n";
  ASSERT_FALSE(counter.epochHasReaders(0));
  std::array<std::atomic<int>, 4096> epoch_counter {0};
  std::vector<std::thread> threads;
  for (int i=0; i < 1024; ++i) {
    threads.emplace_back([&counter, &epoch_counter, i](){
      int epoch = (i & 1);
      auto tid = counter.increment(epoch, 1);
      // sleep for a bit so that all threads run and we actually get different TIDs,
      // otherwise most of the threads
      // should complete before the other threads start, getting only a TID:1
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      std::cout << "Got TID=" << tid << " local tid= "
                << eos::common::tlocalID.get() << "\n";
      epoch_counter[tid]++;
      ASSERT_EQ(counter.getReaders(tid), epoch_counter[tid]);
    });
  }

  for (auto& t: threads) {
    t.join();
  }

}


TEST(VersionEpochCounter, Basic)
{
  eos::common::experimental::VersionEpochCounter counter;
  ASSERT_FALSE(counter.epochHasReaders(0));
  int epoch=1;
  auto tid = counter.increment(epoch, 1);
  ASSERT_TRUE(counter.epochHasReaders(epoch));
  ASSERT_EQ(counter.getReaders(tid), 1);
  counter.decrement(epoch);
  ASSERT_FALSE(counter.epochHasReaders(epoch));
}

TEST(VersionEpochCounter, MultiThreaded)
{
  eos::common::experimental::VersionEpochCounter<2> counter;
  ASSERT_FALSE(counter.epochHasReaders(0));
  std::array<std::atomic<int>, 2> epoch_counter = {0,0};
  std::vector<std::thread> threads;
  for (int i=0; i < 100; ++i) {
    threads.emplace_back([&counter, &epoch_counter, i](){
      int epoch = (i & 1);
      auto tid = counter.increment(epoch, 1);
      epoch_counter[tid]++;
    });
  }

  for (auto& t: threads) {
    t.join();
  }

  ASSERT_EQ(epoch_counter[0], counter.getReaders(0));
  ASSERT_EQ(epoch_counter[1], counter.getReaders(1));
}
