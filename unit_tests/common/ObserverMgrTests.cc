#include "common/ObserverMgr.hh"
#include "common/StringUtils.hh"
#include "gtest/gtest.h"
#include <iostream>
#include <folly/executors/IOThreadPoolExecutor.h>

using eos::common::ObserverMgr;
using eos::common::observer_tag_t;

TEST(ObserverMgr, NotifyChangeSync)
{
  ObserverMgr<int> mgr;
  int gval = 0;
  int gval2 = 0;
  ASSERT_NO_THROW(mgr.notifyChangeSync(0));
  auto tag1 = mgr.addObserver([&gval](int i) {
    gval += i;
  });
  auto tag2 = mgr.addObserver([&gval2](int i) {
    gval2 += 2 * i;
  });
  auto tag3 = mgr.addObserver([](int i) {
    std::cout << "You've a new message: " << i << "\n";
  });
  mgr.notifyChangeSync(1);
  ASSERT_EQ(gval, 1);
  ASSERT_EQ(gval2, 2);
  mgr.notifyChangeSync(2);
  ASSERT_EQ(gval, 3);
  ASSERT_EQ(gval2, 6);
  mgr.rmObserver(tag2);
  mgr.notifyChangeSync(3);
  ASSERT_EQ(gval, 6);
  ASSERT_EQ(gval2, 6);
  mgr.rmObserver(tag1);
  ASSERT_NO_THROW(mgr.notifyChangeSync(100));
  ASSERT_EQ(gval, 6);
  ASSERT_EQ(gval2, 6);
  mgr.rmObserver(tag3);
  ASSERT_NO_THROW(mgr.notifyChangeSync(101));
}

TEST(ObserverMgr, SimpleAsync)
{
  ObserverMgr<int> mgr;
  std::atomic<int> gval {0};
  std::atomic<int> gval2 {0};
  ASSERT_NO_THROW(mgr.notifyChange(0));
  auto tag1 = mgr.addObserver([&gval](int i) {
    gval += i;
  });
  auto tag2 = mgr.addObserver([&gval2](int i) {
    gval2 += 2 * i;
  });
  auto tag3 = mgr.addObserver([](int i) {
    std::cout << "You've a new message: " << i << "\n";
  });
  mgr.notifyChange(1);
  mgr.notifyChange(2);
  // NOTE: This is not meant to be called in normal code unless really necessary
  // to drain all pending jobs in the ObserverMgr. This is a blocking call.
  // We only do this in tests to ensure that we can see the values
  mgr.syncAllNotifications();
  ASSERT_EQ(gval, 3);
  ASSERT_EQ(gval2, 6);
  mgr.rmObserver(tag2);
  mgr.notifyChange(3);
  mgr.rmObserver(tag1);
  ASSERT_NO_THROW(mgr.notifyChange(100));
  mgr.syncAllNotifications();
  ASSERT_EQ(gval.load(), 6);
  ASSERT_EQ(gval2.load(), 6);
  mgr.rmObserver(tag3);
  ASSERT_NO_THROW(mgr.notifyChange(101));
}

TEST(ObserverMgr, observer_tag_t)
{
  observer_tag_t default_tag {};
  ASSERT_FALSE(default_tag);
}

TEST(ObserverMgr, moveArguments)
{
  ObserverMgr<std::string> mgr;
  // A simple string observers that expect 9 character messages! "message 1,2... "
  // We use std::string as we'll know in case of multiple observers that we'll
  // end up passing the argument by value correctly as otherwise the string will be
  // emptied in case of a move
  auto obs_strlen = mgr.addObserver([](std::string s) {
    EXPECT_EQ(s.size(), 9);
    std::cout << "Observer 1 (value) received msg=" << s << std::endl;
  });
  auto obs_startswith = mgr.addObserver([](std::string s) {
    EXPECT_TRUE(eos::common::startsWith(s, "message "));
    std::cout << "Observer 2 (value) received msg=" << s << std::endl;
  });
  int ctr {0};
  auto gen_string = [&ctr]() {
    std::string s = "message " + std::to_string(ctr++);
    return s;
  };
  // 2 Observers
  std::cout << "Starting with 2 observers!" << std::endl;
  ASSERT_NO_THROW(mgr.notifyChange(gen_string()));
  auto tag3 = mgr.addObserver([](std::string && s) {
    EXPECT_EQ(s.size(), 9);
    std::cout << "Observer 3 (r-value ref) received msg=" << s << std::endl;
  });
  // This is likely std::function's magic? ideally conversions shouldn't be allowed
  // however it looks like if the arg. is constructable from the arg_type
  // it might just work
  auto tag4 = mgr.addObserver([](std::string_view s) {
    EXPECT_EQ(s.size(), 9);
    std::cout << "Observer 4 (stringview) received msg=" << s << std::endl;
  });
  mgr.syncAllNotifications();
  std::cout << "Adding 2 more observers! Total: 4" << std::endl;
  // 4 Observers
  ASSERT_NO_THROW(mgr.notifyChange(gen_string()));
  auto tag5 = mgr.addObserver([](const std::string & s) {
    EXPECT_EQ(s.size(), 9);
    std::cout << "Observer 5 (c-ref) recieved msg=" << s << std::endl;
  });
  mgr.syncAllNotifications();
  std::cout << "Adding 1 more observer! Total: 5" << std::endl;
  // 5 Observers
  ASSERT_NO_THROW(mgr.notifyChange(gen_string()));
  mgr.rmObserver(obs_startswith);
  mgr.syncAllNotifications();
  std::cout << "Dropping startswith observer! Total: 4; sending testermsg twice"
            << std::endl;
  std::string msg = "testermsg";
  ASSERT_NO_THROW(mgr.notifyChange(msg));
  ASSERT_NO_THROW(mgr.notifyChange(std::move(msg)));
  // msg is now moved! While impl defined, all implementations usually empty the string on move
  EXPECT_TRUE(msg.empty());
  mgr.rmObserver(obs_strlen);
  mgr.syncAllNotifications();
  std::cout << "Dropping 1 observer! Total: 3" << std::endl;
  // 3 Observers
  ASSERT_NO_THROW(mgr.notifyChange(gen_string()));
  ASSERT_NO_THROW(mgr.notifyChange("randommsg"));
  mgr.syncAllNotifications();
  std::cout << "Dropping 1 observer! Total: 2" << std::endl;
  // 2 Observers
  mgr.rmObserver(tag4);
  ASSERT_NO_THROW(mgr.notifyChange(gen_string()));
  mgr.rmObserver(tag3);
  // 1 Observer
  mgr.syncAllNotifications();
  std::cout << "Dropping 1 observer! Total: 1" << std::endl;
  ASSERT_NO_THROW(mgr.notifyChange("some9char"));
  mgr.rmObserver(tag5);
  // Now there should be no one listening! Should hit the 9 char violation in
  // case anyone listens
  ASSERT_NO_THROW(mgr.notifyChange("A tree fell in a forest!!!"));
}

TEST(ObserverMgr, notifyMultiThreaded)
{
  // This test will crash before EOS-6357 is merged
   ObserverMgr<std::string> mgr;
   auto obs_startswith = mgr.addObserver([](std::string s) {
      ASSERT_TRUE(eos::common::startsWith(s, "message "));
   });
  std::atomic<int> ctr {0};
  auto gen_string = [&ctr]() {
    std::string s = "message " + std::to_string(ctr++);
    return s;
  };
  std::vector<std::thread> threads;
  for (int i=0; i< 100; ++i) {
    threads.emplace_back([&mgr, &gen_string]() {
      for (int i=0; i< 100; ++i) {
        mgr.notifyChange(gen_string());
      }
    });
  }
  for (auto &t: threads) {
    t.join();
  }

  ASSERT_EQ(ctr,10000);
  mgr.rmObserver(obs_startswith);
}

TEST(ObserverMgr, moveArgumentsSync)
{
  ObserverMgr<std::string> mgr;
  // A simple string observers that expect 9 character messages! "message 1,2... "
  // We use std::string as we'll know in case of multiple observers that we'll
  // end up passing the argument by value correctly as otherwise the string will be
  // emptied in case of a move
  auto obs_strlen = mgr.addObserver([](std::string s) {
    EXPECT_EQ(s.size(), 9);
  });
  auto obs_startswith = mgr.addObserver([](std::string s) {
    EXPECT_TRUE(eos::common::startsWith(s, "message "));
  });
  int ctr {0};
  auto gen_string = [&ctr]() {
    std::string s = "message " + std::to_string(ctr++);
    return s;
  };
  // 2 Observers
  ASSERT_NO_THROW(mgr.notifyChangeSync(gen_string()));
  auto tag3 = mgr.addObserver([](std::string && s) {
    EXPECT_EQ(s.size(), 9);
  });
  // This is likely std::function's magic? ideally conversions shouldn't be allowed
  // however it looks like if the arg. is constructable from the arg_type
  // it might just work
  auto tag4 = mgr.addObserver([](std::string_view s) {
    EXPECT_EQ(s.size(), 9);
  });
  mgr.syncAllNotifications();
  // 4 Observers
  ASSERT_NO_THROW(mgr.notifyChangeSync(gen_string()));
  auto tag5 = mgr.addObserver([](const std::string & s) {
    EXPECT_EQ(s.size(), 9);
  });
  // 5 Observers
  ASSERT_NO_THROW(mgr.notifyChangeSync(gen_string()));
  mgr.rmObserver(obs_startswith);
  std::string msg = "testermsg";
  ASSERT_NO_THROW(mgr.notifyChangeSync(msg));
  ASSERT_NO_THROW(mgr.notifyChangeSync(std::move(msg)));
  // msg is now moved! While impl defined, all implementations usually empty the string on move
  EXPECT_TRUE(msg.empty());
  mgr.rmObserver(obs_strlen);
  // 3 Observers
  ASSERT_NO_THROW(mgr.notifyChangeSync(gen_string()));
  ASSERT_NO_THROW(mgr.notifyChangeSync("randommsg"));
  // 2 Observers
  mgr.rmObserver(tag4);
  ASSERT_NO_THROW(mgr.notifyChangeSync(gen_string()));
  mgr.rmObserver(tag3);
  // 1 Observer
  ASSERT_NO_THROW(mgr.notifyChangeSync("some9char"));
  mgr.rmObserver(tag5);
  // Now there should be no one listening! Should hit the 9 char violation in
  // case anyone listens
  ASSERT_NO_THROW(mgr.notifyChangeSync("A tree fell in a forest!!!"));
}
