#include "common/ObserverMgr.hh"
#include "gtest/gtest.h"
#include <iostream>
#include <folly/executors/IOThreadPoolExecutor.h>

using eos::common::ObserverMgr;

TEST(ObserverMgr, NotifyChangeSync)
{
  ObserverMgr<int> mgr;
  int gval=0;
  int gval2 = 0;
  ASSERT_NO_THROW(mgr.notifyChangeSync(0));

  auto tag1 = mgr.addObserver([&gval](int i){
    gval += i;
  });

  auto tag2 = mgr.addObserver([&gval2](int i) {
    gval2 += 2*i;
  });

  auto tag3 = mgr.addObserver([](int i) {
    std::cout << "You've a new message: "<< i << "\n";
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

  auto tag1 = mgr.addObserver([&gval](int i){
    gval += i;
  });

  auto tag2 = mgr.addObserver([&gval2](int i) {
    gval2 += 2*i;
  });

  auto tag3 = mgr.addObserver([](int i) {
    std::cout << "You've a new message: "<< i << "\n";
  });

  mgr.notifyChange(1);
  mgr.notifyChange(2);

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