//------------------------------------------------------------------------------
// File: AtomicUniquePtrTests.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/concurrency/AtomicUniquePtr.h"
#include <thread>
#include <mutex>
#include <gtest/gtest.h>

TEST(AtomicUniquePtr, Basic)
{
  eos::common::atomic_unique_ptr<int> p(new int(1));
  EXPECT_EQ(*p.get(), 1);
  auto old_ptr = p.release();
  EXPECT_EQ(*old_ptr, 1);
  EXPECT_EQ(p.get(), nullptr);
  std::unique_ptr<int> old_ptr_guard(old_ptr);
}

TEST(AtomicUniquePtr, Reset)
{
  eos::common::atomic_unique_ptr<int> p(new int(1));
  EXPECT_EQ(*p.get(), 1);
  auto old_ptr = p.reset(new int(2));
  std::shared_ptr<int> old_ptr_guard(old_ptr);
  EXPECT_EQ(*p.get(), 2);
  EXPECT_EQ(*old_ptr_guard.get(), 1);
}

TEST(AtomicUniquePtr, MoveCtor)
{
  eos::common::atomic_unique_ptr<int> p1(new int(1));
  eos::common::atomic_unique_ptr<int> p2(std::move(p1));
  EXPECT_EQ(*p2.get(), 1);
  EXPECT_EQ(p1.get(), nullptr);
}

TEST(AtomicUniquePtr, reset_from_null)
{
  eos::common::atomic_unique_ptr<int> p;
  EXPECT_EQ(p.get(), nullptr);
  p.reset_from_null(new int(1));
  EXPECT_EQ(*p.get(), 1);
}

TEST(AtomicUniquePtr, MemberAccessOperator)
{
  struct A  {
    std::string data;
    explicit A(std::string d) : data(d) {}
  };
  eos::common::atomic_unique_ptr<A> p(new A("hello"));
  EXPECT_EQ(p->data, "hello");
}

TEST(AtomicUniquePtr, VectorOfAtomics)
{
  std::vector<eos::common::atomic_unique_ptr<int>> v;
  v.emplace_back(new int(1));
  v.emplace_back(new int(2));
  v.emplace_back(new int(3));
  EXPECT_EQ(*v[0].get(), 1);
  EXPECT_EQ(*v[1].get(), 2);
  EXPECT_EQ(*v[2].get(), 3);
}

TEST(AtomicUniquePtr, SimpleGC)
{
  std::vector<eos::common::atomic_unique_ptr<int>> v;
  eos::common::atomic_unique_ptr p(new int(1));
  int * p_ = p.reset(new int(2));   // p_ points to 1
  v.emplace_back(p_);
  EXPECT_EQ(*p.get(), 2);
  EXPECT_EQ(*v[0].get(), 1); // v[0] points to 1
}


TEST(AtomicUniquePtr, multireadwrite)
{

  std::mutex gc_mtx;
  std::vector<std::unique_ptr<std::string>> old_ptrs;
  old_ptrs.reserve(20'000'000);
  eos::common::atomic_unique_ptr<std::string> p(new std::string("start"));
  auto writer_fn = [&p, &old_ptrs, &gc_mtx]() {
    auto tid_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
    for (int i = 0; i<100'000; ++i) {
      std::string new_str = "greetings from thread" + std::to_string(tid_hash);
      auto old_ptr = p.reset(new std::string(new_str));
      std::scoped_lock lock(gc_mtx);
      old_ptrs.emplace_back(old_ptr);
    }
    //std::cout << "Done with writer="<<std::this_thread::get_id() << "\n";
  };

  auto reader_fn = [&p] {
    for (int i=0; i<1'000'000; ++i) {
      auto* q = p.get();
      ASSERT_TRUE(q);
    }
    //std::cout << "Done with reader="<< std::this_thread::get_id() << "\n";
  };

  std::vector<std::thread> reader_threads;
  std::vector<std::thread> writer_threads;
  for (int i=0;i<2000;i++) {
    reader_threads.emplace_back(reader_fn);
    if (i%10==0) {
      writer_threads.emplace_back(writer_fn);
    }
  }

  for (auto& t: reader_threads) {
    t.join();
  }

  for (auto& t: writer_threads) {
    t.join();
  }

}

TEST(SharedPtrNonTSSEGV, multireadwrite)
{
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_DEATH(
  {
    std::shared_ptr<std::string> p(new std::string("start"));
    auto writer_fn = [&p]() {
      auto tid_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
      for (int i = 0; i < 100'000; ++i) {
        std::string new_str =
            "greetings from thread" + std::to_string(tid_hash);
        p.reset(new std::string(new_str));
      }
      // std::cout << "Done with writer="<<std::this_thread::get_id() << "\n";
    };

    auto reader_fn = [&p] {
      for (int i = 0; i < 1'000'000; ++i) {
        auto q = p;
        ASSERT_TRUE(q);
      }
      // std::cout << "Done with reader="<< std::this_thread::get_id() << "\n";
    };

    std::vector<std::thread> reader_threads;
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < 2000; i++) {
      reader_threads.emplace_back(reader_fn);
      if (i % 10 == 0) {
        writer_threads.emplace_back(writer_fn);
      }
    }

    for (auto& t : reader_threads) {
      t.join();
    }

    for (auto& t : writer_threads) {
      t.join();
    }
  }
      , ".*");
}

struct MyDataSP {
  std::shared_ptr<std::string> get_data() {
    std::shared_ptr<std::string> data_copy(data);
    return data_copy;
  }

  void reset(std::string* new_val) {
    std::scoped_lock wlock(mtx);
    data.reset(new_val);
  }

  MyDataSP(std::string&& val) : data(std::make_shared<std::string>(val)) {}

private:
  std::shared_ptr<std::string> data;
  std::mutex mtx;
};



TEST(SharedPtrNonTS2SEGV, multireadwrite)
{
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_DEATH({
    MyDataSP p("start");
    auto writer_fn = [&p]() {
      auto tid_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
      for (int i = 0; i < 100'000; ++i) {
        std::string new_str =
            "greetings from thread" + std::to_string(tid_hash);
        p.reset(new std::string(new_str));
      }
      // std::cout << "Done with writer="<<std::this_thread::get_id() << "\n";
    };

    auto reader_fn = [&p] {
      for (int i = 0; i < 1'000'000; ++i)
        ASSERT_TRUE(p.get_data());
      // std::cout << "Done with reader="<< std::this_thread::get_id() << "\n";
    };

    std::vector<std::thread> reader_threads;
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < 2000; i++) {
      reader_threads.emplace_back(reader_fn);
      if (i % 10 == 0) {
        writer_threads.emplace_back(writer_fn);
      }
    }

    for (auto& t : reader_threads) {
      t.join();
    }

    for (auto& t : writer_threads) {
      t.join();
    }
               }, ".*");
}

struct MyDataAtomicSP {
  std::shared_ptr<std::string> get_data() {
    return std::atomic_load_explicit(&data, std::memory_order_acquire);
  }

  void reset(std::string* new_val) {
    std::shared_ptr<std::string> new_data(new_val);
    std::atomic_store_explicit(&data, new_data, std::memory_order_release);
  }
  MyDataAtomicSP(std::string&& val) : data(std::make_shared<std::string>(val)) {}

private:
  std::shared_ptr<std::string> data;
};

TEST(SharedPtrTS, multireadwrite)
{

  MyDataAtomicSP p("start");
  auto writer_fn = [&p]() {
    auto tid_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
    for (int i = 0; i<100'000; ++i) {
      std::string new_str = "greetings from thread" + std::to_string(tid_hash);
      p.reset(new std::string(new_str));
    }
    //std::cout << "Done with writer="<<std::this_thread::get_id() << "\n";
  };

  auto reader_fn = [&p] {
    for (int i=0; i<1'000'000; ++i)
      ASSERT_TRUE(p.get_data());
    //std::cout << "Done with reader="<< std::this_thread::get_id() << "\n";
  };

  std::vector<std::thread> reader_threads;
  std::vector<std::thread> writer_threads;
  for (int i=0;i<2000;i++) {
    reader_threads.emplace_back(reader_fn);
    if (i%10==0) {
      writer_threads.emplace_back(writer_fn);
    }
  }

  for (auto& t: reader_threads) {
    t.join();
  }

  for (auto& t: writer_threads) {
    t.join();
  }

}
