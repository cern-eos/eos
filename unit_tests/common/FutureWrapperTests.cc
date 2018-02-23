//------------------------------------------------------------------------------
// File: FutureWrapperTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "Namespace.hh"
#include "common/FutureWrapper.hh"

EOSCOMMONTESTING_BEGIN

TEST(FutureWrapper, BasicSanity) {
  std::promise<int> promise;
  common::FutureWrapper<int> fut(promise.get_future());
  ASSERT_FALSE(fut.ready());

  promise.set_value(5);
  ASSERT_TRUE(fut.ready());
  ASSERT_EQ(fut.get(), 5);
}

TEST(FutureWrapper, Exception) {
  std::promise<int> promise;
  common::FutureWrapper<int> fut(promise.get_future());
  ASSERT_FALSE(fut.ready());

  promise.set_exception(std::make_exception_ptr(std::string("something terrible happened")));
  ASSERT_TRUE(fut.ready());

  try {
    fut.get();
    FAIL(); // should never reach here
  }
  catch(const std::string &exc) { // yes, you can use strings as exceptions
    ASSERT_EQ(exc, "something terrible happened");
  }
}

EOSCOMMONTESTING_END
