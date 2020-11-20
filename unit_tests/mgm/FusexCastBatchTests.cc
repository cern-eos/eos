//------------------------------------------------------------------------------
// File: FusexCastBatchTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "mgm/FuseServer/FusexCastBatch.hh"

TEST(FusexCastBatch, BasicFunctionality)
{
  eos::mgm::FusexCastBatch batch;
  int value = 0;
  batch.Register([ = ]() mutable { value++;});
  batch.Register([ = ]() mutable { value = +2;});
  ASSERT_EQ(2, batch.GetSize());
  batch.Execute();
  ASSERT_EQ(0, value);
  batch.Register([&]() {
    value++;
  });
  batch.Register([&]() {
    value += 2;
  });
  batch.Register([&]() {
    value += 3;
  });
  ASSERT_EQ(3, batch.GetSize());
  batch.Execute();
  ASSERT_EQ(6, value);
}
