//------------------------------------------------------------------------------
// File: login-identifier.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include "auth/LoginIdentifier.hh"

TEST(LoginIdentifier, BasicSanity) {
  ASSERT_EQ(LoginIdentifier(1).getStringID(), "AAAAAAAE");
  ASSERT_EQ(LoginIdentifier(2).getStringID(), "AAAAAAAI");
  ASSERT_EQ(LoginIdentifier(3).getStringID(), "AAAAAAAM");

  ASSERT_EQ(LoginIdentifier(1000, 1000, 178, 3).getStringID(), "*APoA-gM");

  ASSERT_EQ(LoginIdentifier(41).getStringID(), "AAAAAACk");
}
