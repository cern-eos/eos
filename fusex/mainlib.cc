//------------------------------------------------------------------------------
//! @file main.cc
//! @author Andreas-Joachim Peters
//! @brief EOS C++ Fuse eosd executable
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "eosfuse.hh"

int
main(int argc, char* argv[])
{
  EosFuse& eosfuse = EosFuse::instance();
  eosfuse.run(argc, argv, NULL, true);

  for (auto i = 0; i< 100; ++i) {
    sleep (1);
    fprintf(stderr,".");
    fflush(stderr);
  }
}
