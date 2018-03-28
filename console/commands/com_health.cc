// ----------------------------------------------------------------------
// File com_health.cc
// Author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
// ----------------------------------------------------------------------

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

#include "HealthCommand.hh"
#include <iostream>

int com_health(char* arg1)
{
  HealthCommand health(arg1);

  try {
    if (wants_help(arg1)) {
      health.PrintHelp();
    } else {
      health.Execute();
    }
  } catch (std::string& e) {
    std::cout << "Error: " << e << std::endl;
  }

  return (0);
}
