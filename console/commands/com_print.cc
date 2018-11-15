// ----------------------------------------------------------------------
// File: com_print.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include <string.h>
#include <sstream>
#include <iomanip>
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Print help text for ARG or print help text
// for all of the commands if ARG is not present
//------------------------------------------------------------------------------
int
com_help (char *arg)
{
  std::string sarg;
  int printed = 0;

  // Unquote argument
  std::stringstream ss;
  ss << arg;
  ss >> std::quoted(sarg);

  // Print specific help text or print all commands if null argument
  for (int i = 0; commands[i].name; i++) {
    if (!*arg || (strcmp(sarg.c_str(), commands[i].name) == 0)) {
      printf("%-20s %s\n", commands[i].name, commands[i].doc);
      printed++;
    }
  }

  if (!printed) {
    printf("No commands match '%s'. Possibilities are:\n", sarg.c_str());

    for (int i = 0; commands[i].name; i++) {
      /* Print in six columns. */
      if (printed == 6) {
        printed = 0;
        printf("\n");
      }

      printf("%-12s", commands[i].name);
      printed++;
    }

    if (printed) { printf("\n"); }
  }

  return 0;
}
