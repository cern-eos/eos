// ----------------------------------------------------------------------
// File: com_clear.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
// ----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "console/ConsoleCliCommand.hh"
/*----------------------------------------------------------------------------*/

/* Clear the terminal screen */
int
com_clear (char *arg) {
  int rc = 0;
  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  ConsoleCliCommand clearCmd("clear", "is equivalent to the interactive shell "
                             "command to clear the screen");
  clearCmd.addOption(helpOption);

  clearCmd.parse(arg);

  if (clearCmd.hasValue("help"))
    clearCmd.printUsage();
  else
    rc = system("clear");

  return (rc); 
}
