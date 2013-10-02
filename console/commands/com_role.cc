// ----------------------------------------------------------------------
// File: com_role.cc
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
/*----------------------------------------------------------------------------*/

/* Set the client user and group role */
int
com_role (char *arg)
{
  ConsoleCliCommand roleCmd("role", "select user role <user-role> [and group "
                            "role <group-role>]");
  roleCmd.addOptions({{"user-role", "can be a virtual user ID (unsigned int) "
                       "or a user mapping alias", 1, 1, "<user-role>", true},
                      {"group-role", "can be a virtual group ID (unsigned int) "
                       "or a group mapping alias", 2, 1, "<group-role>", false}
                     });

  addHelpOptionRecursively(&roleCmd);

  roleCmd.parse(arg);

  if (checkHelpAndErrors(&roleCmd))
    return 0;

  user_role = roleCmd.getValue("user-role").c_str();

  if (roleCmd.hasValue("group-role"))
    group_role = roleCmd.getValue("group-role").c_str();

  if (!silent)
    fprintf(stdout, "=> selected user role ruid=<%s> and group role rgid=<%s>\n", user_role.c_str(), group_role.c_str());

  return (0);
}
