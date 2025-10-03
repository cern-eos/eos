//  File: color.hh
//  Author: Ilkay Yanar - 42Lausanne / CERN
//  ----------------------------------------------------------------------

/*************************************************************************
 *  EOS - the CERN Disk Storage System                                   *
 *  Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                       *
 *  This program is free software: you can redistribute it and/or modify *
 *  it under the terms of the GNU General Public License as published by *
 *  the Free Software Foundation, either version 3 of the License, or    *
 *  (at your option) any later version.                                  *
 *                                                                       *
 *  This program is distributed in the hope that it will be useful,      *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 *  GNU General Public License for more details.                         *
 *                                                                       *
 *  You should have received a copy of the GNU General Public License    *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 *************************************************************************/

#pragma once

//--------------------------------------------
/// File that defines all the colors for the terminal
//--------------------------------------------
# define C_RED "\001\033[0;31m\002"
# define C_GREEN "\001\033[0;32m\002"
# define C_YELLOW "\001\033[0;33m\002"
# define C_BLUE "\001\033[1;34m\002"
# define C_MAGENTA "\001\033[1;35m\002"
# define C_CYAN "\001\033[36m\002"
# define C_BLACK "\001\033[30m\002"
# define C_WHITE "\001\033[37m\002"
# define C_RESET "\001\033[0m\002"

//--------------------------------------------
/// Text effects definition
//--------------------------------------------
# define C_BOLD "\001\033[1m\002"
# define C_UNDERLINE "\001\033[4m\002"
# define C_BLINK "\001\033[5m\002"
# define C_REVERSE "\001\033[7m\002"
# define C_INVISIBLE "\001\033[8m\002"
# define C_STRIKETHROUGH "\001\033[9m\002"
# define C_CLEAR "\001\033[2J\001\033[1;1H"

//--------------------------------------------
/// Background colors definition
//--------------------------------------------
# define C_BBLACK "\001\033[40m\002"
# define C_BWHITE "\001\033[47m\002"
# define C_BRED "\001\033[41m\002"
# define C_BGREEN "\001\033[42m\002"
# define C_BYELLOW "\001\033[43m\002"
# define C_BBLUE "\001\033[44m\002"
# define C_BMAGENTA "\001\033[45m\002"
# define C_BCYAN "\001\033[46m\002"
