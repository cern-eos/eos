//------------------------------------------------------------------------------
// File: TableFormatting.hh
// Author: Ivan Arizanovic & Stefan Isidorovic - Comtrade
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef __TABLE__FORMATTING__HH__
#define __TABLE__FORMATTING__HH__

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <tuple>

//! Forward declaration
class TableCell;

//------------------------------------------------------------------------------
//! Typedefs for easier reading and understanding
//------------------------------------------------------------------------------
using HeadCell = std::tuple<std::string, unsigned, std::string>;
using TableHeader = std::vector<HeadCell>;
using TableRow = std::vector<TableCell>;
using TableData = std::vector<TableRow>;
using TableString = std::vector<std::string>;

enum TableFormatterColor {
  NONE         = 0,
  //Normal display
  DEFAULT, // used to finish off any of the below formats
  RED,
  GREEN,
  YELLOW,
  BLUE,
  MARGARITA,
  CYAN,
  WHITE,
  //Bold display (B...)
  BOLD,
  BRED,
  BGREEN,
  BYELLOW,
  BBLUE,
  BMARGARITA,
  BCYAN,
  BWHITE,
  //Dark display (D...)
  DARK,
  DRED,
  DGREEN,
  DYELLOW,
  DBLUE,
  DMARGARITA,
  DCYAN,
  DWHITE,
  //Bold font with white BackGround (B..._BGWHITE)
  BRED_BGWHITE,
  BGREEN_BGWHITE,
  BYELLOW_BGWHITE,
  BBLUE_BGWHITE,
  BMARGARITA_BGWHITE,
  BCYAN_BGWHITE,
  //Bold white font with BackGround (BWHITE_BG...)
  BWHITE_BGRED,
  BWHITE_BGGREEN,
  BWHITE_BGYELLOW,
  BWHITE_BGBLUE,
  BWHITE_BGMARGARITA,
  BWHITE_BGCYAN,
  //Bold yellow font with BackGround (BYELLOW_BG...)
  BYELLOW_BGRED,
  BYELLOW_BGGREEN,
  BYELLOW_BGBLUE,
  BYELLOW_BGMARGARITA,
  BYELLOW_BGCYAN
};

enum TableFormatterStyle {
  FULL,
  FULLBOLD,
  FULLDOUBLE,
  HEADER,
  HEADER2,
  HEADERBOLD,
  HEADERDOUBLE,
  MINIMAL,
  OLD,
  OLDWIDE
};

//------------------------------------------------------------------------------
//! Operators
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& stream, const TableCell& cell);

#endif //__TABLE__FORMATTING__HH__
