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
using Row = std::vector<TableCell>;
using TableData = std::vector<Row>;
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
  BDEFAULT,
  BRED,
  BGREEN,
  BYELLOW,
  BBLUE,
  BMARGARITA,
  BCYAN,
  BWHITE,
  //Normal display with white BackGround (BG...)
  BGDEFAULT,
  BGRED,
  BGGREEN,
  BGYELLOW,
  BGBLUE,
  BGMARGARITA,
  BGCYAN,
  BGWHITE,
  //Bold display with white BackGround (BBG...)
  BBGDEFAULT,
  BBGRED,
  BBGGREEN,
  BBGYELLOW,
  BBGBLUE,
  BBGMARGARITA,
  BBGCYAN,
  BBGWHITE
};

enum TableFormatterStyle {
  FULL          =  0,
  FULLBOLD      =  1,
  FULLDOUBLE    =  2,
  HEADER        =  3,
  HEADERBOLD    =  4,
  HEADERDOUBLE  =  5,
  MINIMAL       =  6,
  OLD           =  7,
  OLDWIDE       =  8,
  HEADER_QUOTA  =  9
};

//------------------------------------------------------------------------------
//! Operators
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& stream, const TableCell& cell);

#endif //__TABLE__FORMATTING__HH__
