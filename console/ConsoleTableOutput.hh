//------------------------------------------------------------------------------
//! @file ConsoleTableOutput.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
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

#ifndef __CONSOLETABLEOUTPUT__HH__
#define __CONSOLETABLEOUTPUT__HH__

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

typedef std::vector<std::pair<std::string, unsigned>> HeaderInfo;
typedef std::vector<std::string> ColorVec;

//------------------------------------------------------------------------------
//! Class ConsoleTableOutput
//!
//! @description Implementig logic for various table printing on console
//------------------------------------------------------------------------------
class ConsoleTableOutput
{

  ColorVec m_colors; //< Supported colors
  HeaderInfo m_header; //< Header of table
  std::stringstream m_output; //< Containing output of class
  size_t m_curr_field; //< current field of header
  std::string m_separator;

  //----------------------------------------------------------------------------
  //! Doing processing of a row, calculating widths and printing
  //!
  //! @param T Value to be printed in table cell
  //----------------------------------------------------------------------------
  template<typename T>
  void ProcessRow(const T& t)
  {
    if (m_curr_field == m_header.size()) {
      throw std::string("Row has more items than it should!");
    }

    m_output.fill(' ');
    m_output.width(m_header[m_curr_field].second);
    m_output << t << m_colors[ConsoleTableOutput::DEFAULT];
    ++m_curr_field;
  }

public:
  //----------------------------------------------------------------------------
  //! @enum Color
  //!
  //! Supported coloring.
  //----------------------------------------------------------------------------
  enum Color {
    RED      = 0,
    GREEN    = 1,
    YELLOW   = 2,
    DEFAULT  = 3
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConsoleTableOutput()
    : m_curr_field(0)
  {
    m_colors = ColorVec{"\33[31m", "\33[32m", "\33[33m", "\33[0m"};
  }

  //----------------------------------------------------------------------------
  //! Setting header data
  //!
  //! @param HeaderInfo header data
  //----------------------------------------------------------------------------
  void SetHeader(HeaderInfo heads)
  {
    m_header = heads;
    int sum = 0;

    for (auto item = m_header.begin(); item != m_header.end(); ++item) {
      sum += item->second;
    }

    m_separator = std::string(sum + 2, '-');
    m_output << m_separator << std::endl;

    for (auto item = m_header.begin(); item != m_header.end(); ++item) {
      m_output.fill(' ');
      m_output.width(item->second);
      m_output << item->first;
    }

    m_output << std::endl;
    m_output << m_separator << std::endl;
  }

  //----------------------------------------------------------------------------
  //! Coloring name of first column in table
  //----------------------------------------------------------------------------
  template <typename T>
  T Colorify(ConsoleTableOutput::Color c, const T t)
  {
    m_output << ConsoleTableOutput::m_colors[c];
    return t;
  }

  //----------------------------------------------------------------------------
  //! Adding row to "table"
  //!
  //! @param T First parameter
  //! @param ...Args Rest of parameters
  //----------------------------------------------------------------------------
  template<typename T, typename... Args>
  void AddRow(const T& t, const Args& ... args)
  {
    ProcessRow<T>(t);
    AddRow(args...);
  }

  //----------------------------------------------------------------------------
  //! Base case for AddRow variadic template
  //!
  //! @param T Last/only parameter
  //----------------------------------------------------------------------------
  template<typename T>
  void AddRow(const T& t)
  {
    ProcessRow<T>(t);
    m_curr_field = 0;
    m_output << std::endl;
  }

  //----------------------------------------------------------------------------
  //! Support for adding custom row, without enforcing header given data
  //!
  //! @param pair<T,int> First parameter
  //! @param ...Args Rest of parameters
  //----------------------------------------------------------------------------
  template<typename T, typename... Args>
  void CustomRow(const std::pair<T, int>& t, const Args& ... args)
  {
    m_output.fill(' ');
    m_output.width(t.second);
    m_output << t.first << m_colors[ConsoleTableOutput::DEFAULT];
    CustomRow(args...);
  }

  //----------------------------------------------------------------------------
  //! Base case for variadic template for CustomRow
  //!
  //! @param pair<T,int> Last/only parameter
  //----------------------------------------------------------------------------
  template<typename T>
  void CustomRow(const std::pair<T, int>& t)
  {
    m_output.fill(' ');
    m_output.width(t.second);
    m_output << t.first << m_colors[ConsoleTableOutput::DEFAULT];
    m_output << std::endl;
    m_curr_field = 0;
  }

  void Separator()
  {
    m_output << m_separator << std::endl;
  }
  //----------------------------------------------------------------------------
  //! Getting current string of class.
  //----------------------------------------------------------------------------
  std::string Str()
  {
    return m_output.str();
  }

};

#endif //__CONSOLETABLEOUTPUT__HH__
