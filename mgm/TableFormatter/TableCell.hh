//------------------------------------------------------------------------------
// File: TableCell.hh
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

#ifndef __TABLE_CELL__HH__
#define __TABLE_CELL__HH__

#include "TableFormatting.hh"

class TableCell
{
public:
  //----------------------------------------------------------------------------
  //! Different types of constructors depending on the type of value added to
  //! the current cell
  //!
  //! @param value of cell
  //! @param format of cell: "s" - print cell as string
  //!                        "l" - print cell as long long
  //!                        "f" - print cell as double
  //!                        "t" - print cell as tree arrows
  //!                        "o" - print cell as monitoring view (TODO (Ivan): Move this in TableFormatterBase::GenerateBody())
  //!                        "-" - left align the printout (TODO (Ivan): Move this only in TableFormatterBase::SetHeader())
  //!                        "+" - convert numbers into f,p,n,u,m,K,M,G,T,P,E scale
  //!                              (e.g. 2200 with format="+" will be "2.2 K")
  //!                        "±" - prefix "±" for value (e.g. "± 22 ms")
  //!                        "." - postfix "." for value (e.g. first one "1.")
  //! @param unit Postfix of cell (e.g. "2.2 K" with unit=B will be "2.2 KB")
  //! @param empty If we don't want to see cell in monitoring view
  //! @param col Color of cell
  //----------------------------------------------------------------------------
  TableCell(unsigned int value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(unsigned long long int value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(int value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(long long int value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(float value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(double value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(const char* value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  TableCell(const std::string& value, const std::string& format,
            const std::string& unit = "", bool empty = false,
            TableFormatterColor col = TableFormatterColor::NONE);

  //------------------------------------------------------------------------------
  //! Set color of cell
  //------------------------------------------------------------------------------
  void SetColor(TableFormatterColor color);

  //----------------------------------------------------------------------------
  //! Print table cell to stream. Needed to dump data into a stringstream or
  //! anything overloading std::stringstream
  //!
  //! @param ostream output string stream
  //! @param width_left left padding
  //! @param width_right  right padding
  //----------------------------------------------------------------------------
  void Print(std::ostream& ostream, size_t width_left = 0,
             size_t width_right = 0) const;
  std::string Str();

  //----------------------------------------------------------------------------
  //! Calculate print width of table cell
  //----------------------------------------------------------------------------
  size_t Length();
  bool Empty();
  unsigned Tree();

protected:
  //----------------------------------------------------------------------------
  //! Set value of the table cell data (convert into K,M,G,T,P,E scale).
  //! Implementled with guards to prevent the cell having any other value then
  //! the one initially set.
  //----------------------------------------------------------------------------
  void SetValue(unsigned long long int value);
  void SetValue(long long int value);
  void SetValue(double value);
  void SetValue(const std::string& value);

  //----------------------------------------------------------------------------
  //! Store value for cell
  //----------------------------------------------------------------------------
  unsigned long long int m_ullValue = 0;
  long long int m_llValue = 0;
  double mDoubleValue = 0.f;
  std::string mStrValue = "";
  std::string mFormat;
  std::string mUnit;
  bool mEmpty;
  unsigned mTree = 0; //0="",1="│  ",2="└─▶",3="├─▶",4="└──",5="├──",6="───",7="──▶"

  //----------------------------------------------------------------------------
  //! Color of the cell
  //----------------------------------------------------------------------------
  TableFormatterColor mColor;

  //----------------------------------------------------------------------------
  //! Type of value stored in the current cell
  //----------------------------------------------------------------------------
  enum TypeContainingValue {
    UINT   = 1,
    INT    = 2,
    DOUBLE = 3,
    STRING = 4,
    TREE   = 5
  };

  //! Indicate which value if carrying information
  TypeContainingValue mSelectedValue;

  //----------------------------------------------------------------------------
  //! Making sure that a cell will not be created with no arguments or proper
  //! handling.
  //----------------------------------------------------------------------------
  TableCell() = delete;
};

#endif //__TABLE_CELL__HH__
