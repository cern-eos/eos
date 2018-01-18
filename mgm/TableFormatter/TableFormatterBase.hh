//------------------------------------------------------------------------------
// File: TableFormatterBase.hh
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

#ifndef __TABLE__FORMATTER__HH__
#define __TABLE__FORMATTER__HH__

#include "mgm/Namespace.hh"
#include "TableCell.hh"

EOSMGMNAMESPACE_BEGIN

class TableFormatterBase
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  TableFormatterBase();

  //----------------------------------------------------------------------------
  //! Set table header
  //----------------------------------------------------------------------------
  void SetHeader(const TableHeader& header);

  //----------------------------------------------------------------------------
  //! Add table data
  //----------------------------------------------------------------------------
  void AddRows(const TableData& body);

  //----------------------------------------------------------------------------
  //! Add string to the current table. This can be anything, even another
  //! table.
  //!
  //! @param string blob to be added to the current table
  //----------------------------------------------------------------------------
  void AddString(std::string string);

  //----------------------------------------------------------------------------
  //! Add separator
  //----------------------------------------------------------------------------
  void AddSeparator();

  //----------------------------------------------------------------------------
  //! Generate table
  //!
  //! @param style of the table
  //! @param selections of the table
  //!
  //! @return string representation of the table
  //----------------------------------------------------------------------------
  std::string GenerateTable(TableFormatterStyle style = FULL,
                            const TableString& selections = TableString());

protected:
  std::stringstream mSink;
  TableHeader mHeader;
  TableData mData;
  TableString mString;

  //----------------------------------------------------------------------------
  //! Set cell color
  //!
  //! @param header name
  //! @param value of cell
  //----------------------------------------------------------------------------
  TableFormatterColor ChangeColor(std::string header, std::string value);

  //----------------------------------------------------------------------------
  //! Set table style. This will set the border, separator and body border
  //! string to be used when generating the table.
  //----------------------------------------------------------------------------
  void Style(TableFormatterStyle style);

  //----------------------------------------------------------------------------
  //! Generate monitoring output
  //----------------------------------------------------------------------------
  bool GenerateMonitoring();

  //----------------------------------------------------------------------------
  //! Generate table header
  //----------------------------------------------------------------------------
  void GenerateHeader();

  //----------------------------------------------------------------------------
  //! Generate table body
  //----------------------------------------------------------------------------
  bool GenerateBody(const TableString& selections = TableString());

  //----------------------------------------------------------------------------
  //! Generate separator
  //!
  //! @param left left separator
  //! @param center between cells in the center separator
  //! @param right right separator
  //! @param line line separator
  //----------------------------------------------------------------------------
  std::string GenerateSeparator(std::string left, std::string center,
                                std::string right, std::string line);

  //----------------------------------------------------------------------------
  //! Recompute the width of the cells taking into account the data
  //----------------------------------------------------------------------------
  void WidthCorrection();

private:
  std::string mBorderHead [11];
  std::string mBorderSep [4];
  std::string mBorderBody [7];
};

EOSMGMNAMESPACE_END

#endif // __TABLE__FORMATTER__HH__
