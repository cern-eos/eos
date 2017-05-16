//------------------------------------------------------------------------------
// File: TableFormatterBase.cc
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

#include "TableFormatterBase.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TableFormatterBase::TableFormatterBase()
  : mSink("")
{
}

//------------------------------------------------------------------------------
// Generate table
//------------------------------------------------------------------------------
std::string TableFormatterBase::GenerateTable(int style)
{
  Style(style);

  // Generate monitoring information in line (option "-m")
  if (!mHeader.empty() &&
      std::get<2>(mHeader[0]).find("o") != std::string::npos) {
    GenerateMonitoring();
  }

  // Generate classic table (with/without second table)
  if (!mHeader.empty() &&
      std::get<2>(mHeader[0]).find("o") == std::string::npos) {
    WidthCorrection();
    GenerateHeader();
    GenerateBody();
  }

  // Generate string (e.g.second table)
  if (mHeader.empty()) {
    GenerateBody();
  }

  return mSink.str();
}

//------------------------------------------------------------------------------
// Generate monitoring output
//------------------------------------------------------------------------------
void TableFormatterBase::GenerateMonitoring()
{
  for (auto& row : mData) {
    if (!row.empty()) {
      for (size_t i = 0, size = mHeader.size(); i < size; ++i) {
        mSink << std::get<0>(mHeader[i]) << "=";
        row[i].Print(mSink);
        mSink << " ";
      }

      mSink << "\n";
    }
  }
}

//------------------------------------------------------------------------------
// Generate table separator
//------------------------------------------------------------------------------
std::string
TableFormatterBase::GenerateSeparator(std::string left, std::string center,
                                      std::string right, std::string line)
{
  std::string separator = left;

  for (size_t i = 0, size = mHeader.size(); i < size; ++i) {
    for (size_t i2 = 0; i2 < std::get<1>(mHeader[i]); i2++) {
      separator += line;
    }

    if (i < size - 1) {
      separator += center;
    }
  }

  separator += right;
  return separator;
}

//------------------------------------------------------------------------------
// Generate table header
//------------------------------------------------------------------------------
void TableFormatterBase::GenerateHeader()
{
  // Top edge of header
  mSink << GenerateSeparator(mBorderHead[0], mBorderHead[1],
                             mBorderHead[2], mBorderHead[3])
        << std::endl;

  for (size_t i = 0, size = mHeader.size(); i < size; ++i) {
    // Left edge of header
    if (i == 0) {
      mSink << mBorderHead[4];
    }

    // Generate cell
    if (std::get<2>(mHeader[i]).find("-") == std::string::npos) {
      mSink.width(std::get<1>(mHeader[i]));
    }

    mSink << std::get<0>(mHeader[i]);

    if (std::get<2>(mHeader[i]).find("-") != std::string::npos) {
      mSink.width(std::get<1>(mHeader[i]) - std::get<0>(mHeader[i]).length() +
                  mBorderHead[5].length());
    }

    // Add right edge of cell
    if (i < size - 1) {
      mSink << mBorderHead[5];
    }
  }

  // Right edge of the header
  mSink << mBorderHead[6] << std::endl;
  // Bottom edge of the header
  mSink << GenerateSeparator(mBorderHead[7], mBorderHead[8], mBorderHead[9],
                             mBorderHead[10])
        << std::endl;
}

//------------------------------------------------------------------------------
// Generate table body
//------------------------------------------------------------------------------
void TableFormatterBase::GenerateBody()
{
  size_t row_size = 0;
  size_t string_size = 0;

  for (auto& row : mData) {
    if (row.empty()) {
      // Generate string
      if (!mString[string_size].empty()) {
        // Bottom edge for last table
        if (row_size > 0 && !mData[row_size - 1].empty())
          mSink << GenerateSeparator(mBorderBody[3], mBorderBody[4],
                                     mBorderBody[5], mBorderBody[6])
                << std::endl;

        mSink << mString[string_size];
      } else {
        // Generate separator
        mSink << GenerateSeparator(mBorderSep[0], mBorderSep[1],
                                   mBorderSep[2], mBorderSep[3])
              << std::endl;
      }

      string_size++;
    }

    //Generate rows
    if (!row.empty() && !mHeader.empty()) {
      if (row_size > 0 && mData[row_size - 1].empty() &&
          string_size > 0 && !mString[string_size - 1].empty()) {
        GenerateHeader();
      }

      for (size_t i = 0, size = mHeader.size(); i < size; ++i) {
        // Left edge
        if (i == 0) {
          mSink << mBorderBody[0];
        }

        // Generate cell
        if (std::get<2>(mHeader[i]).find("-") == std::string::npos) {
          row[i].Print(mSink, std::get<1>(mHeader[i]) - row[i].Length());
        } else {
          row[i].Print(mSink, 0, std::get<1>(mHeader[i]) - row[i].Length() +
                       mBorderBody[1].length());
        }

        // Right edge of cell
        if (i < size - 1) {
          mSink << mBorderBody[1];
        }
      }

      // Right edge of row
      mSink << mBorderBody[2] << std::endl;
    }

    row_size++;
  }

  // Bottom edge
  if (!mHeader.empty() && !mData[mData.size() - 1].empty()) {
    mSink << GenerateSeparator(mBorderBody[3], mBorderBody[4],
                               mBorderBody[5], mBorderBody[6])
          << std::endl;
  }
}

//------------------------------------------------------------------------------
// Recompute the width of the cells taking into account the data
//------------------------------------------------------------------------------
void TableFormatterBase::WidthCorrection()
{
  for (auto& row : mData) {
    if (!row.empty())
      for (size_t i = 0, size = mHeader.size(); i < size; i++) {
        if (std::get<1>(mHeader[i]) < std::get<0>(mHeader[i]).length()) {
          std::get<1>(mHeader[i]) = std::get<0>(mHeader[i]).length();
        }

        if (std::get<1>(mHeader[i]) < row[i].Length()) {
          std::get<1>(mHeader[i]) = row[i].Length();
        }
      }
  }
}

//------------------------------------------------------------------------------
// Set table header
//------------------------------------------------------------------------------
void TableFormatterBase::SetHeader(const TableHeader& header)
{
  if (mHeader.empty()) {
    mHeader = header;
  }
}

//------------------------------------------------------------------------------
// Add separator to table
//------------------------------------------------------------------------------
void TableFormatterBase::AddSeparator()
{
  mData.emplace_back();
  mString.emplace_back();
}

//------------------------------------------------------------------------------
// Add table data
//------------------------------------------------------------------------------
void TableFormatterBase::AddRows(const TableData& body)
{
  std::copy(body.begin(), body.end(), std::back_inserter(mData));
}

//------------------------------------------------------------------------------
// Add string
//------------------------------------------------------------------------------
void TableFormatterBase::AddString(std::string string)
{
  mData.emplace_back();
  mString.push_back(string);
}

//------------------------------------------------------------------------------
// Set table style
// TODO (ivan): Add style as an enum for easier use in the code when calling
//              GenerateTAble.
//------------------------------------------------------------------------------
void TableFormatterBase::Style(int style)
{
  switch (style) {
  //DEFAULT
  case 0: { //Full normal border ("│","┌","┬","┐","├","┼","┤","└","┴","┘","─")
    std::string head [11] = {"┌", "┬", "┐", "─",
                             "│", "│", "│",
                             "├", "┴", "┤", "─"
                            };
    std::string sep [4]   = {"│", "-", "│", "-"};
    std::string body [7]  = {"│", " ", "│",
                             "└", "─", "┘", "─"
                            };
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 1: { //Full bold border ("┃","┏","┳","┓","┣","╋","┫","┗","┻","┛","━")
    std::string head [11] = {"┏", "┳", "┓", "━",
                             "┃", "┃", "┃",
                             "┣", "┻", "┫", "━"
                            };
    std::string sep [4]   = {"┃", "-", "┃", "-"};
    std::string body [7]  = {"┃", " ", "┃",
                             "┗", "━", "┛", "━"
                            };
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 2: { //Full double border ("║","╔","╦","╗","╠","╬","╣","╚","╩","╝","═")
    std::string head [11] = {"╔", "╦", "╗", "═",
                             "║", "║", "║",
                             "╠", "╩", "╣", "═"
                            };
    std::string sep [4]   = {"║", "-", "║", "-"};
    std::string body [7]  = {"║", " ", "║",
                             "╚", "═", "╝", "═"
                            };
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 3: { //Header normal border ("│","┌","┬","┐","├","┼","┤","└","┴","┘","─")
    std::string head [11] = {"┌", "┬", "┐", "─",
                             "│", "│", "│",
                             "└", "┴", "┘", "─"
                            };
    std::string sep [4]   = {" ", "-", " ", "-"};
    std::string body [7]  = {" ", " ", " "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 4: { //Header bold border ("┃","┏","┳","┓","┣","╋","┫","┗","┻","┛","━")
    std::string head [11] = {"┏", "┳", "┓", "━",
                             "┃", "┃", "┃",
                             "┗", "┻", "┛", "━"
                            };
    std::string sep [4]   = {" ", "-", " ", "-"};
    std::string body [7]  = {" ", " ", " "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 5: { //Header double border ("║","╔","╦","╗","╠","╬","╣","╚","╩","╝","═")
    std::string head [11] = {"╔", "╦", "╗", "═",
                             "║", "║", "║",
                             "╚", "╩", "╝", "═"
                            };
    std::string sep [4]   = {" ", "-", " ", "-"};
    std::string body [7]  = {" ", " ", " "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 6: { //Minimal style
    std::string head [11] = {" ", "  ", " ", "-",
                             " ", "  ", " ",
                             " ", "  ", " ", "-"
                            };
    std::string sep [4]   = {" ", "  ", " ", "-"};
    std::string body [7]  = {" ", "  ", " "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 7: { // Old style
    std::string head [11] = {"#-", "--", "-", "-",
                             "# ", "# ", "#",
                             "#-", "--", "-", "-"
                            };
    std::string sep [4]   = {" -", "--", " ", "-"};
    std::string body [7]  = {"  ", "  ", " "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }

  case 8: { // Old style - wide
    std::string head [11] = {"#-", "---", "--", "-",
                             "# ", " # ", " #",
                             "#-", "---", "--", "-"
                            };
    std::string sep [4]   = {" -", "---", "- ", "-"};
    std::string body [7]  = {"  ", "   ", "  "};
    std::copy(head, head + 11, mBorderHead);
    std::copy(sep, sep + 4, mBorderSep);
    std::copy(body, body + 7, mBorderBody);
    break;
  }
  }
}
