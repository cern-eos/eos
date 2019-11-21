//------------------------------------------------------------------------------
// File: TableCell.cc
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

#include "TableCell.hh"

static std::string getColorSequence(TableFormatterColor color) {

  switch(color) {
    case TableFormatterColor::NONE: {
      return "";
    }
    case TableFormatterColor::DEFAULT: {
      return "\33[0m";
    }
    case TableFormatterColor::RED: {
      return "\33[0;31m";
    }
    case TableFormatterColor::GREEN: {
      return "\33[0;32m";
    }
    case TableFormatterColor::YELLOW: {
      return "\33[0;33m";
    }
    case TableFormatterColor::BLUE: {
      return "\33[0;34m";
    }
    case TableFormatterColor::MARGARITA: {
      return "\33[0;35m";
    }
    case TableFormatterColor::CYAN: {
      return "\33[0;36m";
    }
    case TableFormatterColor::WHITE: {
      return "\33[0;39m";
    }
    case TableFormatterColor::BOLD: {
      return "\33[1m";
    }
    case TableFormatterColor::BRED: {
      return "\33[1;31m";
    }
    case TableFormatterColor::BGREEN: {
      return "\33[1;32m";
    }
    case TableFormatterColor::BYELLOW: {
      return "\33[1;33m";
    }
    case TableFormatterColor::BBLUE: {
      return "\33[1;34m";
    }
    case TableFormatterColor::BMARGARITA: {
      return "\33[1;35m";
    }
    case TableFormatterColor::BCYAN: {
      return "\33[1;36m";
    }
    case TableFormatterColor::BWHITE: {
      return "\33[1;39m";
    }
    case TableFormatterColor::DARK: {
      return "\33[2m";
    }
    case TableFormatterColor::DRED: {
      return "\33[2;31m";
    }
    case TableFormatterColor::DGREEN: {
      return "\33[2;32m";
    }
    case TableFormatterColor::DYELLOW: {
      return "\33[2;33m";
    }
    case TableFormatterColor::DBLUE: {
      return "\33[2;34m";
    }
    case TableFormatterColor::DMARGARITA: {
      return "\33[2;35m";
    }
    case TableFormatterColor::DCYAN: {
      return "\33[2;36m";
    }
    case TableFormatterColor::DWHITE: {
      return "\33[2;39m";
    }
    case TableFormatterColor::BRED_BGWHITE: {
      return "\33[1;31;47m";
    }
    case TableFormatterColor::BGREEN_BGWHITE: {
      return "\33[1;32;47m";
    }
    case TableFormatterColor::BYELLOW_BGWHITE: {
      return "\33[1;33;47m";
    }
    case TableFormatterColor::BBLUE_BGWHITE: {
      return "\33[1;34;47m";
    }
    case TableFormatterColor::BMARGARITA_BGWHITE: {
      return "\33[1;35;47m";
    }
    case TableFormatterColor::BCYAN_BGWHITE: {
      return "\33[1;36;47m";
    }
    case TableFormatterColor::BWHITE_BGRED: {
      return "\33[1;39;41m";
    }
    case TableFormatterColor::BWHITE_BGGREEN: {
      return "\33[1;39;42m";
    }
    case TableFormatterColor::BWHITE_BGYELLOW: {
      return "\33[1;39;43m";
    }
    case TableFormatterColor::BWHITE_BGBLUE: {
      return "\33[1;39;44m";
    }
    case TableFormatterColor::BWHITE_BGMARGARITA: {
      return "\33[1;39;45m";
    }
    case TableFormatterColor::BWHITE_BGCYAN: {
      return "\33[1;39;46m";
    }
    case TableFormatterColor::BYELLOW_BGRED: {
      return "\33[1;33;41m";
    }
    case TableFormatterColor::BYELLOW_BGGREEN: {
      return "\33[1;33;42m";
    }
    case TableFormatterColor::BYELLOW_BGBLUE: {
      return "\33[1;33;44m";
    }
    case TableFormatterColor::BYELLOW_BGMARGARITA: {
      return "\33[1;33;45m";
    }
    case TableFormatterColor::BYELLOW_BGCYAN: {
      return "\33[1;33;46m";
    }
    default: {
      return "";
    }
  }

}

//------------------------------------------------------------------------------
// Constructor for unsigned int data
//------------------------------------------------------------------------------
TableCell::TableCell(unsigned int value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::UINT;
    SetValue((unsigned long long int)value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue((double)value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for unsigned long long int data
//------------------------------------------------------------------------------
TableCell::TableCell(unsigned long long int value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::UINT;
    SetValue(value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue((double)value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for int data
//------------------------------------------------------------------------------
TableCell::TableCell(int value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::INT;
    SetValue((long long int)value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue((double)value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for long long int data
//------------------------------------------------------------------------------
TableCell::TableCell(long long int value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::INT;
    SetValue(value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue((double)value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for float data
//------------------------------------------------------------------------------
TableCell::TableCell(float value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::INT;
    SetValue((long long int)value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue((double)value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for double data
//------------------------------------------------------------------------------
TableCell::TableCell(double value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::DOUBLE)
{
  if (mFormat.find("l") != std::string::npos) {
    mSelectedValue = TypeContainingValue::INT;
    SetValue((long long int)value);
  }

  if (mFormat.find("f") != std::string::npos) {
    mSelectedValue = TypeContainingValue::DOUBLE;
    SetValue(value);
  }

  if (mFormat.find("s") != std::string::npos) {
    mSelectedValue = TypeContainingValue::STRING;
    std::string value_temp = std::to_string(value);
    SetValue(value_temp);
  }

  if (mFormat.find("t") != std::string::npos) {
    mSelectedValue = TypeContainingValue::TREE;
    mTree = (unsigned)value;
  }
}

//------------------------------------------------------------------------------
// Constructor for char* data
//------------------------------------------------------------------------------
TableCell::TableCell(const char* value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::STRING)
{
  std::string value_temp(value);
  SetValue(value_temp);
}

//------------------------------------------------------------------------------
// Constructor for string data
//------------------------------------------------------------------------------
TableCell::TableCell(const std::string& value, const std::string& format,
                     const std::string& unit, bool empty,
                     TableFormatterColor col)
  : mFormat(format), mUnit(unit), mEmpty(empty), mColor(col),
    mSelectedValue(TypeContainingValue::STRING)
{
  SetValue(value);
}

//------------------------------------------------------------------------------
// Set color of cell
//------------------------------------------------------------------------------
void TableCell::SetColor(TableFormatterColor color)
{
  if (color != DEFAULT) {
    mColor = color;
  }
}

//------------------------------------------------------------------------------
// Set unsigned long long int value
//------------------------------------------------------------------------------
void TableCell::SetValue(unsigned long long int value)
{
  if (mSelectedValue == TypeContainingValue::UINT) {
    // If convert unsigned int value into K,M,G,T,P,E scale,
    // we convert unsigned int value to double
    if (mFormat.find("+") != std::string::npos && value >= 1000) {
      mSelectedValue = TypeContainingValue::DOUBLE;
      SetValue((double)value);
    } else {
      m_ullValue = value;
    }
  }
}

//------------------------------------------------------------------------------
// Set long long int value
//------------------------------------------------------------------------------
void TableCell::SetValue(long long int value)
{
  if (mSelectedValue == TypeContainingValue::INT) {
    // If convert int value into K,M,G,T,P,E scale,
    // we convert int value to double
    if (mFormat.find("+") != std::string::npos &&
        (value >= 1000 || value <= -1000)) {
      mSelectedValue = TypeContainingValue::DOUBLE;
      SetValue((double)value);
    } else {
      m_llValue = value;
    }
  }
}

//------------------------------------------------------------------------------
// Set double value
//------------------------------------------------------------------------------
void TableCell::SetValue(double value)
{
  if (mSelectedValue == TypeContainingValue::DOUBLE) {
    // Convert value into f,p,n,u,m,K,M,G,T,P,E scale
    // double scale = (mUnit == "B") ? 1024.0 : 1000.0;
    double scale = 1000.0;

    // Use IEC standard to display values power of 2
    // if (mUnit == "B") {
    //   mUnit.insert(0, "i");
    // }

    if (mFormat.find("+") != std::string::npos && value != 0) {
      bool value_negative = false;

      if (value < 0) {
        value *= -1;
        value_negative = true;
      }

      if (value >= scale * scale * scale * scale * scale * scale) {
        mUnit.insert(0, "E");
        value /= scale * scale * scale * scale * scale * scale;
      } else if (value >= scale * scale * scale * scale * scale) {
        mUnit.insert(0, "P");
        value /= scale * scale * scale * scale * scale;
      } else if (value >= scale * scale * scale * scale) {
        mUnit.insert(0, "T");
        value /= scale * scale * scale * scale;
      } else if (value >= scale * scale * scale) {
        mUnit.insert(0, "G");
        value /= scale * scale * scale;
      } else if (value >= scale * scale) {
        mUnit.insert(0, "M");
        value /= scale * scale;
      } else if (value >= scale) {
        mUnit.insert(0, "K");
        value /= scale;
        //} else if (value >= 1) {
        //  value = value;
      } else if (value >= 1 / scale) {
        mUnit.insert(0, "m");
        value *= scale;
      } else if (value >= 1 / (scale * scale)) {
        mUnit.insert(0, "u");
        value *= scale * scale;
      } else if (value >= 1 / (scale * scale * scale)) {
        mUnit.insert(0, "n");
        value *= scale * scale * scale;
      } else if (value >= 1 / (scale * scale * scale * scale)) {
        mUnit.insert(0, "p");
        value *= scale * scale * scale * scale;
      } else if (value >= 1 / (scale * scale * scale * scale * scale)) {
        mUnit.insert(0, "f");
        value *= scale * scale * scale * scale * scale;
      }

      if (value_negative) {
        value *= -1;
      }
    }

    mDoubleValue =  value;
  }
}

//------------------------------------------------------------------------------
// Set string value
//------------------------------------------------------------------------------
void TableCell::SetValue(const std::string& value)
{
  if (mSelectedValue == TypeContainingValue::STRING) {
    // " " -> "%20" is for monitoring output
    if (mFormat.find("o") != std::string::npos) {
      std::string cpy_val = value;
      std::string search = " ";
      std::string replace = "%20";
      size_t pos = 0;

      while ((pos = cpy_val.find(search, pos)) != std::string::npos) {
        cpy_val.replace(pos, search.length(), replace);
        pos += replace.length();
      }

      mStrValue = cpy_val;
    } else {
      mStrValue = value;
    }
  }
}

//------------------------------------------------------------------------------
// Print tablecell
//------------------------------------------------------------------------------
void TableCell::Print(std::ostream& ostream, size_t width_left,
                      size_t width_right) const
{
  ostream.fill(' ');

  // Left space before cellValue
  if (width_left) {
    // Because of prefix
    if (mFormat.find("±") != std::string::npos) {
      width_left += 3;
    }

    // Because of escape characters - see TableFromatterColorContainer, we need
    // to add 4 for bold and dark display, 7 for colored display,
    // 10 for colored display with background etc.
    if (mColor == TableFormatterColor::NONE) {
      // Normal display
      ostream.width(width_left);
    } else if (mColor == TableFormatterColor::DEFAULT ||
               mColor == TableFormatterColor::BOLD ||
               mColor == TableFormatterColor::DARK) {
      // Default, bold and dark display
      ostream.width(width_left + 4);
    } else if (TableFormatterColor::RED <= mColor &&
               mColor <= TableFormatterColor::DWHITE) {
      // Display with color
      ostream.width(width_left + 7);
    } else {
      // Display with color and background
      ostream.width(width_left + 10);
    }
  }

  // Prefix "±"
  if (mFormat.find("±") != std::string::npos) {
    if (mFormat.find("o") != std::string::npos) {
      ostream << "±%20" ;
    } else {
      ostream << "± ";
    }
  }

  // Color
  if (mFormat.find("o") == std::string::npos) {
    ostream << getColorSequence(mColor);
  }

  // Value
  if (mSelectedValue == TypeContainingValue::UINT) {
    ostream << m_ullValue;
  } else if (mSelectedValue == TypeContainingValue::INT) {
    ostream << m_llValue;
  } else if (mSelectedValue == TypeContainingValue::DOUBLE) {
    auto flags = ostream.flags();
    ostream << std::setprecision(2) << std::fixed << mDoubleValue;
    ostream.flags(flags);
  } else if (mSelectedValue == TypeContainingValue::STRING) {
    ostream << mStrValue;
  }

  // Color (return color to default)
  if ((mFormat.find("o") == std::string::npos) &&
      (mColor != TableFormatterColor::NONE)) {
    ostream << getColorSequence(TableFormatterColor::DEFAULT);
  }

  // Postfix "."
  if (mFormat.find(".") != std::string::npos) {
    ostream << ".";
  }

  // Unit
  if (!mUnit.empty()) {
    if (mFormat.find("o") != std::string::npos) {
      ostream << "%20" << mUnit;
    } else {
      ostream << " " << mUnit;
    }
  }

  // Right space after cellValue
  if (width_right) {
    ostream.width(width_right);
  }
}

//------------------------------------------------------------------------------
// Print value of tablecell in string, without unit and without color
//------------------------------------------------------------------------------
std::string TableCell::Str()
{
  std::stringstream ostream;

  if (mSelectedValue == TypeContainingValue::UINT) {
    ostream << m_ullValue;
  } else if (mSelectedValue == TypeContainingValue::INT) {
    ostream << m_llValue;
  } else if (mSelectedValue == TypeContainingValue::DOUBLE) {
    auto flags = ostream.flags();
    ostream << std::setprecision(2) << std::fixed << mDoubleValue;
    ostream.flags(flags);
  } else if (mSelectedValue == TypeContainingValue::STRING) {
    ostream << mStrValue;
  }

  return ostream.str();
}

//------------------------------------------------------------------------------
// Operators
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& stream, const TableCell& cell)
{
  cell.Print(stream);
  return stream;
}

//------------------------------------------------------------------------------
// if we don't need print for this cell (for monitoring option)
//------------------------------------------------------------------------------
bool TableCell::Empty()
{
  return mEmpty;
}

//------------------------------------------------------------------------------
// Tree
//------------------------------------------------------------------------------
unsigned TableCell::Tree()
{
  if (mSelectedValue == TypeContainingValue::TREE) {
    return mTree;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Calculating print width of table cell
//------------------------------------------------------------------------------
size_t TableCell::Length()
{
  size_t ret = 0;

  // Value length
  if (mSelectedValue == TypeContainingValue::UINT) {
    // Get length of unsigned integer value
    unsigned long long int temp = m_ullValue;

    if (temp == 0) {
      ret = 1;
    }

    while (temp != 0) {
      ++ret;
      temp /= 10;
    }
  } else   if (mSelectedValue == TypeContainingValue::INT) {
    // Get length of integer value
    long long int temp = m_llValue;

    if (temp <= 0) {
      ret = 1;
    }

    while (temp != 0) {
      ++ret;
      temp /= 10;
    }
  } else if (mSelectedValue == TypeContainingValue::DOUBLE) {
    // Get length of double value
    std::stringstream temp;
    auto flags = temp.flags();
    temp << std::setprecision(2) << std::fixed << mDoubleValue;
    temp.flags(flags);
    ret = temp.str().length() ;
  } else if (mSelectedValue == TypeContainingValue::STRING) {
    // Get length of string
    ret = mStrValue.length();
  }

  // Prefix "±"
  if (mFormat.find("±") != std::string::npos) {
    ret += 2;
  }

  // Postfix "."
  if (mFormat.find(".") != std::string::npos) {
    ret += 1;
  }

  // Unit length
  if (!mUnit.empty()) {
    ret += mUnit.length() + 1;
  }

  return ret;
}
