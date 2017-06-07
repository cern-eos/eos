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

//------------------------------------------------------------------------------
// Constructor for unsigned int data
//------------------------------------------------------------------------------
TableCell::TableCell(unsigned int value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for unsigned long long int data
//------------------------------------------------------------------------------
TableCell::TableCell(unsigned long long int value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for int data
//------------------------------------------------------------------------------
TableCell::TableCell(int value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for long long int data
//------------------------------------------------------------------------------
TableCell::TableCell(long long int value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for float data
//------------------------------------------------------------------------------
TableCell::TableCell(float value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for double data
//------------------------------------------------------------------------------
TableCell::TableCell(double value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col)
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
}

//------------------------------------------------------------------------------
// Constructor for char* data
//------------------------------------------------------------------------------
TableCell::TableCell(const char* value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col),
    mSelectedValue(TypeContainingValue::STRING)
{
  std::string value_temp(value);
  SetValue(value_temp);
}

//------------------------------------------------------------------------------
// Constructor for string data
//------------------------------------------------------------------------------
TableCell::TableCell(std::string& value, std::string format,
                     std::string unit, TableFormatterColor col)
  : mFormat(format), mUnit(unit), mColor(col),
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
    //Convert value into K,M,G,T,P,E scale
    if (mFormat.find("+") != std::string::npos && value != 0) {
      if (value >= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "E");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "P");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000) {
        mUnit.insert(0, "T");
        value /= 1000ll * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000) {
        mUnit.insert(0, "G");
        value /= 1000ll * 1000 * 1000;
      } else if (value >= 1000ll * 1000) {
        mUnit.insert(0, "M");
        value /= 1000ll * 1000;
      } else if (value >= 1000ll) {
        mUnit.insert(0, "K");
        value /= 1000ll;
      }
    }

    m_ullValue = value;
  }
}

//------------------------------------------------------------------------------
// Set long long int value
//------------------------------------------------------------------------------
void TableCell::SetValue(long long int value)
{
  if (mSelectedValue == TypeContainingValue::INT) {
    //Convert value into K,M,G,T,P,E scale
    if (mFormat.find("+") != std::string::npos && value != 0) {
      bool value_negative = false;

      if (value < 0) {
        value *= -1;
        value_negative = true;
      }

      if (value >= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "E");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "P");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000) {
        mUnit.insert(0, "T");
        value /= 1000ll * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000) {
        mUnit.insert(0, "G");
        value /= 1000ll * 1000 * 1000;
      } else if (value >= 1000ll * 1000) {
        mUnit.insert(0, "M");
        value /= 1000ll * 1000;
      } else if (value >= 1000ll) {
        mUnit.insert(0, "K");
        value /= 1000ll;
      }

      if (value_negative) {
        value *= -1;
      }
    }

    m_llValue = value;
  }
}

//------------------------------------------------------------------------------
// Set double value
//------------------------------------------------------------------------------
void TableCell::SetValue(double value)
{
  if (mSelectedValue == TypeContainingValue::DOUBLE) {
    //Convert value into K,M,G,T,P,E scale
    if (mFormat.find("+") != std::string::npos && value != 0) {
      bool value_negative = false;

      if (value < 0) {
        value *= -1;
        value_negative = true;
      }

      if (value >= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "E");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000 * 1000) {
        mUnit.insert(0, "P");
        value /= 1000ll * 1000 * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000 * 1000) {
        mUnit.insert(0, "T");
        value /= 1000ll * 1000 * 1000 * 1000;
      } else if (value >= 1000ll * 1000 * 1000) {
        mUnit.insert(0, "G");
        value /= 1000ll * 1000 * 1000;
      } else if (value >= 1000ll * 1000) {
        mUnit.insert(0, "M");
        value /= 1000ll * 1000;
      } else if (value >= 1000ll) {
        mUnit.insert(0, "K");
        value /= 1000ll;
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
void TableCell::SetValue(std::string& value)
{
  if (mSelectedValue == TypeContainingValue::STRING) {
    // " " -> "%20" is for monitoring output
    if (mFormat.find("o") != std::string::npos) {
      std::string search = " ";
      std::string replace = "%20";
      size_t pos = 0;

      while ((pos = value.find(search, pos)) != std::string::npos) {
        value.replace(pos, search.length(), replace);
        pos += replace.length();
      }

      mStrValue = value;
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
    // Because of escape characters - see TableFromatterColorContainer, we need
    // to add 4 normal display, 5 for bold display, 6 for normal display with
    // white background etc.
    // Normal display
    if (mColor == TableFormatterColor::DEFAULT) {
      ostream.width(width_left + 4);
    } else if (TableFormatterColor::RED <= mColor &&
               mColor <= TableFormatterColor::WHITE) {
      ostream.width(width_left + 5);
    } else  if (mColor == TableFormatterColor::BDEFAULT) {
      // Bold display
      ostream.width(width_left + 6);
    } else if (TableFormatterColor::BRED <= mColor &&
               mColor <= TableFormatterColor::BWHITE) {
      ostream.width(width_left + 7);
    } else if (mColor == TableFormatterColor::BGDEFAULT) {
      // Normal display with white background
      ostream.width(width_left + 7);
    } else if (TableFormatterColor::BGRED <= mColor &&
               mColor <= TableFormatterColor::BGWHITE) {
      ostream.width(width_left + 8);
    } else if (mColor == TableFormatterColor::BBGDEFAULT) {
      // Bold display with white background
      ostream.width(width_left + 9);
    } else if (TableFormatterColor::BBGRED <= mColor &&
               mColor <= TableFormatterColor::BBGWHITE) {
      ostream.width(width_left + 10);
    }
  }

  ostream << sColorVector[mColor];

  // Value
  if (mSelectedValue == TypeContainingValue::UINT) {
    ostream << m_ullValue;
  } else if (mSelectedValue == TypeContainingValue::INT) {
    ostream << m_llValue;
  } else if (mSelectedValue == TypeContainingValue::DOUBLE) {
    ostream << std::setprecision(2) << std::fixed << mDoubleValue;
  } else if (mSelectedValue == TypeContainingValue::STRING) {
    ostream << mStrValue;
  }

  ostream << *sColorVector.begin();

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
    ostream << std::setprecision(2) << std::fixed << mDoubleValue;
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
    temp << std::setprecision(2) << std::fixed << mDoubleValue;
    ret = temp.str().length() ;
  } else if (mSelectedValue == TypeContainingValue::STRING) {
    // Get length of string
    ret = mStrValue.length();
  }

  // Unit length
  if (!mUnit.empty()) {
    if (mFormat.find("o") != std::string::npos) {
      ret += mUnit.length() + 3;
    } else {
      ret += mUnit.length() + 1;
    }
  }

  return ret;
}
