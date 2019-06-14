// ----------------------------------------------------------------------
// File: StringTokenizer.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "common/StringTokenizer.hh"
#include <cstring>
#include <sstream>
#include <iomanip>
#include <algorithm>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
StringTokenizer::StringTokenizer(const char* s):
  fCurrentLine(-1), fCurrentArg(-1)
{
  // the constructor just parses lines not token's within a line
  if (s) {
    fBuffer = strdup(s);
  } else {
    fBuffer = 0;
    return;
  }

  bool inquote = false;

  if (fBuffer[0] != 0) {
    // set the first pointer to offset 0
    fLineStart.push_back(0);
  }

  // intelligent parsing considering quoting
  for (size_t i = 0; i < std::strlen(fBuffer); i++) {
    if ((fBuffer[i] == '"') &&
        ((i == 0) ||
         ((fBuffer[i - 1] != '\\')))) {
      if (inquote) {
        inquote = false;
      } else {
        inquote = true;
      }
    }

    if ((!inquote) && fBuffer[i] == '\n') {
      fLineStart.push_back(i + 1);
    }
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
StringTokenizer::~StringTokenizer()
{
  if (fBuffer) {
    free(fBuffer);
    fBuffer = 0;
  }
}

//------------------------------------------------------------------------------
// Return the next parsed line
//------------------------------------------------------------------------------
const char*
StringTokenizer::GetLine()
{
  fCurrentLine++;

  if (fCurrentLine < (int) fLineStart.size()) {
    char* line = fBuffer + fLineStart[fCurrentLine];
    char* wordptr = line;
    bool inquote = false;
    size_t len = strlen(line) + 1;

    for (size_t i = 0; i < len; i++) {
      if ((line[i] == '"') &&
          ((i == 0) ||
           ((line[i - 1] != '\\')))) {
        if (inquote) {
          inquote = false;
        } else {
          inquote = true;
        }
      }

      if ((line[i] == ' ') || (line[i] == 0) || (line[i] == '\n')) {
        if (!inquote) {
          if ((i > 1) && (line[i - 1] == '\\')) {
            // don't start a new word here
          } else {
            char val = line[i];
            line[i] = 0;
            fLineArgs.push_back(wordptr);
            line[i] = val;

            // start a new word here
            wordptr = line + i + 1;
          }
        }
      }

      if ((!inquote) && (line[i] == '\n')) {
        line[i] = 0;
      }
    }

    return line;
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Return next parsed space separated token taking into account escaped
// blanks and quoted strings.
//
// Note: Quotes enclosing the token are removed, but other type of quotes
//       are left untouched
//------------------------------------------------------------------------------
const char*
StringTokenizer::GetToken(bool escapeand)
{
  fCurrentArg++;

  if (fCurrentArg < (int) fLineArgs.size()) {
    // patch out quotes
    XrdOucString item = fLineArgs[fCurrentArg].c_str();

    if (item.beginswith("\"")) {
      item.erase(0, 1);
    }

    if (item.endswith("\"") &&
        (!item.endswith("\\\""))) {
      item.erase(item.length() - 1);
    }

    if (escapeand) {
      int pos = 0;

      while ((pos = item.find("&", pos)) != STR_NPOS) {
        if ((pos == 0) || (item[pos - 1] != '\\')) {
          item.erase(pos, 1);
          item.insert("#AND#", pos);
        }

        pos++;
      }
    }

    fLineArgs[fCurrentArg] = item.c_str();
    return fLineArgs[fCurrentArg].c_str();
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Return next parsed space separated token taking into account escaped
// blanks and quoted strings.
//
// Note: Quotes enclosing the token are removed, while any other
//       type of quotes will be unescaped
//------------------------------------------------------------------------------
const char*
StringTokenizer::GetTokenUnquoted(bool escapeand)
{
  fCurrentArg++;

  if (fCurrentArg < (int) fLineArgs.size()) {
    std::string token;
    std::stringstream ss;

    // Dequote token
    ss << fLineArgs[fCurrentArg].c_str();
    ss >> std::quoted(token);

    if (escapeand) {
      size_t pos = 0;

      while ((pos = token.find("&", pos)) != std::string::npos) {
        if ((pos == 0) || (token[pos - 1] != '\\')) {
          token.replace(pos, 1, "#AND#");
        }

        pos++;
      }
    }

    fLineArgs[fCurrentArg] = token.c_str();
    return fLineArgs[fCurrentArg].c_str();
  } else {
    return 0;
  }
}

bool
StringTokenizer::IsUnsignedNumber(const std::string& str)
{
  return !str.empty() &&
         str.find_first_not_of("0123456789") == std::string::npos &&
         (str.front() != '0' || str.size() == 1);
}

EOSCOMMONNAMESPACE_END
