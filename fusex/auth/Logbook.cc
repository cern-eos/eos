//------------------------------------------------------------------------------
// File: Logbook.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#include "Logbook.hh"
#include <sstream>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Logbook::Logbook(bool active_) : activated(active_) {}

//------------------------------------------------------------------------------
// Record message into the log
//------------------------------------------------------------------------------
void Logbook::insert(const std::string& msg)
{
  if (activated) {
    messages.emplace_back(msg);
  }
}

//------------------------------------------------------------------------------
// Get a new scope
//------------------------------------------------------------------------------
LogbookScope Logbook::makeScope(const std::string& header)
{
  return LogbookScope(this, header, 0);
}

//----------------------------------------------------------------------------
// Check if activated
//----------------------------------------------------------------------------
bool Logbook::active() const
{
  return activated;
}

//------------------------------------------------------------------------------
// Build a string out of all messages
//------------------------------------------------------------------------------
std::string Logbook::toString() const
{
  std::stringstream ss;

  for (size_t i = 0; i < messages.size(); i++) {
    ss << messages[i] << std::endl;
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
LogbookScope::LogbookScope() : logbook(nullptr), indentationLevel(false)
{
}



//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LogbookScope::LogbookScope(Logbook* lb, const std::string& header,
                           size_t indent) : logbook(lb), indentationLevel(indent)
{
  std::stringstream ss;

  for (size_t i = 0; i < indentationLevel; i++) {
    ss << " ";
  }

  ss << "-- " << header;
  logbook->insert(ss.str());
}

//----------------------------------------------------------------------------
// Get a new sub-scope
//----------------------------------------------------------------------------
LogbookScope LogbookScope::makeScope(const std::string& header)
{
  if (!logbook) {
    return LogbookScope();
  }

  return LogbookScope(logbook, header, indentationLevel + 2);
}

//----------------------------------------------------------------------------
// Record message into the log, under the given scope
//----------------------------------------------------------------------------
void LogbookScope::insert(const std::string& msg)
{
  if (logbook) {
    std::stringstream ss;

    for (size_t i = 0; i < indentationLevel + 2; i++) {
      ss << " ";
    }

    ss << msg;
    logbook->insert(ss.str());
  }
}

//----------------------------------------------------------------------------
// Check if activated
//----------------------------------------------------------------------------
bool LogbookScope::active() const
{
  if (!logbook) {
    return false;
  }

  return logbook->active();
}
