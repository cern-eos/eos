//------------------------------------------------------------------------------
// File: Logbook.hh
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

#ifndef EOS_FUSEX_LOGBOOK_HH
#define EOS_FUSEX_LOGBOOK_HH

#include <vector>
#include <string>

class Logbook;

//------------------------------------------------------------------------------
// Helper class to record messages regarding a specific logbook scope. A scope
// indents any messages that appear inside it and inserts a special header
// message at the beginning of the scope.
//------------------------------------------------------------------------------
class LogbookScope
{
public:
  //----------------------------------------------------------------------------
  // Empty constructor
  //----------------------------------------------------------------------------
  LogbookScope();

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  LogbookScope(Logbook* logbook, const std::string& header, size_t indent);

  //----------------------------------------------------------------------------
  // Get a new sub-scope
  //----------------------------------------------------------------------------
  LogbookScope makeScope(const std::string& header);

  //----------------------------------------------------------------------------
  // Record message into the log, under the given scope
  //----------------------------------------------------------------------------
  void insert(const std::string& msg);

  //----------------------------------------------------------------------------
  // Check if activated
  //----------------------------------------------------------------------------
  bool active() const;

private:
  Logbook* logbook;
  size_t indentationLevel = 0;
};

//------------------------------------------------------------------------------
// Use this class to keep a log for a stream of events.
//------------------------------------------------------------------------------
class Logbook
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  Logbook(bool active);

  //----------------------------------------------------------------------------
  // Record message into the log
  //----------------------------------------------------------------------------
  void insert(const std::string& msg);

  //----------------------------------------------------------------------------
  // Get a new scope
  //----------------------------------------------------------------------------
  LogbookScope makeScope(const std::string& header);

  //----------------------------------------------------------------------------
  // Check if activated
  //----------------------------------------------------------------------------
  bool active() const;

  //----------------------------------------------------------------------------
  // Build a string out of all messages
  //----------------------------------------------------------------------------
  std::string toString() const;

private:
  bool activated;
  std::vector<std::string> messages;
};

//------------------------------------------------------------------------------
// Macro to avoid building the string if logbook is inactive
// Usage:
//   Logbook logbook( .. );
//   LOGBOOK_INSERT(logbook, "my" << "string here" << someVariable);
//
// The same macro works with LogbookScope as well.
//------------------------------------------------------------------------------
#define LOGBOOK_INSERT(logger, msg) if(logger.active()) { logger.insert(static_cast<std::ostringstream&>(std::ostringstream().flush() << msg).str()); }

#endif
