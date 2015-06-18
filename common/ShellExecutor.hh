// ----------------------------------------------------------------------
// File: ShellExecutor.hh
// Author: Michal Kamin Simon - CERN
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

#ifndef __EOSCOMMON_SHELLEXECUTOR__HH__
#define __EOSCOMMON_SHELLEXECUTOR__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <string>
#include <exception>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/*                                                                            */
/*----------------------------------------------------------------------------*/
class ShellException : public std::exception {
public:
  // ---------------------------------------------------------------------------
  // constructor
  // ---------------------------------------------------------------------------

  ShellException (std::string const & msg) : msg (msg)
  {
  }

  // ---------------------------------------------------------------------------
  // destructor
  // ---------------------------------------------------------------------------

  virtual ~ShellException () throw ()
  {
  }

  // ---------------------------------------------------------------------------
  // getter for message
  // ---------------------------------------------------------------------------

  char const * what () const throw ()
  {
    return msg.c_str();
  }
private:
  std::string const msg;
};

class ShellCmd;

class ShellExecutor {
  friend class ShellCmd;

public:
  // ---------------------------------------------------------------------------
  // typedef for UUID in 'C string'
  // ---------------------------------------------------------------------------
  typedef char fifo_uuid_t[37];

  // ---------------------------------------------------------------------------
  // get an instance
  // ---------------------------------------------------------------------------

  static ShellExecutor & instance ()
  {
    static ShellExecutor executor;
    return executor;
  }

  // ---------------------------------------------------------------------------
  // destructor
  // ---------------------------------------------------------------------------
  virtual ~ShellExecutor ();

  // ---------------------------------------------------------------------------
  // execute a shell command
  // ---------------------------------------------------------------------------
  pid_t execute (std::string const & cmd, fifo_uuid_t uuid = 0) const;

  // ---------------------------------------------------------------------------
  // generate fifo name
  // ---------------------------------------------------------------------------
  static std::string fifo_name (fifo_uuid_t uuid, std::string const & sufix);

  // ---------------------------------------------------------------------------
  // 'stdout', 'stderr' and 'stdin' file descriptors of the 'command' process
  // ---------------------------------------------------------------------------
  static const std::string stdout;
  static const std::string stderr;
  static const std::string stdin;

private:

  // ---------------------------------------------------------------------------
  // constructor
  // ---------------------------------------------------------------------------
  ShellExecutor ();

  // ---------------------------------------------------------------------------
  // not implemented
  // ---------------------------------------------------------------------------
  ShellExecutor (const ShellExecutor& orig);

  // ---------------------------------------------------------------------------
  // not implemented
  // ---------------------------------------------------------------------------
  ShellExecutor & operator= (const ShellExecutor& orig);

  // ---------------------------------------------------------------------------
  // message for parent-child communication
  // ---------------------------------------------------------------------------

  struct msg_t {
    // size of the buffer
    static const size_t max_size = 1024;
    // default constructor
    msg_t ();
    // initialize with UUID
    msg_t (fifo_uuid_t uuid);

    char buff[max_size];
    bool complete;
    fifo_uuid_t uuid;
  };

  void run_child () const;

  pid_t system (char const * cmd, fifo_uuid_t uuid) const;

  // ---------------------------------------------------------------------------
  /// pipes for IPC
  // ---------------------------------------------------------------------------
  int outfd[2];
  int infd[2];
};

EOSCOMMONNAMESPACE_END

#endif	/* __EOSCOMMON_SHELLEXECUTOR__HH__ */

