// ----------------------------------------------------------------------
// File: AsyncResult.hh
// Author: Steven Murray - CERN
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

#ifndef __EOSMGMTGC_ASYNCRESULT_HH__
#define __EOSMGMTGC_ASYNCRESULT_HH__

#include "mgm/Namespace.hh"

#include <string>
#include <optional>

/**
 * @file AsyncResult.hh
 *
 * @brief Class representing the result of polling an asynchronous task which
 * may still be running.  In addition this class can store the result of a
 * previous execution of the task.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class representing the result of polling an asynchronous task which
//! may still be running.  In addition this class can store the result of a
//! previous execution of the task.
//------------------------------------------------------------------------------
template<typename Value> class AsyncResult {
private:
  //----------------------------------------------------------------------------
  //! Private constructor to force the use of the factory methods
  //----------------------------------------------------------------------------
  AsyncResult(): m_state(State::PENDING_AND_NO_PREVIOUS_VALUE) {}

public:

  enum class State {
    PENDING_AND_NO_PREVIOUS_VALUE, //! Task still running and there is no result from a previous task
    PENDING_AND_PREVIOUS_VALUE, //! Task still running and there is a result from a previous task
    VALUE, //! Task has completed successfully and has written a syntactically valid value to its standard out
    ERROR //! Task has failed with an error
  };

  static const char* stateToStr(const State &state) {
    switch(state) {
    case State::PENDING_AND_NO_PREVIOUS_VALUE: return "PENDING_AND_NO_PREVIOUS_VALUE";
    case State::PENDING_AND_PREVIOUS_VALUE: return "PENDING_AND_PREVIOUS_VALUE";
    case State::VALUE: return "VALUE";
    case State::ERROR: return "ERROR";
    default: return "UNKNOWN";
    }
  }

  //----------------------------------------------------------------------------
  //! Create a PENDING_AND_NO_PREVIOUS_VALUE result
  //----------------------------------------------------------------------------
  static AsyncResult createPendingAndNoPreviousValue() {
    AsyncResult result;
    result.m_state = State::PENDING_AND_NO_PREVIOUS_VALUE;
    return result;
  }

  //----------------------------------------------------------------------------
  //! Create a PENDING_AND_PREVIOUS_VALUE result
  //----------------------------------------------------------------------------
  static AsyncResult createPendingAndPreviousValue(const Value &previousValue) {
    AsyncResult result;
    result.m_state = State::PENDING_AND_PREVIOUS_VALUE;
    result.m_previousValue = previousValue;
    return result;
  }

  //----------------------------------------------------------------------------
  //! Create a VALUE result
  //----------------------------------------------------------------------------
  static AsyncResult createValue(const Value &value) {
    AsyncResult result;
    result.m_state = State::VALUE;
    result.m_value = value;
    return result;
  }

  //----------------------------------------------------------------------------
  //! Create an ERROR result
  //----------------------------------------------------------------------------
  static AsyncResult createError(const std::string &error) {
    AsyncResult result;
    result.m_state = State::ERROR;
    result.m_error = error;
    return result;
  }

  //----------------------------------------------------------------------------
  //! @return state of the result
  //----------------------------------------------------------------------------
  State getState() const {
    return m_state;
  }

  //----------------------------------------------------------------------------
  //! @return optional previous value
  //----------------------------------------------------------------------------
  std::optional<Value> getPreviousValue() const {
    return m_previousValue;
  }

  //----------------------------------------------------------------------------
  //! @return optional value
  //----------------------------------------------------------------------------
  std::optional<Value> getValue() const {
    return m_value;
  }

  //----------------------------------------------------------------------------
  //! @return optional error
  //----------------------------------------------------------------------------
  std::optional<std::string> getError() const {
    return m_error;
  }

private:

  State m_state;
  std::optional<Value> m_previousValue;
  std::optional<Value> m_value;
  std::optional<std::string> m_error;
};

EOSTGCNAMESPACE_END

#endif
