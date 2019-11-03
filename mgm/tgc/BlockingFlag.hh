// ----------------------------------------------------------------------
// File: BlockingFlag.hh
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

#ifndef __EOSMGM_BLOCKINGFLAG_HH__
#define __EOSMGM_BLOCKINGFLAG_HH__

#include <condition_variable>
#include <mutex>
#include <stdexcept>

/*----------------------------------------------------------------------------*/
/**
 * @file BlockingFlag.hh
 *
 * @brief Boolean flag that starts with a value of false and can have timed
 * waits on its value becoming true.
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Boolean flag that starts with a value of false and can have timed waits on
//! its value becoming true.
//----------------------------------------------------------------------------
class BlockingFlag {
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  BlockingFlag(): m_flag(false) {
  }

  //--------------------------------------------------------------------------
  //! Boolean operator
  //--------------------------------------------------------------------------
  operator bool() const {
  std::unique_lock<std::mutex> lock(m_mutex);
    return m_flag;
  }

  //--------------------------------------------------------------------------
  //! Waits the specified duration for the flag to become true
  //!
  //! @param duration The amount of time to wait
  //! @return True if the flag has been set to true, else false if a timeout
  //! has occurred
  //--------------------------------------------------------------------------
  template<class Duration> bool waitForTrue(Duration duration) noexcept {
    try {
      std::unique_lock<std::mutex> lock(m_mutex);
      return m_cond.wait_for(lock, duration, [&]{return m_flag;});
    } catch(std::exception &ex) {
      eos_static_err("msg=\"%s\"", ex.what());
    } catch(...) {
      eos_static_err("msg=\"Caught an unknown exception\"");
    }
    return false;
  }

  //--------------------------------------------------------------------------
  //! Sets the flag to true and wakes all threads waiting on waitForTrue()
  //--------------------------------------------------------------------------
  void setToTrue() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_flag = true;
    m_cond.notify_all();
  }

private:

  //--------------------------------------------------------------------------
  //! Mutex protecting the flag
  //--------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //--------------------------------------------------------------------------
  //! The condition variable of the flag
  //--------------------------------------------------------------------------
  std::condition_variable m_cond;

  //--------------------------------------------------------------------------
  //! The flag
  //--------------------------------------------------------------------------
  bool m_flag;

}; // class BlockingFlag

EOSTGCNAMESPACE_END

#endif
