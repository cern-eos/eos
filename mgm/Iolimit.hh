//------------------------------------------------------------------------------
//! @file Iolimit.hh
//! @authors Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#pragma once
#include "common/AssistedThread.hh"
#include "common/StringConversion.hh"
#include "mgm/FsView.hh"
#include "mgm/Namespace.hh"
#include <set>
#include <string>
#include <mutex>

EOSMGMNAMESPACE_BEGIN

class Iolimit
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Iolimit() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Iolimit() {}

  void readLimits();
  void readCurrent();
  void computeScaler();

  std::string Print(std::string filter="app", std::string rangefilter="1min", std::string keyfilter="bytes");

  bool Start();
  void Stop();



private:
  AssistedThread mThread; ///< Thread computing scaler
  void Computer(ThreadAssistant& assistant) noexcept;
    
  std::mutex scalerMutex;
  std::map<std::string, double> idScaler;

  std::mutex limitMutex;
  std::map<std::string, double> idLimit;
  
  std::mutex currentMutex;
  std::map<std::string, double> idCurrent;
};

  
EOSMGMNAMESPACE_END
