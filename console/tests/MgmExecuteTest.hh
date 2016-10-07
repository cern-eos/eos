//------------------------------------------------------------------------------
//! @file MgmExecuteTest.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __MGMEXECUTETEST__HH__
#define __MGMEXECUTETEST__HH__

#include <iostream>
#include <string>
#include <queue>
#include <utility>

typedef std::pair<std::string, std::string> ReqRes;
typedef std::queue<ReqRes> QueueComm;

class MgmExecute {
public:
  std::string m_result;
  std::string m_error;
  bool test_failed;
  QueueComm m_queue;

  MgmExecute() : test_failed(false) {}

  bool ExecuteCommand(const char* command){
    std::string comm = std::string(command);
    if(m_queue.front().first == comm){
      this->m_result = m_queue.front().second;
    }
    else{
      this->test_failed = true;
    }

    m_queue.pop();

    return true;
  }

  inline std::string& GetResult() {return this->m_result;}
  inline std::string& GetError()  {return this->m_error;}
};

#endif //__MGMEXECUTETEST__HH__
