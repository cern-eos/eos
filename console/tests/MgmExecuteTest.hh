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
#include <fstream>
#include <string>
#include <queue>
#include <utility>

using ReqRes = std::pair<std::string, std::string>;
using QueueComm = std::queue<ReqRes>;

class MgmExecute
{
public:
  std::string m_result;
  std::string m_error;
  bool test_failed;
  QueueComm m_queue;

  MgmExecute() : test_failed(false) {}

  bool ExecuteCommand(const char* command)
  {
    std::string comm = std::string(command);

    if (m_queue.front().first == comm) {
      m_result = m_queue.front().second;
    } else {
      test_failed = true;
    }

    m_queue.pop();
    return true;
  }

  bool ExecuteAdminCommand(const char* command)
  {
    std::string comm = std::string(command);

    if (m_queue.front().first == comm) {
      m_result = m_queue.front().second;
    } else {
      test_failed = true;
    }

    m_queue.pop();
    return true;
  }

  void LoadResponsesFromFile(const std::string& path)
  {
    std::ifstream file;
    file.open(path);
    std::string line;
    ReqRes temp;

    while (std::getline(file, line,  '#')) {
      temp.first = line;

      if (std::getline(file, line,  '#')) {
        temp.second = line;
      } else {
        throw std::string("Load failed!!");
      }

      m_queue.push(temp);
    }

    file.close();
  }

  inline std::string& GetResult()
  {
    return m_result;
  }

  inline std::string& GetError()
  {
    return m_error;
  }
};

#endif //__MGMEXECUTETEST__HH__
