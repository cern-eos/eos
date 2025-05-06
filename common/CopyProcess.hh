// ----------------------------------------------------------------------
// File: CopyProcess.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/ASwitzerland                                  *
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

#include <vector>
#include <memory>
#include <atomic>
#include <cstddef> // for size_t
#include <mutex>

#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClPropertyList.hh>

/**
 * @file   CopyProcess.hh
 *
 * @brief  Class overcoming XRootD limitation in number of copy jobs
 *
 *
 */

#pragma once
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN;

class CopyProcess {
public:
  CopyProcess(size_t initialProcesses = 1, size_t jobsPerProc = 8192)
    : vJobs(initialProcesses), jobCounter(0), jobsPerProcess(jobsPerProc) {
    for (auto &jobPtr : vJobs) {
      jobPtr = std::make_shared<XrdCl::CopyProcess>();
    }
  }

  // Add a job to the appropriate XrdCopyProcess instance
  XRootDStatus AddJob(const PropertyList &properties, PropertyList *results) {
    size_t count = jobCounter.fetch_add(1);
    size_t index = count / jobsPerProcess;

    {
      std::lock_guard<std::mutex> lock(resizeMutex);
      if (index >= vJobs.size()) {
        vJobs.resize(index + 1);
        for (size_t i = 0; i <= index; ++i) {
          if (!vJobs[i]) {
            vJobs[i] = std::make_shared<XrdCl::CopyProcess>();
          }
        }
      }
    }

    return vJobs[index]->AddJob(properties, results);
  }

  // Prepare all copy processes; stop if any fails
  XRootDStatus Prepare() {
    for (auto &job : vJobs) {
      if (job) {
        XRootDStatus status = job->Prepare();
        if (!status.IsOK()) {
          return status;
        }
      }
    }
    return XRootDStatus(); // OK
  }

  // Run all copy processes; stop if any fails
  XRootDStatus Run(CopyProgressHandler *handler) {
    for (auto &job : vJobs) {
      if (job) {
        XRootDStatus status = job->Run(handler);
        if (!status.IsOK()) {
          return status;
        }
      }
    }
    return XRootDStatus(); // OK
  }

  const size_t Jobs() {
    return jobCounter;
  }

private:
  std::vector<std::shared_ptr<XrdCl::CopyProcess>> vJobs;
  std::atomic<size_t> jobCounter;
  const size_t jobsPerProcess;
  std::mutex resizeMutex;
};

EOSCOMMONNAMESPACE_END
