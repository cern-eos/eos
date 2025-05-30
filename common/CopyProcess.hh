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

#include <XrdCl/XrdClStatus.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClPropertyList.hh>

/**
 * @file   CopyProcess.hh
 *
 * @brief  Class overcoming XRootD limitation in number of copy jobs by managing multiple CopyProcess instances
 * 
 * The CopyProcess class provides a scalable solution for handling large numbers of copy operations
 * in the EOS system. It addresses XRootD's limitation on the number of copy jobs per process by
 * automatically managing multiple XrdCl::CopyProcess instances.
 *
 * Key features:
 * - Dynamic scaling of copy processes based on job count
 * - Automatic job distribution across multiple processes
 * - Thread-safe operation for concurrent job additions
 * - Configurable number of jobs per process
 * - Support for parallel copy streams within each process
 * 
 * Example usage:
 * @code
 * CopyProcess cp(2, 1000); // 2 initial processes, 1000 jobs per process
 * XrdCl::PropertyList props, *results;
 * // Configure copy properties...
 * cp.AddJob(props, results);
 * cp.Prepare(4); // Prepare with 4 parallel streams per process
 * cp.Run(progressHandler);
 * @endcode
 */

#pragma once
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN;

/**
 * @class CopyProcess
 * @brief Manages multiple XRootD copy processes for scalable file transfer operations
 *
 * This class provides a wrapper around XrdCl::CopyProcess that automatically manages
 * multiple copy process instances to handle large numbers of copy jobs. It distributes
 * jobs across processes and ensures thread-safe operation.
 */
class CopyProcess {
public:
  /**
   * @brief Constructor for CopyProcess
   * @param initialProcesses Number of XrdCl::CopyProcess instances to create initially
   * @param jobsPerProc Maximum number of jobs per process before creating a new one
   */
  CopyProcess(size_t initialProcesses = 1, size_t jobsPerProc = 8192)
    : vJobs(initialProcesses), jobCounter(0), jobsPerProcess(jobsPerProc) {
    for (auto &jobPtr : vJobs) {
      jobPtr = std::make_shared<XrdCl::CopyProcess>();
    }
  }

  /**
   * @brief Add a new copy job to the appropriate process
   * @param properties Configuration for the copy job
   * @param results PropertyList to store job results
   * @return XRootDStatus indicating success or failure
   *
   * Automatically distributes jobs across processes, creating new ones if needed.
   * Thread-safe for concurrent job additions.
   */
  XrdCl::XRootDStatus AddJob(const XrdCl::PropertyList &properties, XrdCl::PropertyList *results) {
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

  /**
   * @brief Prepare all copy processes for execution
   * @param parallel Number of parallel copy streams per process (default: 1)
   * @return XRootDStatus indicating success or failure
   *
   * Configures and prepares all copy processes. If any process fails to prepare,
   * returns the error status immediately.
   */
  XrdCl::XRootDStatus Prepare(size_t parallel=1) {
    for (auto &job : vJobs) {
      if (job) {
	XrdCl::PropertyList processConfig;
	processConfig.Set( "jobType", "configuration" );
	processConfig.Set( "parallel", parallel );
	job->AddJob( processConfig, 0 );
	XrdCl::XRootDStatus status = job->Prepare();
        if (!status.IsOK()) {
          return status;
        }
      }
    }
    return XrdCl::XRootDStatus(); // OK
  }

  /**
   * @brief Execute all prepared copy jobs
   * @param handler Progress handler for monitoring copy operations
   * @return XRootDStatus indicating success or failure
   *
   * Runs all copy processes sequentially. If any process fails,
   * returns the error status immediately. Clears all processes after completion.
   */
  XrdCl::XRootDStatus Run(XrdCl::CopyProgressHandler *handler) {
    for (auto &job : vJobs) {
      if (job) {
	XrdCl::XRootDStatus status = job->Run(handler);
        if (!status.IsOK()) {
          return status;
        }
      }
    }
    std::lock_guard<std::mutex> lock(resizeMutex);
    vJobs.clear();
    return XrdCl::XRootDStatus(); // OK
  }

  /**
   * @brief Get the total number of jobs added
   * @return Number of jobs currently managed
   */
  const size_t Jobs() {
    return jobCounter;
  }

private:
  std::vector<std::shared_ptr<XrdCl::CopyProcess>> vJobs;    ///< Vector of copy process instances
  std::atomic<size_t> jobCounter;                            ///< Total number of jobs added
  const size_t jobsPerProcess;                               ///< Maximum jobs per process
  std::mutex resizeMutex;                                    ///< Mutex for thread-safe vector resizing
};

EOSCOMMONNAMESPACE_END
