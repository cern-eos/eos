// ----------------------------------------------------------------------
// File: Converter.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_CONVERTER__
#define __EOSMGM_CONVERTER__

#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/AssistedThread.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "Xrd/XrdJob.hh"
#include <string>
#include <cstring>

class XrdScheduler;

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! @brief Class executing a third-party conversion job
//----------------------------------------------------------------------------
class ConverterJob : XrdJob
{
public:
  //----------------------------------------------------------------------------
  //! @brief Constructor of a conversion job
  //!
  //! @param fid file id of the file to convert
  //! @param conversionlayout string describing the conversion layout to use
  //! @param convertername to be used
  //----------------------------------------------------------------------------
  ConverterJob(eos::common::FileId::fileid_t fid,
               const char* conversionlayout,
               std::string& convertername);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConverterJob();

  //----------------------------------------------------------------------------
  // Job execution function
  //----------------------------------------------------------------------------
  void DoIt();

private:
  //----------------------------------------------------------------------------
  //! Merge origial and the newly converted one so that the initial file
  //! identifier and all the rest of the metadata information is preserved.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Merge();

  eos::common::FileId::fileid_t mFid; ///< file id of the conversion job
  std::string mTargetPath; ///< target path of the conversion job
  std::string mSourcePath; ///< source path of the conversion job
  std::string mProcPath; ///< proc path of the conversion job
  std::string mTargetCGI; ///< target CGI of the conversion job
  XrdOucString mConversionLayout; ///< layout name of the target file
  std::string mConverterName; ///< target space name of the conversion
};

//------------------------------------------------------------------------------
//! @brief Class running the file layout conversion service per space
//!
//! This class run's an eternal thread per configured space which is responsible
//! to pick-up conversion
//! jobs from the directory /eos/../proc/conversion/\n\n
//! It uses the XrdScheduler class to run third party clients copying files
//! into the conversion definition files named !<fid(016x)!>:!<conversionlayout!>
//! If a third party conversion finished successfully the layout & replica of the
//! converted temporary file will be merged into the existing file and the
//! previous layout will be dropped.
//! !<conversionlayout!> is formed like !<space[.group]~>=!<layoutid(08x)!>
//------------------------------------------------------------------------------
class Converter
{
public:
  //----------------------------------------------------------------------------
  //! @brief Constructor by space name
  //!
  //! @param spacename name of the associated space
  //----------------------------------------------------------------------------
  Converter(const char* spacename);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Converter();

  //----------------------------------------------------------------------------
  //! Thread stop function
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Service implementation e.g. eternal conversion loop running third-party
  //! conversion
  //----------------------------------------------------------------------------
  void Convert(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Return the condition variable to signal when a job finishes
  //----------------------------------------------------------------------------
  XrdSysCondVar* GetSignal()
  {
    return &mDoneSignal;
  }

  //----------------------------------------------------------------------------
  //! Decrement the number of active jobs in this converter
  //----------------------------------------------------------------------------
  void DecActiveJobs()
  {
    if (mActiveJobs) {
      mActiveJobs--;
    }

    PublishActiveJobs();
  }

  //----------------------------------------------------------------------------
  //! Increment the number of active jobs in this converter
  //----------------------------------------------------------------------------
  void IncActiveJobs()
  {
    mActiveJobs++;
    PublishActiveJobs();
  }

  //----------------------------------------------------------------------------
  //! Publish the number of active jobs in this converter
  //----------------------------------------------------------------------------
  void PublishActiveJobs();

  //----------------------------------------------------------------------------
  //! Return active jobs
  //----------------------------------------------------------------------------
  size_t GetActiveJobs() const
  {
    return mActiveJobs;
  }

  //----------------------------------------------------------------------------
  //! Reset pending conversion entries
  //----------------------------------------------------------------------------
  void ResetJobs();

  static XrdSysMutex gSchedulerMutex; ///< Used for scheduler singleton
  static XrdScheduler* gScheduler; ///< Scheduler singleton
  static XrdSysMutex gConverterMapMutex; ///< Mutex protecting converter map
  //! Map containing the current allocated converter objects
  static std::map<std::string, Converter*> gConverterMap;

private:
  AssistedThread mThread; ///< Thread id
  std::string mSpaceName; ///< name of the espace this converter serves
  size_t mActiveJobs; ///< All the queued jobs that didn't run yet
  XrdSysCondVar mDoneSignal; ///< Condition variable signalled when a job is done
};

EOSMGMNAMESPACE_END
#endif
