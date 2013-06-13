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

/* -------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/FileId.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
#include "Xrd/XrdScheduler.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>

/* -------------------------------------------------------------------------- */
/**
 * @file Converter.hh
 * 
 * @brief File Layout Conversion Service class and Conversion Job class
 * 
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN


/* -------------------------------------------------------------------------- */
class ConverterJob : XrdJob
{
  // ---------------------------------------------------------------------------
  // This class executes a conversion job
  // ---------------------------------------------------------------------------

private:
  eos::common::FileId::fileid_t mFid;
  std::string mTargetPath;
  std::string mSourcePath;
  std::string mTargetCGI;

  XrdOucString mConversionLayout;
  std::string mConverterName;

public:

  ConverterJob (eos::common::FileId::fileid_t fid, const char* conversionlayout, std::string &convertername);
  ~ConverterJob () {};

  void DoIt ();
};

/*----------------------------------------------------------------------------*/
/**
 * @brief  This class run's the file layout conversion service per space.
 * 
 * This class run's an eternal thread per configured space which is responsible 
 * to pick-up conversion
 * jobs from the directory /eos/../proc/conversion/\n\n
 * It uses the XrdScheduler class to run third party clients copying files
 * into the conversion definition files named !<fid(016x)!>:!<conversionlayout!>
 * If a third party conversion finished successfully the layout & replica of the
 * converted temporary file will be merged into the existing file and the previous
 * layout will be dropped.
 */
/*----------------------------------------------------------------------------*/
class Converter
{
private:
  //< thread id
  pthread_t mThread;
  
  //< name of th espace this converter serves
  std::string mSpaceName;
  
  //< this are all jobs which are queued and didn't run yet
  size_t      mActiveJobs; 
  
  //< condition variabl to get signalled for a done job
  XrdSysCondVar mDoneSignal;
public:

  // ---------------------------------------------------------------------------
  // Constructor (per space)
  // ---------------------------------------------------------------------------
  Converter (const char* spacename);

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~Converter ();

  // ---------------------------------------------------------------------------
  // Service thread static startup function
  // ---------------------------------------------------------------------------
  static void* StaticConverter (void*);

  // ---------------------------------------------------------------------------
  // Service implementation e.g. eternal conversion loop running third-party 
  // conversion
  // ---------------------------------------------------------------------------
  void* Convert (void);

  // ---------------------------------------------------------------------------
  //! Return the condition variable to signal when a job finishes
  // ---------------------------------------------------------------------------
  XrdSysCondVar* GetSignal() { return &mDoneSignal; }

  // ---------------------------------------------------------------------------
  //! Decrement the number of active jobs in this converter
  // ---------------------------------------------------------------------------
  void DecActiveJobs() { mActiveJobs--; }

  // ---------------------------------------------------------------------------
  //! Return active jobs
  // ---------------------------------------------------------------------------
  size_t GetActiveJobs() const { return mActiveJobs; }

  //< the scheduler class is providing a destructor-less object, 
  //< so we have to create once a singleton of this and keep/share it
  static XrdSysMutex gSchedulerMutex; 
  
  //< singelton objct of a scheduler
  static XrdScheduler* gScheduler; 
  
  //< the mutex protecting the map of existing converter objects
  static XrdSysMutex gConverterMapMutex; 
  
  //< map containing the current allocated converter objects
  static std::map<std::string,Converter*> gConverterMap; 
};

EOSMGMNAMESPACE_END
#endif

