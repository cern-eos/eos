// ----------------------------------------------------------------------
// File: TransferJob.hh
// Author: Elvin Sindrilaru/Andreas-Joachim Peters - CERN
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

#ifndef __EOSFST_TRANSFER_JOB__
#define __EOSFST_TRANSFER_JOB__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "common/TransferJob.hh"
/* ------------------------------------------------------------------------ */
#include "Xrd/XrdJob.hh"
/* ------------------------------------------------------------------------- */
#include <string>

/* ------------------------------------------------------------------------- */


EOSFSTNAMESPACE_BEGIN

class TransferJob: public XrdJob {

private:
  TransferQueue* mQueue;
  eos::common::TransferJob* mJob;
  int mBandWidth; // band width in Mb/s
  int mTimeOut;   // max duration for a transfer in seconds
  int mStreams;   // number of streams to use
  XrdOucString mSourceUrl;
  XrdOucString mTargetUrl;
  long long mId;  // the ID is only used for scheduled gateway transfers (managed via 'transfer' console)

public:

  TransferJob(TransferQueue* queue, eos::common::TransferJob* cjob,  int bw, int timeout=7200);
  ~TransferJob();

  void DoIt();
  std::string NewUuid();

  const char* GetSourceUrl();
  const char* GetTargetUrl();

  void SendState(int state, const char* logfile=0);
};

EOSFSTNAMESPACE_END

#endif
