// ----------------------------------------------------------------------
// File: TransferJob.cc
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

/* ------------------------------------------------------------------------- */
#include "common/Logging.hh"
#include "common/CloExec.hh"
#include "fst/txqueue/TransferJob.hh"
/* ------------------------------------------------------------------------- */
#include <fstream>
#include <sstream>
#include <cstdio>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <uuid/uuid.h>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
template <class T>
inline std::string ToString(const T& t){
  std::ostringstream oss;
  oss << t;
  return oss.str();
}

TransferJob::TransferJob(TransferQueue* queue, eos::common::TransferJob* job,  int bw, int timeout){
  mQueue = queue;
  mBandWidth = bw;
  mTimeOut   = timeout;
  mJob = job;
  mSourceUrl = "";
  mTargetUrl = "";
}

/* ------------------------------------------------------------------------- */
TransferJob::~TransferJob()
{
  if (mJob) {
    delete mJob;
  }
}


/* ------------------------------------------------------------------------- */
std::string TransferJob::NewUuid() {
  // create message ID;
  std::string sTmp;
  char uuidstring[40];
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid,uuidstring);
  sTmp = uuidstring;
  return sTmp;
}

/* ------------------------------------------------------------------------- */
const char*
TransferJob::GetSourceUrl() 
{
  if ((!mJob) || (!mJob->GetEnv()))
    return 0;
  mSourceUrl = mJob->GetEnv()->Get("source.url");
  mSourceUrl += "?";
  mSourceUrl += "cap.sym=";
  mSourceUrl += mJob->GetEnv()->Get("source.cap.sym");
  mSourceUrl += "&cap.msg=";
  mSourceUrl += mJob->GetEnv()->Get("source.cap.msg");
  return mSourceUrl.c_str();
}

/* ------------------------- ------------------------------------------------ */
const char*
TransferJob::GetTargetUrl()  
{ 
  if ((!mJob) || (!mJob->GetEnv()))
    return 0;
  mTargetUrl = mJob->GetEnv()->Get("target.url");
  mTargetUrl += "?";
  mTargetUrl += "cap.sym=";
  mTargetUrl += mJob->GetEnv()->Get("target.cap.sym");
  mTargetUrl += "&cap.msg=";
  mTargetUrl += mJob->GetEnv()->Get("target.cap.msg");
  return mTargetUrl.c_str();
}

/* ------------------------------------------------------------------------- */
void TransferJob::DoIt(){
  std::string fileName = "/tmp/", sTmp, strBand;
  
  std::stringstream command, ss;
  std::string fileOutput = fileName + NewUuid();
  std::string fileResult = fileName + NewUuid() + ".ok";
  
  ss << "#!/bin/bash" << std::endl;
  ss << "SCRIPTNAME=$0" << std::endl;
  ss << "SOURCE=$1" << std::endl;
  ss << "DEST=$2" << std::endl;
  ss << "TOTALTIME=$3" << std::endl;
  ss << "BANDWIDTH=$4" << std::endl;
  ss << "FILEOUTPUT=$5" << std::endl;
  ss << "FILERETURN=$6" << std::endl;
  ss << "BEFORE=$(date +%s)" << std::endl;
  ss << "[ -f $FILEOUTPUT ] && rm $FILEOUTPUT" << std::endl;
  ss << "[ -f $FILERETURN ] && rm $FILERETURN" << std::endl;
  ss << "touch $FILEOUTPUT" << std::endl;
  ss << "touch $FILERETURN" << std::endl;
  ss << "chown daemon:daemon $FILEOUTPUT" << std::endl;
  ss << "chown daemon:daemon $FILERETURN" << std::endl;
  ss << "eoscp -u 2 -g 2 -R -n -p -t $BANDWIDTH \"$SOURCE\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
  ss << "PID=$!" << std::endl;
  ss << "AFTER=$(date +%s)" << std::endl;
  ss << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
  ss << "while kill -0 $PID 2>/dev/null && [[ $DIFFTIME -lt $TOTALTIME ]]; do" << std::endl;
  ss << "sleep 1" << std::endl;
  ss << "AFTER=$(date +%s)" << std::endl;
  ss << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
  ss << "done" << std::endl;
  ss << "if kill -0 $PID 2>/dev/null " << std::endl;
  ss << "then" << std::endl;
  ss << "kill -9 $PID 2> /dev/null " << std::endl;
  ss << "fi" << std::endl;
  ss << "rm -rf $SCRIPTNAME" << std::endl;
  ss << "if [ -e $FILERETURN ] " << std::endl;
  ss << "then" << std::endl;
  ss << "rm -rf $FILERETURN "<< std::endl;
  ss << "exit 0; " << std::endl;
  ss << "else " << std::endl;
  ss << "exit 255; " << std::endl;
  ss << "fi" << std::endl;
   
  fileName = fileName + NewUuid() + ".sh";
  std::ofstream file;
  file.open(fileName.c_str());
  file << ss.str();
  file.close();
  
  std::string mSource      = GetSourceUrl();
  std::string mDestination = GetTargetUrl();

  command << "/bin/sh ";  command << fileName + " \"";
  command << mSource + "\" \"";    
  command << mDestination + "\" ";
  command << ToString(mTimeOut) + " ";
  command << ToString(mBandWidth) + " ";
  command << fileOutput + " ";
  command << fileResult + " ";

  eos_static_debug("executing %s", command.str().c_str());

  // avoid cloning of FDs on fork
  eos::common::CloExec::All();

  int rc = system(command.str().c_str());
  if (WEXITSTATUS(rc)) {
    eos_static_err("%s returned %d", command.str().c_str(), rc);
  }

  // move the output to the log file
  std::string cattolog = "touch /var/log/eos/fst/eoscp.log; cat "; cattolog += fileOutput.c_str(); cattolog +=" >> /var/log/eos/fst/eoscp.log 2>/dev/null";
  system(cattolog.c_str());

  // remove the result files
  rc = unlink(fileOutput.c_str());
  if (rc) rc = 0; // for compiler happyness
  rc = unlink(fileResult.c_str());
  if (rc) rc = 0; // for compiler happyness

  // we are over running
  mQueue->DecRunning();
}
  
EOSFSTNAMESPACE_END
