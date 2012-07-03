

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
#include "common/SymKeys.hh"
#include "common/StringConversion.hh"
#include "fst/txqueue/TransferJob.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "mgm/txengine/TransferEngine.hh"
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
  mStreams = 1;
  mId = 0;
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
  if (mJob->GetEnv()->Get("cap.sym")) {
    mSourceUrl += "?";
    mSourceUrl += "cap.sym=";
    mSourceUrl += mJob->GetEnv()->Get("source.cap.sym");
    mSourceUrl += "&cap.msg=";
    mSourceUrl += mJob->GetEnv()->Get("source.cap.msg");
  } else {
    XrdOucString sourceenv = mJob->GetEnv()->Get("source.env");
    if (sourceenv.length()) {
      mSourceUrl += "?";
      
      XrdMqMessage::UnSeal(sourceenv,"_AND_");
      mSourceUrl += sourceenv.c_str();
    }
  } 

  return mSourceUrl.c_str();
}

/* ------------------------- ------------------------------------------------ */
const char*
TransferJob::GetTargetUrl()  
{ 
  if ((!mJob) || (!mJob->GetEnv()))
    return 0;
  mTargetUrl = mJob->GetEnv()->Get("target.url");
  if (mJob->GetEnv()->Get("cap.sym")) {
    mTargetUrl += "?";
    mTargetUrl += "cap.sym=";
    mTargetUrl += mJob->GetEnv()->Get("target.cap.sym");
    mTargetUrl += "&cap.msg=";
    mTargetUrl += mJob->GetEnv()->Get("target.cap.msg");
  } else {
    XrdOucString targetenv = mJob->GetEnv()->Get("target.env");
    if (targetenv.length()) {
      mTargetUrl += "?";
      XrdMqMessage::UnSeal(targetenv,"_AND_");
      mTargetUrl += targetenv.c_str();
    }
  }
  return mTargetUrl.c_str();
}

/* ------------------------------------------------------------------------- */
void TransferJob::SendState(int state, const char* logfile) 
{
  // assemble the opaque tags to be send to the manager
  XrdOucString txinfo = "/?mgm.pcmd=txstate&tx.id=";
  XrdOucString sizestring;
  txinfo += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mId);
  txinfo += "&tx.state="; txinfo += state;
  if (logfile) {
    XrdOucString loginfob64="";
    std::string loginfo;
    eos::common::StringConversion::LoadFileIntoString(logfile, loginfo);

    eos::common::SymKey::Base64Encode((char*)loginfo.c_str(), loginfo.length(),loginfob64);
    if (loginfob64.length()) {
      // append this log
      txinfo += "&tx.log.b64=";
      txinfo += loginfob64.c_str();
    }
    eos_static_info("Sending %s", txinfo.c_str());
  }
  
  std::string manager = "";
  {
    XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
    manager = eos::fst::Config::gConfig.Manager.c_str();
  }
  
  if (manager.length()) {
    int rc = gOFS.CallManager(0,0, manager.c_str(), txinfo,0);
    if (rc) {
      eos_static_err("unable to contact manager %s", manager.c_str());
    } else {
      eos_static_debug("send %s to manager %s", txinfo.c_str(), manager.c_str());
    }
  } else {
    eos_static_err("don't know our manager");
  }
  return ;
}

/* ------------------------------------------------------------------------- */
void TransferJob::DoIt(){
  std::string fileName = "/var/eos/auth/", sTmp, strBand;
  
  std::stringstream command, ss;
  std::string uuid = NewUuid();
  std::string fileOutput = fileName + uuid;
  std::string fileResult = fileName + uuid + ".ok";
  std::string fileCredential = fileName + "." + uuid + ".cred";

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
  ss << "chown daemon:daemon $FILEOUTPUT" << std::endl;
  ss << "eoscp -u 2 -g 2 -R -n -p -t $BANDWIDTH \"$SOURCE\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
  ss << "PID=$!" << std::endl;
  ss << "AFTER=$(date +%s)" << std::endl;
  ss << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
  ss << "while kill -0 $PID 2>/dev/null && [[ $DIFFTIME -lt $TOTALTIME ]]; do" << std::endl;
  ss << "sleep 1" << std::endl;
  ss << "AFTER=$(date +%s)" << std::endl;
  ss << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
  ss << "done" << std::endl;
  ss << "chown daemon:daemon $FILERETURN 2>/dev/null" << std::endl;
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

  // retrieve bandwidth from the opaque tx.bandwidth tag if defined
  if ((mJob) && (mJob->GetEnv()) && (mJob->GetEnv()->Get("tx.bandwidth") ) ) {
    int bandwidth = atoi(mJob->GetEnv()->Get("tx.bandwidth"));
    if ( (bandwidth <0) || (bandwidth > 100000) ) {
      // we limit at 100 GB/s ;-)
      bandwidth = 100000;
    }
  }

  // retrieve timeout from the opaque tx.timeout tag if defined
  if ((mJob) && (mJob->GetEnv()) && (mJob->GetEnv()->Get("tx.expires") ) ) {
    time_t exp = (unsigned long) strtoul(mJob->GetEnv()->Get("tx.expires"),0,10);
    unsigned long timeout = (time(NULL) - exp);
    if (timeout > 86400) {
      // we cut off at a day - this makes otherwise no sense
      timeout = 86400;
    }
    if (timeout < 1) {
      // we cut off timeouts less than 1 seconds
      timeout = 1;
    }
    mTimeOut = timeout;
  }
  
  // retrieve streams from the opaque tx.streams tag if defined
  if ((mJob) && (mJob->GetEnv()) && (mJob->GetEnv()->Get("tx.streams") ) ) {
    mStreams = atoi(mJob->GetEnv()->Get("tx.streams"));
    if ( (mStreams <0) || (mStreams > 16) ) {
      // for crazy numbers we default to a single stream
      mStreams=1;
    }
  }

  // check if this is a scheduled transfer
  if ((mJob) && (mJob->GetEnv()) && (mJob->GetEnv()->Get("tx.id") ) ) {
    mId = strtoll(mJob->GetEnv()->Get("tx.id"),0,10);
  }

  bool iskrb5=false;
  bool isgsi=false;
  
  // extract credentials (if any)
  if ((mJob) && (mJob->GetEnv()) && (mJob->GetEnv()->Get("tx.auth.cred")) && (mJob->GetEnv()->Get("tx.auth.digest"))) {
    const char* symmsg = mJob->GetEnv()->Get("tx.auth.cred");
    const char* symkey = mJob->GetEnv()->Get("tx.auth.digest");
    eos::common::SymKey* key = 0;
    if ((key = eos::common::gSymKeyStore.GetKey(symkey))) {
      XrdOucString todecrypt = symmsg;
      XrdOucString decrypted ="";

      if (XrdMqMessage::SymmetricStringDecrypt(todecrypt, decrypted, (char*)key->GetKey())) {
	if (decrypted.beginswith("krb5:")) {
	  decrypted.erase(0,5);
	  iskrb5=true;
	}
	if (decrypted.beginswith("gsi:")) {
	  decrypted.erase(0,4);
	  isgsi=true;
	}
	
	// now base64decode the decrypted credential
	char* credential=0;
	unsigned int credentiallen=0;
	if (eos::common::SymKey::Base64Decode(decrypted, credential, credentiallen)) {
	  if (credential) {
	    int fd = open(fileCredential.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	    if (fd>=0) {
	      if ( (write(fd, credential, credentiallen)) != (ssize_t)credentiallen) {
		eos_static_err("unable to write all bytes to %s", fileCredential.c_str());
		iskrb5=isgsi=false;
	      }
	      close(fd);
	    } else {
	      eos_static_err("unable to open credential file %s", fileCredential.c_str());
	      iskrb5=isgsi=false;
	    }
	    memset(credential,0, credentiallen);
	    free(credential);
	  } else {
	    iskrb5=isgsi=false;
	    eos_static_err("unable to base64 decode the credential %s", decrypted.c_str());
	  }
	}
      } else {
	eos_static_err("cannot decode message %s", symmsg);
	iskrb5=isgsi=false;
      } 
    } else {
      eos_static_err("miss the symkey for digest %s", symkey);
      iskrb5=isgsi=false;
    } 
  }
  

  if (mId) {SendState(eos::mgm::TransferEngine::kRunning);}
  if (iskrb5) {
    command << "unset XrdSecPROTOCOL; KRB5CCNAME="; command << fileCredential; command << " ";
  } else {
    if (isgsi) {
      command <<"unset XrdSecPROTOCOL; X509_USER_PROXY="; command << fileCredential; command << " ";
    } else {
      command <<"unset XrdSecPROTOCOL; ";
    }
  }

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

  // now set the transfer state and send the log output
  if (WEXITSTATUS(rc)) {
    eos_static_err("%s returned %d", command.str().c_str(), rc);
    if (mId) {SendState(eos::mgm::TransferEngine::kFailed, fileOutput.c_str());}
  } else {
    if (mId) {SendState(eos::mgm::TransferEngine::kDone, fileOutput.c_str());}
  }

  // move the output to the log file
  std::string cattolog = "touch /var/log/eos/fst/eoscp.log; cat "; cattolog += fileOutput.c_str(); cattolog +=" >> /var/log/eos/fst/eoscp.log 2>/dev/null";
  system(cattolog.c_str());

  // remove the result files
  rc = unlink(fileOutput.c_str());
  if (rc) rc = 0; // for compiler happyness
  rc = unlink(fileResult.c_str());
  if (rc) rc = 0; // for compiler happyness
  rc = unlink(fileCredential.c_str());
  if (rc) rc = 0; // for compiler happyness
  rc = unlink(fileName.c_str());
  if (rc) rc = 0; // for compiler happyness

  // we are over running
  mQueue->DecRunning();
}
  
EOSFSTNAMESPACE_END
