

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
#include <math.h>

/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
template <class T>
inline std::string
ToString (const T& t)
{
 std::ostringstream oss;
 oss << t;
 return oss.str();
}

TransferJob::TransferJob (TransferQueue* queue, eos::common::TransferJob* job, int bw, int timeout)
{
 mQueue = queue;
 mBandWidth = bw;
 mTimeOut = timeout;
 mJob = job;
 mSourceUrl = "";
 mTargetUrl = "";
 mStreams = 1;
 mId = 0;
 mProgressThread = 0;
 mProgressFile = "";
 mLastProgress = 0.0;
 mDoItThread = 0;
 mCanceled = false;
 mLastState = 0;
}

/* ------------------------------------------------------------------------- */
TransferJob::~TransferJob ()
{
 if (mJob)
 {
   delete mJob;
 }

 if (mProgressThread)
 {
   XrdSysThread::Cancel(mProgressThread);
   XrdSysThread::Join(mProgressThread, NULL);
   mProgressThread = 0;
 }
}

/* ------------------------------------------------------------------------- */
void*
TransferJob::StaticProgress (void* arg)
{
 return reinterpret_cast<TransferJob*> (arg)->Progress();
}

/* ------------------------------------------------------------------------- */
void*
TransferJob::Progress ()
{
 XrdSysThread::SetCancelOn();
 while (1)
 {
   eos_static_debug("progress loop");
   float progress = 0;
   // try to read the progress filename
   XrdSysThread::SetCancelOff();
   FILE* fd = fopen(mProgressFile.c_str(), "r");
   if (fd)
   {
     int item = fscanf(fd, "%f\n", &progress);
     eos_static_debug("progress=%.02f", progress);
     if (item == 1)
     {
       if (fabs(mLastProgress - progress) > 1)
       {
         // send only if there is a significant change
         int rc = SendState(0, 0, progress);
         if (rc == -EIDRM)
         {
           eos_static_warning("job %lld has been canceled", mId);
           // cancel this job !
           mCancelMutex.Lock();
           mCanceled = true;
           mCancelMutex.UnLock();
           return 0;
         }
         mLastProgress = progress;
       }
     }
     fclose(fd);
   }
   XrdSysThread::SetCancelOn();
   XrdSysTimer sleeper;
   sleeper.Wait(1000); // don't report more than 1 Hz
 }
 return 0;
}

std::string
TransferJob::NewUuid ()
{
 // create message ID;
 std::string sTmp;
 char uuidstring[40];
 uuid_t uuid;
 uuid_generate_time(uuid);
 uuid_unparse(uuid, uuidstring);
 sTmp = uuidstring;
 return sTmp;
}

/* ------------------------------------------------------------------------- */
const char*
TransferJob::GetSourceUrl ()
{
 if ((!mJob) || (!mJob->GetEnv()))
   return 0;
 mSourceUrl = mJob->GetEnv()->Get("source.url");
 if (mJob->GetEnv()->Get("source.cap.sym"))
 {
   mSourceUrl += "?";
   mSourceUrl += "cap.sym=";
   mSourceUrl += mJob->GetEnv()->Get("source.cap.sym");
   mSourceUrl += "&cap.msg=";
   mSourceUrl += mJob->GetEnv()->Get("source.cap.msg");
 }
 else
 {
   XrdOucString sourceenv = mJob->GetEnv()->Get("source.env");
   if (sourceenv.length())
   {
     mSourceUrl += "?";

     XrdMqMessage::UnSeal(sourceenv, "_AND_");
     mSourceUrl += sourceenv.c_str();
   }
 }

 return mSourceUrl.c_str();
}

/* ------------------------- ------------------------------------------------ */
const char*
TransferJob::GetTargetUrl ()
{
 if ((!mJob) || (!mJob->GetEnv()))
   return 0;
 mTargetUrl = mJob->GetEnv()->Get("target.url");
 if (mJob->GetEnv()->Get("target.cap.sym"))
 {
   mTargetUrl += "?";
   mTargetUrl += "cap.sym=";
   mTargetUrl += mJob->GetEnv()->Get("target.cap.sym");
   mTargetUrl += "&cap.msg=";
   mTargetUrl += mJob->GetEnv()->Get("target.cap.msg");
 }
 else
 {
   XrdOucString targetenv = mJob->GetEnv()->Get("target.env");
   if (targetenv.length())
   {
     mTargetUrl += "?";
     XrdMqMessage::UnSeal(targetenv, "_AND_");
     mTargetUrl += targetenv.c_str();
   }
 }
 return mTargetUrl.c_str();
}

/* ------------------------------------------------------------------------- */
int
TransferJob::SendState (int state, const char* logfile, float progress)
{
 XrdSysMutexHelper lock(SendMutex);
 // assemble the opaque tags to be send to the manager
 XrdOucString txinfo = "/?mgm.pcmd=txstate&tx.id=";
 XrdOucString sizestring;

 txinfo += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mId);

 if (progress)
 {
   char sprogress[16];
   snprintf(sprogress, sizeof (sprogress) - 1, "%.02f", progress);
   // progress information only
   txinfo += "&tx.progress=";
   txinfo += sprogress;
 }
 else
 {
   // state and/or log
   txinfo += "&tx.state=";
   txinfo += state;
   // log state transitions in the FST log file
   eos_static_info("txid=%lld state=%s", mId, eos::mgm::TransferEngine::GetTransferState(state));
   if (logfile)
   {
     XrdOucString loginfob64 = "";
     std::string loginfo;
     eos::common::StringConversion::LoadFileIntoString(logfile, loginfo);

     eos::common::SymKey::Base64Encode((char*) loginfo.c_str(), loginfo.length(), loginfob64);
     if (loginfob64.length())
     {
       // append this log
       txinfo += "&tx.log.b64=";
       txinfo += loginfob64.c_str();
     }
   }
 }

 if (mLastState == eos::mgm::TransferEngine::kDone)
 {
   eos_static_debug("txid=%lld skipping update - we have already a 'done' state", mId);
   // when the done state is reached, it does not make sense to update the progress - moreover the transfer can be autoarchived and the update would fail
   return 0;
 }

 if (!progress)
 {
   mLastState = state;
 }

 eos_static_debug("sending %s", txinfo.c_str());

 std::string manager = "";
 {
   XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
   manager = eos::fst::Config::gConfig.Manager.c_str();
 }

 int rc = 0;
 if (manager.length())
 {
   rc = gOFS.CallManager(0, 0, manager.c_str(), txinfo, 0);
   if (rc)
   {
     if (rc != -EIDRM)
     {
       eos_static_err("unable to contact manager %s", manager.c_str());
     }
   }
   else
   {
     eos_static_debug("send %s to manager %s", txinfo.c_str(), manager.c_str());
   }
 }
 else
 {
   eos_static_err("don't know our manager");
   rc = EINVAL;
 }
 return rc;
}

/* ------------------------------------------------------------------------- */
void
TransferJob::DoIt ()
{
 // This is the execution part of a transfer
 // - in the standard case where we use only 'root:' protocol, we prepare a single script 
 //   running the transfer with a given timeout
 // - if we use an external protocol for the destination the transfer is split into two parts
 //   - stagein 
 //   - stageout

 mDoItThread = XrdSysThread::ID();
 std::string fileName = "/var/eos/auth/", sTmp, strBand; // script name for the transfer script
 std::string fileStageName = fileName;
 std::stringstream command, ss, commando, so;
 std::string uuid = NewUuid();
 std::string fileOutput = fileName + uuid; // output file of the transfer script
 std::string fileResult = fileName + uuid + ".ok"; // return code of the transfer script
 std::string fileStageOutput = fileName + uuid + ".stageout"; // output file of the stageout script
 std::string fileStageResult = fileName + uuid + ".stageout" + ".ok"; // file containing credentails
 std::string fileCredential = fileName + "." + uuid + ".cred";

 std::string progressFileName = fileName + "." + uuid + ".progress";

 mProgressFile = progressFileName.c_str(); // used by the progress report thread

 std::string downloadcmd = "";
 std::string uploadcmd = "";

 std::string stagefile = "";

 static XrdSysMutex eoscpLogMutex; // avoids that several transfers write interleaved into the log file;

 XrdOucString mSource = GetSourceUrl();
 XrdOucString mDestination = GetTargetUrl();

 bool iskrb5 = false;
 bool isgsi = false;

 if ((mJob) && (mJob->GetEnv()))
 {
   // retrieve bandwidth from the opaque tx.bandwidth tag if defined
   if ((mJob->GetEnv()->Get("tx.bandwidth")))
   {
     int bandwidth = atoi(mJob->GetEnv()->Get("tx.bandwidth"));
     if ((bandwidth < 0) || (bandwidth > 100000))
     {
       // we limit at 100 GB/s ;-)
       bandwidth = 100000;
     }
   }

   // retrieve timeout from the opaque tx.timeout tag if defined
   if ((mJob->GetEnv()->Get("tx.expires")))
   {
     time_t exp = (unsigned long) strtoul(mJob->GetEnv()->Get("tx.expires"), 0, 10);
     unsigned long timeout = (time(NULL) - exp);
     if (timeout > 86400)
     {
       // we cut off at a day - this makes otherwise no sense
       timeout = 86400;
     }
     if (timeout < 1)
     {
       // we cut off timeouts less than 1 seconds
       timeout = 1;
     }
     mTimeOut = timeout;
   }

   // retrieve streams from the opaque tx.streams tag if defined
   if ((mJob->GetEnv()->Get("tx.streams")))
   {
     mStreams = atoi(mJob->GetEnv()->Get("tx.streams"));
     if ((mStreams < 0) || (mStreams > 16))
     {
       // for crazy numbers we default to a single stream
       mStreams = 1;
     }
   }

   // check if this is a scheduled transfer
   if ((mJob->GetEnv()->Get("tx.id")))
   {
     mId = strtoll(mJob->GetEnv()->Get("tx.id"), 0, 10);
   }

   // extract credentials (if any)
   if ((mJob->GetEnv()->Get("tx.auth.cred")) && (mJob->GetEnv()->Get("tx.auth.digest")))
   {
     const char* symmsg = mJob->GetEnv()->Get("tx.auth.cred");
     const char* symkey = mJob->GetEnv()->Get("tx.auth.digest");
     eos::common::SymKey* key = 0;
     if ((key = eos::common::gSymKeyStore.GetKey(symkey)))
     {
       XrdOucString todecrypt = symmsg;
       XrdOucString decrypted = "";

       if (XrdMqMessage::SymmetricStringDecrypt(todecrypt, decrypted, (char*) key->GetKey()))
       {
         if (decrypted.beginswith("krb5:"))
         {
           decrypted.erase(0, 5);
           iskrb5 = true;
         }
         if (decrypted.beginswith("gsi:"))
         {
           decrypted.erase(0, 4);
           isgsi = true;
         }

         // now base64decode the decrypted credential
         char* credential = 0;
         unsigned int credentiallen = 0;
         if (eos::common::SymKey::Base64Decode(decrypted, credential, credentiallen))
         {
           if (credential)
           {
             int fd = open(fileCredential.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
             if (fd >= 0)
             {
               if ((write(fd, credential, credentiallen)) != (ssize_t) credentiallen)
               {
                 eos_static_err("unable to write all bytes to %s", fileCredential.c_str());
                 iskrb5 = isgsi = false;
               }
               close(fd);
             }
             else
             {
               eos_static_err("unable to open credential file %s", fileCredential.c_str());
               iskrb5 = isgsi = false;
             }
             memset(credential, 0, credentiallen);
             free(credential);
           }
           else
           {
             iskrb5 = isgsi = false;
             eos_static_err("unable to base64 decode the credential %s", decrypted.c_str());
           }
         }
       }
       else
       {
         eos_static_err("cannot decode message %s", symmsg);
         iskrb5 = isgsi = false;
       }
     }
     else
     {
       eos_static_err("miss the symkey for digest %s", symkey);
       iskrb5 = isgsi = false;
     }
   }
 }

 if (mDestination.beginswith("root://"))
 {
   if ((mSource.beginswith("as3://")) ||
       (mSource.beginswith("http://")) ||
       (mSource.beginswith("https://")) ||
       (mSource.beginswith("gsiftp://")))
   {
     // this is a download using an external protocol into XRootD protocol, we can use a pipe with STDIN/OUT
     if (mSource.beginswith("as3://"))
     {
       // setup the s3 hostname
       int spos = mSource.find("/", 6);
       if (spos != STR_NPOS)
       {
         XrdOucString hname;
         hname.assign(mSource, 6, spos - 1);
         setenv("S3_HOSTNAME", hname.c_str(), 1);
         mSource.erase(4, spos - 3);
         mSource.erase(0, 4);
       }
       XrdOucString mSourceEnv = mSource;
       spos = mSourceEnv.find("?");
       if (spos != STR_NPOS)
       {
         mSourceEnv.erase(0, spos + 1);
         mSource.erase(spos);
       }
       XrdOucEnv mEnv(mSourceEnv.c_str());

       // setup the s3 id & keys
       setenv("S3_SECRET_ACCESS_KEY", mEnv.Get("s3.key") ? mEnv.Get("s3.key") : "", 1);
       setenv("S3_ACCESS_KEY_ID", mEnv.Get("s3.id") ? mEnv.Get("s3.id") : "", 1);
       eos_static_debug("S3_HOSTNAME=%s S3_SECRET_ACCESS_KEY=%s S3_ACCESS_KEY_ID=%s", getenv("S3_HOSTNAME"), getenv("S3_SECRET_ACCESS_KEY"), getenv("S3_ACCESS_KEY_ID"));
       // S3 download
       downloadcmd = "s3 get ";
       downloadcmd += mSource.c_str();
       downloadcmd += " |";
     }

     if (mSource.beginswith("http://"))
     {
       // HTTP download
       downloadcmd = "curl ";
       downloadcmd += mSource.c_str();
       downloadcmd += " |";
     }

     if (mSource.beginswith("https://"))
     {
       // HTTPS download disabling certificate check (for the moment)
       downloadcmd = "curl ";
       downloadcmd += mSource.c_str();
       downloadcmd += " -k |";
     }

     if (mSource.beginswith("gsiftp://"))
     {
       // GSIFTP download
       downloadcmd = "globus-url-copy ";
       downloadcmd += mSource.c_str();
       downloadcmd += " - |";
     }
   }
   else
   {
     // check for root protocol otherwise discard 
     if (!mSource.beginswith("root://"))
     {
       eos_static_err("illegal source protocol specified: %s", mSource.c_str());
     }
   }
 }
 else
 {
   // this is an external destination protocol, we have to setup a stagefile
   XrdOucString stagesuffix = mDestination;
   stagesuffix.erase(mDestination.find("?"));
   while (stagesuffix.replace("/", ""))
   {
   }
   // we need to do a staged transfer with a temporary copy on a local disk
   unsetenv("TMPDIR");
   // we create a unique name here (it is the target name + some random tmp name)
   stagefile = tempnam("/var/eos/stage/", "txj");
   stagefile += stagesuffix.c_str();

   if (mDestination.beginswith("as3://"))
   {
     // S3 upload 
     // setup the s3 hostname
     int spos = mDestination.find("/", 6);
     if (spos != STR_NPOS)
     {
       XrdOucString hname;
       hname.assign(mDestination, 6, spos - 1);
       setenv("S3_HOSTNAME", hname.c_str(), 1);
       mDestination.erase(4, spos - 3);
       mDestination.erase(0, 4);
     }

     XrdOucString mDestinationEnv = mDestination;
     spos = mDestinationEnv.find("?");
     if (spos != STR_NPOS)
     {
       mDestinationEnv.erase(0, spos + 1);
       mDestination.erase(spos);
     }

     XrdOucEnv mEnv(mDestinationEnv.c_str());

     // setup the s3 id & keys
     setenv("S3_SECRET_ACCESS_KEY", mEnv.Get("s3.key") ? mEnv.Get("s3.key") : "", 1);
     setenv("S3_ACCESS_KEY_ID", mEnv.Get("s3.id") ? mEnv.Get("s3.id") : "", 1);
     eos_static_debug("S3_HOSTNAME=%s S3_SECRET_ACCESS_KEY=%s S3_ACCESS_KEY_ID=%s", getenv("S3_HOSTNAME"), getenv("S3_SECRET_ACCESS_KEY"), getenv("S3_ACCESS_KEY_ID"));
     // extract the s3 keys and setup temporary file with them
     uploadcmd = "s3 put ";
     uploadcmd += "\"";
     uploadcmd += mDestination.c_str();
     uploadcmd += "\" filename=\"";
     uploadcmd += stagefile.c_str();
     uploadcmd += "\" 2>&1 ";
   }

   if (mDestination.beginswith("http://"))
   {
     // HTTP upload
     eos_static_err("illegal target protocol specified: %s [not supported]", mDestination.c_str());
   }

   if (mDestination.beginswith("https://"))
   {
     // HTTPS upload disabling certificate check (for the moment)
     eos_static_err("illegal target protocol specified: %s [not supported]", mDestination.c_str());
   }

   if (mDestination.beginswith("gsiftp://"))
   {
     // GSIFTP upload
     uploadcmd = "globus-url-copy ";
     uploadcmd += stagefile.c_str();
     uploadcmd += " ";
     uploadcmd += mDestination.c_str();
   }
 }

 // --------------------------------------------------------------------
 // create a transfer/stagein script
 // --------------------------------------------------------------------
 ss << "#!/bin/bash" << std::endl;
 ss << "SCRIPTNAME=$0" << std::endl;
 ss << "SOURCE=$1" << std::endl;
 ss << "DEST=$2" << std::endl;
 ss << "TOTALTIME=$3" << std::endl;
 ss << "BANDWIDTH=$4" << std::endl;
 ss << "FILEOUTPUT=$5" << std::endl;
 ss << "FILERETURN=$6" << std::endl;
 ss << "PROGRESS=$7" << std::endl;
 ss << "BEFORE=$(date +%s)" << std::endl;
 ss << "[ -f $FILEOUTPUT ] && rm $FILEOUTPUT" << std::endl;
 ss << "[ -f $FILERETURN ] && rm $FILERETURN" << std::endl;
 ss << "touch $FILEOUTPUT" << std::endl;
 ss << "chown daemon:daemon $FILEOUTPUT" << std::endl;
 if (downloadcmd.length())
 {
   // if we use an external protocol
   ss << downloadcmd.c_str();
   if (mId)
   {
     ss << "eoscp -u 2 -g 2 -R -n -p -O $PROGRESS -t $BANDWIDTH \"-\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
   }
   else
   {
     ss << "eoscp -u 2 -g 2 -n -p -O $PROGRESS -t $BANDWIDTH \"-\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
   }

 }
 else
 {
   // if we use XRootD protocol on both ends
   if (mId)
   {
     ss << "eoscp -u 2 -g 2 -n -p -O $PROGRESS -t $BANDWIDTH \"$SOURCE\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
   }
   else
   {
     ss << "eoscp -u 2 -g 2 -R -n -p -O $PROGRESS -t $BANDWIDTH \"$SOURCE\" \"$DEST\" 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
   }
 }
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
 //  ss << "rm -rf $SCRIPTNAME" << std::endl;
 ss << "if [ -e $FILERETURN ] " << std::endl;
 ss << "then" << std::endl;
 //  ss << "rm -rf $FILERETURN "<< std::endl;
 ss << "exit 0; " << std::endl;
 ss << "else " << std::endl;
 ss << "exit 255; " << std::endl;
 ss << "fi" << std::endl;

 // store the script
 fileName = fileName + uuid + ".sh";
 std::ofstream file;
 file.open(fileName.c_str());
 file << ss.str();
 file.close();

 if (stagefile.length())
 {
   // --------------------------------------------------------------------
   // create a stageout script
   // --------------------------------------------------------------------
   so << "#!/bin/bash" << std::endl;
   so << "SCRIPTNAME=$0" << std::endl;
   so << "SOURCE=$1" << std::endl;
   so << "DEST=$2" << std::endl;
   so << "TOTALTIME=$3" << std::endl;
   so << "BANDWIDTH=$4" << std::endl;
   so << "FILEOUTPUT=$5" << std::endl;
   so << "FILERETURN=$6" << std::endl;
   so << "PROGRESS=$7" << std::endl;
   so << "BEFORE=$(date +%s)" << std::endl;
   so << "[ -f $FILEOUTPUT ] && rm $FILEOUTPUT" << std::endl;
   so << "[ -f $FILERETURN ] && rm $FILERETURN" << std::endl;
   so << "touch $FILEOUTPUT" << std::endl;
   so << "chown daemon:daemon $FILEOUTPUT" << std::endl;
   so << uploadcmd.c_str();
   so << " 1>$FILEOUTPUT 2>&1 && touch $FILERETURN &" << std::endl;
   so << "PID=$!" << std::endl;
   so << "AFTER=$(date +%s)" << std::endl;
   so << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
   so << "while kill -0 $PID 2>/dev/null && [[ $DIFFTIME -lt $TOTALTIME ]]; do" << std::endl;
   so << "sleep 1" << std::endl;
   so << "AFTER=$(date +%s)" << std::endl;
   so << "DIFFTIME=$(( $AFTER - $BEFORE ))" << std::endl;
   so << "done" << std::endl;
   so << "chown daemon:daemon $FILERETURN 2>/dev/null" << std::endl;
   so << "if kill -0 $PID 2>/dev/null " << std::endl;
   so << "then" << std::endl;
   so << "kill -9 $PID 2> /dev/null " << std::endl;
   so << "fi" << std::endl;
   //    so << "rm -rf $SCRIPTNAME" << std::endl;
   so << "if [ -e $FILERETURN ] " << std::endl;
   so << "then" << std::endl;
   //    so << "rm -rf $FILERETURN "<< std::endl;
   so << "exit 0; " << std::endl;
   so << "else " << std::endl;
   so << "exit 255; " << std::endl;
   so << "fi" << std::endl;

   fileStageName = fileName + uuid + ".stageout.sh";

   // store the script    
   std::ofstream file;
   file.open(fileStageName.c_str());
   file << so.str();
   file.close();
 }

 if (mId)
 {
   if (stagefile.length())
   {
     // we are staging, set it to stagein
     SendState(eos::mgm::TransferEngine::kStageIn);
   }
   else
   {
     SendState(eos::mgm::TransferEngine::kRunning);
   }
 }

 // -----------------------------------------------------------------------
 // setup the command to run for the transfer/stagein and evt. the stageout
 // -----------------------------------------------------------------------
 if (iskrb5)
 {
   command << "unset XrdSecPROTOCOL; KRB5CCNAME=";
   command << fileCredential;
   command << " ";
   commando << "KRB5CCNAME=";
   commando << fileCredential;
   commando << " ";
 }
 else
 {
   if (isgsi)
   {
     command << "unset XrdSecPROTOCOL; X509_USER_PROXY=";
     command << fileCredential;
     command << " ";
     commando << "X509_USER_PROXY=";
     commando << fileCredential;
     commando << " ";
   }
   else
   {
     command << "unset XrdSecPROTOCOL; ";
   }
 }

 command << "/bin/sh ";
 command << fileName + " \"";
 command << mSource.c_str();
 command << "\" \"";
 if (!stagefile.length())
 {
   // the target is XRootD protocol
   command << mDestination.c_str();
   command << "\" ";
   // the target is an external protocol, we use a stage file
 }
 else
 {
   command << stagefile.c_str();
   command << "\" ";
 }
 command << ToString(mTimeOut) + " ";
 command << ToString(mBandWidth) + " ";
 command << fileOutput + " ";
 command << fileResult + " ";
 command << progressFileName + " ";

 eos_static_debug("executing transfer/stagein %s", command.str().c_str());

 if (stagefile.length())
 {
   commando << "/bin/sh ";
   commando << fileStageName + " \"";
   commando << mSource.c_str();
   commando << "\" \"";
   commando << mDestination.c_str();
   commando << "\" ";
   commando << ToString(mTimeOut) + " ";
   commando << ToString(mBandWidth) + " ";
   commando << fileStageOutput + " ";
   commando << fileResult + " ";
   commando << progressFileName + " ";
   eos_static_debug("executing stagout %s", commando.str().c_str());
 }

 if (mId)
 {
   // start the progress thread
   XrdSysThread::Run(&mProgressThread, TransferJob::StaticProgress, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Progress Report Thread");
 }

 // avoid cloning of FDs on fork
 eos::common::CloExec::All();
 std::string cattolog;
 int rc = 0;

 static XrdSysMutex forkMutex;

 if (mId)
 {
   int spid = 0;
   forkMutex.Lock();
   if (!(spid = fork()))
   {
     forkMutex.UnLock();
     setpgrp();
     execlp("/bin/sh", "sh", "-c", command.str().c_str(), NULL);
   }
   else
   {
     forkMutex.UnLock();
     // check if we should cancel the call
     do
     {
       if ((waitpid(spid, &rc, WNOHANG)) == 0)
       {
         bool canceled = false;
         // check if we should cancel
         mCancelMutex.Lock();
         canceled = mCanceled;
         mCancelMutex.UnLock();
         if (canceled)
         {
           eos_static_warning("sending kill to %d\n", spid);
           kill(-spid, SIGKILL);
           waitpid(spid, &rc, 0);
           eoscpLogMutex.Lock();
           FILE* fout = fopen("/var/log/eos/fst/eoscp.log", "a+");
           if (fout)
           {
             time_t rawtime;
             struct tm* timeinfo;
             time(&rawtime);
             timeinfo = localtime(&rawtime);
             fprintf(fout, "[eoscp] #################################################################\n");
             fprintf(fout, "[eoscp] # Date                     : ( %lu ) %s", (unsigned long) rawtime, asctime(timeinfo));
             fprintf(fout, "[eoscp] # Aborted transfer id=%lld\n", mId);
             fprintf(fout, "[eoscp] # Source Name [00]         : %s\n", mSource.c_str());
             fprintf(fout, "[eoscp] # Destination Name [00]    : %s\n", mDestination.c_str());
             fclose(fout);
           }
           eoscpLogMutex.UnLock();
           goto cleanup;
         }
         XrdSysTimer sleeper;
         sleeper.Wait(100);
         continue;
       }
       break;
     }
     while (1);
   }
 }
 else
 {
   rc = system(command.str().c_str());
 }

 // now set the transfer state and send the log output
 if (WEXITSTATUS(rc))
 {
   eos_static_err("transfer returned %d", command.str().c_str(), rc);
   if (mId)
   {
     SendState(eos::mgm::TransferEngine::kFailed, fileOutput.c_str());
   }
 }
 else
 {
   if (stagefile.length())
   {
     SendState(eos::mgm::TransferEngine::kStageOut);
     // we have still to do the stage-out step with the external protocol
     if (mId)
     {
       int spid = 0;
       forkMutex.Lock();
       if (!(spid = fork()))
       {
         forkMutex.UnLock();
         setpgrp();
         execlp("/bin/sh", "sh", "-c", commando.str().c_str(), NULL);
       }
       else
       {
         forkMutex.UnLock();
         // check if we should cancel the call
         do
         {
           if ((waitpid(spid, &rc, WNOHANG)) == 0)
           {
             bool canceled = false;
             // check if we should cancel
             mCancelMutex.Lock();
             canceled = mCanceled;
             mCancelMutex.UnLock();
             if (canceled)
             {
               eos_static_crit("sending kill to %d\n", spid);
               kill(-spid, SIGKILL);
               waitpid(spid, &rc, 0);
               eoscpLogMutex.Lock();
               FILE* fout = fopen("/var/log/eos/fst/eoscp.log", "a+");
               if (fout)
               {
                 time_t rawtime;
                 struct tm* timeinfo;
                 time(&rawtime);
                 timeinfo = localtime(&rawtime);
                 fprintf(fout, "[eoscp] #################################################################\n");
                 fprintf(fout, "[eoscp] # Date                     : ( %lu ) %s", (unsigned long) rawtime, asctime(timeinfo));
                 fprintf(fout, "[eoscp] # Aborted transfer id=%lld\n", mId);
                 fprintf(fout, "[eoscp] # Source Name [00]         : %s\n", mSource.c_str());
                 fprintf(fout, "[eoscp] # Destination Name [00]    : %s\n", mDestination.c_str());
                 fclose(fout);
               }
               eoscpLogMutex.UnLock();
               goto cleanup;
             }
             XrdSysTimer sleeper;
             sleeper.Wait(100);
             continue;
           }
           break;
         }
         while (1);
       }
     }
     else
     {
       rc = system(commando.str().c_str());
     }
     if (WEXITSTATUS(rc))
     {
       eos_static_err("transfer returned %d", commando.str().c_str(), rc);
       // send failed status
       if (mId)
       {
         SendState(eos::mgm::TransferEngine::kFailed, fileOutput.c_str());
       }
     }
     else
     {
       // send done status
       if (mId)
       {
         SendState(eos::mgm::TransferEngine::kDone, fileOutput.c_str());
       }
     }
   }
   else
   {
     // send done status
     if (mId)
     {
       SendState(eos::mgm::TransferEngine::kDone, fileOutput.c_str());
     }
   }
 }

 // ---- get the static log lock mutex ----
 eoscpLogMutex.Lock();


 // move the output to the log file  
 cattolog = "touch /var/log/eos/fst/eoscp.log; cat ";
 cattolog += fileOutput.c_str();
 cattolog += " >> /var/log/eos/fst/eoscp.log 2>/dev/null";
 rc = system(cattolog.c_str());
 if (WEXITSTATUS(rc))
 {
   fprintf(stderr, "error: failed to append to eoscp log file (%s)\n", cattolog.c_str());
 }
 if (stagefile.length())
 {
   // move the output to the log file
   std::string cattolog = "touch /var/log/eos/fst/eoscp.log; echo ______________________ STAGEOUT _____________________ >> /var/log/eos/fst/eoscp.log 2>/dev/null; cat ";
   cattolog += fileStageOutput.c_str();
   cattolog += " | grep -v \"bytes remaining\" >> /var/log/eos/fst/eoscp.log 2>/dev/null;";
   rc = system(cattolog.c_str());
   if (WEXITSTATUS(rc))
   {
     fprintf(stderr, "error: failed to append to eoscp log file (%s)\n", cattolog.c_str());
   }
 }

 // ---- release the static log lock mutex ----
 eoscpLogMutex.UnLock();

cleanup:

 // remove the result files
 rc = unlink(fileOutput.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(fileStageOutput.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(fileResult.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(fileCredential.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(fileName.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(fileStageName.c_str());
 if (rc) rc = 0; // for compiler happyness
 rc = unlink(progressFileName.c_str());
 if (rc) rc = 0; // for compiler happyness

 if (stagefile.length())
 {
   rc = unlink(stagefile.c_str());
   if (rc) rc = 0; // for compiler happyness
 }
 // we are over running
 mQueue->DecRunning();
 delete this;
}

EOSFSTNAMESPACE_END
