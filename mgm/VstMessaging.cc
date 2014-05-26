// ----------------------------------------------------------------------
// File: VstMessaging.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/

#include "mgm/VstMessaging.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/VstView.hh"
#include "common/RWMutex.hh"
#include "mq/XrdMqTiming.hh"

EOSMGMNAMESPACE_BEGIN

void*
VstMessaging::Start (void *pp)
{
  ((VstMessaging*) pp)->Listen();
  return 0;
}

/*----------------------------------------------------------------------------*/
VstMessaging::VstMessaging (const char* url, const char* defaultreceiverqueue, bool advisorystatus, bool advisoryquery, XrdMqSharedObjectManager* som)
{
  // we add to a broker with the flushbacklog flag since we don't want to block message flow in case of a master/slave MGM where one got stuck or too slow
  eos_info("vst-broker-url=%s default-receiver=%s", url, defaultreceiverqueue);
  if (mMessageClient.AddBroker(url, advisorystatus, advisoryquery, true))
  {
    zombie = false;
  }
  else
  {
    zombie = true;
  }

  XrdOucString clientid = url;
  int spos;
  spos = clientid.find("//");
  if (spos != STR_NPOS)
  {
    spos = clientid.find("//", spos + 1);
    clientid.erase(0, spos + 1);
    mMessageClient.SetClientId(clientid.c_str());
  }


  mMessageClient.Subscribe();
  mMessageClient.SetDefaultReceiverQueue(defaultreceiverqueue);

  eos::common::LogId();
}

/*----------------------------------------------------------------------------*/
bool
VstMessaging::Update (XrdAdvisoryMqMessage* advmsg)
{
  if (!advmsg)
    return false;

  std::string nodequeue = advmsg->kQueue.c_str();
  if (advmsg->kOnline)
  {
    // online
  }
  else
  {
    // offline
  }
  return true;
}

/*----------------------------------------------------------------------------*/
void
VstMessaging::Listen ()
{
  static int lPublishTime = 0;
  {
    // we give some time for startup
    XrdSysTimer sleeper;
    sleeper.Wait(30000);
  }
  while (1)
  {
    XrdSysThread::SetCancelOff();
    //eos_static_debug("RecvMessage");
    XrdMqMessage* newmessage = mMessageClient.RecvMessage();

    if (newmessage)
    {
      Process(newmessage);
      delete newmessage;
    }
    else
    {
      XrdSysTimer sleeper;
      sleeper.Wait(1000);
    }

    XrdSysMutexHelper(gOFS->InitializationMutex);
    if (gOFS->Initialized == gOFS->kBooted)
    {
      if ((!lPublishTime) || ((time(NULL) - lPublishTime) > 15))
      {
        XrdMqMessage message("VST-Info");
        message.SetBody(PublishVst().c_str());
        message.MarkAsMonitor();
        // send this message async ...
        mMessageClient.SendMessage(message, 0, false, false, true);
        lPublishTime = time(NULL);
        eos_static_info("sending vst message %s", PublishVst().c_str());

      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysThread::CancelPoint();
  }
}

/*----------------------------------------------------------------------------*/
void
VstMessaging::Process (XrdMqMessage* newmessage)
{
  static bool discardmode = false;
  if ((newmessage->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage)
      || (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage))
  {
    if (discardmode)
    {
      return;
    }

    XrdAdvisoryMqMessage* advisorymessage =
      XrdAdvisoryMqMessage::Create(newmessage->GetMessageBuffer());

    if (advisorymessage)
    {
      eos_debug("queue=%s online=%d", advisorymessage->kQueue.c_str(),
                advisorymessage->kOnline);

      if (advisorymessage->kQueue.endswith("/vst"))
      {
        if (!Update(advisorymessage))
        {
          eos_err("cannot update node status for %s",
                  advisorymessage->GetBody());
        }
      }
      delete advisorymessage;
    }
  }
  else
  {
    if (EOS_LOGS_DEBUG)
      newmessage->Print();

    if ((!discardmode) &&
        ((newmessage->kMessageHeader.kReceiverTime_sec -
          newmessage->kMessageHeader.kBrokerTime_sec) > 120))
    {
      eos_crit("dropping vst message because of message delays of %d seconds",
               (newmessage->kMessageHeader.kReceiverTime_sec -
                newmessage->kMessageHeader.kBrokerTime_sec));
      discardmode = true;
      return;
    }
    else
    {
      // we accept when we catched up
      if ((newmessage->kMessageHeader.kReceiverTime_sec -
           newmessage->kMessageHeader.kBrokerTime_sec) <= 30)
      {
        discardmode = false;
        XrdSysMutexHelper vLock(VstView::gVstView.ViewMutex);
        // extract the map
        if (!eos::common::StringConversion::GetKeyValueMap(newmessage->GetBody(),
                                                           VstView::gVstView.mView[newmessage->kMessageHeader.kSenderId.c_str()], "=", ","))
        {
          eos_static_err("msg=\"illegal format in vst message\" body=\"%s\"",
                         newmessage->GetBody());
        }
        else
        {
          XrdOucString rt;
          VstView::gVstView.mView[newmessage->kMessageHeader.kSenderId.c_str()]["timestamp"] =
            XrdMqMessageHeader::ToString(rt, (long) newmessage->kMessageHeader.kReceiverTime_sec);
          eos_static_info("msg=\"received new VST report\" sender=\"%s\"",
                          newmessage->kMessageHeader.kSenderId.c_str());
        }
      }
      else
      {
        if (discardmode)
        {
          eos_crit("dropping vst message because of message delays of %d seconds",
                   (newmessage->kMessageHeader.kReceiverTime_sec
                    - newmessage->kMessageHeader.kBrokerTime_sec));
          return;
        }
      }
    }
  }
  return;
}

/*----------------------------------------------------------------------------*/
std::string&
VstMessaging::PublishVst ()
{
  mVstMessage = "instance=";
  mVstMessage += gOFS->MgmOfsInstanceName.c_str();
  mVstMessage += ",host=";
  mVstMessage += gOFS->HostName;
  mVstMessage += ",version=";
  mVstMessage += VERSION;
  if (gOFS->MgmMaster.IsMaster())
  {
    mVstMessage += ",mode=master";
  }
  else
  {
    mVstMessage += ",mode=slave";
  }
  XrdOucString uptime;
  uptime += (int) gOFS->StartTime;
  mVstMessage += ",uptime=";
  mVstMessage += uptime.c_str();

  unsigned long long freebytes, freefiles, maxbytes, maxfiles, ethin,
    ethout, diskin, diskout, ropen, wopen, clients, lock_r, lock_w, nfsrw, iops, bw;
  freebytes = freefiles = maxbytes = maxfiles = clients =
    ethin = ethout = diskin = diskout = ropen = wopen = nfsrw = iops = bw = 0;
  {
    // take the sum's from all file systems in 'default'
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mSpaceView.count("default"))
    {
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
      freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes?configstatus@rw");
      freefiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.ffree?configstatus@rw");

      maxbytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity?configstatus@rw");
      maxfiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.files?configstatus@rw");

      ethin = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.net.inratemib");
      ethout = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.net.outratemib");

      diskin = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.disk.readratemb");
      diskout = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.disk.writeratemb");

      ropen = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.ropen");
      wopen = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.wopen");
      
      nfsrw = FsView::gFsView.mSpaceView["default"]->SumLongLong("<n>?configstatus@rw");
      iops  = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.disk.iops?configstatus@rw");
      bw = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.disk.bw?configstatus@rw");
    }
  }
  {
    XrdSysMutexHelper aLock(eos::common::Mapping::ActiveLock);
    eos::common::Mapping::ActiveExpire(300, true);
    clients = eos::common::Mapping::ActiveTidents.size();
  }

  {
    XrdSysMutexHelper sLock(gOFS->MgmStats.Mutex);
    lock_r = (unsigned long long) gOFS->MgmStats.GetTotalAvg300("NsLockR");
    lock_w = (unsigned long long) gOFS->MgmStats.GetTotalAvg300("NsLockW");
  }
  
  unsigned long long files = 0;
  unsigned long long container = 0;

  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    files = (unsigned long long) gOFS->eosFileService->getNumFiles();
    container = (unsigned long long) gOFS->eosDirectoryService->getNumContainers();
  }

  char info[65536];
  snprintf(info, sizeof (info) - 1, ",freebytes=%llu,freefiles=%llu,maxbytes=%llu,maxfiles=%llu,nfsrw=%llu,iops=%llu,bw=%llu,ethin=%llu,ethout=%llu"
           ",diskin=%llu,diskout=%llu,ropen=%llu,wopen=%llu,clients=%llu,url=root://%s,manager=%s,ip=%s,ns_files=%llu,ns_container=%llu,rlock=%llu,wlock=%llu",
           freebytes,
           freefiles,
           maxbytes,
           maxfiles,
           nfsrw,
           iops,
           bw,
           ethin,
           ethout,
           diskin,
           diskout,
           ropen,
           wopen,
           clients,
           gOFS->MgmOfsAlias.c_str(),
           gOFS->ManagerId.c_str(),
           gOFS->ManagerIp.c_str(),
           files,
           container,
           lock_r,
           lock_w);

  {
    // publish our state also in our own map
    XrdSysMutexHelper vLock(VstView::gVstView.ViewMutex);
    XrdOucString rt;
    std::map<std::string, std::string>& mymap = VstView::gVstView.mView[mMessageClient.GetDefaultReceiverQueue().c_str()];
    mymap["timestamp"] =
      XrdMqMessageHeader::ToString(rt, (long) time(NULL));
    mymap["instance"] = gOFS->MgmOfsInstanceName.c_str();
    mymap["host"] = gOFS->HostName;
    mymap["version"] = VERSION;
    mymap["uptime"] = uptime.c_str();
    if (gOFS->MgmMaster.IsMaster())
      mymap["mode"] = "master";
    else
      mymap["mode"] = "slave";

    mymap["freebytes"] = eos::common::StringConversion::GetSizeString(rt, freebytes);
    mymap["freefiles"] = eos::common::StringConversion::GetSizeString(rt, freefiles);
    mymap["maxbytes"] = eos::common::StringConversion::GetSizeString(rt, maxbytes);
    mymap["maxfiles"] = eos::common::StringConversion::GetSizeString(rt, maxfiles);
    mymap["ethin"] = eos::common::StringConversion::GetSizeString(rt, ethin);
    mymap["ethout"] = eos::common::StringConversion::GetSizeString(rt, ethout);
    mymap["diskin"] = eos::common::StringConversion::GetSizeString(rt, diskin);
    mymap["diskout"] = eos::common::StringConversion::GetSizeString(rt, diskout);
    mymap["ropen"] = eos::common::StringConversion::GetSizeString(rt, ropen);
    mymap["wopen"] = eos::common::StringConversion::GetSizeString(rt, wopen);
    mymap["clients"] = eos::common::StringConversion::GetSizeString(rt, clients);
    mymap["url"] = std::string("root://") + gOFS->MgmOfsAlias.c_str();
    mymap["manager"] = gOFS->ManagerId.c_str();
    mymap["ip"] = gOFS->ManagerIp.c_str();
    mymap["ns_files"] = eos::common::StringConversion::GetSizeString(rt, files);
    mymap["ns_container"] = eos::common::StringConversion::GetSizeString(rt, container);
    mymap["rlock"] = eos::common::StringConversion::GetSizeString(rt, lock_r);
    mymap["wlock"] = eos::common::StringConversion::GetSizeString(rt, lock_w);
    mymap["nfsrw"] = eos::common::StringConversion::GetSizeString(rt, nfsrw);
    mymap["iops"] = eos::common::StringConversion::GetSizeString(rt, iops);
    mymap["bw"] = eos::common::StringConversion::GetSizeString(rt, bw);
  }
  mVstMessage += info;
  return mVstMessage;
}
EOSMGMNAMESPACE_END

