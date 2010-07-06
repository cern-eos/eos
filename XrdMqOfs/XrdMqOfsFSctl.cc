//         $Id: XrdMqOfsFSctl.cc,v 1.00 2007/10/04 01:34:19 abh Exp $

#include "XrdMqOfs/XrdMqOfs.hh"
#include "XrdMqOfs/XrdMqMessage.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"

#define XRDMQOFS_FSCTLPATHLEN 4096
#define XRDMQOFS_FSCTLOPAQUELEN 16384

extern XrdOucTrace OfsTrace;
extern XrdMqOfs   XrdOfsFS;

/////////////////////////////////////////////////////////////////////////////
// Helper Classes & Functions
/////////////////////////////////////////////////////////////////////////////

int 
XrdMqOfs::AddToMatch(const char* key, XrdMqMessageOut *Out, void* Arg) {
  EPNAME("AddToMatch");
  const char* tident = ((XrdMqOfsMatches*)Arg)->tident;
  if (Arg) {
    //    char debugline[4096];
    
    //    sprintf(debugline,"Matching %s -> %s [%d : %d %d]\n", ((XrdMqOfsMatches*)Arg)->queuename.c_str(), key, (((XrdMqOfsMatches*)Arg)->messagetype), Out->AdvisoryStatus, Out->AdvisoryQuery);
    //    TRACES(debugline);
    XrdOucString skey = key;

    // skip status messages if the queue was not asking for them
    if ((((XrdMqOfsMatches*)Arg)->messagetype) == XrdMqMessageHeader::kStatusMessage) {
      if (!Out->AdvisoryStatus)
	return 0;
      if (skey == ((XrdMqOfsMatches*)Arg)->sendername) {
	ZTRACE(open,"Loopback message discarded");
	// we don't do message feedback to the some queue for advisory
	return 0;
      }

    }
    
    // skip query messages if the queue was not asking for them
    if ((((XrdMqOfsMatches*)Arg)->messagetype) == XrdMqMessageHeader::kQueryMessage) {
      if (!Out->AdvisoryQuery)
	return 0;
      if (skey == ((XrdMqOfsMatches*)Arg)->sendername) {
	ZTRACE(open,"Loopback message discarded");
	// we don't do message feedback to the some queue for advisory
	return 0;
      }
    }

    ZTRACE(open,"Trying to match ...");
    //    XrdMqOfsMatches* match = (XrdMqOfsMatches*) Arg;
    XrdOucString Key = key;
    XrdOucString nowildcard = ((XrdMqOfsMatches*)Arg)->queuename;
    nowildcard.replace("*","");
    int nmatch = Key.matches(((XrdMqOfsMatches*)Arg)->queuename.c_str(),'*');
    if (nmatch == nowildcard.length()) {
      // this is a match

      // check for backlog on this queue and set a warning flag
      if (Out->nQueued > MQOFSMAXQUEUEBACKLOG) {
	((XrdMqOfsMatches*)Arg)->backlog =true;
	((XrdMqOfsMatches*)Arg)->backlogqueues += Out->QueueName;
	((XrdMqOfsMatches*)Arg)->backlogqueues += ":";
	XrdOfsFS.QueueBacklogHits++;
	TRACES("warning: queue " << Out->QueueName << " exceeds backlog of " << MQOFSMAXQUEUEBACKLOG << " message!");
      }

      if (Out->nQueued > MQOFSREJECTQUEUEBACKLOG) {
	((XrdMqOfsMatches*)Arg)->backlogrejected =true;
	((XrdMqOfsMatches*)Arg)->backlogqueues += Out->QueueName;
	((XrdMqOfsMatches*)Arg)->backlogqueues += ":";
	XrdOfsFS.BacklogDeferred++;
	TRACES("error: queue " << Out->QueueName << " exceeds max. accepted backlog of " << MQOFSREJECTQUEUEBACKLOG << " message!");
      }

      ((XrdMqOfsMatches*)Arg)->message->AddRefs(1);
      if (!((XrdMqOfsMatches*)Arg)->matches) {
	// add to the message hash first
	XrdOfsFS.MessagesMutex.Lock();
	XrdOfsFS.Messages.Add(((XrdMqOfsMatches*)Arg)->message->Get(XMQHEADER),((XrdMqOfsMatches*)Arg)->message, 0, Hash_keep);
	XrdOfsFS.MessagesMutex.UnLock();
      }
      
      ((XrdMqOfsMatches*)Arg)->matches++;
      
      Out->Lock();
      ZTRACE(open,"Adding Message to Queuename: "<< Key.c_str());
      Out->MessageQueue.Add((((XrdMqOfsMatches*)Arg)->message));
      Out->nQueued++;
      //      Out->MessageSem.Post();
      Out->UnLock();
    }
  }
  return 0;
}

size_t
XrdMqMessageOut::RetrieveMessages() {
  XrdSmartOucEnv* message;
  while ((message = (XrdSmartOucEnv*) MessageQueue.Remove())) {
    message->procmutex.Lock();
    int len;
    MessageBuffer += message->Env(len);
    XrdOfsFS.DeliveredMessages++;
    XrdOfsFS.MessagesMutex.Lock();
    message->DecRefs();
    if (message->Refs()<=0) {
      // we can delete this message from the queue!
      XrdOucString msg = message->Get(XMQHEADER);
      XrdOfsFS.Messages.Del(msg.c_str());
      message->procmutex.UnLock();
      delete message;
      XrdOfsFS.FanOutMessages++;
    } else {
      message->procmutex.UnLock();
    }
    nQueued--;
    XrdOfsFS.MessagesMutex.UnLock();
  }
  return MessageBuffer.length();
}

/////////////////////////////////////////////////////////////////////////////
int
XrdMqOfs::FSctl(const int               cmd,
                    XrdSfsFSctl            &args,
                    XrdOucErrInfo          &error,
                    const XrdSecEntity     *client) {
  char ipath[XRDMQOFS_FSCTLPATHLEN];
  char iopaque[XRDMQOFS_FSCTLOPAQUELEN];

  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  // accept only plugin calls!
  ZTRACE(fsctl,"Calling FSctl");

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return XrdOfs::FSctl(cmd, args, error, client);
  }

  // check for backlog
  if (Messages.Num() > MQOFSMAXMESSAGEBACKLOG) {
    // this is not absolutely threadsafe .... better would lock
    BacklogDeferred++;
    XrdMqOfs::Emsg(epname, error, ENOMEM, "accept message - too many pending messages", "");
    return SFS_ERROR;
  }

  if (args.Arg1Len) {
    if (args.Arg1Len < XRDMQOFS_FSCTLPATHLEN) {
      strncpy(ipath,args.Arg1,args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      XrdMqOfs::Emsg(epname, error, EINVAL, "convert path argument - string too long", "");
      return SFS_ERROR;
    }
  } else {
    ipath[0] = 0;
  }
  if (args.Arg2Len) {
    if (args.Arg2Len <  XRDMQOFS_FSCTLOPAQUELEN ) {
      strncpy(iopaque,args.Arg2,args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      XrdMqOfs::Emsg(epname, error, EINVAL, "convert opaque argument - string too long", "");
      return SFS_ERROR;
    }
  } else {
    iopaque[0] = 0;
  }

  // from here on we can deal with XrdOucString which is more 'comfortable' 
  XrdOucString path    = ipath;
  XrdOucString opaque  = iopaque;
  XrdOucString result  = "";

  ZTRACE(fsctl,ipath);
  ZTRACE(fsctl,iopaque);


  XrdSmartOucEnv* env = new XrdSmartOucEnv(opaque.c_str());

  // look into the header
  XrdMqMessageHeader mh;
  if (!mh.Decode(opaque.c_str())) {
    XrdMqOfs::Emsg(epname, error, EINVAL, "decode message header", "");
    return SFS_ERROR;
  }
  // add the broker ID
  mh.kBrokerId=BrokerId;
  // update broker time
  mh.GetTime(mh.kBrokerTime_sec,mh.kBrokerTime_nsec);
  // dump it 
  //  mh.Print();
  // encode the new values
  mh.Encode();
  
  // replace the old header with the new one .... that's ugly :-(
  int envlen;
  XrdOucString envstring = env->Env(envlen);
  int p1 = envstring.find(XMQHEADER);
  int p2 = envstring.find("&",p1+1);
  envstring.erase(p1,p2-1);
  envstring.insert(mh.GetHeaderBuffer(),p1);
  
  delete env;
  env = new XrdSmartOucEnv(envstring.c_str());

  XrdMqOfsMatches matches(mh.kReceiverQueue.c_str(), env, tident, mh.kType);
  {
    XrdMqOfsOutMutex qm;
    env->procmutex.Lock();
    QueueOut.Apply(AddToMatch,&matches);
    env->procmutex.UnLock();
  }

  if (matches.backlogrejected) {
    XrdOucString backlogmessage = "queue message on all receivers - maximum backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;
    XrdMqOfs::Emsg(epname, error, E2BIG, backlogmessage.c_str(), ipath);
    TRACES(backlogmessage.c_str());
    return SFS_ERROR;
  }


  if (matches.backlog) {
    XrdOucString backlogmessage = "guarantee quick delivery - backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;
    XrdMqOfs::Emsg(epname, error, ENFILE, backlogmessage.c_str(), ipath);
    TRACES(backlogmessage.c_str());
    return SFS_ERROR;
  } 

  if (matches.matches) {
    const char* result="OK";
    error.setErrInfo(3,(char*)result);
    XrdOfsFS.ReceivedMessages++;
    return SFS_DATA;
  } else {
    bool ismonitor=false;
    if (env->Get(XMQMONITOR)) {
      ismonitor=true;
    }
	
    delete env;

    // this is a new hook for special monitoring message, to just accept them and if nobody listens they just go to nirvana
    if (!ismonitor) {
      XrdOfsFS.UndeliverableMessages++;
      XrdMqOfs::Emsg(epname, error, EINVAL, "submit message - no listener on requested queue: ", ipath);
      TRACES("no listener on requeste queue: ");
      TRACES(ipath);
      return SFS_ERROR;
    } else {
      const char* result="OK";
      error.setErrInfo(3,(char*)result);
      XrdOfsFS.ReceivedMessages++;
      return SFS_DATA;
    }
  }
}
