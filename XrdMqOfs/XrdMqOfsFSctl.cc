//         $Id: XrdMqOfsFSctl.cc,v 1.00 2007/10/04 01:34:19 abh Exp $

#include "XrdMqOfs/XrdMqOfs.hh"
#include "XrdMqOfs/XrdMqMessage.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"

#define XRDMQOFS_FSCTLPATHLEN 1024

extern XrdOucTrace OfsTrace;
extern XrdMqOfs   XrdOfsFS;

/////////////////////////////////////////////////////////////////////////////
// Helper Classes & Functions
/////////////////////////////////////////////////////////////////////////////

bool
XrdMqOfs::Deliver(XrdMqOfsMatches &Matches) {
  EPNAME("AddToMatch");

  const char* tident = Matches.tident;
  
  std::string sendername = Matches.sendername.c_str();

  // here we store all the queues where we need to deliver this message
  std::vector<XrdMqMessageOut*> MatchedOutputQueues;

  Matches.message->procmutex.Lock();

  if ( ((Matches.messagetype) == XrdMqMessageHeader::kStatusMessage) || ((Matches.messagetype) == XrdMqMessageHeader::kQueryMessage) ) {
    //////////////////////////////////////////////////////////////////////////////////////
    // if we have a status message we have to do a complete loop
    //////////////////////////////////////////////////////////////////////////////////////
    
    std::map<std::string, XrdMqMessageOut*>::const_iterator QueueOutIt;
    for (QueueOutIt = QueueOut.begin(); QueueOutIt != QueueOut.end(); QueueOutIt++ ) {
      XrdMqMessageOut* Out = QueueOutIt->second;
      
      // if this would be a loop back message we continue
      if (sendername == QueueOutIt->first) {
	// avoid feedback to the same queue
	continue;
      }
      
      // if this queue does not take advisory status messages we continue
      if ( ( Matches.messagetype == XrdMqMessageHeader::kStatusMessage) && (!Out->AdvisoryStatus) )
	continue;

      // if this queue does not take advisory query messages we continue
      if ( ( Matches.messagetype == XrdMqMessageHeader::kQueryMessage)  && (!Out->AdvisoryQuery) )
	continue;
      

      else {
	ZTRACE(open,"Adding Advisory Message to Queuename: "<< Out->QueueName.c_str());
	MatchedOutputQueues.push_back(Out);
      }
    }
  } else {
    //////////////////////////////////////////////////////////////////////////////////////
    // if we have a wildcard match we have to do a complete loop
    //////////////////////////////////////////////////////////////////////////////////////
    if ( ( Matches.queuename.find("*") != STR_NPOS) ) {
      std::map<std::string, XrdMqMessageOut*>::const_iterator QueueOutIt;
      for (QueueOutIt = QueueOut.begin(); QueueOutIt != QueueOut.end(); QueueOutIt++ ) {
	XrdMqMessageOut* Out = QueueOutIt->second;
	
	//	fprintf(stderr,"%s <=> %s\n", sendername.c_str(), QueueOutIt->first.c_str());
	// if this would be a loop back message we continue
	if (sendername == QueueOutIt->first) {
	  // avoid feedback to the same queue
	  continue;
	}
	
	XrdOucString Key = QueueOutIt->first.c_str();
	XrdOucString nowildcard = Matches.queuename;
	nowildcard.replace("*","");
	int nmatch = Key.matches(Matches.queuename.c_str(),'*');
	if (nmatch == nowildcard.length()) {
	  // this is a match
	  ZTRACE(open,"Adding Wildcard matched Message to Queuename: "<< Out->QueueName.c_str());
	  MatchedOutputQueues.push_back(Out);
	}
      }
    } else {
      //////////////////////////////////////////////////////////////////////////////////////
      // we have just to find one named queue
      //////////////////////////////////////////////////////////////////////////////////////

      std::string queuename = Matches.queuename.c_str();

      XrdMqMessageOut* Out = 0;
      if (QueueOut.count(queuename))
	Out = QueueOut[queuename];
      if (Out) {
	ZTRACE(open,"Adding full matched Message to Queuename: "<< Out->QueueName.c_str());
	MatchedOutputQueues.push_back(Out);
      }
    }
  }

  if (MatchedOutputQueues.size()) {
    // this is a match
    Matches.backlog = false;
    Matches.backlogrejected = false;
    
    // lock all matched queues at once
    for (unsigned int i=0; i< MatchedOutputQueues.size(); i++) {
      XrdMqMessageOut* Out = MatchedOutputQueues[i];	
      Out->Lock();
    }

    for (unsigned int i=0; i< MatchedOutputQueues.size(); i++) {
      XrdMqMessageOut* Out = MatchedOutputQueues[i];

      // check for backlog on this queue and set a warning flag
      if (Out->nQueued > MQOFSMAXQUEUEBACKLOG) {
	Matches.backlog =true;
	Matches.backlogqueues += Out->QueueName;
	Matches.backlogqueues += ":";
	XrdOfsFS.QueueBacklogHits++;
	TRACES("warning: queue " << Out->QueueName << " exceeds backlog of " << MQOFSMAXQUEUEBACKLOG << " message!");
      }
    
      if (Out->nQueued > MQOFSREJECTQUEUEBACKLOG) {
	Matches.backlogrejected =true;
	Matches.backlogqueues += Out->QueueName;
	Matches.backlogqueues += ":";
	XrdOfsFS.BacklogDeferred++;
	TRACES("error: queue " << Out->QueueName << " exceeds max. accepted backlog of " << MQOFSREJECTQUEUEBACKLOG << " message!");
      } else {
	Matches.matches++;
	if (Matches.matches == 1) {
	  // add to the message hash 
	  XrdOfsFS.MessagesMutex.Lock();
	  std::string messageid = Matches.message->Get(XMQHEADER);
	  XrdOfsFS.Messages.insert(std::pair<std::string, XrdSmartOucEnv*> (messageid,Matches.message));
	  XrdOfsFS.MessagesMutex.UnLock();
	}

	ZTRACE(open,"Adding Message to Queuename: "<< Out->QueueName.c_str());
	//	fprintf(stderr, "%s adding message %llu\n", Out->QueueName.c_str(), (unsigned long long)Matches.message);
	Out->MessageQueue.push_back((Matches.message));
	Matches.message->AddRefs(1);

	Out->nQueued++;
	//      Out->MessageSem.Post();
      }
    }
    
	

    // unlock all matched queues at once
    for (unsigned int i=0; i< MatchedOutputQueues.size(); i++) {
      XrdMqMessageOut* Out = MatchedOutputQueues[i];	
      Out->UnLock();
    }
  }

  Matches.message->procmutex.UnLock();
  
  if (Matches.matches>0) {
    return true;
  } else {
    return false;
  }
}
						  
size_t
XrdMqMessageOut::RetrieveMessages() {
  XrdSmartOucEnv* message;
  while (MessageQueue.size()) {
    message = MessageQueue.front();
    MessageQueue.pop_front();
    message->procmutex.Lock();
    //    fprintf(stderr,"%llu %s Message %llu nref: %d\n", (unsigned long long) &MessageQueue, QueueName.c_str(), (unsigned long long) message, message->Refs());

    int len;
    MessageBuffer += message->Env(len);
    XrdOfsFS.MessagesMutex.Lock();
    XrdOfsFS.DeliveredMessages++;
    message->DecRefs();
    if (message->Refs()<=0) {
      // we can delete this message from the queue!
      XrdOucString msg = message->Get(XMQHEADER);
      XrdOfsFS.Messages.erase(msg.c_str());
      message->procmutex.UnLock();
      //      fprintf(stderr,"%s delete %llu \n", QueueName.c_str(), (unsigned long long) message);
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
  char ipath[XRDMQOFS_FSCTLPATHLEN+1];
  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  // accept only plugin calls!
  ZTRACE(fsctl,"Calling FSctl");

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return XrdOfs::FSctl(cmd, args, error, client);
  }

  // check for backlog
  if (Messages.size() > MQOFSMAXMESSAGEBACKLOG) {
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

  // from here on we can deal with XrdOucString which is more 'comfortable' 
  XrdOucString path    = ipath;
  XrdOucString result  = "";

  XrdOucString opaque  = "";

  if (args.Arg2Len) {
    opaque.assign(args.Arg2, 0, args.Arg2Len);
  }

  ZTRACE(fsctl,path.c_str());
  ZTRACE(fsctl,opaque.c_str());


  XrdSmartOucEnv* env = new XrdSmartOucEnv(opaque.c_str());

  if (!env) {
    XrdMqOfs::Emsg(epname, error, ENOMEM, "allocate memory", "");
    return SFS_ERROR;
  }
  // look into the header
  XrdMqMessageHeader mh;
  if (!mh.Decode(opaque.c_str())) {
    XrdMqOfs::Emsg(epname, error, EINVAL, "decode message header", "");
    delete env;
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

  XrdMqOfsMatches matches(mh.kReceiverQueue.c_str(), env, tident, mh.kType, mh.kSenderId.c_str());
  {
    XrdMqOfsOutMutex qm;
    Deliver(matches);
  }

  if (matches.backlogrejected) {
    XrdOucString backlogmessage = "queue message on all receivers - maximum backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;
    XrdMqOfs::Emsg(epname, error, E2BIG, backlogmessage.c_str(), ipath);
    if (backlogmessage.length() > 255) {
      backlogmessage.erase(255);
      backlogmessage += "...";
    }
    TRACES(backlogmessage.c_str());
    if (!matches.message->Refs())
      delete env;
    return SFS_ERROR;
  }


  if (matches.backlog) {
    XrdOucString backlogmessage = "guarantee quick delivery - backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;
    if (backlogmessage.length() > 255) {
      backlogmessage.erase(255);
      backlogmessage += "...";
    }

    XrdMqOfs::Emsg(epname, error, ENFILE, backlogmessage.c_str(), ipath);
    TRACES(backlogmessage.c_str());
    return SFS_ERROR;
  } 

  if (matches.matches) {
    const char* result="OK";
    error.setErrInfo(3,(char*)result);

    if ( ((matches.messagetype) != XrdMqMessageHeader::kStatusMessage) && ((matches.messagetype) != XrdMqMessageHeader::kQueryMessage) ) {
      XrdOfsFS.ReceivedMessages++;
    }

    return SFS_DATA;
  } else {
    bool ismonitor=false;
    if (env->Get(XMQMONITOR)) {
      ismonitor=true;
    }

    int envlen;

    std::string c = env->Env(envlen);

    if (env)
      delete env;

    // this is a new hook for special monitoring message, to just accept them and if nobody listens they just go to nirvana
    if (!ismonitor) {
      XrdOfsFS.UndeliverableMessages++;
      XrdMqOfs::Emsg(epname, error, EINVAL, "submit message - no listener on requested queue: ", ipath);
      TRACES("no listener on requeste queue: ");
      TRACES(ipath);
      return SFS_ERROR;
    } else {
      fprintf(stderr,"Dropped Monitor message %s\n",c.c_str());
      ZTRACE(open,"Discarding montor message without receiver");
      const char* result="OK";
      error.setErrInfo(3,(char*)result);
      XrdOfsFS.DiscardedMonitoringMessages++;
      return SFS_DATA;
    }
  }
}
