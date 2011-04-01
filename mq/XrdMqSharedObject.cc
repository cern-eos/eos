/*----------------------------------------------------------------------------*/
#include "mq/XrdMqSharedObject.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqStringConversion.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
// system includes

/*----------------------------------------------------------------------------*/

bool XrdMqSharedObjectManager::debug=0;

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::XrdMqSharedObjectManager() 
{
  EnableQueue    = false;
  DumperFile     = "";
  AutoReplyQueue = "";
  AutoReplyQueueDerive = false;
}

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::~XrdMqSharedObjectManager() 
{

}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::SetAutoReplyQueue(const char* queue) 
{
  AutoReplyQueue = queue;
}

/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::CreateSharedHash(const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som) 
{
  std::string ss = subject;

  HashMutex.LockWrite();
  
  if (hashsubjects.count(ss)>0) {
    hashsubjects[ss]->SetBroadCastQueue(broadcastqueue);
    HashMutex.UnLockWrite();
    return false;
  } else {
    XrdMqSharedHash* newhash = new XrdMqSharedHash(subject, broadcastqueue, som);

    hashsubjects.insert( std::pair<std::string, XrdMqSharedHash*> (ss, newhash));
    
    HashMutex.UnLockWrite();

    if (EnableQueue) {
      SubjectsMutex.Lock();
      CreationSubjects.push_back(ss);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::CreateSharedQueue(const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som) 
{
  std::string ss = subject;

  ListMutex.LockWrite();
  
  if (queuesubjects.count(ss)>0) {
    ListMutex.UnLockWrite();
    return false;
  } else {
    XrdMqSharedQueue newlist(subject, broadcastqueue, som);

    queuesubjects.insert( std::pair<std::string, XrdMqSharedQueue> (ss, newlist));
    
    ListMutex.UnLockWrite();

    if (EnableQueue) {
      SubjectsMutex.Lock();
      CreationSubjects.push_back(ss);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }
    return true;
  }
}


/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::DeleteSharedHash(const char* subject, bool broadcast) 
{
  std::string ss = subject;
  
  HashMutex.LockWrite();
  
  if ((hashsubjects.count(ss)>0)) {
    if (broadcast) {
      XrdOucString txmessage="";
      hashsubjects[ss]->MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message);
    }
    
    delete (hashsubjects[ss]);
    hashsubjects.erase(ss);
    HashMutex.UnLockWrite();

    if (EnableQueue) {
      SubjectsMutex.Lock();
      DeletionSubjects.push_back(ss);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }    

    return true;
  } else {
    HashMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::DeleteSharedQueue(const char* subject, bool broadcast) 
{
  std::string ss = subject;
  
  ListMutex.LockWrite();
  
  if ((queuesubjects.count(ss)>0)) {
    if (broadcast) {
      XrdOucString txmessage="";
      hashsubjects[ss]->MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message);
    }

    queuesubjects.erase(ss);
    ListMutex.UnLockWrite();

    if (EnableQueue) {
      SubjectsMutex.Lock();
      DeletionSubjects.push_back(ss);
      SubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  } else {
    ListMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMqSharedObjectManager::DumpSharedObjects(XrdOucString& out) 
{
  out="";

  XrdMqRWMutexReadLock lock(HashMutex);
  std::map<std::string , XrdMqSharedHash*>::iterator it_hash;
  for (it_hash=hashsubjects.begin(); it_hash!= hashsubjects.end(); it_hash++) {
    out += "===================================================\n";
    out += it_hash->first.c_str(); out += " [ "; out += it_hash->second->GetBroadCastQueue();
    out += " ]\n";
    out += "---------------------------------------------------\n";
    it_hash->second->Dump(out);
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMqSharedObjectManager::DumpSharedObjectList(XrdOucString& out) 
{
  out="";
  char formatline[1024];

  XrdMqRWMutexReadLock lock(HashMutex);

  std::map<std::string , XrdMqSharedHash*>::iterator it_hash;
  for (it_hash=hashsubjects.begin(); it_hash!= hashsubjects.end(); it_hash++) {
    snprintf(formatline,sizeof(formatline)-1,"subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n",it_hash->first.c_str(), it_hash->second->GetBroadCastQueue(),(unsigned int) it_hash->second->GetSize(), it_hash->second->GetChangeId());
    out += formatline;
  }

  //  ListMutex.LockRead();
  //  std::map<std::string , XrdMqSharedQueue>::const_iterator it_list;
  //  for (it_list=queuesubjects.begin(); it_list!= queuesubjects.end(); it_list++) {
  //    snprintf(formatline,sizeof(formatline)-1,"subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n",it_hash.first.c_str(), it_hash.second.GetBroadCastQueue(),(unsigned int) it_hash.second.GetSize(), it_hash.second.GetChangeId());
  //out += formatline;
  //  }
  //  ListMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/
void 
XrdMqSharedObjectManager::StartDumper(const char* file) 
{
  pthread_t tid;
  int rc=0;
  DumperFile = file;
  if ((rc = XrdSysThread::Run(&tid, XrdMqSharedObjectManager::StartHashDumper, static_cast<void *>(this),
                              0, "HashDumper"))) {
    fprintf(stderr,"XrdMqSharedObjectManager::StartDumper=> failed to run dumper thread\n");
  }
}

/*----------------------------------------------------------------------------*/
void*
XrdMqSharedObjectManager::StartHashDumper(void* pp) 
{
  XrdMqSharedObjectManager* man = (XrdMqSharedObjectManager*) pp;
  man->FileDumper();
  // should never return
  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::FileDumper()
{
  while (1) {
    XrdOucString s;
    DumpSharedObjects(s);
    std::string df = DumperFile;
    df += ".tmp";
    FILE* f = fopen(df.c_str(),"w+");
    if (f) {
      fprintf(f,"%s\n", s.c_str());
      fclose(f);
    }
    if (rename(df.c_str(),DumperFile.c_str())) {
      fprintf(stderr,"XrdMqSharedObjectManager::FileDumper=> unable to write dumper file %s\n", DumperFile.c_str());
    }
    sleep(10);
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::PostModificationTempSubjects() 
{
  std::deque<std::string>::iterator it;
  if (debug)fprintf(stderr,"XrdMqSharedObjectManager::PostModificationTempSubjects=> posting now\n");
  SubjectsMutex.Lock();
  for (it=ModificationTempSubjects.begin(); it!=ModificationTempSubjects.end(); it++) {
    if (debug)fprintf(stderr,"XrdMqSharedObjectManager::PostModificationTempSubjects=> %s\n", it->c_str());
    ModificationSubjects.push_back(*it);
    SubjectsSem.Post();
  }
  ModificationTempSubjects.clear();
  SubjectsMutex.UnLock();
}


/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::ParseEnvMessage(XrdMqMessage* message, XrdOucString &error) 
{
  error = "";
  std::string subject = "";
  std::string reply   = "";
  std::string type = "";

  if (!message) {
    error = "no message provided";
    return false;
  }

  XrdOucEnv env(message->GetBody());

  int envlen;
  if (debug)fprintf(stderr,"XrdMqSharedObjectManager::ParseEnvMessage=> %s\n", env.Env(envlen));
  if (env.Get(XRDMQSHAREDHASH_SUBJECT)) {
    subject = env.Get(XRDMQSHAREDHASH_SUBJECT);
  } else {
    error = "no subject in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_REPLY)) {
    reply = env.Get(XRDMQSHAREDHASH_REPLY);
  } else {
    reply = "";
  }

  if (env.Get(XRDMQSHAREDHASH_TYPE)) {
    type = env.Get(XRDMQSHAREDHASH_TYPE);
  } else {
    error = "no hash type in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_CMD)) {
    HashMutex.LockRead();
    XrdMqSharedHash* sh = 0;

    std::vector<std::string> subjectlist;

    // support 'wild card' broadcasts with <name>/*
    int wpos=0;
    if ((wpos = subject.find("/*")) != STR_NPOS) {
      XrdOucString wmatch = subject.c_str();
      wmatch.erase(wpos);
      if (type == "hash") {
        std::map<std::string, XrdMqSharedHash*>::iterator it;
        for (it=hashsubjects.begin(); it != hashsubjects.end(); it++) {
          XrdOucString hs = it->first.c_str();
          if (hs.beginswith(wmatch)) {
            subjectlist.push_back(hs.c_str());
          }
        }
      }
      if (type == "queue") {
        std::map<std::string, XrdMqSharedQueue>::iterator it;
        for (it=queuesubjects.begin(); it != queuesubjects.end(); it++) {
          XrdOucString hs = it->first.c_str();
          if (hs.beginswith(wmatch)) {
            subjectlist.push_back(hs.c_str());
          }
        }
      }
    } else {
      subjectlist.push_back(subject);
    }

    XrdOucString ftag = XRDMQSHAREDHASH_CMD; ftag += "="; ftag += env.Get(XRDMQSHAREDHASH_CMD);

    if (subjectlist.size()>0)
      sh = GetObject(subjectlist[0].c_str(),type.c_str());

    if ( (ftag == XRDMQSHAREDHASH_BCREQUEST) || (ftag == XRDMQSHAREDHASH_DELETE) || (ftag == XRDMQSHAREDHASH_REMOVE) ) {
      // if we don't know the subject, we don't create it with a BCREQUEST
      if ( ( ftag == XRDMQSHAREDHASH_BCREQUEST) && (reply == "")) {
        HashMutex.UnLockRead();
        error = "bcrequest: no reply address present";
        return false;
      }
      
      if (!sh) {
        if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
          error = "bcrequest: don't know this subject";
        } 
        if (ftag == XRDMQSHAREDHASH_DELETE) {
          error = "delete: don't know this subject";
        }
	if (ftag == XRDMQSHAREDHASH_REMOVE) {
	  error = "remove: don't know this subject";
	}
        HashMutex.UnLockRead();
        return false;
      } else {
        HashMutex.UnLockRead();
      }
    } else {
      // automatically create the subject, if it does not exist
      if (!sh) {
        HashMutex.UnLockRead();
        if (AutoReplyQueueDerive) {
          AutoReplyQueue = subject.c_str();
          int pos=0;
          for (int i=0; i< 4; i++) {
            pos= subject.find("/",pos); 
            if (i<3) {
              if (pos== STR_NPOS) {
                AutoReplyQueue="";
                error = "cannot derive the reply queue from "; error += subject.c_str();
                return false;
              } else {
                pos++;
              }
            } else {
              AutoReplyQueue.erase(pos);
            }
          }
        }

        if (!CreateSharedObject(subject.c_str(),AutoReplyQueue.c_str() ,type.c_str())) {
          error = "cannot create shared object for "; error += subject.c_str(); error += " and type "; error += type.c_str();
          return false;
        }

	{
	  XrdMqRWMutexReadLock lock(HashMutex);
	  sh = GetObject(subject.c_str(), type.c_str());
	}
      } else {
        HashMutex.UnLockRead();
      }
    }

    {
      XrdMqRWMutexReadLock lock(HashMutex);
      // from here on we have a read lock on 'sh'
      
      if ( (ftag == XRDMQSHAREDHASH_UPDATE) || (ftag == XRDMQSHAREDHASH_BCREPLY) ) {
	std::string val = (env.Get(XRDMQSHAREDHASH_PAIRS)?env.Get(XRDMQSHAREDHASH_PAIRS):"");
	if ( val.length() <=0 ) {
	  error = "no pairs in message body";
	  return false;
	}
	
	if (ftag == XRDMQSHAREDHASH_BCREPLY) {
	  sh->Clear();
	}
	
	std::string key;
	std::string value;
	std::string cid;
	std::vector<int> keystart;
	std::vector<int> valuestart;
	std::vector<int> cidstart;
	for (unsigned int i=0; i< val.length(); i++) {
	  if (val.c_str()[i] == '|') {
	    keystart.push_back(i);
	  }
	  if (val.c_str()[i] == '~') {
	    valuestart.push_back(i);
	  }
	  if (val.c_str()[i] == '%') {
	    cidstart.push_back(i);
	  }
	}
	
	if (keystart.size() != valuestart.size()) {
	  error = "update: parsing error in pairs tag";
	  return false;
	}
	
	if (keystart.size() != cidstart.size()) {
	  error = "update: parsing error in pairs tag";
	  return false;
	}
	
	std::string sstr;
	
	XrdSysMutexHelper(TransactionMutex);
	for (unsigned int i=0 ; i< keystart.size(); i++) {
	  sstr = val.substr(keystart[i]+1, valuestart[i]-1 - (keystart[i]));
	  key = sstr;
	  sstr = val.substr(valuestart[i]+1, cidstart[i]-1 - (valuestart[i]));
	  value = sstr;
	  if (i == (keystart.size()-1)) {
	    sstr = val.substr(cidstart[i]+1);
	  } else {
	    sstr = val.substr(cidstart[i]+1,keystart[i+1]-1 - (cidstart[i]));
	  }
	  cid = sstr;
	  if (debug)fprintf(stderr,"XrdMqSharedObjectManager::ParseEnvMessage=>Setting [%s] %s=>%s\n", subject.c_str(),key.c_str(), value.c_str());
	  sh->Set(key, value, false, true);

	}
	PostModificationTempSubjects();
	return true;
      }
      
      if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
	bool success = true;
	for (unsigned int l=0; l< subjectlist.size(); l++) {
	  // try 'queue' and 'hash' to have wildcard broadcasts for both
	  sh = GetObject(subjectlist[l].c_str(), "hash");
	  if (!sh) {
	    sh = GetObject(subjectlist[l].c_str(),"queue");
	  }
	  
	  if (sh) {
	    success *= sh->BroadCastEnvString(reply.c_str());
	  }
	}
	return success;
      }
      
      if (ftag == XRDMQSHAREDHASH_DELETE) {
	std::string val = (env.Get(XRDMQSHAREDHASH_KEYS)?env.Get(XRDMQSHAREDHASH_KEYS):"");
	if ( val.length() <= strlen(XRDMQSHAREDHASH_KEYS+1)) {
	  error = "no keys in message body";
	  return false;
	}
      
	std::string key;
	std::vector<int> keystart;
	
	for (unsigned int i=0; i< val.length(); i++) {
	  if (val.c_str()[i] == '|') {
          keystart.push_back(i);
	  }
	}
	
	XrdSysMutexHelper(TransactionMutex);
	std::string sstr;
	for (unsigned int i=0 ; i< keystart.size(); i++) {
	  if (i < (keystart.size()-1)) 
	    sstr = val.substr(keystart[i]+1, keystart[i+1]-1 - (keystart[i]));
	  else 
	    sstr = val.substr(keystart[i]+1);
	  
	  key = sstr;
	  if (debug)fprintf(stderr,"XrdMqSharedObjectManager::ParseEnvMessage=>Deleting [%s] %s\n", subject.c_str(),key.c_str());
	  sh->Delete(key.c_str(), false);
	  
	}
      }
    } // end of read mutex on HashMutex

    if (ftag == XRDMQSHAREDHASH_REMOVE) {
      for (unsigned int l=0; l< subjectlist.size(); l++) {
	if (!DeleteSharedObject(subjectlist[l].c_str(),type.c_str(),false)) {
	  error = "cannot delete subject "; error += subjectlist[l].c_str();
	  return false;
	}
      }
    }
    return true;

  }
  error="unknown message: ";error += message->GetBody();
  return false;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectManager::Clear() 
{
  XrdMqRWMutexReadLock lock(HashMutex);
  std::map<std::string , XrdMqSharedHash*>::iterator it_hash;
  for (it_hash=hashsubjects.begin(); it_hash!= hashsubjects.end(); it_hash++) {
    it_hash->second->Clear();
  }
  std::map<std::string , XrdMqSharedQueue>::iterator it_queue;
  for (it_queue=queuesubjects.begin(); it_queue!= queuesubjects.end(); it_queue++) {
    it_queue->second.Clear();
  }
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash::XrdMqSharedHash(const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som) 
{
  BroadCastQueue = broadcastqueue;
  Subject        = subject;
  ChangeId       = 0;
  IsTransaction  = false;
  Type           = "hash";
  SOM            = som;
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash::~XrdMqSharedHash() 
{

}

/*----------------------------------------------------------------------------*/
std::string 
XrdMqSharedHash::StoreAsString(const char* notprefix)
{
  std::string s="";
  StoreMutex.LockRead();
  std::map<std::string, XrdMqSharedHashEntry>::iterator it;
  for (it=Store.begin(); it!= Store.end(); it++) {
    XrdOucString key = it->first.c_str();
    if ( (!notprefix) || (notprefix && (!strlen(notprefix))) || (!key.beginswith(notprefix))) {
      s+= it->first.c_str(); s+= "=";
      s+= it->second.GetEntry(); s+= " ";
    }
  }
  StoreMutex.UnLockRead();
  return s;
}


/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::CloseTransaction() 
{
  if (Transactions.size()) {
    XrdOucString txmessage="";
    MakeUpdateEnvHeader(txmessage);
    AddTransactionEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    XrdMqMessaging::gMessageClient.SendMessage(message, BroadCastQueue.c_str());
  }

  if (Deletions.size()) {
    XrdOucString txmessage="";
    MakeDeletionEnvHeader(txmessage);
    AddDeletionEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    XrdMqMessaging::gMessageClient.SendMessage(message, BroadCastQueue.c_str());
  }

  IsTransaction = false;
  TransactionMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeBroadCastEnvHeader(XrdOucString &out)
{
  out = XRDMQSHAREDHASH_BCREPLY; out += "&"; out += XRDMQSHAREDHASH_SUBJECT; out += "="; out += Subject.c_str();
  out += "&"; out += XRDMQSHAREDHASH_TYPE;    out += "="; out += Type.c_str();
}


/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeUpdateEnvHeader(XrdOucString &out)
{
  out = XRDMQSHAREDHASH_UPDATE; out += "&"; out += XRDMQSHAREDHASH_SUBJECT; out += "="; out += Subject.c_str();
  out += "&"; out += XRDMQSHAREDHASH_TYPE;    out += "="; out += Type.c_str();
}


/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeDeletionEnvHeader(XrdOucString &out)
{
  out = XRDMQSHAREDHASH_DELETE; out += "&"; out += XRDMQSHAREDHASH_SUBJECT; out += "="; out += Subject.c_str();
  out += "&"; out += XRDMQSHAREDHASH_TYPE;    out += "="; out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::MakeRemoveEnvHeader(XrdOucString &out)
{
  out = XRDMQSHAREDHASH_REMOVE; out += "&"; out += XRDMQSHAREDHASH_SUBJECT; out += "="; out += Subject.c_str();
  out += "&"; out += XRDMQSHAREDHASH_TYPE;    out += "="; out += Type.c_str();
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::BroadCastEnvString(const char* receiver)
{
  TransactionMutex.Lock();
  Transactions.clear();
  IsTransaction = true;

  std::map<std::string, XrdMqSharedHashEntry>::iterator it;

  for (it=Store.begin(); it!=Store.end(); it++) {
    Transactions.insert(it->first);
  }
  
  XrdOucString txmessage="";
  MakeBroadCastEnvHeader(txmessage);
  AddTransactionEnvString(txmessage);
  TransactionMutex.UnLock();

  XrdMqMessage message("XrdMqSharedHashMessage");
  message.SetBody(txmessage.c_str());
  message.MarkAsMonitor();
  if (XrdMqSharedObjectManager::debug)fprintf(stderr,"XrdMqSharedObjectManager::BroadCastEnvString=>[%s]=>%s \n", Subject.c_str(),receiver);
  return XrdMqMessaging::gMessageClient.SendMessage(message,receiver);
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::AddTransactionEnvString(XrdOucString &out)
{
  // encoding works as "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ...."
  out += "&"; out += XRDMQSHAREDHASH_PAIRS; out += "=";
  std::set<std::string>::const_iterator transit;

  XrdMqRWMutexReadLock lock(StoreMutex);
  for (transit= Transactions.begin(); transit != Transactions.end(); transit++) {
    if ( (Store.count(transit->c_str() ))) {
      out += "|";
      out += transit->c_str();
      out += "~";
      out += Store[transit->c_str()].entry.c_str();
      out += "%";
      char cid[1024];snprintf(cid,sizeof(cid)-1,"%llu",Store[transit->c_str()].ChangeId);
      out += cid;
    }
  }
  Transactions.clear();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::AddDeletionEnvString(XrdOucString &out)
{
  // encoding works as "mysh.keys=|<key1>|<key2> ...."
  out += "&"; out += XRDMQSHAREDHASH_KEYS; out += "=";

  std::set<std::string>::const_iterator delit;

  for (delit = Deletions.begin(); delit != Deletions.end(); delit++) {
    out += "|";
    out += delit->c_str();
  }
  Deletions.clear();
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::Dump(XrdOucString &out) 
{
  std::map<std::string, XrdMqSharedHashEntry>::iterator it;
  char keyprint[64];
  StoreMutex.LockRead();
  for (it=Store.begin(); it!=Store.end(); it++) {
    snprintf(keyprint,sizeof(keyprint)-1,"key=%-24s", it->first.c_str());
    out += keyprint;
    out += " ";
    it->second.Dump(out);
    out += "\n";
  }
  StoreMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/

bool 
XrdMqSharedHash::BroadCastRequest(const char* requesttarget) {
  XrdOucString out;
  XrdMqMessage message("XrdMqSharedHashMessage");
  out += XRDMQSHAREDHASH_BCREQUEST; 
  out += "&"; out += XRDMQSHAREDHASH_SUBJECT; out += "="; out += Subject.c_str();
  out += "&"; out += XRDMQSHAREDHASH_REPLY;   out += "="; out += XrdMqMessaging::gMessageClient.GetClientId();
  out += "&"; out += XRDMQSHAREDHASH_TYPE;    out += "="; out += Type.c_str();
  message.SetBody(out.c_str());
  message.MarkAsMonitor();
  return XrdMqMessaging::gMessageClient.SendMessage(message, requesttarget);  
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedHash::Set(const char* key, const char* value, bool broadcast, bool tempmodsubjects)
{
  if (!value)
    return false;
  
  std::string skey = key;

  {
    XrdMqRWMutexWriteLock lock(StoreMutex);
    bool callback=false;
    
    if (!Store.count(skey)) {
      callback=true;
    }
    
    Store[skey].Set(value);
    if (callback) {
      CallBackInsert(&Store[skey], skey.c_str());
    }
    
    // we emulate a transaction for a single Set
    if (broadcast && (!IsTransaction) ) {
      TransactionMutex.Lock(); Transactions.clear();
    }
    
    if (broadcast) {
      Transactions.insert(skey);
    } 
    
    // check if we have to do posts for this subject
    if (SOM) {
      SOM->SubjectsMutex.Lock();
      if (SOM->ModificationWatchKeys.size()) {
	if (SOM->ModificationWatchKeys.count(skey)) {
	  std::string fkey = Subject.c_str();
	  fkey += ";" ; fkey+= skey;
	  if (XrdMqSharedObjectManager::debug)fprintf(stderr,"XrdMqSharedObjectManager::Set=>[%s:%s]=>%s notified\n", Subject.c_str(),skey.c_str(),value);
	  if (tempmodsubjects) 
	    SOM->ModificationTempSubjects.push_back(fkey);
	  else {
	    SOM->ModificationSubjects.push_back(fkey);
	    SOM->SubjectsSem.Post();
	  }
	}
      }
      SOM->SubjectsMutex.UnLock();
    }
  }

  if (broadcast && (!IsTransaction))
    CloseTransaction();
  
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedHash::Print(std::string &out, std::string format) 
{
  //-------------------------------------------------------------------------------
  // listformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>"  -> to print a key of the attached children
  // "sep=<seperator>"                                          -> to put a seperator
  // "header=1"                                                 -> to put a header with description on top! This must be the first format tag!
  // "indent=<n>"                                               -> indent the output
  // "headeronly=1"                                             -> only prints the header and nothnig else
  // the formats are:
  // 's' : print as string
  // 'l' : print as long long
  // 'f' : print as double
  // 'o' : print as <key>=<val>
  // '-' : left align the printout
  // '+' : convert numbers into k,M,G,T,P ranges
  // the unit is appended to every number:
  // e.g. 1500 with unit=B would end up as '1.5 kB'
  // the command only appends to <out> and DOES NOT initialize it

  std::vector<std::string> formattoken;
  bool buildheader=false;
  std::string indent="";

  std::string header="";
  std::string body = "";
  bool headeronly=false;

  XrdMqStringConversion::Tokenize(format, formattoken, "|");

  for (unsigned int i=0; i< formattoken.size(); i++) {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;

    XrdMqStringConversion::Tokenize(formattoken[i],tagtoken,":");
    for (unsigned int j=0; j< tagtoken.size(); j++) {
      std::vector<std::string> keyval;
      XrdMqStringConversion::Tokenize(tagtoken[j], keyval,"=");
      formattags[keyval[0]] = keyval[1];
    }

    //---------------------------------------------------------------------------------------
    // "key=<key>:width=<width>:format=[slfo]:uniq=<unit>"
    
    bool alignleft=false;
    if ( (formattags["format"].find("-") != std::string::npos) ) {
      alignleft = true;
    }

    if (formattags.count("header") ) {
      // add the desired seperator
      if (formattags.count("header") == 1) {
        buildheader=true;
      }
    }

    if (formattags.count("headeronly") ) {
      headeronly=true;
    }

    if (formattags.count("indent") ) {
      for (int i=0; i< atoi(formattags["indent"].c_str()); i++) {
	indent+= " ";
      }
    }
    
    if (formattags.count("width") && formattags.count("format")) {
      unsigned int width = atoi(formattags["width"].c_str());
      // string
      char line[1024];
      char tmpline[1024];
      char lformat[1024];
      char lenformat[1024];
      line[0]=0;
      
      if ((formattags["format"].find("s"))!= std::string::npos) 
        snprintf(lformat,sizeof(lformat)-1, "%%s");
      
      if ((formattags["format"].find("l"))!= std::string::npos)
        snprintf(lformat,sizeof(lformat)-1, "%%lld");
      
      
      if ((formattags["format"].find("f"))!= std::string::npos)
        snprintf(lformat,sizeof(lformat)-1, "%%.02f");
      
      
      if (alignleft) {
        snprintf(lenformat,sizeof(lenformat)-1, "%%-%ds",width);
      } else {
        snprintf(lenformat,sizeof(lenformat)-1, "%%%ds",width);
      }
      
      // normal member printout
      if (formattags.count("key")) {
        if ((formattags["format"].find("s"))!= std::string::npos) 
          snprintf(tmpline,sizeof(tmpline)-1,lformat,Get(formattags["key"].c_str()).c_str());
        if ((formattags["format"].find("l"))!= std::string::npos) 
          snprintf(tmpline,sizeof(tmpline)-1,lformat,GetLongLong(formattags["key"].c_str()));
        if ((formattags["format"].find("f"))!= std::string::npos) 
          snprintf(tmpline,sizeof(tmpline)-1,lformat,GetDouble(formattags["key"].c_str()));

        if (buildheader) {
          char headline[1024];
          char lenformat[1024];
          snprintf(lenformat, sizeof(lenformat)-1, "%%%ds", width-1);
          snprintf(headline,sizeof(headline)-1, lenformat,formattags["key"].c_str());
          std::string sline = headline;
          if (sline.length() > (width-1)) {
            sline.erase(0, ((sline.length()-width+1+3)>0)?(sline.length()-width+1+3):0);
            sline.insert(0,"...");
          }
          header += "#";
          header += sline;
        }

        snprintf(line,sizeof(line)-1,lenformat,tmpline);
      }
      body += indent;
      if ( (formattags["format"].find("o")!= std::string::npos) ) {
        char keyval[4096];
        buildheader = false; // auto disable header
        if (formattags.count("key")) {
          snprintf(keyval,sizeof(keyval)-1,"%s=%s", formattags["key"].c_str(), line);
        }
        body += keyval;
      }  else {
        std::string sline = line;
        if (sline.length() > width) {
          sline.erase(0, ((sline.length()-width+3)>0)?(sline.length()-width+3):0);
          sline.insert(0,"...");
        }
        body += sline;
      }
    }

    if (formattags.count("sep") ) {
      // add the desired seperator
      body += formattags["sep"];
      if (buildheader) {
        header += formattags["sep"];
      }
    }
  }

  body += "\n";

  if (buildheader) {
    std::string line ="";
    line += "#";
    for (unsigned int i=0; i< (header.length()-1); i++) {
      line += ".";
    }
    line += "\n";
    out += line;
    out += indent;
    out += header; out += "\n";
    out += indent;
    out += line;
    if (!headeronly) {
      out += body;
    }
  } else {
    out += body;
  }
}


/*----------------------------------------------------------------------------*/
void
XrdMqSharedQueue::CallBackInsert(XrdMqSharedHashEntry *entry, const char* key) 
{
  entry->SetKey(key);
  Queue.push_back(entry);
  LastObjectId++;
  //fprintf(stderr,"XrdMqSharedObjectManager::CallBackInsert=> on %s => LOID=%llu\n", key, LastObjectId);
}
 
/*----------------------------------------------------------------------------*/
void
XrdMqSharedQueue::CallBackDelete(XrdMqSharedHashEntry *entry)
{
  std::deque<XrdMqSharedHashEntry*>::iterator it;
  for (it = Queue.begin(); it != Queue.end(); it++) {
    if (*it == entry) {
      Queue.erase(it);
      break;
    }
  }
  //fprintf(stderr,"XrdMqSharedObjectManager::CallBackDelete=> on %s \n", entry->GetKey());
}

/*----------------------------------------------------------------------------*/
