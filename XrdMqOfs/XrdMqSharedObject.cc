/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqSharedObject.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
// system includes

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::XrdMqSharedObjectManager() 
{
  
}

/*----------------------------------------------------------------------------*/
XrdMqSharedObjectManager::~XrdMqSharedObjectManager() 
{

}


/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::CreateSharedHash(const char* subject, const char* broadcastqueue) 
{
  std::string ss = subject;

  HashMutex.LockWrite();
  
  if (hashsubjects.count(ss)>0) {
    hashsubjects[ss]->SetBroadCastQueue(broadcastqueue);
    HashMutex.UnLockWrite();
    return false;
  } else {
    XrdMqSharedHash* newhash = new XrdMqSharedHash(subject, broadcastqueue);

    hashsubjects.insert( std::pair<std::string, XrdMqSharedHash*> (ss, newhash));
    
    HashMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::CreateSharedQueue(const char* subject, const char* broadcastqueue) 
{
  std::string ss = subject;

  ListMutex.LockWrite();
  
  if (queuesubjects.count(ss)>0) {
    ListMutex.UnLockWrite();
    return false;
  } else {
    XrdMqSharedQueue newlist(subject, broadcastqueue);

    queuesubjects.insert( std::pair<std::string, XrdMqSharedQueue> (ss, newlist));
    
    ListMutex.UnLockWrite();
    return true;
  }
}


/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::DeleteSharedHash(const char* subject) 
{
  std::string ss = subject;
  
  HashMutex.LockWrite();
  
  if (hashsubjects.count(ss)>0) {
    delete (hashsubjects[ss]);
    hashsubjects.erase(ss);
    HashMutex.UnLockWrite();
    return true;
  } else {
    HashMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMqSharedObjectManager::DeleteSharedQueue(const char* subject) 
{
  std::string ss = subject;
  
  ListMutex.LockWrite();
  
  if (queuesubjects.count(ss)>0) {
    queuesubjects.erase(ss);
    ListMutex.UnLockWrite();
    return true;
  } else {
    ListMutex.UnLockWrite();
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMqSharedObjectManager::DumpSharedObjectList(XrdOucString& out) 
{
  out="";
  char formatline[1024];

  HashMutex.LockRead();
  std::map<std::string , XrdMqSharedHash*>::iterator it_hash;
  for (it_hash=hashsubjects.begin(); it_hash!= hashsubjects.end(); it_hash++) {
    snprintf(formatline,sizeof(formatline)-1,"subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n",it_hash->first.c_str(), it_hash->second->GetBroadCastQueue(),(unsigned int) it_hash->second->GetSize(), it_hash->second->GetChangeId());
    out += formatline;
  }
  HashMutex.UnLockRead();
  
  ListMutex.LockRead();
  std::map<std::string , XrdMqSharedQueue>::const_iterator it_list;
  for (it_list=queuesubjects.begin(); it_list!= queuesubjects.end(); it_list++) {
    //    snprintf(formatline,sizeof(formatline)-1,"subject=%32s broadcastqueue=%32s size=%u changeid=%llu\n",it_hash->first.c_str(), it_hash->second.GetBroadCastQueue(),(unsigned int) it_hash->second.GetSize(), it_hash->second.GetChangeId());
    out += formatline;
  }
  ListMutex.UnLockRead();
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
    XrdMqSharedHash* sh;
    sh = GetObject(subject.c_str(), type.c_str());

    XrdOucString ftag = XRDMQSHAREDHASH_CMD; ftag += "="; ftag += env.Get(XRDMQSHAREDHASH_CMD);


    if ( (ftag == XRDMQSHAREDHASH_BCREQUEST) || (ftag == XRDMQSHAREDHASH_DELETE) ) {
      // if we don't know the subject, we don't create it with a BCREQUEST
      if ( ( ftag == XRDMQSHAREDHASH_BCREQUEST) && (reply == "")) {
	error = "bcrequest: no reply address present";
	return false;
      }
      
      HashMutex.LockRead();
      XrdMqSharedHash* sh;
      sh = GetObject(subject.c_str(),type.c_str());

      if (!sh) {
	HashMutex.UnLockRead();
	if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
	  error = "bcrequest: don't know this subject";
	} 
	if (ftag == XRDMQSHAREDHASH_DELETE) {
	  error = "delete: don't know this subject";
	}
	return false;
      }
    } else {
      // automatically create the subject, if it does not exist
      if (!sh) {
	HashMutex.UnLockRead();
	HashMutex.LockWrite();
	if (!CreateSharedObject(subject.c_str(),"",type.c_str())) {
	  HashMutex.UnLockWrite();
	  error = "cannot create shared object for "; error += subject.c_str(); error += " and type "; error += type.c_str();
	  return false;
	}
	
	sh = GetObject(subject.c_str(), type.c_str());
	HashMutex.UnLockWrite();
      } else {
	HashMutex.UnLockRead();
      }
    }

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
	sh->Set(key, value, false);

      }
      return true;
    }

    if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
      return (sh->BroadCastEnvString(reply.c_str()));
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
      return true;
    }
  }
  error="unknown message: ";error += message->GetBody();
  return false;
}


/*----------------------------------------------------------------------------*/
XrdMqSharedHash::XrdMqSharedHash(const char* subject, const char* broadcastqueue) 
{
  BroadCastQueue = broadcastqueue;
  Subject        = subject;
  ChangeId       = 0;
  IsTransaction  = false;
  Type           = "hash";
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash::~XrdMqSharedHash() 
{

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
    XrdMqMessaging::gMessageClient.SendMessage(message);
  }

  if (Deletions.size()) {
    XrdOucString txmessage="";
    MakeDeletionEnvHeader(txmessage);
    AddDeletionEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    XrdMqMessaging::gMessageClient.SendMessage(message);
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
  StoreMutex.LockRead();
  for (it=Store.begin(); it!=Store.end(); it++) {
    out += "key="; out += it->first.c_str(); out += " ";
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
void
XrdMqSharedQueue::CallBackInsert(XrdMqSharedHashEntry *entry, const char* key) 
{
  entry->SetKey(key);
  Queue.push_back(entry);
  LastObjectId++;
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
}

/*----------------------------------------------------------------------------*/
