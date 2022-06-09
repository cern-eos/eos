#include "common/RWMutex.hh"
#include <sstream>
#include <sys/syscall.h>
#include <XrdSys/XrdSysPthread.hh>

#pragma once

EOSCOMMONNAMESPACE_BEGIN

class TrackMutex {
  // track code location where a mutex is locked
  
public:
  TrackMutex(const char* _class   = EOS_CLASS,
	     const char* function = EOS_FUNCTION,
	     const char* file     = EOS_FILE,
	     int line             = EOS_LINE) : mClass(_class), mFunc(function), mFile(file), mLine(line), mLockOwner(0) {}
  
  virtual ~TrackMutex() {}

  TrackMutex(const TrackMutex& mtx) {}

  TrackMutex& operator=(const TrackMutex& t) { return *this;}
  
  void Tag(const char* _class     = EOS_CLASS,
	   const char* function   = EOS_FUNCTION,
	   const char* file       = EOS_FILE,
	   int line               = EOS_LINE) {
					       XrdSysMutexHelper t(mTag);
					       mClass = _class;
					       mFunc = function;
					       mFile = file;
					       mLine = line;
  }
  
  
  inline void Lock(const char* _class   = EOS_CLASS,
		   const char* function = EOS_FUNCTION,
		   const char* file     = EOS_FILE,
		   int line             = EOS_LINE) {
						     pid_t owner=0;
						     pid_t tid = syscall(SYS_gettid);
						     {
						       XrdSysMutexHelper t(mTag);						       
						       if (mSerial.count(tid)) {
							 fprintf(stderr,"[%llx] MUTEX LOCK violation: %s:%s:%s:%d [%s:%s:%s:%d]\n",
								 &mMutex, _class, function,file,line,
								 mClass.c_str(),mFunc.c_str(), mFile.c_str(), mLine);
							 for ( auto s : mSerial ) {
							   fprintf(stderr, "[%llx] %x %x \n",tid, s);
							 }
							 return;
						       }
						       mSerial.insert(tid);
						     }
						     {
						      XrdSysMutexHelper t(mTag);
						      if (sDebug) {
							fprintf(stderr,"[%llx] MUTEX LOCK %lu %s:%s:%s:%d [%s:%s:%s:%d]\n",
								&mMutex,tid, _class, function, file, line,
								mClass.c_str(), mFunc.c_str(),  mFile.c_str(), mLine);
						      }
						     }
						     
						     mMutex.Lock();
						     mLocked = true;
						     Tag(_class, function,file,line);
						     mLockOwner = tid;
						     sTracker.Add(this, mClass,mFunc, mFile, mLine);
  }
  
  inline int CondLock(const char* _class   = EOS_CLASS,
		      const char* function = EOS_FUNCTION,
		      const char* file     = EOS_FILE,
		      int line             = EOS_LINE) { 
      pid_t owner=0;
      pid_t tid = syscall(SYS_gettid);
      {
	XrdSysMutexHelper t(mTag);						       
	if (mSerial.count(tid)) {
	  fprintf(stderr,"[%llx] MUTEX CONDLOCK violation: %s:%s:%s:%d [%s:%s:%s:%d]\n",
		  &mMutex, _class, function,file,line,
		  mClass.c_str(),mFunc.c_str(), mFile.c_str(), mLine);
	  return 0;
	}
      }
      int rc = 0;
      if((rc = mMutex.CondLock())) {
	{
	  XrdSysMutexHelper t(mTag);
	  mSerial.insert(tid);

	  if (sDebug) {
	    fprintf(stderr,"[%llx] MUTEX CONDLOCK %lu %s:%s:%s:%d [%s:%s:%s:%d]\n",
		    &mMutex,tid, _class, function, file, line,
		    mClass.c_str(), mFunc.c_str(),  mFile.c_str(), mLine);
	  }
	}
	Tag(_class,function,file,line);
      }
      
      return rc;
  }
  

  inline bool ShouldUnLock() {
			      return mLocked;
  }
  
  inline void UnLock(const char* _class   = EOS_CLASS,
		     const char* function = EOS_FUNCTION,
		     const char* file     = EOS_FILE,
		     int line             = EOS_LINE,
		     bool check_violation=true) {
			pid_t tid = syscall(SYS_gettid);
			pid_t owner = 0;
			
			{
			  XrdSysMutexHelper t(mTag);
			  if (!mSerial.count(tid)) {
			    owner = mLockOwner;
			    if (check_violation) {
			      fprintf(stderr,"[%llx] MUTEX UNLOCK violation %d (%d) %lu %s:%s:%s:%d [%s:%s:%s:%d] \n",
				      &mMutex, (int)mLocked, tid, owner,
				      _class, function, file, line,
				      mClass.c_str(), mFunc.c_str(),  mFile.c_str(), mLine);
			      return;
			    } else {
			      if (sDebug) {
				fprintf(stderr,"[%llx] MUTEX UNLOCK skipped %d (%d) %lu %s:%s:%s:%d [%s:%s:%s:%d] \n",
					&mMutex, (int)mLocked, tid, owner,
					_class, function, file, line,
					mClass.c_str(), mFunc.c_str(),  mFile.c_str(), mLine);
				return;
			      }
			    }
			  }
			}
			if (sDebug) {
			  fprintf(stderr,"[%llx] MUTEX UNLOCK %d (%d) %lu %s:%s:%s:%d [%s:%s:%s:%d]\n",
				  &mMutex, (int)mLocked, tid, owner,
				  _class, function, file, line,
				  mClass.c_str(), mFunc.c_str(),  mFile.c_str(), mLine);
			}
			mMutex.UnLock();
			{
			  XrdSysMutexHelper t(mTag);
			  mSerial.erase(tid);
			}
			if (sDebug) {
			  fprintf(stderr,"[%llx] MUTEX UNLOCKED %d (%d) %lu \n",
				  &mMutex, (int)mLocked, tid, owner);
			}
			mLockOwner = 0;
			mLocked = false;
			  
			sTracker.Remove(this);
  }
  
  std::string Dump() {
		      std::stringstream s;
		      s << "Mutex: " << mClass << "::" << mFunc << "::" << mFile << ":" << mLine << "::" << mLockOwner << std::endl;
		      return s.str();
  }

  uint64_t Address() { return (uint64_t) &mMutex; }
  
  class Tracker {
  public:
    Tracker(){}
    virtual ~Tracker(){}
    
    void Add(TrackMutex* m, const std::string& _class, const std::string& function, const std::string& file, int line) {
											     Inc();
											     std::lock_guard<std::mutex> lock(mLock);
											     snprintf(signature,sizeof(signature), "%s:%s:%s:%d",
												      _class.c_str(),function.c_str(),file.c_str(),line);
											     mLocks[(uint64_t)(m)] = signature;
    }
    
    void Remove(TrackMutex* m) {
				Dec();
				std::lock_guard<std::mutex> lock(mLock);
				if (mLocks.erase((uint64_t)(m))) {
				  Dec();
				}
				  
    }
    
    void Inc() {nLock++;}
    void Dec() {nUnLock++;}
    
    uint64_t getLocks() { return nLock;}
    uint64_t getUnLocks() { return nUnLock;}
    uint64_t getTracked() {
			   std::lock_guard<std::mutex> lock(mLock);
			   return mLocks.size();
    }

    std::string Dump() {
			std::stringstream ss;
			std::lock_guard<std::mutex> lock(mLock);
			for ( auto it : mLocks ) {
			  snprintf(signature, sizeof(signature), "LOCK %016llx %s", it.first, it.second.c_str());
			  ss << signature << std::endl;
			}
			return ss.str();
    }
  private:
    std::mutex mLock;
    std::map<uint64_t, std::string> mLocks;
    char signature[4096];
    std::atomic<uint64_t> nLock;
    std::atomic<uint64_t> nUnLock;
  };
  
  static TrackMutex::Tracker sTracker;
  static bool sDebug;
  
private:
  std::set<pid_t>mSerial;
  std::string mClass;
  std::string mFunc;
  std::string mFile;
  int mLine;
  XrdSysMutex mMutex;
  XrdSysMutex mTag;
  std::atomic<pid_t> mLockOwner;
  std::atomic<bool> mLocked;
};

class LockMonitor  {
  // track code location where a lock monitor is instantiated/used
  
public:
  LockMonitor(TrackMutex* mutex,
	      const char* _class   = EOS_CLASS,
	      const char* function = EOS_FUNCTION,
	      const char* file     = EOS_FILE,
	      int line             = EOS_LINE) : mMutex(mutex) {

    ILocked = false;
    if (mutex) {
      Lock(mutex, _class, function,file,line);
    }
  }
  
  LockMonitor(TrackMutex& mutex,
	      const char* _class   = EOS_CLASS,
	      const char* function = EOS_FUNCTION,
	      const char* file     = EOS_FILE,
	      int line             = EOS_LINE) : mMutex(&mutex) {
    ILocked = false;
    Lock(&mutex, _class, function,file,line);
  }
  
  virtual ~LockMonitor() {
    UnLock(EOS_CLASS, EOS_FUNCTION, EOS_FILE, EOS_LINE);
  }

  void Tag(const char* _class = EOS_CLASS,
	   const char* function = EOS_FUNCTION,
	   const char* file = EOS_FILE,
             int line = EOS_LINE) {
    mMutex->Tag(_class, function,file,line);
  }
  
  std::string Dump() {
    return mMutex->Dump();
  }

  inline void Lock(TrackMutex* mtx,
		   const char* _class   = EOS_CLASS,
		   const char* function = EOS_FUNCTION,
		   const char* file     = EOS_FILE,
		   int line             = EOS_LINE) {
    if (!ILocked) {
      // avoid re-entrant locking
      if (mtx) {
	mMutex = mtx;
	mMutex->Lock(_class, function,file,line);
	ILocked = true;
      }
    }
  }
  
  inline int CondLock(const char* _class   = EOS_CLASS,
		      const char* function = EOS_FUNCTION,
		      const char* file     = EOS_FILE,
		      int line             = EOS_LINE) {if (mMutex) {int rc = mMutex->CondLock(_class, function,file,line); if(rc) ILocked = true; return rc;}}
  
  
  inline void UnLock(
		     const char* _class   = EOS_CLASS,
		     const char* function = EOS_FUNCTION,
		     const char* file     = EOS_FILE,
		     int line             = EOS_LINE
		     ) {
    bool s = ILocked;
    if (mMutex && ILocked) {
      // avoid double unlocking
      mMutex->UnLock(_class, function, file, line);
    }
    ILocked = false;
  }
  
private:
  std::string mClass;
  std::string mFunc;
  std::string mFile;
  std::atomic<bool> ILocked;
  
  volatile int mLine;
  TrackMutex* mMutex;
};

EOSCOMMONNAMESPACE_END
