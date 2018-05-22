#pragma once

#include <string>
#include <map>
#include <sys/types.h>
#include <fcntl.h>
#include <mutex>
#include <memory>
#include <iostream>
#include <sstream>
#include <rados/librados.h>
#include <rados/librados.hpp>

#define EXOSMANAGER_OBJECT "EXOS/ROOT"
#define EXOSMANAGER_INODE_KEY "exos.inode"
#define EXOSMANAGER_POOL_KEY "exos.pool"
#define EXOSMANAGER_SIZE_KEY "exos.size"
#define EXOSMANAGER_MTIME_KEY "exos.mtime"
#define EXOSMANAGER_XATTR_RESERVED_PREFIX "exos."
#define EXOSMANAGER_DEFAULT_BLOCKSIZE 33554432

class exosmanager
{
public:
  exosmanager() : debug(false), mConnected(false) {}
  ~exosmanager();
  
  int connect(std::map<std::string,std::string>& params);

  typedef std::shared_ptr<librados::IoCtx> ioctx;
  ioctx getIoCtx(const std::string& pool);

  librados::Rados& getCluster() { return mCluster;}

  bool debug;
private:
  bool mConnected;
  librados::Rados mCluster;
  std::map<std::string, ioctx> mIoCtx;
};


class exosfile {

public:

  exosfile(const std::string& name, const std::string& cgi);
  exosfile() {mOpened = false; mPrepared = false;};
  void init(const std::string& name, const std::string& cgi);

  ~exosfile();

  int stat(struct stat* buf);

  int prepare();
  int open(int flags);
  int close();

  ssize_t write(const char* buffer, off_t offset, size_t len);
  ssize_t aio_write(const char* buffer, off_t offset, size_t len);
  ssize_t read(char* buffer, off_t offset, size_t len);

  ssize_t truncate(off_t offset);

  int aio_flush();
  int aio_collect();

  int setxattr(const std::map<std::string,std::string>& xattr);
  int getxattr(std::map<std::string,std::string>& xattr);
  int rmxattr(const std::set<std::string>& xattr);

  int lock(bool exclusive=false, time_t duration=300);
  int unlock(bool breakall=false);
  bool locked();
  bool locked_exclusive();

  int unlink(ssize_t offset = -1);

  static exosmanager* sManager;

  std::string nextInode();

  std::string dump();

  typedef struct extent
  {
    std::string oid;
    off_t offset;
    uint64_t len;
    off_t oid_offset;
  } extents_t;

  enum READAHEAD_STRATEGY
  {
    NONE = 0,
    STATIC = 1,
  } ;
  

  static READAHEAD_STRATEGY readahead_strategy_from_string(const std::string& strategy)
  {
    if (strategy == "static")
      return STATIC;
    return NONE;
  }

  void set_readahead_strategy(READAHEAD_STRATEGY rhs,
			      size_t min, size_t nom, size_t max)
  {
    std::lock_guard<std::mutex> lock(position_mutex);
    XReadAheadStrategy = rhs;
    XReadAheadMin = min;
    XReadAheadNom = nom;
    XReadAheadMax = max;
  }

  float get_readahead_efficiency()
  {
    std::lock_guard<std::mutex> lock(position_mutex);
    return (mTotalBytes) ? (100.0 * mTotalReadAheadHitBytes / mTotalBytes) : 0.0;
  }

  off_t aligned_offset(off_t offset)
  {
    std::lock_guard<std::mutex> lock(position_mutex);
    return offset / XReadAheadNom*XReadAheadNom;
  }

  // object listing interface
  void* objectlist();

  std::string nextobject(const void* handle);

  int closelist(const void* handle);

  void debug() {
    sManager->debug = true;
  }

private:

  std::map<std::string, std::string> params;

  std::map<std::string, std::string> parse(const std::string& query)
  {
    std::map<std::string, std::string> data;
    std::istringstream ss(query);
    std::string token;
    while (std::getline(ss, token, '&'))
    {
      size_t npos = token.find("=");
      if (npos != std::string::npos)
      {
	std::string key = token.substr(0, npos);
	std::string value = token.substr(npos+1);
	data[key] = value;
      }
    }
    return data;
  }

  std::string mName;
  std::string mInode;
  std::string mPool;
  std::string mDataPool;
  char mUUID[37];
  size_t mBlockSize;

  int mFlags;
  bool mOpened;
  bool mPrepared;
  uint64_t mSize;
  struct timespec mMtime;
  bool mLocked;
  bool mLockedExclusive;
  time_t mLockExpires;

  std::string timespec2string(struct timespec& ltime);
  struct timespec string2timespec(std::string stime);

  int connect() {
    if (!params.count("rados.md"))
      return EINVAL;
    if (!params.count("rados.data"))
      return EINVAL;
    
    return sManager->connect(params);
  }


  int open_md();
  int create_md();
  int get_md();
  int store_md();

  std::vector<extents_t> object_extents(off_t offset, uint64_t len);

  class AsyncHandler 
  {
  public:
    AsyncHandler() : offset(0), len(0), completion(0) {}
    AsyncHandler(uint64_t _offset, uint64_t _len) {completion = librados::Rados::aio_create_completion(); offset = _offset; len = _len;}
    virtual ~AsyncHandler() { delete completion; }
    off_t offset;
    uint32_t len;
    librados::AioCompletion* completion;
    librados::bufferlist buffer;
    bool matches(off_t off, uint32_t size,
		 off_t& match_offset, uint32_t& match_size)
    {
      fprintf(stderr,"match: %lu/%lu %u/%u\n", off, offset, size, len);
      if ( (off >= offset) &&
	   (off < ((off_t) (offset + len) )) ) {
	match_offset = off;
	if ( (off + size) <= (off_t) (offset + len) )
	  match_size = size;
	else
	  match_size = (offset + len - off);
	return true;
      }
      
      return false;
    }

    bool successor(off_t off, uint32_t size, uint64_t nominal_read_ahead)
    {
      off_t match_offset;
      uint32_t match_size;
      fprintf(stderr,"successor: off:%lu size:%u nom:%lu\n", off, size, nominal_read_ahead);
      if (matches((off_t) (off + nominal_read_ahead), size, match_offset, match_size)) {
	return true;
      } else {
	return false;
      }
    }

    bool isEOF() { if (buffer.length() == len) return false; else return true; }
  };

  typedef std::shared_ptr<AsyncHandler> io_handler;

  std::map<uint64_t, io_handler> ChunkRMap;
  std::set<io_handler> ChunkWMap;

  std::mutex position_mutex;
  std::mutex read_mutex;
  std::mutex write_mutex;
  std::mutex exist_mutex;


  READAHEAD_STRATEGY XReadAheadStrategy;
  size_t XReadAheadMin;
  size_t XReadAheadNom;
  size_t XReadAheadMax;

  off_t mPosition;
  off_t mWritePosition;
  off_t mTotalBytes;
  off_t mTotalReadAheadHitBytes;
  io_handler mSeqWriteHandler;
  bool mSeqWrite;
};

