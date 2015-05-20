#include "KineticIo.hh"
#include <sstream>
#include <iomanip>
#include <thread>

EOSFSTNAMESPACE_BEGIN

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::duration_cast;


/* Static ClusterMap for all KineticIo objects */
static KineticClusterMap & cmap()
{
  static KineticClusterMap clustermap;
  return clustermap;
}

shared_ptr<const string> path_util::chunkKey(const char* path, int chunk_number)
{
  std::ostringstream ss;
  ss << path << "_" << std::setw(10) << std::setfill('0') << chunk_number;
  return make_shared<const string>(ss.str());
}

std::string path_util::extractID(const std::string& path)
{
  size_t id_start = path.find_first_of(':') + 1;
  size_t id_end   = path.find_first_of(':', id_start);
  return path.substr(id_start, id_end-id_start);
}

KineticIo::KineticIo (size_t cache_capacity) :
    cluster(), cache(*this, cache_capacity), lastChunkNumber(*this)
{
  mType = "KineticIo";
}

KineticIo::~KineticIo ()
{
}

/* All necessary checks have been done in the 993 line long
 * XrdFstOfsFile::open method before we are called. */
int KineticIo::Open (const std::string& p, XrdSfsFileOpenMode flags,
		mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_debug("Opening path %s",p.c_str());

  errno = cmap().getCluster(path_util::extractID(p), cluster);
  if(errno) return SFS_ERROR;

  /* Setting path variables. supplied file path has the form
   * kinetic:cluster_id:path there is no need to encode kinetic:cluster_id in
   * all keys. */
  mFilePath = p;
  chunkNameBase = mFilePath.c_str() + mFilePath.find_last_of(':') + 1;

  /* Put the metadata key... if it already exists the operation will fail with
   * a version missmatch error, which is fine... */
  shared_ptr<const string> version(new string());
  KineticStatus s = cluster->put(make_shared<string>(mFilePath),
          version, make_shared<const string>(), false);

  if(s.ok())
    lastChunkNumber.set(0);
  else if(s.statusCode() != StatusCode::REMOTE_VERSION_MISMATCH){
    eos_err("Invalid Connection Status: %d, error message: %s",
               s.statusCode(), s.message().c_str());
    errno = EIO;
    return SFS_ERROR;
  }
  eos_debug("Opening path %s successful",p.c_str());
  return SFS_OK;
}

int KineticIo::Close (uint16_t timeout)
{
  eos_debug("Closing path %s", mFilePath.c_str());

  if(Sync(timeout) == SFS_ERROR)
      return SFS_ERROR;
  cluster.reset();

  eos_debug("Closing path %s successful", mFilePath.c_str());
  mFilePath.clear(); chunkNameBase = NULL;
  return SFS_OK;
}

enum rw {READ, WRITE};
int64_t KineticIo::doReadWrite (XrdSfsFileOffset off, char* buffer,
		XrdSfsXferSize length, uint16_t timeout, int mode)
{
  eos_debug("%s %d bytes from offset %ld for path %s",
          mode == rw::READ ? "Reading" : "Writing",
          length, off, mFilePath.c_str());
  if(!cluster){
      errno = ENXIO;
      return SFS_ERROR;
  }
  shared_ptr<KineticChunk> chunk;
  const size_t chunk_capacity = cluster->limits().max_value_size;
  size_t length_todo = length;
  off_t  off_done = 0;

  while(length_todo){
    int chunk_number = (off+off_done) / chunk_capacity;
    off_t chunk_offset = (off+off_done) - chunk_number * chunk_capacity;
    size_t chunk_length = std::min(length_todo, chunk_capacity - chunk_offset);

   /* Increase last chunk number if we write past currently known file size...
    * also assume the chunk doesn't exist yet in this case.  */
    bool create=false;
    if(chunk_number > lastChunkNumber.get() && (mode == rw::WRITE)){
      lastChunkNumber.set(chunk_number);
      create = true;
    }

    errno = cache.get(chunk_number, chunk, create);
    if(errno) return SFS_ERROR;

    if(mode == rw::WRITE){
      errno = chunk->write(buffer+off_done, chunk_offset, chunk_length);
      if(errno) return SFS_ERROR;

      /* flush chunk in background if writing to chunk capacity. */
      if(chunk_offset + chunk_length == chunk_capacity)
        cache.requestFlush(chunk_number);
    }
    else if (mode == rw::READ){
      errno = chunk->read(buffer+off_done, chunk_offset, chunk_length);
      if(errno) return SFS_ERROR;

      /* If we are reading the last chunk (or past it) */
      if(chunk_number >= lastChunkNumber.get()){
        /* make sure length doesn't indicate that we read past filesize. */
        if(chunk->size() > chunk_offset)
          length_todo -= std::min(chunk_length,
                   (size_t) chunk->size() - chunk_offset);
        break;
      }
    }
    length_todo -= chunk_length;
    off_done += chunk_length;
  }

  eos_debug("%s %d bytes from offset %ld for path %s successfully",
      mode == rw::READ ? "Read" : "Wrote",
      length-length_todo, off, mFilePath.c_str());
  return length-length_todo;
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer,
        XrdSfsXferSize length, uint16_t timeout)
{
  return doReadWrite(offset, buffer, length, timeout, rw::READ);
}

int64_t KineticIo::Write (XrdSfsFileOffset offset, const char* buffer,
        XrdSfsXferSize length, uint16_t timeout)
{
  return doReadWrite(offset, const_cast<char*>(buffer), length, timeout,
          rw::WRITE);
}

int64_t KineticIo::ReadAsync (XrdSfsFileOffset offset, char* buffer,
        XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
  // ignore async for now
  return Read(offset, buffer, length, timeout);
}

int64_t KineticIo::WriteAsync (XrdSfsFileOffset offset, const char* buffer,
        XrdSfsXferSize length, uint16_t timeout)
{
  // ignore async for now
  return Write(offset, buffer, length, timeout);
}

int KineticIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("Truncating path %s to offset %ld",mFilePath.c_str(), offset);
  if(!cluster){
      errno = ENXIO;
      return SFS_ERROR;
  }

  const size_t chunk_capacity = cluster->limits().max_value_size;
  int chunk_number = offset /chunk_capacity;
  int chunk_offset = offset - chunk_number * chunk_capacity;

  /* Step 1) truncate the chunk containing the offset. */
  shared_ptr<KineticChunk> chunk;
  errno = cache.get(chunk_number, chunk);
  if(errno) return SFS_ERROR;

  errno = chunk->truncate(chunk_offset);
  if(errno) return SFS_ERROR;
  
  /* Step 2) Ensure we don't have chunks past chunk_number in the cache. Since
   * truncate isn't super common, go the easy way and just sync+drop the
   * cache... this will also sync the just truncated chunk.  */
  errno = Sync();
  if(errno) return SFS_ERROR;
  cache.clear();

  /* Step 3) Delete all chunks past chunk_number. When truncating to size 0,
   * (and only then) also delete the first chunk. */
  std::unique_ptr<std::vector<string>> keys;
  const size_t max_keys_requested = 100;
  do{
    KineticStatus status = cluster->range(
            path_util::chunkKey(chunkNameBase, offset ? chunk_number+1 : 0),
            path_util::chunkKey(chunkNameBase, 99999999),
            max_keys_requested, keys);
    for (auto iter = keys->begin(); iter != keys->end() && status.ok(); ++iter){
        status = cluster->remove(make_shared<string>(*iter),
                make_shared<string>(""), true);
    }
    if(!status.ok()){
       eos_err("Invalid Connection Status: %d, error message: %s",
           status.statusCode(), status.message().c_str());
       errno = EIO;
       return SFS_ERROR;
    }
  }while(keys->size() == max_keys_requested);

  /* Set last chunk number */
  lastChunkNumber.set(chunk_number);

  eos_debug("Truncating path %s to offset %ld successful",mFilePath.c_str(), offset);
  return SFS_OK;
}

int KineticIo::Fallocate (XrdSfsFileOffset length)
{
  eos_debug("length %ld",length);
  if(!cluster){
    errno = ENXIO;
    return SFS_ERROR;
  }
  return SFS_OK;
}

int KineticIo::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  eos_debug("from offset %ld to offset %ld",fromOffset, toOffset);
  if(!cluster){
    eos_err("Connection Nullptr.");
    errno = ENXIO;
    return SFS_ERROR;
  }
  return SFS_OK;
}

int KineticIo::Remove (uint16_t timeout)
{
  if(Truncate(0))
      return SFS_ERROR;

  KineticStatus status = cluster->remove(make_shared<string>(mFilePath), 
          make_shared<string>(""), true);
  if(!status.ok()){
    eos_err("Invalid Connection Status: %d, error message: %s",
        status.statusCode(), status.message().c_str());
    errno = EIO;
    return SFS_ERROR;
  }
  return SFS_OK;
}

int KineticIo::Sync (uint16_t timeout)
{
    eos_debug("Syncing path %s",mFilePath.c_str());
    if(!cluster){
        errno = ENXIO;
        return SFS_ERROR;
    }
    errno = cache.flush();
    if(errno) return SFS_ERROR;

    eos_debug("Syncing %s successful",mFilePath.c_str());
    return SFS_OK;
}

int KineticIo::Stat (struct stat* buf, uint16_t timeout)
{
  eos_debug("Stat'ing path %s.",mFilePath.c_str());
  if(!cluster){
      errno = ENXIO;
      return SFS_ERROR;
  }

  errno = lastChunkNumber.verify();
  if(errno) return SFS_ERROR;

  std::shared_ptr<KineticChunk> last_chunk;
  errno = cache.get(lastChunkNumber.get(), last_chunk);
  if(errno) return SFS_ERROR;

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = cluster->limits().max_value_size;
  buf->st_blocks = lastChunkNumber.get() + 1;
  buf->st_size = lastChunkNumber.get() * buf->st_blksize + last_chunk->size();

  eos_debug("Stat successful for path %s, size is: %ld",
          mFilePath.c_str(), buf->st_size);
  return SFS_OK;
}

void* KineticIo::GetAsyncHandler ()
{
  // no async for now
  return NULL;
}

int KineticIo::Statfs (const char* p, struct statfs* sfs)
{
  eos_debug("Requesting statfs for path %s",p);

  /* We don't want to allow Statfs on an Opened() object. */
  if(mFilePath.length() && mFilePath.compare(p))
    return EPERM;

  if(!cluster &&(errno = cmap().getCluster(path_util::extractID(p), cluster)))
    return errno;
  mFilePath=p;

  long capacity = cluster->size().nominal_capacity_in_bytes;
  long free     = capacity - (capacity * cluster->size().portion_full);
  

  /* Minimal allocated block size. Set to 4K because that's the
   * maximum accepted value by Linux. */
  sfs->f_frsize = 4096;
   /* Preferred file system block size for I/O requests. This is sometimes
    * evaluated as the actual block size (e.g. by EOS). We set the bsize equal
    * to the frsize to avoid confusion. This approach is also taken by all
    * kernel level file systems. */
  sfs->f_bsize  = sfs->f_frsize;
  /* Blocks on FS in units of f_frsize */
  sfs->f_blocks = (fsblkcnt_t) (capacity / sfs->f_frsize);
  /* Free blocks */
  sfs->f_bavail = (fsblkcnt_t) (free / sfs->f_frsize);
  /* Free blocks available to non root user */
  sfs->f_bfree  = sfs->f_bavail;
  /* Total inodes. */
  sfs->f_files   = capacity;
  /* Free inodes */
  sfs->f_ffree   = free;

  eos_info("Capacity is %ld bytes, %ld GB, %ld blocks of %d size each",
          capacity, capacity / (1024*1024*1024), sfs->f_blocks, sfs->f_frsize);
  eos_debug("Statfs successful for path %s",p);
  return 0;
}

struct ftsState{
  std::unique_ptr<std::vector<string>> keys;
  shared_ptr<string> end_key;
  size_t index;

  ftsState(std::string subtree):keys(new std::vector<string>({subtree})),index(1)
  {
    end_key = make_shared<string>(subtree+"~");
  }
};

void* KineticIo::ftsOpen(std::string subtree)
{
  eos_debug("ftsOpen path %s",subtree.c_str());

  if((errno = cmap().getCluster(path_util::extractID(subtree), cluster)))
    return NULL;

  return new ftsState(subtree);
}

std::string KineticIo::ftsRead(void* fts_handle)
{
  if(!fts_handle) return "";
  ftsState * state = (ftsState*) fts_handle;

  if(state->keys->size() <= state->index){
    const size_t max_key_requests = 100;
    state->index = 0;
    /* add a space character (lowest ascii printable) to make the range request
       non-including. */
    cluster->range(make_shared<string>(state->keys->back()+" "),
            state->end_key,
            max_key_requests, state->keys);
  }
  return state->keys->empty() ? "" : state->keys->at(state->index++);
}

int KineticIo::ftsClose(void* fts_handle)
{
  ftsState * state = (ftsState*) fts_handle;
  if(state){
    delete state;
    return 0;
  }
  return -1;
}

KineticIo::LastChunkNumber::LastChunkNumber(KineticIo & parent) : 
            parent(parent), last_chunk_number(0), last_chunk_number_timestamp()
{}

KineticIo::LastChunkNumber::~LastChunkNumber()
{}

int KineticIo::LastChunkNumber::get() const
{
  return last_chunk_number;
}

void KineticIo::LastChunkNumber::set(int chunk_number)
{
  last_chunk_number = chunk_number;
  last_chunk_number_timestamp = system_clock::now();
}

int KineticIo::LastChunkNumber::verify()
{
  /* chunk number verification independent of standard expiration verification
   * in KinetiChunk class. validate last_chunk_number (another client might have
   * created new chunks we know nothing about, or truncated the file. */
  if(duration_cast<milliseconds>(system_clock::now() -
          last_chunk_number_timestamp).count() < KineticChunk::expiration_time)
    return 0;

  /* Technically, we could start at chunk 0 to catch all cases... but that the
   * file is truncated by another client while opened here is highly unlikely.
   * And for big files this would mean unnecessary GetKeyRange requests for the
   * regular case.  */
  const size_t max_keys_requested = 100;
  std::unique_ptr<std::vector<string>> keys;
  do{
    KineticStatus status = parent.cluster->range(keys ?
            make_shared<const string>(keys->back()) :
            path_util::chunkKey(parent.chunkNameBase, last_chunk_number),
            path_util::chunkKey(parent.chunkNameBase, 99999999),
            max_keys_requested,
            keys);

    if(!status.ok()){
      eos_err("Invalid Connection Status: %d, error message: %s",
          status.statusCode(), status.message().c_str());
      return EIO;
    }
  }while(keys->size() == max_keys_requested);

  /* Success: get chunk number from last key.*/
  if(keys->size() > 0){
    std::string key = keys->back();
    std::string number = key.substr(key.find_last_of('_')+1, key.length());
    set(std::stoll(number));
    return 0;
  }

  /* No keys found. the file might have been truncated, retry but start the
   * search from chunk 0 this time. */
  if(last_chunk_number > 0){
      last_chunk_number = 0;
      return verify();
  }

  /* No keys found. Ensure that the key has not been removed by testing for
     the existance of the metadata key. */
  shared_ptr<const string> version(new string());
  shared_ptr<string> value;

  if(parent.cluster->get(make_shared<string>(parent.mFilePath),
                  version, value, true).ok())
    return 0;
  
  /* Metadata key has been removed by someone else since this file has been
   * openend. This case should be exceedingly rare. */
  return ENOENT;
}

KineticIo::Attr::Attr(const char* path, ClusterPointer c) :
            eos::common::Attr(path), cluster(c)
{
}

KineticIo::Attr::~Attr()
{
}

bool KineticIo::Attr::Get(const char* name, char* content, size_t& size)
{
  if(!cluster){
      return false;
  }
  shared_ptr<const string> key(new string(mName+"_attr_"+name));
  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string());
  KineticStatus status = cluster->get(key, version, value, false);

  if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND){
      eos_info("Requested attribute '%s' does not exist.", name);
      return false;
  }
  if(!status.ok()){
      eos_err("Invalid Connection Status: %d, error message: %s",
      status.statusCode(), status.message().c_str());
      return false;
  }
  if(size < value->size()){
      eos_info("Requested attribute bigger than supplied buffer.");
      return false;
  }
  size = value->size();
  value->copy(content, size, 0);
  return true;
}

string KineticIo::Attr::Get(string name)
{
  size_t size = sizeof(mBuffer);
  if(Get(name.c_str(), mBuffer, size) == true)
    return string(mBuffer, size);
  return string("");
}

bool KineticIo::Attr::Set(const char * name, const char * content, size_t size)
{
  if(!cluster){
    return false;
  }
  shared_ptr<const string> key(new string(mName+"_attr_"+name));
  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string(content, size));


  KineticStatus status = cluster->put(key, version, value, true);
  if(!status.ok()){
    eos_err("Invalid Connection Status: %d, error message: %s",
    status.statusCode(), status.message().c_str());
    return false;
  }
  return true;
}

bool KineticIo::Attr::Set(std::string key, std::string value)
{
  return Set(key.c_str(), value.c_str(), value.size());
}

/* As in Attr.cc implementation, ensure that the file exists
* in static OpenAttr function. */
KineticIo::Attr* KineticIo::Attr::OpenAttr (const char* path)
{
  ClusterPointer c;
  if(cmap().getCluster(path_util::extractID(path), c))
    return 0;

  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string());
  
  if(!c->get(make_shared<const string>(path), version, value, true).ok())
    return 0;

  return new Attr(path, c);
}

KineticIo::Attr* KineticIo::Attr::OpenAttribute (const char* path)
{
  return OpenAttr(path);
}

KineticIo::KineticChunkCache::KineticChunkCache(KineticIo & parent, size_t cache_capacity):
    parent(parent), capacity(cache_capacity), background_run(true), background_shutdown(false)
{
  std::thread(&KineticChunkCache::background, this).detach();
}

KineticIo::KineticChunkCache::~KineticChunkCache()
{
  background_run = false;
  background_trigger.notify_all();

  std::unique_lock<std::mutex> lock(background_mutex);      
  while(background_shutdown==false)
      background_trigger.wait(lock);
}

void KineticIo::KineticChunkCache::clear()
{
  {
    std::lock_guard<std::mutex> lock(background_mutex);
    background_queue = std::queue<int>();
  }
  cache.clear();
  lru_order.clear();
}

int KineticIo::KineticChunkCache::flush()
{
  for (auto it=cache.begin(); it!=cache.end(); ++it){
    int err = it->second->flush();
    if(err) return err;
  }
  return 0;
}

int KineticIo::KineticChunkCache::get(int chunk_number, std::shared_ptr<KineticChunk>& chunk, bool create)
{
  if(cache.count(chunk_number)){
    chunk = cache.at(chunk_number);
    lru_order.remove(chunk_number);
    lru_order.push_back(chunk_number);
    return 0;
  }

  if(lru_order.size() >= capacity){
    if(int err = cache.at(lru_order.front())->flush())
      return err;

    cache.erase(lru_order.front());
    lru_order.pop_front();
  }

  chunk.reset(new KineticChunk(parent.cluster,
      path_util::chunkKey(parent.chunkNameBase, chunk_number), create));
  cache.insert(std::make_pair(chunk_number,chunk));
  lru_order.push_back(chunk_number);
  return 0;
}

void KineticIo::KineticChunkCache::requestFlush(int chunk_number)
{
  {
    std::lock_guard<std::mutex> lock(background_mutex);
    background_queue.push(chunk_number);
  }
  background_trigger.notify_all();
}

void KineticIo::KineticChunkCache::background()
{
  while(background_run){
    /* Obtain chunk number from background queue. */
    int chunk_number;
    {
      std::unique_lock<std::mutex> lock(background_mutex);
      while(background_queue.empty() && background_run)
        background_trigger.wait(lock);

      chunk_number = background_queue.front();
      background_queue.pop();
    }

    /* The chunk is not guaranteed to actually be in the cache. If it isn't
       no harm no foul. */
    shared_ptr<KineticChunk> chunk;
    try{
        chunk = cache.at(chunk_number);
    } catch (const std::out_of_range& oor){}
    if(chunk) chunk->flush();
  }

  background_shutdown = true;
  background_trigger.notify_all();
}



EOSFSTNAMESPACE_END
