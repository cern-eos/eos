#include "KineticIo.hh"
#include <sstream>
#include <iomanip>
#include <thread>

EOSFSTNAMESPACE_BEGIN

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::duration_cast;


/* Static KineticDrive Map for all KineticIo objects, as well as methods for 
 * drive map interaction.  */
static KineticDriveMap & dm()
{
    static KineticDriveMap dmap;
    return dmap;
}
static int getConnection(const std::string &path, ConnectionPointer &con)
{
    return dm().getConnection(path_util::driveID(path), con);
}
static int invalidateConnection(const std::string &path, ConnectionPointer &con)
{
    /* invalidate connection... forget local pointer and inform drive map to forget
     * about the connection state. */
    dm().invalidateConnection(path_util::driveID(path));                  
    con.reset(); 
    return EIO;   
}

std::string path_util::chunkString(const std::string& path, int chunk_number)
{
    std::ostringstream ss;
    ss << std::setw(10) << std::setfill('0') << chunk_number;
    return path+"_"+ss.str();
}

std::string path_util::driveID(const std::string& path)
{
    size_t id_start = path.find_first_of(':') + 1;
    size_t id_end   = path.find_first_of(':', id_start);
    return path.substr(id_start, id_end-id_start);
}

KineticIo::KineticIo (size_t cache_capacity) :
    connection(), cache(*this, cache_capacity), lastChunkNumber(*this)
{
    mType = "KineticIo";
}

KineticIo::~KineticIo ()
{
}

int KineticIo::Open (const std::string& p, XrdSfsFileOpenMode flags,
		mode_t mode, const std::string& opaque, uint16_t timeout)
{
    eos_debug("Opening path %s",p.c_str());
    
    errno = getConnection(p, connection);
    if(errno) return SFS_ERROR;
    mFilePath = p;
    
    /* All necessary checks have been done in the 993 line long XrdFstOfsFile::open method before we are called. */
    
    kinetic::KineticRecord record("", "created", "", Command_Algorithm_SHA1);
    kinetic::KineticStatus status = connection->Put("~"+mFilePath,"",WriteMode::REQUIRE_SAME_VERSION,record);
    /* Freshly created file. */
    if(status.ok()){ 
        std::shared_ptr<KineticChunk> chunk;
        if(cache.get(0,chunk,true) || chunk->flush()){
            eos_err("Invalid Connection Status: %d, error message: %s", 
                status.statusCode(), status.message().c_str());   
            errno = invalidateConnection(mFilePath, connection);                                        
            return SFS_ERROR;  
        }
        this->lastChunkNumber.set(0);
    }
    else if(status.statusCode() != StatusCode::REMOTE_VERSION_MISMATCH){
        eos_err("Invalid Connection Status: %d, error message: %s", 
                 status.statusCode(), status.message().c_str());   
        errno = invalidateConnection(mFilePath, connection);                                        
        return SFS_ERROR;                                        
    }
    
    eos_debug("Opening path %s successful",p.c_str());
    return SFS_OK;
}

int KineticIo::Close (uint16_t timeout)
{
    eos_debug("Closing path %s",mFilePath.c_str());
    
    if(Sync(timeout) == SFS_ERROR)
        return SFS_ERROR;
    connection.reset();
    
    eos_debug("Closing path %s successful",mFilePath.c_str());
    mFilePath = "";
    return SFS_OK;
}

enum rw {READ, WRITE};
int64_t KineticIo::doReadWrite (XrdSfsFileOffset offset, char* buffer,
		XrdSfsXferSize length, uint16_t timeout, int mode)
{
    eos_debug("%s %d bytes from offset %ld for path %s", 
            mode == rw::READ ? "Reading" : "Writing", 
            length, offset, mFilePath.c_str());
    
    if(!connection){ 
        eos_err("Connection Nullptr."); 
        errno = ENXIO; 
        return SFS_ERROR; 
    }
    shared_ptr<KineticChunk> chunk;
    int length_todo = length;
    int offset_done = 0;

    while(length_todo){

        int chunk_number = (offset+offset_done) / KineticChunk::capacity;
        int chunk_offset = (offset+offset_done) - chunk_number * KineticChunk::capacity;
        int chunk_length = std::min(length_todo, KineticChunk::capacity - chunk_offset);
      
       /* Increase last chunk number if we write past currently known file size...
        * also assume the file doesn't exist yet.  */
        bool create=false; 
        if(chunk_number > lastChunkNumber.get() && (mode == rw::WRITE)){
           lastChunkNumber.set(chunk_number);
           create = true; 
        }

        errno = cache.get(chunk_number, chunk, create);
        if(errno) return SFS_ERROR;
                   
        if(mode == rw::WRITE){            
            errno = chunk->write(buffer+offset_done, chunk_offset, chunk_length);
            if(errno) return SFS_ERROR;
           
            /* flush chunk in background if writing to chunk capacity. */
            if(chunk_offset + chunk_length == KineticChunk::capacity)
                cache.requestFlush(chunk_number); 
        }
        else if (mode == rw::READ){
            errno = chunk->read(buffer+offset_done, chunk_offset, chunk_length);
            if(errno) return SFS_ERROR;
  
            /* If we are reading the last chunk (or past it) */
            if(chunk_number >= lastChunkNumber.get()){    
                /* make sure length doesn't indicate that we read past filesize. */
                if(chunk->size() > chunk_offset)
                    length_todo -= std::min(chunk_length, chunk->size() - chunk_offset);  
                break;
            }          
        }

        length_todo -= chunk_length;
        offset_done += chunk_length;      
    }

    eos_debug("%s %d bytes from offset %ld for path %s successfully", 
        mode == rw::READ ? "Read" : "Wrote", 
        length-length_todo, offset, mFilePath.c_str());
    return length-length_todo;
    
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, uint16_t timeout)
{
    return doReadWrite(offset, buffer, length, timeout, rw::READ);
}

int64_t KineticIo::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, uint16_t timeout)
{
    return doReadWrite(offset, const_cast<char*>(buffer), length, timeout, rw::WRITE);
}

int64_t KineticIo::ReadAsync (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
    // ignore async for now
    return Read(offset, buffer, length, timeout);
}

int64_t KineticIo::WriteAsync (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, uint16_t timeout)
{
    // ignore async for now
    return Write(offset, buffer, length, timeout);
}

int KineticIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
    eos_debug("Truncating path %s to offset %ld",mFilePath.c_str(), offset);
    
    if(!connection){ 
        eos_err("Connection Nullptr."); 
        errno = ENXIO; 
        return SFS_ERROR; 
    }
   
    int chunk_number = offset / KineticChunk::capacity;
    int chunk_offset = offset - chunk_number * KineticChunk::capacity;
    
    /* Ensure we don't have chunks past chunk_number in the cache. Since truncate isn't super common, go the easy way 
       and just sync+drop the cache. */
    errno = Sync();
    if(errno) return SFS_ERROR;
    cache.clear();
        
    /* Delete all chunks from the drive past chunk_number. If chunk_offset is zero, we also want to delete the chunk_number chunk.  */
    bool including = (chunk_offset == 0);
    std::unique_ptr<std::vector<string>> keys;
    do{
        KineticStatus status = connection->GetKeyRange( 
                path_util::chunkString(mFilePath, chunk_number), 
                including, "|", true, false, 100, keys);
        
        if(!status.ok()){                                      
            eos_err("Invalid Connection Status: %d, error message: %s", 
                status.statusCode(), status.message().c_str());   
            errno = invalidateConnection(mFilePath, connection);                                        
            return SFS_ERROR;                                                        
        }     

        for (auto iter = keys->begin(); iter != keys->end(); ++iter){
            status = connection->Delete(*iter,"",WriteMode::IGNORE_VERSION);
            if(!status.ok()){                                      
                eos_err("Invalid Connection Status: %d, error message: %s", 
                    status.statusCode(), status.message().c_str());   
                errno = invalidateConnection(mFilePath, connection);                                        
                return SFS_ERROR;                                                        
            }     
        }
    }while(keys->size() == 100);
 
    if(chunk_offset){
        shared_ptr<KineticChunk> chunk;
        errno = cache.get(chunk_number, chunk);
        if(errno) return SFS_ERROR;

        errno = chunk->truncate(chunk_offset);
        if(errno) return SFS_ERROR;
        
        lastChunkNumber.set(chunk_number);
    }
    else{
        lastChunkNumber.set(chunk_number-1);
    }
    
    eos_debug("Truncating path %s to offset %ld successful",mFilePath.c_str(), offset);
    return SFS_OK;
}

int KineticIo::Fallocate (XrdSfsFileOffset length)
{
    eos_debug("length %ld",length);
    if(!connection){ 
        eos_err("Connection Nullptr."); 
        errno = ENXIO; 
        return SFS_ERROR; 
    }
    return SFS_OK;
}

int KineticIo::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
    eos_debug("from offset %ld to offset %ld",fromOffset, toOffset);
    if(!connection){ 
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
    
    KineticStatus status = connection->Delete("~"+mFilePath,"",WriteMode::IGNORE_VERSION);
    if(!status.ok()){
        eos_err("Invalid Connection Status: %d, error message: %s", 
            status.statusCode(), status.message().c_str());   
        errno = invalidateConnection(mFilePath, connection);                                        
        return SFS_ERROR;         
    }
    return SFS_OK; 
}

int KineticIo::Sync (uint16_t timeout)
{
    eos_debug("Syncing path %s",mFilePath.c_str());
    
    if(!connection){ 
        eos_err("Connection Nullptr."); 
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
    
    if(!connection){ 
        eos_err("Connection Nullptr."); 
        errno = ENXIO; 
        return SFS_ERROR; 
    }
    
    errno = lastChunkNumber.verify(); 
    if(errno) return SFS_ERROR;

    std::shared_ptr<KineticChunk> last_chunk;
    errno = cache.get(lastChunkNumber.get(), last_chunk);
    if(errno) return SFS_ERROR;    
    
    memset(buf, 0, sizeof(struct stat));
    buf->st_blksize = KineticChunk::capacity;
    buf->st_blocks  = lastChunkNumber.get() + 1;
    buf->st_size    = lastChunkNumber.get() * KineticChunk::capacity + last_chunk->size();

    eos_debug("Stat successful for path %s, size is: %ld",mFilePath.c_str(), buf->st_size);
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
    
    if(!connection && (errno = getConnection(string(p), connection)))
        return errno;
    mFilePath=p;
       
    unique_ptr<kinetic::DriveLog> log;
    vector<kinetic::Command_GetLog_Type> types;
    types.push_back(kinetic::Command_GetLog_Type::Command_GetLog_Type_CAPACITIES);

    KineticStatus status = connection->GetLog(types,log);
    if(!status.ok()){                                      
        eos_err("Invalid Connection Status: %d, error message: %s", 
        status.statusCode(), status.message().c_str());   
        return invalidateConnection(mFilePath, connection);                                                                                             
    }       

    long capacity = log->capacity.nominal_capacity_in_bytes;
    long free     = capacity - (capacity * log->capacity.portion_full); 
  
    sfs->f_frsize = 4096; /* Minimal allocated block size. Set to 4K because that's the maximum accepted value. */
    sfs->f_bsize  = sfs->f_frsize;  /* Preferred file system block size for I/O requests */
    
    /* set f_frsize to the same value as f_bsize... otherwise EOS gets confused 
     * (which is what all kernel filesystems do as well, as confusion is apparently common) */
    
    sfs->f_blocks = (fsblkcnt_t) (capacity / sfs->f_frsize); /* Blocks on FS in units of f_frsize */
    sfs->f_bavail = (fsblkcnt_t) (free / sfs->f_frsize); /* Free blocks */
    sfs->f_bfree  = sfs->f_bavail; /* Free blocks available to non root user */
    sfs->f_files   = capacity / KineticChunk::capacity; /* Total inodes */
    sfs->f_ffree   = free / KineticChunk::capacity ; /* Free inodes */
    
    eos_info("Capacity is %ld bytes, %ld GB, %ld blocks of %d size each",
            capacity, capacity / (1024*1024*1024), sfs->f_blocks, sfs->f_frsize);
    
    eos_debug("Statfs successful for path %s",p);
    return 0;
}



struct ftsState{
    std::unique_ptr<std::vector<string>> keys;
    size_t index; 
    
    ftsState():keys(new std::vector<string>()),index(0){}
};

void* KineticIo::ftsOpen(std::string subtree)
{
    eos_debug("ftsOpne path %s",subtree.c_str());
    
    if((errno = getConnection(subtree, connection)))
        return NULL;

    return new ftsState();
}

std::string KineticIo::ftsRead(void* fts_handle)
{
    ftsState * state = (ftsState*) fts_handle;

    if(state->keys->size() <= state->index){       
        std::string last_entry = state->keys->empty() ? "~" : state->keys->back();
        state->keys->clear();
        state->index = 0;
        connection->GetKeyRange( std::move(last_entry), 
                false, "~~", true, false, 100, state->keys);        
    }
    if(state->keys->empty())
        return "";
    
    std::string name = state->keys->at(state->index++);
    return std::move(name.erase(0,1));
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
    /* chunk number verification independent of standard expiration verification in KinetiChunk class. 
     * validate last_chunk_number (another client might have created new chunks we know nothing
       about, or truncated the file. */
    if(duration_cast<milliseconds>(system_clock::now() - last_chunk_number_timestamp).count() < KineticChunk::expiration_time)
        return 0; 
         
    /* Technically, we could start at chunk 0 to catch all cases... but that the file is truncated by another client
     * while opened here is highly unlikely. And for big files this would mean unnecessary GetKeyRange requests for the 
     * regular case.  */
    std::unique_ptr<std::vector<string>> keys;
    do{
        KineticStatus status = parent.connection->GetKeyRange( 
                keys ? keys->back() : path_util::chunkString(parent.mFilePath, last_chunk_number), 
                true, "|", true, false, 100, keys);
        if(!status.ok()){                                      
            eos_err("Invalid Connection Status: %d, error message: %s", 
                status.statusCode(), status.message().c_str());   
            return invalidateConnection(parent.mFilePath, parent.connection);                                                                                                
        }     
    }while(keys->size() == 100);

    /* Success: get chunk number from last key.*/    
    if(keys->size() > 0){
        std::string chunkstr = keys->back().substr(keys->back().find_last_of('_') + 1,keys->back().length());
        set(std::stoll(chunkstr));
        return 0;
    }
    
    /* No keys found. the file might have been truncated, retry but start the search from chunk 0 this time. */
    if(last_chunk_number > 0){
        last_chunk_number = 0;
        return verify(); 
    }
    
    /* Somebody removed the file, even chunk 0 is no longer available. */
    eos_info("File %s has been removed by another client.", parent.mFilePath.c_str());
    return ENOENT; 
}

KineticIo::Attr::Attr(const char* path, ConnectionPointer con) : 
            eos::common::Attr(path), connection(con)
{      
}

KineticIo::Attr::~Attr()
{
}

bool KineticIo::Attr::Get(const char* name, char* value, size_t& size)
{
    if(!connection){
        eos_err("Connection Nullptr.");
        return false;
    }
    unique_ptr<KineticRecord> record;
    KineticStatus status = connection->Get(mName+"_attr_"+name,record);
    
    if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND){
        eos_info("Requested attribute '%s' does not exist.", name);
        return false;
    }    
    if(!status.ok()){                                      
        eos_err("Invalid Connection Status: %d, error message: %s", 
        status.statusCode(), status.message().c_str());   
        invalidateConnection(mName, connection);                                        
        return false;                                                     
    }    
    if(size < record->value()->size()){
        eos_info("Requested attribute bigger than supplied buffer.");
        return false; 
    }    
    size = record->value()->size();
    record->value()->copy(value, size, 0);
    return true;
}

string KineticIo::Attr::Get(string name)
{
    size_t size = sizeof(mBuffer);
    if(Get(name.c_str(), mBuffer, size) == true)
        return string(mBuffer, size);
    return string("");
}

bool KineticIo::Attr::Set(const char * name, const char * value, size_t size)
{
    if(!connection){
        eos_err("Connection Nullptr.");
        return false;
    }
    KineticRecord record(string(value, size), "", "", Command_Algorithm_SHA1);
    KineticStatus status = connection->Put(mName+"_attr_"+name, "", WriteMode::IGNORE_VERSION, record);
    if(!status.ok()){                                      
        eos_err("Invalid Connection Status: %d, error message: %s", 
        status.statusCode(), status.message().c_str());   
        invalidateConnection(mName, connection);                                        
        return false;                                                        
    }    
    return true;
}

bool KineticIo::Attr::Set(std::string key, std::string value)
{
    return Set(key.c_str(), value.c_str(), value.size());
}

KineticIo::Attr* KineticIo::Attr::OpenAttr (const char* path)
{
    /* As in Attr.cc implementation, ensure that the file exists 
     * in static OpenAttr function. */
    if (!path)
        return 0;
    
    ConnectionPointer con; 
    if(getConnection(path, con))
        return 0;
    
    unique_ptr<string> version;
    KineticStatus status = con->GetVersion("~"+string(path), version);
    if(!status.ok())
        return 0;
    
    return new Attr(path, con);
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
    
    chunk.reset(new KineticChunk(parent.connection, path_util::chunkString(parent.mFilePath, chunk_number), create));
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
