#include "KineticIo.hh"
#include "KineticChunk.hh"

EOSFSTNAMESPACE_BEGIN

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;

static int getConnection(const std::string &path, ConnectionPointer &connection)
{
    static KineticDriveMap dm(""); // initialized once due to being static
     
    size_t wwn_start = path.find_first_of(':') + 1;
    size_t wwn_end   = path.find_first_of(':', wwn_start);
    std::string wwn  = path.substr(wwn_start, wwn_end-wwn_start);
            
    std::shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> con;
    if(int err = dm.getConnection(wwn, con))
        return err;
    
    connection = con;
    return 0; 
}


KineticIo::KineticIo (size_t cache_capacity) :
    cache_capacity(cache_capacity)
{
}


KineticIo::~KineticIo ()
{}

int KineticIo::Open (const std::string& p, XrdSfsFileOpenMode flags,
		mode_t mode, const std::string& opaque, uint16_t timeout)
{
    if((errno = getConnection(p, connection)))
        return SFS_ERROR;
    mFilePath = p;
    
    /* create operation */
    if (flags & SFS_O_CREAT){
 	KineticRecord record("", "0", "", Command_Algorithm_SHA1);
	KineticStatus status = connection->Put(mFilePath+"_0", "", WriteMode::REQUIRE_SAME_VERSION, record);
        if(status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH){
            eos_info("Cannot create file %s, it already exists.", mFilePath.c_str());
            errno = EEXIST;
            return SFS_ERROR;
        }
        if(!status.ok()){
            eos_err("Invalid Connection Status: %d", status.statusCode());
            connection.reset();
            errno = EIO;
            return SFS_ERROR;
        }
        return SFS_OK;
    }
    
    /* regular open operation */
    unique_ptr<string> version;
    KineticStatus status = connection->GetVersion(mFilePath+"_0", version);
    if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND){
        eos_info("Cannot open file %s, it does not exist.", mFilePath.c_str());
        errno = ENOENT;
        return SFS_ERROR;
    }
    if(!status.ok()){
        eos_err("Invalid Connection Status: %d", status.statusCode());
        connection.reset();
        errno = EIO;
        return SFS_ERROR;
    }
    return SFS_OK;
}

int KineticIo::getChunk (int chunk_number, std::shared_ptr<KineticChunk>& chunk)
{
    if(cache.count(chunk_number)){
        chunk = cache.at(chunk_number);
        return 0;
    }

    if(cache_fifo.size() >= cache_capacity){
        auto old = cache.at(cache_fifo.front());
        if(old->dirty())
            if(int err = old->flush()){
                eos_err("Flushing dirty chunk failed with error code: %d",err);
                return err;
            }
       cache_fifo.pop();
     }

    chunk.reset(new KineticChunk(connection, mFilePath + "_" + std::to_string((long long int)chunk_number)));
    cache.insert(std::make_pair(chunk_number, chunk));
    cache_fifo.push(chunk_number);
    return 0;
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer,
		XrdSfsXferSize length, uint16_t timeout)
{
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

        errno = getChunk(chunk_number, chunk);
        if(errno) return SFS_ERROR;
        errno = chunk->read(buffer+offset_done, chunk_offset, chunk_length);
        if(errno) return SFS_ERROR;

        length_todo -= chunk_length;
        offset_done += chunk_length;
    }

    return length;
}

int64_t KineticIo::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, uint16_t timeout)
{
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

		errno = getChunk(chunk_number, chunk);
		if(errno) return SFS_ERROR;
		errno = chunk->write(buffer+offset_done, chunk_offset, chunk_length);
		if(errno) return SFS_ERROR;

		if(chunk_offset + chunk_length == KineticChunk::capacity){
			errno = chunk->flush();
			if(errno) return SFS_ERROR;
		}

		length_todo -= chunk_length;
		offset_done += chunk_length;
	}

	return length;
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
    	if(!connection){
            eos_err("Connection Nullptr.");
            errno = ENXIO;
            return SFS_ERROR;
	}
	shared_ptr<KineticChunk> chunk;
	int chunk_number = offset / KineticChunk::capacity;
	int chunk_offset = offset - chunk_number * KineticChunk::capacity;

	errno = getChunk(chunk_number, chunk);
	if(errno) return SFS_ERROR;

	errno = chunk->truncate(chunk_offset);
	if(errno) return SFS_ERROR;

	return SFS_OK;
}

int KineticIo::Fallocate (XrdSfsFileOffset lenght)
{
    	if(!connection){
            eos_err("Connection Nullptr.");
            errno = ENXIO;
            return SFS_ERROR;
	}
	// TODO: handle quota here
	return SFS_OK;
}

int KineticIo::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
    	if(!connection){
            eos_err("Connection Nullptr.");
            errno = ENXIO;
            return SFS_ERROR;
	}
	// TODO: handle quota here
	return SFS_OK;
}

int KineticIo::Remove (uint16_t timeout)
{
    	if(!connection){
            eos_err("Connection Nullptr.");
            errno = ENXIO;
            return SFS_ERROR;
	}
	cache.clear();
	cache_fifo = std::queue<int>();

	unique_ptr<vector<std::string>> keys(new vector<string>());
	size_t max_size = 100;
	do {
		keys->clear();
		connection->GetKeyRange(mFilePath,false,mFilePath+"__",false,false,max_size,keys);
                for (auto iter = keys->begin(); iter != keys->end(); ++iter){
                    KineticStatus status = connection->Delete(*iter,"",WriteMode::IGNORE_VERSION);
                    if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND){
                        eos_err("Invalid Connection Status: %d", status.statusCode());
                        connection.reset();
                        errno = EIO;
                        return SFS_ERROR;
                    }
                }
	}while(keys->size() == max_size);

 
	return SFS_OK;
}

int KineticIo::Sync (uint16_t timeout)
{
    if(!connection){
        eos_err("Connection Nullptr.");
        errno = ENXIO;
        return SFS_ERROR;
    }
    for (auto it=cache.begin(); it!=cache.end(); ++it){
        if(it->second->dirty()){
            errno = it->second->flush();
            if(errno) return SFS_ERROR;
        }
    }
    return SFS_OK;
}

int KineticIo::Close (uint16_t timeout)
{
    if(Sync(timeout) == SFS_ERROR)
        return SFS_ERROR;
    connection.reset();
    mFilePath = "";
    return SFS_OK;
}

int KineticIo::Stat (struct stat* buf, uint16_t timeout)
{
    	if(!connection){
            eos_err("Connection Nullptr.");
            errno = ENXIO;
            return SFS_ERROR;
	}
	errno = Sync(timeout);
	if(errno) return SFS_ERROR;

	unique_ptr<string> key;
	unique_ptr<KineticRecord> record;

	KineticStatus status = connection->GetPrevious(mFilePath+"__",key,record);

	if(!status.ok()){
            eos_err("Invalid Connection Status: %d", status.statusCode());
            connection.reset();
            errno = EIO;
            return SFS_ERROR;
	}
	memset(buf,0,sizeof(struct stat));
	buf->st_blksize = KineticChunk::capacity;
        
        
        std::string chunkstr = key->substr(key->find_last_of('_')+1,key->length());
	int chunk_number = chunkstr.empty() ?  0 : std::stoll(chunkstr); 
        
	buf->st_blocks = chunk_number+1;
	buf->st_size = chunk_number * KineticChunk::capacity + record->value()->size();

	return SFS_OK;
}

void* KineticIo::GetAsyncHandler ()
{
    // no async for now
    return NULL;
}

int KineticIo::Statfs (const char* p, struct statfs* sfs)
{
    if((errno = getConnection(p, connection)))
        return errno;
    
    unique_ptr<kinetic::DriveLog> log;
    vector<kinetic::Command_GetLog_Type> types;
    types.push_back(kinetic::Command_GetLog_Type::Command_GetLog_Type_CAPACITIES);

    KineticStatus status = connection->GetLog(types,log);
    if(!status.ok()){
        eos_err("Invalid Connection Status: %d", status.statusCode());
        connection.reset();
        return EIO;
    }

    long capacity = log->capacity.nominal_capacity_in_bytes;
    long free     = capacity - (capacity * log->capacity.portion_full); 
  
    sfs->f_frsize = 4096; /* Minimal allocated block size. Set to 4K because that's the maximum accepted value. */
    sfs->f_bsize  = KineticChunk::capacity; /* Preferred file system block size for I/O requests */
    sfs->f_blocks = (fsblkcnt_t) (capacity / sfs->f_frsize); /* Blocks on FS in units of f_frsize */
    sfs->f_bavail = (fsblkcnt_t) (free / sfs->f_frsize); /* Free blocks */
    sfs->f_bfree  = sfs->f_bavail; /* Free blocks available to non root user */
    sfs->f_files   = capacity / KineticChunk::capacity; /* Total inodes */
    sfs->f_ffree   = free / KineticChunk::capacity ; /* Free inodes */

    connection.reset();
    return 0;
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
    KineticStatus status = connection->Get(mName+"_"+name,record);
    if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND){
        eos_info("Requested attribute '%s' does not exist.", name);
        return false;
    }
    if(!status.ok()){
        eos_err("Invalid Connection Status: %d", status.statusCode());
        connection.reset();
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
    KineticStatus status = connection->Put(mName+"_"+name, "", WriteMode::IGNORE_VERSION, record);
    if(!status.ok()){
        connection.reset();
        eos_err("Invalid Connection Status: %d", status.statusCode());
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
    /* As in Attr.cc implementation, ensure that file exists 
     * in static OpenAttr function. */
    if (!path)
        return 0;
    
    ConnectionPointer con; 
    if(getConnection(path, con) != 0)
        return 0;
    
    unique_ptr<string> version;
    KineticStatus status = con->GetVersion(string(path)+"_0", version);
    if(!status.ok())
        return 0;
    
    return new Attr(path, con);
}

KineticIo::Attr* KineticIo::Attr::OpenAttribute (const char* path)
{
    return OpenAttr(path);
}



EOSFSTNAMESPACE_END
