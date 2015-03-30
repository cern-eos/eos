#include "KineticIo.hh"
#include "KineticChunk.hh"

EOSFSTNAMESPACE_BEGIN

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;


KineticIo::KineticIo (ConnectionPointer con, size_t cache_capacity) :
		connection(con), cache_capacity(cache_capacity)
{
}


KineticIo::~KineticIo ()
{}

int KineticIo::Open (const std::string& p, XrdSfsFileOpenMode flags,
		mode_t mode, const std::string& opaque, uint16_t timeout)
{
	if(!connection){
		errno = ENXIO;
		return SFS_ERROR;
	}
	path = p;
	unique_ptr<string> version;
	KineticStatus status = connection->GetVersion(path+"_0", version);
	if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND){
		errno = EIO;
		return SFS_ERROR;
	}

	// can only create when file doesn't exist already
	if (flags & SFS_O_CREAT){
		if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND)
			return SFS_OK;
		errno = EEXIST;
		return SFS_ERROR;
	}

	// can only open without create if file exists
	if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND){
		errno = ENOENT;
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
		   if(int err = old->flush())
			   return err;
	   cache_fifo.pop();
	 }

	chunk.reset(new KineticChunk(connection, path + "_" + std::to_string(chunk_number)));
	cache.insert(std::make_pair(chunk_number, chunk));
	cache_fifo.push(chunk_number);
	return 0;
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer,
		XrdSfsXferSize length, uint16_t timeout)
{
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
	// TODO: handle quota here
	return true;
}

int KineticIo::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
	// TODO: handle quota here
	return true;
}

int KineticIo::Remove (uint16_t timeout)
{
	cache.clear();
	cache_fifo = std::queue<int>();

	unique_ptr<vector<std::string>> keys(new vector<string>());
	size_t max_size = 100;
	do {
		keys->clear();
		connection->GetKeyRange(path,false,path+"__",false,false,max_size,keys);
		for (auto& element : *keys){
			KineticStatus status = connection->Delete(element,"",WriteMode::IGNORE_VERSION);
			if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND){
				errno = EIO;
				return SFS_ERROR;
			}
		}
	}while(keys->size() == max_size);

	return SFS_OK;
}

int KineticIo::Sync (uint16_t timeout)
{
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
	return Sync(timeout);
}

int KineticIo::Stat (struct stat* buf, uint16_t timeout)
{
	errno = Sync(timeout);
	if(errno) return SFS_ERROR;

	unique_ptr<string> key;
	unique_ptr<KineticRecord> record;

	KineticStatus status = connection->GetPrevious(path+"__",key,record);

	if(!status.ok()){
		errno = EIO;
		return SFS_ERROR;
	}
	memset(buf,0,sizeof(struct stat));
	buf->st_blksize = KineticChunk::capacity;

	// empty file -> no data keys for path
	if(key->substr(0,path.length()).compare(path))
		return SFS_OK;

	int chunk_number = std::stoll(key->substr(key->find_first_of('_')+1,key->length()));
	buf->st_blocks = chunk_number+1;
	buf->st_size = chunk_number * KineticChunk::capacity + record->value()->size();

	return 0;
}

void* KineticIo::GetAsyncHandler ()
{
    // no async for now
    return nullptr;
}

int KineticIo::Statfs (const char* path, struct statfs* sfs)
{
   unique_ptr<kinetic::DriveLog> log;
   vector<kinetic::Command_GetLog_Type> types;
   types.push_back(kinetic::Command_GetLog_Type::Command_GetLog_Type_CAPACITIES);

   KineticStatus status = connection->GetLog(types,log);
   if(!status.ok())
       return EIO;
 
   long capacity = log->capacity.nominal_capacity_in_bytes;
   long free     = capacity - (capacity * log->capacity.portion_full); 
   
    sfs->f_frsize = 4096; /* Minimal allocated block size. Set to 4K because that's the maximum accepted value. */
    sfs->f_bsize  = KineticChunk::capacity; /* Preferred file system block size for I/O requests */
    sfs->f_blocks = (fsblkcnt_t) (capacity / sfs->f_frsize); /* Blocks on FS in units of f_frsize */
    sfs->f_bavail = (fsblkcnt_t) (free / sfs->f_frsize); /* Free blocks */
    sfs->f_bfree  = sfs->f_bavail; /* Free blocks available to non root user */
    sfs->f_files   = capacity / KineticChunk::capacity; /* Total inodes */
    sfs->f_ffree   = free / KineticChunk::capacity ; /* Free inodes */

   return 0;
}





KineticIo::Attr::Attr(const char* path, KineticIo& parent) : kio(parent)
{}

KineticIo::Attr::~Attr()
{}

bool KineticIo::Attr::Get(const char* name, char* value, size_t& size)
{
    unique_ptr<KineticRecord> record;
    KineticStatus status = kio.connection->Get(kio.path+"_"+name,record);
    if(!status.ok())
        return false;
    size = record->value()->size();
    record->value()->copy(value, size, 0);
    return true;
}

string KineticIo::Attr::Get(string name)
{
	char buffer[1024];
	string value;
	size_t size;
	if(Get(name.c_str(),buffer, size))
            return string(buffer,size);
        return string("");
}

bool KineticIo::Attr::Set(const char * name, const char * value, size_t size)
{
	KineticRecord record(string(value, size), "", "", Command_Algorithm_SHA1);
	KineticStatus status = kio.connection->Put(kio.path+"_"+name, "", WriteMode::IGNORE_VERSION, record);
	if(!status.ok())
		return false;
	return true;
}

bool KineticIo::Attr::Set(std::string key, std::string value)
{
	return Set(key.c_str(), value.c_str(), value.size());
}

KineticIo::Attr* KineticIo::Attr::OpenAttribute (const char* path)
{
	return new Attr(path, kio);
}



EOSFSTNAMESPACE_END
