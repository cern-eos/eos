#include "KineticChunk.hh"

#include <algorithm>
#include <errno.h>
#include <uuid/uuid.h>

using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;
using std::unique_ptr;
using std::string;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;


const int KineticChunk::expiration_time = 1000;
const int KineticChunk::capacity = 1048576;


KineticChunk::KineticChunk(ConnectionPointer c, std::string k, bool skip_initial_get) :
	key(k),version(),data(),timestamp(),updates(),connection(c)
{
	if(skip_initial_get == false)
		get();
}

KineticChunk::~KineticChunk()
{
}

int KineticChunk::get()
{
	unique_ptr<KineticRecord> record;
	KineticStatus status = connection->Get(key, record);

	if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
		return EIO;

	string x;
	if(status.ok()){
		x = *record->value();
		version = *record->version();
	}
	timestamp = steady_clock::now();

	x.resize(std::max(x.size(), data.size()));
	for (auto& update : updates){
		if(update.second)
			x.replace(update.first, update.second, data, update.first, update.second);
		else
			x.resize(update.first);
	}

	data = x;
	return 0;
}

int KineticChunk::read(char* const buffer, off_t offset, size_t length)
{
	if(buffer==nullptr || offset<0 || offset+length>capacity)
		return EINVAL;

	// ensure data is not too stale to read, re-read if necessary and merge on-drive
	// value with existing local changes
	if(duration_cast<milliseconds>(steady_clock::now() - timestamp).count() >= expiration_time){
		unique_ptr<string> version_on_drive;
		KineticStatus status = connection->GetVersion(key, version_on_drive);

		if(!status.ok()  && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
			return EIO;

		if((status.statusCode() == StatusCode::REMOTE_NOT_FOUND && version.empty()) ||
		   (version_on_drive->compare(version)==0))
			timestamp = steady_clock::now();

		else if(int err = get())
			return err;
	}

	// return 0s if client reads non-existing data (e.g. file with holes)
	memset(buffer,0,length);
	if(data.size()>(unsigned int)offset)
		data.copy(buffer, std::min(length, (unsigned long)(data.size()-offset)), offset);
	return 0;
}

int KineticChunk::write(const char* const buffer, off_t offset, size_t length)
{
	if(buffer==nullptr || offset<0 || offset+length>capacity)
			return EINVAL;

	data.resize(std::max((size_t) offset + length, data.size()));
	data.replace(offset, length, buffer, length);
    updates.push_back(std::pair<off_t, size_t>(offset, length));
	return 0;
}

int KineticChunk::truncate(off_t offset)
{
	if(offset<0 || offset>capacity)
		return EINVAL;

	data.resize(offset);
	updates.push_back(std::pair<off_t, size_t>(offset, 0));
	return 0;
}


int KineticChunk::flush()
{
	uuid_t uuid;
	uuid_generate(uuid);
	std::string new_version(reinterpret_cast<const char *>(uuid), sizeof(uuid_t));

	KineticRecord record(data, new_version, "", Command_Algorithm_SHA1);
	KineticStatus status = connection->Put(key, version, WriteMode::REQUIRE_SAME_VERSION, record);

	if (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH){
		if(int err = get())
			return err;
		return flush();
	}

	if (!status.ok())
		return EIO;

	updates.clear();
	version = new_version;
	timestamp = steady_clock::now();
	return 0;
}

bool KineticChunk::dirty()
{
	return !updates.empty();
}
