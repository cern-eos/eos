#ifndef KINETICCHUNK_HH_
#define KINETICCHUNK_HH_

#include "kinetic/kinetic.h"
#include <chrono>
#include <string>
#include <list>

/*----------------------------------------------------------------------------*/


typedef std::shared_ptr<kinetic::BlockingKineticConnectionInterface> ConnectionPointer;

/* High(er) level API for Kinetic keys. Handles incremental updates and resolves concurrency
 * on chunk-basis. For multi-chunk atomic writes the caller will have to do appropriate locking
 * himself.*/
class KineticChunk {
public:
	static const int expiration_time; // 1 second staleness
	static const int capacity; // 1 MB chunk capacity

private:
	std::string key;
	std::string version;
	std::string data;
	std::chrono::system_clock::time_point timestamp;

	// a list of bit-regions that have been changed since this data block has last been flushed
	std::list<std::pair<off_t, size_t> > updates;
	ConnectionPointer connection;

private:
	// (re)reads the on-drive value, merges in possibly existing writes
	int get();

public:
	// reading is guaranteed up-to-date within expiration_time limits.
	int read(char* const buffer, off_t offset, size_t length);

	// writing in-memory only, never flushes
	int write(const char* const buffer, off_t offset, size_t length);

	// truncate in-memory only, never flushes
	int truncate(off_t offset);

	// flush flushes
	int flush();

	// test for your flushing needs
	bool dirty();

	explicit KineticChunk(ConnectionPointer con, std::string key, bool skip_initial_get=false);
	~KineticChunk();
};


#endif

