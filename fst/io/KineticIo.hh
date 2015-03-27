//------------------------------------------------------------------------------
//! @file KineticIO.hh
//! @author Paul Hermann Lensing
//! @brief Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------

#ifndef __EOSFST_KINETICFILEIO__HH__
#define __EOSFST_KINETICFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include "kinetic/kinetic.h"
#include "KineticChunk.hh"
#include<unordered_map>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

typedef std::shared_ptr<kinetic::BlockingKineticConnectionInterface> ConnectionPointer;

//------------------------------------------------------------------------------
//! Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------
class KineticIo  : public FileIo
{
public:
    class Attr : public FileIo::Attr 
    {
    public:
        bool Set (const char* name, const char* value, size_t len);
        bool Set (std::string key, std::string value);
        bool Get (const char* name, char* value, size_t &size);
        std::string Get (std::string name);
        Attr* OpenAttribute (const char* path);
        
        explicit Attr (const char* path, KineticIo& parent);
        ~Attr();
    private:
        KineticIo & kio;
    };

  //------------------------------------------------------------------------------
  // See FileIO.hh for documentation on the public interface
  int Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode = 0, const std::string& opaque = "", uint16_t timeout = 0);
  int64_t Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, uint16_t timeout = 0);
  int64_t Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, uint16_t timeout = 0);
  int64_t ReadAsync (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, bool readahead = false, uint16_t timeout = 0);
  int64_t WriteAsync (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, uint16_t timeout = 0);
  int Truncate (XrdSfsFileOffset offset, uint16_t timeout = 0);
  int Fallocate (XrdSfsFileOffset lenght);
  int Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);
  int Remove (uint16_t timeout = 0);
  int Sync (uint16_t timeout = 0);
  int Close (uint16_t timeout = 0);
  int Stat (struct stat* buf, uint16_t timeout = 0);
  void* GetAsyncHandler ();

  // For now just using a single connection. In the future, get a cluster description and
  // connection map (shared across objects) and decide on the appropriate connection(s) in
  // here.
  explicit KineticIo (ConnectionPointer connection=nullptr, size_t cache_capacity=10);
  ~KineticIo ();

private:
  int getChunk (int chunk_number, std::shared_ptr<KineticChunk>& chunk);
  
private:
  ConnectionPointer connection;
  std::string path;
  std::unordered_map<int, std::shared_ptr<KineticChunk>> cache;
  std::queue<int> cache_fifo;
  size_t cache_capacity;
  
private:
  KineticIo (const KineticIo&) = delete;
  KineticIo& operator = (const KineticIo&) = delete;
};


EOSFSTNAMESPACE_END

#endif  // __EOSFST_KINETICFILEIO_HH__
