#include "KineticIo.hh"
#include <kio/KineticIoFactory.hh>
#include <kio/LoggingException.hh>
EOSFSTNAMESPACE_BEGIN

/* Yes. Macros are evil. So is code duplication. */
#define KIO_CATCH catch(const kio::LoggingException& ke){ \
                    logexcept(this,ke); errno=ke.errnum(); \
                  }catch(const std::exception& e){ \
                    eos_err(e.what()); errno=ENOEXEC; \
                  }

static void logexcept(const eos::common::LogId* lid, const kio::LoggingException& e)
{
  eos::common::Logging::log(e.function(),e.file(),e.line(),
          lid->logId, lid->vid, lid->cident, (LOG_ERR), e.what()
  );
}

static void logmsg(const char* func, const char* file, int line, int priority, const char *msg)
{
  eos::common::Logging::log(
   func,file,line,"LIBKINETICIO",eos::common::Logging::gZeroVid,"",priority,msg
  );
}

class LogFunctionInitializer{
public:
  LogFunctionInitializer(){
    kio::Factory::registerLogFunction(logmsg, eos::common::Logging::shouldlog);
  }
};
static LogFunctionInitializer kio_loginit;

KineticIo::KineticIo() :
  kio(kio::Factory::makeFileIo())
{
  eos_debug("");
}

KineticIo::~KineticIo()
{
  eos_debug("");
}

int KineticIo::Open(const std::string& path, XrdSfsFileOpenMode flags,
        mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_debug("path: %s, flags: %d, mode: %d, opaque: %s, timeout: %d, result: %d",
        path.c_str(), flags, mode, opaque.c_str(), timeout);
  try{
    kio->Open(path, flags, mode, opaque, timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
      uint16_t timeout)
{
  int64_t rv = SFS_ERROR;
  try{
    rv = kio->Read(offset, buffer, length, timeout);
  } KIO_CATCH
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d, result: %lld",
          offset, buffer, length, timeout, rv);
  return rv;
}

int64_t KineticIo::Write (XrdSfsFileOffset offset, const char* buffer,
    XrdSfsXferSize length, uint16_t timeout)
{
  int64_t rv = SFS_ERROR;
  try{
    rv = kio->Write(offset, buffer, length, timeout);
  } KIO_CATCH
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d, result: %lld",
          offset, buffer, length, timeout, rv);
  return rv;
}

int64_t KineticIo::ReadAsync (XrdSfsFileOffset offset, char* buffer,
    XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %p, length: %d, readahead: %d, timeout: %d",
          offset, buffer, length, readahead, timeout);
  return Read(offset, buffer, length, timeout);
}

int64_t KineticIo::WriteAsync (XrdSfsFileOffset offset, const char* buffer,
    XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d",
       offset, buffer, length, timeout);
  return Write(offset, buffer, length, timeout);
}

int KineticIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset: %lld, timeout %d:", offset, timeout);
  try{
    kio->Truncate(offset,timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int KineticIo::Fallocate (XrdSfsFileOffset length)
{
  eos_debug("length: %d",length);
  return 0;
}

int KineticIo::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  eos_debug("fromOffset: %lld, toOffset: %lld",fromOffset, toOffset);
  return 0;
}

int KineticIo::Remove (uint16_t timeout)
{
  eos_debug("timeout %d:", timeout);
  try{
    kio->Remove(timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int KineticIo::Sync (uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio->Sync(timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int KineticIo::Close (uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio->Close(timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int KineticIo::Stat (struct stat* buf, uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio->Stat(buf, timeout);
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

void* KineticIo::GetAsyncHandler ()
{
  eos_debug("");
  return NULL;
}

int KineticIo::Statfs (const char* path, struct statfs* statFs)
{
  eos_debug("path: %s", path);
  try{
    kio->Statfs(path,statFs);
    return SFS_OK;
  } KIO_CATCH
  return errno;
}

int KineticIo::Delete(const char* path)
{
  eos_debug("path: %s",path);
  try{
    std::string opath=path;
    kio->Open(opath,0);
    kio->Remove();
    kio->Close();
    return SFS_OK;
  } KIO_CATCH
  return SFS_ERROR;
}

int KineticIo::Exists(const char* path)
{
  eos_debug("path: %s",path);
  try{
    if(kio::Factory::makeFileAttr(path))
      return SFS_OK;
    errno = ENOENT;
  } KIO_CATCH
  return SFS_ERROR;
}

void* KineticIo::ftsOpen(std::string subtree)
{
  eos_debug("subtree: %s", subtree.c_str());
  try{
    return kio->ftsOpen(std::move(subtree));
  } KIO_CATCH
  return NULL;
}

std::string KineticIo::ftsRead(void* fts_handle)
{
  std::string rv = "";
  try{
    rv = kio->ftsRead(fts_handle);
  } KIO_CATCH
  eos_debug("fts_handle: %p, result: %s", fts_handle, rv.c_str());
  return rv;
}

int KineticIo::ftsClose(void* fts_handle)
{
  eos_debug("fts_handle: %p", fts_handle);
  try{
    return kio->ftsClose(fts_handle);
  } KIO_CATCH
  return SFS_ERROR;
}

bool KineticIo::Attr::Set (const char* name, const char* value, size_t len)
{
  eos_debug("name: %s, value: %s, len %ld", name, value, len);
  try{
    return kattr->Set(name, value, len);
  } KIO_CATCH
  return false;
}

bool KineticIo::Attr::Set (std::string key, std::string value)
{
  eos_debug("key: %s, value: %s", key.c_str(), value.c_str());
  try{
    return kattr->Set(key, value);
  } KIO_CATCH
  return false;
}

bool KineticIo::Attr::Get (const char* name, char* value, size_t &size)
{
  bool rv = false;
  try{
    rv = kattr->Get(name, value, size);
  } KIO_CATCH
  eos_debug("name: %s, value: %s, size: %ld", name, value, size);
  return rv;
}

std::string KineticIo::Attr::Get (std::string name)
{
  std::string rv = "";
  try{
    rv = kattr->Get(name);
  } KIO_CATCH
  eos_debug("name: %s, result: %s", name.c_str(), rv.c_str());
  return rv;
}

KineticIo::Attr* KineticIo::Attr::OpenAttr (const char* path)
{
  eos_static_debug("path: %s",path);
  try{
    auto ka(kio::Factory::makeFileAttr(path));
    if(ka) return new KineticIo::Attr(std::move(ka));
  }
  catch(const std::exception& e){eos_static_err(e.what());}
  return NULL;
}

KineticIo::Attr* KineticIo::Attr::OpenAttribute (const char* path)
{
  return KineticIo::Attr::OpenAttr(path);
}

KineticIo::Attr::Attr (std::unique_ptr<kio::FileAttrInterface> a):
    kattr(std::move(a))
{
  eos_debug("");
}

KineticIo::Attr::~Attr ()
{
  eos_debug("");
}


EOSFSTNAMESPACE_END
