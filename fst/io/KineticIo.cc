#include "KineticIo.hh"

EOSFSTNAMESPACE_BEGIN

KineticIo::KineticIo() : kio(10)
{
  eos_debug("");
}

KineticIo::~KineticIo()
{
  eos_debug("");
}

void KineticIo::log(const KineticException& e)
{
  eos::common::Logging::log(e.function(),e.file(),e.line(),logId,vid,cident,
          (LOG_ERR),e.what());
  errno=e.errnum();
}

int KineticIo::Open(const std::string& path, XrdSfsFileOpenMode flags,
        mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_debug("path: %s, flags: %d, mode: %d, opaque: %s, timeout: %d",
          path.c_str(), flags, mode, opaque.c_str(), timeout);
  try{
    kio.Open(path, flags, mode, opaque, timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int64_t KineticIo::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
      uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %s, length: %d, timeout: %d",
          offset, buffer, length, timeout);
  try{
    return kio.Read(offset, buffer, length, timeout);
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int64_t KineticIo::Write (XrdSfsFileOffset offset, const char* buffer,
    XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %s, length: %d, timeout: %d",
        offset, buffer, length, timeout);
  try{
    return kio.Write(offset, buffer, length, timeout);
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int64_t KineticIo::ReadAsync (XrdSfsFileOffset offset, char* buffer,
    XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
   eos_debug("offset: %lld, buffer: %s, length: %d, readahead: %d, timeout: %d",
          offset, buffer, length, readahead, timeout);
  return Read(offset, buffer, length, timeout);
}

int64_t KineticIo::WriteAsync (XrdSfsFileOffset offset, const char* buffer,
    XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %s, length: %d, timeout: %d",
       offset, buffer, length, timeout);
  return Write(offset, buffer, length, timeout);
}

int KineticIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset: %lld, timeout %d:", offset, timeout);
  try{
    kio.Truncate(offset,timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
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
    kio.Remove(timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int KineticIo::Sync (uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio.Sync(timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int KineticIo::Close (uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio.Close(timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

int KineticIo::Stat (struct stat* buf, uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try{
    kio.Stat(buf, timeout);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
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
    kio.Statfs(path,statFs);
    return SFS_OK;
  }catch(const KineticException& ke){log(ke);}
  return errno;
}

void* KineticIo::ftsOpen(std::string subtree)
{
  eos_debug("subtree: %s", subtree.c_str());
  try{
    return kio.ftsOpen(std::move(subtree));
  }catch(const KineticException& ke){log(ke);}
  return NULL;
}

std::string KineticIo::ftsRead(void* fts_handle)
{
  eos_debug("fts_handle: %p", fts_handle);
  try{
    return kio.ftsRead(fts_handle);
  }catch(const KineticException& ke){log(ke);}
  return "";
}

int KineticIo::ftsClose(void* fts_handle)
{
  eos_debug("fts_handle: %p", fts_handle);
  try{
    return kio.ftsClose(fts_handle);
  }catch(const KineticException& ke){log(ke);}
  return SFS_ERROR;
}

bool KineticIo::Attr::Set (const char* name, const char* value, size_t len)
{
  eos_debug("name: %s, value: %s, len %ld", name, value, len);
  try{
    return kattr->Set(name, value, len);
  }catch(const KineticException& ke){
    eos_err(ke.what());
  }
  return false;
}

bool KineticIo::Attr::Set (std::string key, std::string value)
{
  eos_debug("key: %s, value: %s", key.c_str(), value.c_str());
  try{
    return kattr->Set(key, value);
  }catch(const KineticException& ke){
    eos_err(ke.what());
  }
  return false;
}

bool KineticIo::Attr::Get (const char* name, char* value, size_t &size)
{
  eos_debug("name: %s, value: %s, size: %ld", name, value, size);
  try{
    return kattr->Get(name, value, size);
  }catch(const KineticException& ke){
    eos_err(ke.what());
  }
  return false;
}

std::string KineticIo::Attr::Get (std::string name)
{
  eos_debug("name: %s", name.c_str());
  try{
    return kattr->Get(name); 
  }catch(const KineticException& ke){
    eos_err(ke.what());
  }
  return "";
}

KineticIo::Attr* KineticIo::Attr::OpenAttr (const char* path)
{
  eos_static_debug("path: %s",path); 
  try{
    std::unique_ptr<KineticFileAttr> ka( KineticFileAttr::OpenAttr(path) );
    if(ka) return new KineticIo::Attr(std::move(ka));
  }catch(const KineticException& ke){
    eos_static_err(ke.what());
  }
  return NULL;
}

KineticIo::Attr* KineticIo::Attr::OpenAttribute (const char* path)
{
  eos_debug("path %s",path);
  return KineticIo::Attr::OpenAttr(path);
}

KineticIo::Attr::Attr (std::unique_ptr<KineticFileAttr> a):
    kattr(std::move(a))
{
  eos_debug("");
}

KineticIo::Attr::~Attr ()
{
  eos_debug("");
}


EOSFSTNAMESPACE_END
