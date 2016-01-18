#include "KineticIo.hh"
#include <kio/KineticIoFactory.hh>
#include <kio/LoggingException.hh>

EOSFSTNAMESPACE_BEGIN

/* Yes. Macros are evil. So is code duplication. */
#define KIO_CATCH catch(const kio::LoggingException& ke){ \
                    logmsg(ke.function(), ke.file(), ke.line(), (LOG_ERR), ke.what()); errno=ke.errnum(); \
                  }catch(const std::exception& e){ \
                    eos_err(e.what()); errno=ENOEXEC; \
                  }

static void
logmsg(const char* func, const char* file, int line, int priority, const char* msg)
{
  eos::common::Logging::log(
      func, file, line, "LIBKINETICIO", eos::common::Logging::gZeroVid, "", priority, msg
  );
}

class LogFunctionInitializer {
public:

  LogFunctionInitializer()
  {
    kio::KineticIoFactory::registerLogFunction(logmsg, eos::common::Logging::shouldlog);
  }
};

static LogFunctionInitializer kio_loginit;

KineticIo::KineticIo(std::string path) :
    FileIo(path, "kinetic"),
    kio(kio::KineticIoFactory::makeFileIo()),
    kattr(kio::KineticIoFactory::makeFileAttr(path.c_str())),
    opened(false)
{
  eos_debug("");
}

KineticIo::~KineticIo()
{
  eos_debug("");
}

int KineticIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_debug("path: %s, flags: %d, mode: %d, opaque: %s, timeout: %d",
            mFilePath.c_str(), flags, mode, opaque.c_str(), timeout);

  if(opened){
    if (flags & SFS_O_CREAT){
      errno = EEXIST;
      return SFS_ERROR;
    }
    return SFS_OK;
  }

  try {
    kio->Open(mFilePath.c_str(), flags, mode, opaque, timeout);
    opened = true;
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int64_t
KineticIo::fileRead(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
                    uint16_t timeout)
{
  int64_t rv = SFS_ERROR;
  try {
    rv = kio->Read(offset, buffer, length, timeout);
  }
  KIO_CATCH
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d, result: %lld",
            offset, buffer, length, timeout, rv);
  return rv;
}

int64_t
KineticIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length, uint16_t timeout)
{
  int64_t rv = SFS_ERROR;
  try {
    rv = kio->Write(offset, buffer, length, timeout);
  }
  KIO_CATCH
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d, result: %lld",
            offset, buffer, length, timeout, rv);
  return rv;
}

int64_t
KineticIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                         XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %p, length: %d, readahead: %d, timeout: %d",
            offset, buffer, length, readahead, timeout);
  return fileRead(offset, buffer, length, timeout);
}

int64_t
KineticIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                          XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset: %lld, buffer: %p, length: %d, timeout: %d",
            offset, buffer, length, timeout);
  return fileWrite(offset, buffer, length, timeout);
}

int
KineticIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset: %lld, timeout %d:", offset, timeout);
  try {
    kio->Truncate(offset, timeout);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::fileFallocate(XrdSfsFileOffset length)
{
  eos_debug("length: %d", length);
  return 0;
}

int
KineticIo::fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  eos_debug("fromOffset: %lld, toOffset: %lld", fromOffset, toOffset);
  return 0;
}

int
KineticIo::fileRemove(uint16_t timeout)
{
  eos_debug("timeout %d:", timeout);
  try {
    kio->Remove(timeout);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::fileSync(uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try {
    kio->Sync(timeout);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::fileClose(uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try {
    kio->Close(timeout);
    opened = false;
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::fileStat(struct stat* buf, uint16_t timeout)
{
  eos_debug("timeout %d", timeout);
  try {
    if (!opened) {
      kio->Open(mFilePath, 0);
      opened = true;
    }
    kio->Stat(buf, timeout);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

void*
KineticIo::fileGetAsyncHandler()
{
  eos_debug("");
  return NULL;
}

int
KineticIo::Statfs(struct statfs* statFs)
{
  eos_debug("");
  try {
    kio->Statfs(mFilePath.c_str(), statFs);
    return SFS_OK;
  }
  KIO_CATCH
  return errno;
}

int
KineticIo::fileExists()
{
  eos_debug("");
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

FileIo::FtsHandle*
KineticIo::ftsOpen()
{
  KineticIo::FtsHandle* handle = new KineticIo::FtsHandle(mFilePath.c_str());

  if (!handle) {
    return NULL;
  }

  try {
    handle->mHandle = kio->ftsOpen(mFilePath);
    return dynamic_cast<FileIo::FtsHandle*> (handle);
  }
  KIO_CATCH
  if (handle) {
    delete handle;
  }
  return NULL;
}

std::string
KineticIo::ftsRead(FileIo::FtsHandle* fts_handle)
{
  std::string rv = "";
  try {
    rv = kio->ftsRead(dynamic_cast<FtsHandle*> (fts_handle)->mHandle);
  }
  KIO_CATCH
  eos_debug("fts_handle: %p, result: %s", dynamic_cast<FtsHandle*> (fts_handle)->mHandle, rv.c_str());
  return rv;
}

int
KineticIo::ftsClose(FileIo::FtsHandle* fts_handle)
{
  eos_debug("fts_handle: %p", fts_handle ? dynamic_cast<FtsHandle*> (fts_handle)->mHandle : 0);
  try {
    return kio->ftsClose(dynamic_cast<FtsHandle*> (fts_handle)->mHandle);
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrGet(const char* name, char* value, size_t& size)
{
  eos_debug("name: %s", name);
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    kattr->Get(name, value, size);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrGet(string name, std::string& value)
{
  eos_debug("name: %s", name.c_str());
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    value = kattr->Get(name);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrSet(const char* name, const char* value, size_t len)
{
  eos_debug("name: %s, value: %s, len: %ld", name, value, len);
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    kattr->Set(name, value, len);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrSet(string name, std::string value)
{
  eos_debug("name: %s, value: %s", name.c_str(), value.c_str());
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    kattr->Set(name, value);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrDelete(const char* name)
{
  eos_debug("name: %s", name);
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    //    kattr->Delete(name);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

int
KineticIo::attrList(std::vector<std::string>& list)
{
  eos_debug("");
  try {
    if(!opened){
      kio->Open(mFilePath.c_str(), 0);
      opened = true;
    }
    //    kattr->List(list);
    return SFS_OK;
  }
  KIO_CATCH
  return SFS_ERROR;
}

EOSFSTNAMESPACE_END
