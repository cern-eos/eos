#include "KineticIo.hh"
#include <kio/KineticIoFactory.hh>
#include <system_error>

EOSFSTNAMESPACE_BEGIN

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
    kio(kio::KineticIoFactory::makeFileIo(path))
{
  eos_debug("path: %s", mFilePath.c_str());
}

KineticIo::~KineticIo()
{
  eos_debug("path: %s", mFilePath.c_str());
}

int KineticIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_debug("path: %s, flags: %d, mode: %d, opaque: %s, timeout: %d",
            mFilePath.c_str(), flags, mode, opaque.c_str(), timeout);
  try {
    kio->Open(flags, mode, opaque, timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int64_t
KineticIo::fileRead(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
                    uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, buffer: %p, length: %d, timeout: %d",
            mFilePath.c_str(), offset, buffer, length, timeout);
  try {
    auto rv = kio->Read(offset, buffer, length, timeout);
    eos_debug("path: %s, result: %lld", mFilePath.c_str(), rv);
    return rv;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int64_t
KineticIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, buffer: %p, length: %d, timeout: %d",
            mFilePath.c_str(), offset, buffer, length, timeout);
  try {
    auto rv = kio->Write(offset, buffer, length, timeout);
    eos_debug("path: %s, result: %lld", mFilePath.c_str(), rv);
    return rv;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int64_t
KineticIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                         XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
  eos_debug("forwarding to sync read");
  return fileRead(offset, buffer, length, timeout);
}

int64_t
KineticIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                          XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("forwarding to sync write");
  return fileWrite(offset, buffer, length, timeout);
}

int
KineticIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, timeout %d:", mFilePath.c_str(), offset, timeout);
  try {
    kio->Truncate(offset, timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::fileFallocate(XrdSfsFileOffset length)
{
  eos_debug("path: %s, length: %d", length, mFilePath.c_str());
  return 0;
}

int
KineticIo::fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  eos_debug("path: %s, fromOffset: %lld, toOffset: %lld", mFilePath.c_str(), fromOffset, toOffset);
  return 0;
}

int
KineticIo::fileRemove(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);
  try {
    kio->Remove(timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::fileSync(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);
  try {
    kio->Sync(timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::fileClose(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);
  try {
    kio->Close(timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::fileStat(struct stat* buf, uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);
  try {
    kio->Stat(buf, timeout);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

void*
KineticIo::fileGetAsyncHandler()
{
  eos_debug("path: %s", mFilePath.c_str());
  return NULL;
}

int
KineticIo::Statfs(struct statfs* statFs)
{
  eos_debug("path: %s", mFilePath.c_str());
  try {
    kio->Statfs(statFs);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return errno;
}

int
KineticIo::fileExists()
{
  eos_debug("path: %s", mFilePath.c_str());
  try {
    kio->Open(0);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

FileIo::FtsHandle*
KineticIo::ftsOpen()
{
  eos_debug("");
  return new KineticIo::FtsHandle(mFilePath.c_str());
}

std::string
KineticIo::ftsRead(FileIo::FtsHandle* fts_handle)
{
  eos_debug("");

  auto handle = dynamic_cast<FtsHandle*>(fts_handle);
  if (handle->cached.size() > handle->current_index) {
    return handle->cached.at(handle->current_index++);
  }

  if (handle->cached.size() != 100 && handle->cached.back() != mFilePath) {
    return "";
  }

  try {
    handle->cached = kio->ListFiles(handle->cached.back() + " ", 100);
    handle->current_index = 0;
    return ftsRead(handle);
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return "";
}

int
KineticIo::ftsClose(FileIo::FtsHandle* fts_handle)
{
  eos_debug("");
  delete fts_handle;
  return 0;
}

int
KineticIo::attrGet(const char* name, char* value, size_t& size)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name);
  try {
    auto val = kio->attrGet(name);
    eos_debug("path: %s, value: %s", mFilePath.c_str(), val.c_str());
    size = std::min(size, val.length());
    strncpy(value, val.c_str(), size);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::attrGet(string name, std::string& value)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name.c_str());
  try {
    value = kio->attrGet(name);
    eos_debug("path: %s, value: %s", mFilePath.c_str(), value.c_str());
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::attrSet(const char* name, const char* value, size_t len)
{
  eos_debug("path: %s, name: %s, value: %s, len: %ld", mFilePath.c_str(), name, value, len);
  try {
    kio->attrSet(name, std::string(value, len));
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::attrSet(string name, std::string value)
{
  eos_debug("path: %s, name: %s, value: %s", mFilePath.c_str(), name.c_str(), value.c_str());
  try {
    kio->attrSet(name, value);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::attrDelete(const char* name)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name);
  try {
    kio->attrDelete(name);
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

int
KineticIo::attrList(std::vector<std::string>& list)
{
  eos_debug("path: %s", mFilePath.c_str());
  try {
    list = kio->attrList();
    return SFS_OK;
  }
  catch (const std::system_error& e) {
    errno = e.code().value();
  }
  return SFS_ERROR;
}

EOSFSTNAMESPACE_END