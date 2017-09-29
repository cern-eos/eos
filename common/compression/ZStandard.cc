
//
// Created by root on 8/31/17.
//

#include "common/Namespace.hh"
#include "common/compression/ZStandard.hh"

EOSCOMMONNAMESPACE_BEGIN

void
eos::common::ZStandard::loadDict(const std::string& dictionaryPath) {
  struct stat statbuf;

  if (stat(dictionaryPath.c_str(), &statbuf) != 0) {
    MDException e(errno);
    e.getMessage() << "Unable to stat source: " << dictionaryPath;
    throw e;
  }

  pDictSize = (size_t)statbuf.st_size;
  pDictBuffer = new char[pDictSize];

  if (pDictBuffer == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Dictionary read failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  //-------------------------------------------------------------------------
  // Dictionary load
  //-------------------------------------------------------------------------
  std::ifstream file(dictionaryPath);

  if (file.is_open()) {
    file.read(pDictBuffer, pDictSize);
  } else {
    MDException ex(EFAULT);
    ex.getMessage() << "Can't open ZSTD dictionary file: " << dictionaryPath;
    throw ex;
  }
}

void
ZStandard::createCDict() {
  pCDict = ZSTD_createCDict(pDictBuffer, pDictSize, 19);

  if (pCDict == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Creation of compression dictionary failed";
    throw ex;
  }

  pCCtx = ZSTD_createCCtx();

  if (pCCtx == nullptr) {
    MDException ex(EFAULT);
    ex.getMessage() << "ZSTD_createCCtx() error";
    throw ex;
  }
}

void
ZStandard::createDDict() {
  pDDict = ZSTD_createDDict(pDictBuffer, pDictSize);

  if (pDDict == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Creation of decompression dictionary failed";
    throw ex;
  }

  pDCtx = ZSTD_createDCtx();

  if (pDCtx == nullptr) {
    MDException ex(EFAULT);
    ex.getMessage() << "ZSTD_createDCtx() error";
    throw ex;
  }
}

void
ZStandard::compress(Buffer& record) {
  size_t const cBuffSize = ZSTD_compressBound(record.size());
  void* const cBuff = malloc(cBuffSize);

  if (cBuff == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Compression failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  size_t cSize;
  {
    RWMutexWriteLock lock(mCompressLock);
    cSize = ZSTD_compress_usingCDict(pCCtx, cBuff, cBuffSize,
                                     record.getDataPtr(),
                                     record.size(),
                                     pCDict);
  }

  if (ZSTD_isError(cSize)) {
    free(cBuff);
    MDException ex(errno);
    ex.getMessage() << "Compression failed: ";
    ex.getMessage() << ZSTD_getErrorName(cSize);
    throw ex;
  }

  record.clear();
  record.putData(cBuff, cSize);
  free(cBuff);
}

void
ZStandard::decompress(Buffer& record) {
  size_t const dBuffSize = ZSTD_DStreamOutSize();
  void* const dBuff = malloc(dBuffSize);

  if (dBuff == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  if (pDDict == nullptr) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << "dictionary was not set";
    throw ex;
  }

  size_t dSize;
  {
    RWMutexWriteLock lock(mCompressLock);
    dSize = ZSTD_decompress_usingDDict(pDCtx, dBuff, dBuffSize,
                                       record.getDataPtr(),
                                       record.getSize(), pDDict);
  }

  if (ZSTD_isError(dSize)) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << ZSTD_getErrorName(dSize);
    throw ex;
  }

  record.clear();
  record.putData(dBuff, dSize);
  free(dBuff);
}

uint32_t
ZStandard::decompression(Buffer& record, uint32_t& crcHead) {
  size_t const dBuffSize = ZSTD_DStreamOutSize();
  void* const dBuff = malloc(dBuffSize);

  if (dBuff == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  if (pDDict == nullptr) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << "dictionary was not set";
    throw ex;
  }

  size_t const dSize = ZSTD_decompress_usingDDict(pDCtx, dBuff, dBuffSize,
                                                  record.getDataPtr(),
                                                  record.getSize(), pDDict);

  if (ZSTD_isError(dSize)) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: ";
    ex.getMessage() << ZSTD_getErrorName(dSize);
    throw ex;
  }

  uint32_t crc = DataHelper::updateCRC32(crcHead, dBuff, dSize);
  free(dBuff);
  return crc;
}

ZStandard::~ZStandard() {
  delete[] pDictBuffer;
  ZSTD_freeDDict(pDDict);
  ZSTD_freeCCtx(pCCtx);
  ZSTD_freeDCtx(pDCtx);
}

void
ZStandard::setDicts(const std::string& dictionaryPath) {
  loadDict(dictionaryPath);
  createCDict();
  createDDict();
}

void
ZStandard::setCDict(const std::string& dictionaryPath) {
  loadDict(dictionaryPath);
  createCDict();
}

void
ZStandard::setDDict(const std::string& dictionaryPath) {
  loadDict(dictionaryPath);
  createDDict();
}

void
ZStandard::setCompressionLevel(unsigned compressionLevel) {
  pCompressionLevel = compressionLevel;
}

uint32_t
ZStandard::updateCRC32(Buffer& record, uint32_t& crcHead) {
  return decompression(record, crcHead);
}

EOSCOMMONNAMESPACE_END