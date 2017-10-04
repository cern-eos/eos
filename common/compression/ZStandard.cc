
//
// Created by root on 8/31/17.
//

#include <thread>
#include "common/Namespace.hh"
#include "common/compression/ZStandard.hh"

EOSCOMMONNAMESPACE_BEGIN

thread_local std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> eos::common::ZStandard::mCCtx {ZSTD_createCCtx(), &ZSTD_freeCCtx};
thread_local std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> eos::common::ZStandard::mDCtx {ZSTD_createDCtx(), &ZSTD_freeDCtx};

void
eos::common::ZStandard::LoadDict(const std::string& dictionaryPath) {
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
    ex.getMessage() << "Dictionary read failed: memory allocation failed";
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
ZStandard::CreateCDict() {
  pCDict = ZSTD_createCDict(pDictBuffer, pDictSize, 19);

  if (pCDict == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Creation of compression dictionary failed";
    throw ex;
  }
}

void
ZStandard::CreateDDict() {
  pDDict = ZSTD_createDDict(pDictBuffer, pDictSize);

  if (pDDict == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Creation of decompression dictionary failed";
    throw ex;
  }
}

void
ZStandard::Compress(Buffer& record) {
  size_t const cBuffSize = ZSTD_compressBound(record.size());
  void* const cBuff = malloc(cBuffSize);

  if (cBuff == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Compression failed: memory allocation failed";
    throw ex;
  }

  size_t const cSize = ZSTD_compress_usingCDict(mCCtx.get(), cBuff, cBuffSize,
                                   record.getDataPtr(),
                                   record.size(),
                                   pCDict);

  if (ZSTD_isError(cSize)) {
    free(cBuff);
    MDException ex(errno);
    ex.getMessage() << "Compression failed: " << ZSTD_getErrorName(cSize);
    throw ex;
  }

  record.clear();
  record.putData(cBuff, cSize);
  free(cBuff);
}

void
ZStandard::Decompress(Buffer& record) {
  size_t const dBuffSize = ZSTD_DStreamOutSize();
  void* const dBuff = malloc(dBuffSize);

  if (dBuff == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: memory allocation failed";
    throw ex;
  }

  if (pDDict == nullptr) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: dictionary was not set";
    throw ex;
  }

  size_t const dSize = ZSTD_decompress_usingDDict(mDCtx.get(), dBuff, dBuffSize,
                                           record.getDataPtr(),
                                           record.getSize(), pDDict);

  if (ZSTD_isError(dSize)) {
    free(dBuff);
    MDException ex(errno);
    ex.getMessage() << "Decompression failed: " << ZSTD_getErrorName(dSize);
    throw ex;
  }

  record.clear();
  record.putData(dBuff, dSize);
  free(dBuff);
}

ZStandard::~ZStandard() {
  delete[] pDictBuffer;
  ZSTD_freeCDict(pCDict);
  ZSTD_freeDDict(pDDict);
}

void
ZStandard::SetDicts(const std::string& dictionaryPath) {
  LoadDict(dictionaryPath);
  CreateCDict();
  CreateDDict();
}

void
ZStandard::SetCDict(const std::string& dictionaryPath) {
  LoadDict(dictionaryPath);
  CreateCDict();
}

void
ZStandard::SetDDict(const std::string& dictionaryPath) {
  LoadDict(dictionaryPath);
  CreateDDict();
}

EOSCOMMONNAMESPACE_END