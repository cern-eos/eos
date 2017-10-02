//
// Created by root on 9/5/17.
//

#ifndef EOS_COMPRESSION_HH
#define EOS_COMPRESSION_HH

#include "common/Namespace.hh"
#include "namespace/utils/Buffer.hh"

EOSCOMMONNAMESPACE_BEGIN

//! @brief Base class for compressing and decompressing binary and text data
class Compression {
public:
  //! @brief Compress binary data represented by the Buffer class
  //! @param record uncompressed data
  virtual void Compress(Buffer& record) = 0;

  //! @brief Decompress binary data represented by the Buffer class
  //! @param record compressed data
  virtual void Decompress(Buffer& record) = 0;

  //! @brief Compress text data
  //! @param input uncompressed text data as string
  //! @return compressed string
  virtual std::string Compress(const std::string& input);

  //! @brief Decompress text data
  //! @param input compressed text data as string
  //! @return decompressed string
  virtual std::string Decompress(const std::string& input);

};

EOSCOMMONNAMESPACE_END

#endif //EOS_COMPRESSION_HH
