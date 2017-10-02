//
// Created by root on 9/5/17.
//

#include "Compression.hh"

EOSCOMMONNAMESPACE_BEGIN

std::string
eos::common::Compression::Compress(const std::string& input)
{
  Buffer buffer;
  buffer.putData(input.c_str(), input.size());
  Compress(buffer);
  return std::string(buffer.getDataPtr(), buffer.getSize());
}

std::string
eos::common::Compression::Decompress(const std::string& input)
{
  Buffer buffer;
  buffer.putData(input.c_str(), input.size());
  Decompress(buffer);
  return std::string(buffer.getDataPtr(), buffer.getSize());
}

EOSCOMMONNAMESPACE_END