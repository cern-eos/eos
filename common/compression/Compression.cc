//
// Created by root on 9/5/17.
//

#include "Compression.hh"

EOSCOMMONNAMESPACE_BEGIN

std::string
eos::common::Compression::compress(const std::string& input)
{
  Buffer buffer;
  buffer.putData(input.c_str(), input.size());
  compress(buffer);
  return std::string(buffer.getDataPtr(), buffer.getSize());
}

std::string
eos::common::Compression::decompress(const std::string& input)
{
  Buffer buffer;
  buffer.putData(input.c_str(), input.size());
  decompress(buffer);
  return std::string(buffer.getDataPtr(), buffer.getSize());
}

EOSCOMMONNAMESPACE_END