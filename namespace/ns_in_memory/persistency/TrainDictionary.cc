/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

//------------------------------------------------------------------------------
// author: Branko Blagojević <branko.blagojevic@comtrade.com>
// desc:   ZSTD dictionary training utility
//------------------------------------------------------------------------------

#include <fstream>
#include <iomanip>
#include "common/Murmur3.hh"
#include <google/sparse_hash_map>
#include <google/dense_hash_map>

#include "namespace/ns_in_memory/persistency/TrainDictionary.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogConstants.hh"

#define MAX_DICT_SIZE 110*(1<<10) /* maximal dictionary size */

namespace
{
//----------------------------------------------------------------------------
// Record scanner
//----------------------------------------------------------------------------
typedef google::dense_hash_map<uint64_t, uint64_t,
        Murmur3::MurmurHasher<uint64_t>,
        Murmur3::eqstr> RecordMap;

class TrainingScanner: public eos::ILogRecordScanner
{
public:
  TrainingScanner(RecordMap& map):
    pMap(map)
  {}

  virtual bool processRecord(uint64_t offset, char type,
                             const eos::Buffer& buffer)
  {
    if (buffer.size() < 8) {
      eos::MDException ex;
      ex.getMessage() << "Record at 0x" << std::setbase(16) << offset;
      ex.getMessage() << " is corrupted. Repair it first.";
      throw ex;
    }

    uint64_t id;
    buffer.grabData(0, &id, 8);
    pMap[id] = offset;
    return true;
  }

private:
  RecordMap&                   pMap;
};
}

namespace eos
{
void TrainDictionary::train(const std::string logfile,
                            const std::string dictionary)
{
  //--------------------------------------------------------------------------
  // Open the input file
  //--------------------------------------------------------------------------
  ChangeLogFile inputFile;
  inputFile.open(logfile,  ChangeLogFile::ReadOnly);

  if (inputFile.getContentFlag() != FILE_LOG_MAGIC &&
      inputFile.getContentFlag() != CONTAINER_LOG_MAGIC) {
    MDException ex;
    ex.getMessage() << "Cannot repack content: " << std::setbase(16);
    ex.getMessage() << inputFile.getContentFlag();
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Scan the input file
  //--------------------------------------------------------------------------
  RecordMap         map;
  TrainingScanner scanner(map);
  map.set_deleted_key(0);
  map.set_empty_key(std::numeric_limits<uint64_t>::max());
  map.resize(10000000);
  inputFile.scanAllRecords(&scanner);
  //--------------------------------------------------------------------------
  // Sort all the offsets to avoid random seeks
  //--------------------------------------------------------------------------
  std::vector<uint64_t> records;
  RecordMap::iterator it;

  for (it = map.begin(); it != map.end(); ++it) {
    records.push_back(it->second);
  }

  std::sort(records.begin(), records.end());
  map.clear();
  //--------------------------------------------------------------------------
  // Changelog reading
  //--------------------------------------------------------------------------
  unsigned nbSamples = records.size() / 2;
  Buffer buffer;
  Buffer samplesBuffer;
  size_t* samplesSizes = new size_t[nbSamples];

  if (samplesSizes == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Dictionary creation failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  for (unsigned i = 0; i < nbSamples; i++) {
    inputFile.readRecord(records[i], buffer);
    samplesBuffer.putData(buffer.getDataPtr(), buffer.getSize());
    samplesSizes[i] = buffer.getSize();
  }

  //--------------------------------------------------------------------------
  // Dictionary creation
  //--------------------------------------------------------------------------
  size_t dictBufferCapacity = MAX_DICT_SIZE;
  char* dictBuffer = new char[dictBufferCapacity];

  if (dictBuffer == nullptr) {
    MDException ex(errno);
    ex.getMessage() << "Dictionary creation failed: ";
    ex.getMessage() << "memory allocation failed";
    throw ex;
  }

  size_t dictSize = ZDICT_trainFromBuffer(dictBuffer, dictBufferCapacity,
                                          samplesBuffer.getDataPtr(),
                                          (const size_t*)samplesSizes,
                                          nbSamples);
  delete[] samplesSizes;

  if (ZDICT_isError(dictSize)) {
    MDException ex(errno);
    ex.getMessage() << "Dictionary creation failed: ";
    ex.getMessage() << ZDICT_getErrorName(dictSize);
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Dictionary saving
  //--------------------------------------------------------------------------
  std::ofstream file(dictionary);

  if (file.is_open()) {
    file.write(dictBuffer, dictSize);
  } else {
    MDException ex(errno);
    ex.getMessage() << "Can't create file for dictionary saving: " << dictionary;
    throw ex;
  }

  delete[] dictBuffer;
  inputFile.close();
}

}
