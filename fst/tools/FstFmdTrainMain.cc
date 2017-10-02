//------------------------------------------------------------------------------
//! @file FmstFmdTrainMain.cc
//! @author Jozsef Makai<jmakai@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "fst/FmdDbMap.hh"
#include "namespace/MDException.hh"
#include "namespace/utils/Buffer.hh"
#include "common/compression/ZStandard.hh"

#include <zdict.h>
#include <chrono>

EOSFSTNAMESPACE_BEGIN

#define MAX_DICT_SIZE 412640 /* maximal dictionary size */

void
TrainFstFmdDictionary(const std::list<Fmd>& fmdList, std::string outputDictionaryFile)
{
  unsigned nbSamples = fmdList.size();
  Buffer buffer;
  Buffer samplesBuffer;
  size_t* samplesSizes = new size_t[nbSamples];

  auto i = 0ul;
  for (const auto& fmd : fmdList) {
    buffer.clear();
    auto fmdSerialized = fmd.SerializePartialAsString();
    buffer.putData(fmdSerialized.c_str(), fmdSerialized.size());
    samplesBuffer.putData(buffer.getDataPtr(), buffer.getSize());
    samplesSizes[i++] = buffer.getSize();
  }

  size_t dictBufferCapacity = MAX_DICT_SIZE;
  char* dictBuffer = new char[dictBufferCapacity];

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
  std::ofstream file(outputDictionaryFile);

  if (file.is_open()) {
    file.write(dictBuffer, dictSize);
  } else {
    MDException ex(errno);
    ex.getMessage() << "Can't create file for dictionary saving: " << outputDictionaryFile;
    throw ex;
  }

  delete[] dictBuffer;
}

EOSFSTNAMESPACE_END

using namespace eos;
using namespace eos::fst;

int main(int argc, char* argv[]) {
  if(argc < 3) {
    cerr << "Usage: eos-fst-fmd-train <db directory> <output dictionary file>" << endl;
    return -1;
  }

  XrdOucString dbfilename;
  gFmdDbMapHandler.CreateDBFileName(argv[1], dbfilename);

  auto fsid = FmdDbMapHandler::GetFsidInMetaDir(argv[1]);
  std::list<Fmd> trainList;
  gFmdDbMapHandler.SetDBFile(dbfilename.c_str(), fsid[0]);
  trainList.splice(trainList.end(), gFmdDbMapHandler.RetrieveAllFmd());
  gFmdDbMapHandler.SetDBFile(dbfilename.c_str(), fsid[1]);
  trainList.splice(trainList.end(), gFmdDbMapHandler.RetrieveAllFmd());

  cout << "training size: " << trainList.size() << endl;

  TrainFstFmdDictionary(trainList, argv[2]);

  return 0;
}