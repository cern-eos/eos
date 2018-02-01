//------------------------------------------------------------------------------
//! @file FmdAttributeScanMain.cc
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

#include "fst/FmdAttributeHandler.hh"

using namespace eos::fst;

int main(int argc, char* argv[]) {
  if(argc < 2) {
    cerr << "Usage: eos-fst-fmd-dbattr-convert <md dictionary path> <file1> <file2> ..." << endl;
    return -1;
  }

  eos::common::ZStandard fmdCompressor;
  fmdCompressor.SetDicts(argv[1]);
  FmdAttributeHandler fmdAttributeHandler{&fmdCompressor, &gMgmCommunicator};

  for(int i = 2; i < argc; i++){
    try {
      Fmd fmd = fmdAttributeHandler.FmdAttrGet(argv[i]);
      cout << argv[i] << ":" << endl << fmd.DebugString() << endl;
    } catch (eos::MDException& error) {
      cout << error.what() << endl << endl;
    }
  }

  return 0;
}