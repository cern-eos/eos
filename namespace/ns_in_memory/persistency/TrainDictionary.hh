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

#ifndef EOS_NS_TRAIN_DICTIONARY_HH
#define EOS_NS_TRAIN_DICTIONARY_HH

#include <zdict.h>
#include <iostream>
#include <string>

#include "namespace/MDException.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFile.hh"

namespace eos
{

//------------------------------------------------------------------------
//! Train ZSTD dictionary for changelog (de)compression
//------------------------------------------------------------------------
class TrainDictionary
{
public:
  TrainDictionary() {};

  virtual ~TrainDictionary() {};

  //------------------------------------------------------------------------
  //! Train dictionary
  //!
  //! @param logfile     path to the changelog sample file (read only)
  //!                    which will be used for dictionary training
  //! @param dictionary  placeholder for the created ZSTD dictionary
  //------------------------------------------------------------------------
  static void train(const std::string logfile, const std::string dictionary);
};
}

#endif // EOS_NS_TRAIN_DICTIONARY_HH
