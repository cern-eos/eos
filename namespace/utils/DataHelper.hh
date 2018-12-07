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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Checksumming, data conversion and other stuff
//------------------------------------------------------------------------------

#ifndef EOS_NS_DATA_HELPER_HH
#define EOS_NS_DATA_HELPER_HH

#include <stdint.h>
#include <string>

namespace eos
{
class DataHelper
{
public:
  //----------------------------------------------------------------------------
  //! Compute crc32 checksum out of a buffer
  //----------------------------------------------------------------------------
  static uint32_t computeCRC32(void* buffer, uint32_t len);

  //----------------------------------------------------------------------------
  //! Update a crc32 checksum
  //----------------------------------------------------------------------------
  static uint32_t updateCRC32(uint32_t crc, void* buffer, uint32_t len);

  //----------------------------------------------------------------------------
  //! Compute crc32c checksum out of a buffer
  //----------------------------------------------------------------------------
  static uint32_t computeCRC32C(void* buffer, uint32_t len);

  //----------------------------------------------------------------------------
  //! Update a crc32c checksum
  //----------------------------------------------------------------------------
  static uint32_t updateCRC32C(uint32_t crc, void* buffer, uint32_t len);

  //----------------------------------------------------------------------------
  //! Finalize crc32c checksum
  //----------------------------------------------------------------------------
  static uint32_t finalizeCRC32C(uint32_t crc);

  //----------------------------------------------------------------------------
  //! Copy file ownership information
  //!
  //! @param target           target file
  //! @param source           source file
  //! @param ignoreWhenNoPerm exit seamlesly when the caller has
  //!                         insufficient permissions to carry out this
  //!                         operation
  //----------------------------------------------------------------------------
  static void copyOwnership(const std::string& target,
                            const std::string& source,
                            bool ignoreNoPerm = true);
};
}

#endif // EOS_NS_DATA_HELPER_HH
