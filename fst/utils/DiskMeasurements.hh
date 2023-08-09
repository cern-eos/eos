//------------------------------------------------------------------------------
// File: DiskMeasurements.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "fst/Namespace.hh"
#include <cstdint>
#include <string>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Generate random data
//!
//! @param data buffer to store data
//! @param length length of the data generated
//------------------------------------------------------------------------------
void GenerateRandomData(char* data, uint64_t length);

//------------------------------------------------------------------------------
//! Create file path with given size
//!
//! @param path file path that should not exit
//! @param lenght lenght of the generated file
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool FillFileGivenSize(const std::string& path, uint64_t length);

//------------------------------------------------------------------------------
//! Create random temporary file in given location
//!
//! @param base_path base path for new file
//!
//! @return string file path if successful, otherwise empty string
//------------------------------------------------------------------------------
std::string MakeTemporaryFile(std::string base_path);

//------------------------------------------------------------------------------
//! Get IOPS measurement for the given path
//!
//! @param fn_path given file path
//! @param rd_buf_size size of buffer used for read operations [default 4096]
//!
//! @return IOPS measurement
//------------------------------------------------------------------------------
int ComputeIops(const std::string& fn_path, uint64_t rd_buf_size = 4096);

//------------------------------------------------------------------------------
//! Get disk bandwidth for the given path
//!
//! @param path file system path
//! @param rd_buf_size size of buffer used for read operations [default 4096]
//!
//! @return disk bandwidth measurement
//------------------------------------------------------------------------------
int ComputeBandwidth(const std::string& fn_path, uint64_t rd_buf_size = 4096);

EOSFSTNAMESPACE_END
