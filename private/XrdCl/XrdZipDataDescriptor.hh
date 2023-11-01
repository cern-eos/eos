//------------------------------------------------------------------------------
// Copyright (c) 2011-2014 by European Organization for Nuclear Research (CERN)
// Author: Michal Simon <michal.simon@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#ifndef SRC_XRDZIP_XRDZIPDATADESCRIPTOR_HH_
#define SRC_XRDZIP_XRDZIPDATADESCRIPTOR_HH_

namespace XrdZip
{
  //---------------------------------------------------------------------------
  // A data structure representing the Data Descriptor record
  //---------------------------------------------------------------------------
  struct DataDescriptor
  {
    static uint8_t GetSize( bool zip64 )
    {
      if( zip64 ) return sizeof( sign ) + 3 * sizeof( uint64_t );
      return sizeof( sign ) + 3 * sizeof( uint32_t );
    }

    static const uint16_t flag = 1 << 3; //< bit 3 is set
    static const uint32_t sign = 0x08074b50;
  };
}

#endif /* SRC_XRDZIP_XRDZIPDATADESCRIPTOR_HH_ */
