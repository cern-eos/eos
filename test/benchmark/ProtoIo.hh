//------------------------------------------------------------------------------
// File: ProtoIo.hh
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#ifndef __EOSBMK_PROTOIO_HH__
#define __EOSBMK_PROTOIO_HH__

/*-----------------------------------------------------------------------------*/
#include <cstring>
#include <fstream>
#include <iostream>
/*-----------------------------------------------------------------------------*/
#include "Namespace.hh"
#include <google/protobuf/message.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
/*-----------------------------------------------------------------------------*/

using namespace google::protobuf::io;

EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ProtoIo used to write or read Protocol Buffer objects from files. It
//! allows to handle multiple ProtoBuf objects in the same file by first writing
//! the size of the object and then the actual object information. Therefore, in
//! the reading process we first read the size of the object and then the object
//! information.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
//! Class ProtoWriter
//------------------------------------------------------------------------------
class ProtoWriter
{

  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file file where the ProtocolBuffer objects are written
    //!
    //--------------------------------------------------------------------------
    ProtoWriter(const std::string& file);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~ProtoWriter();


    //--------------------------------------------------------------------------
    //! Write the object to the file along with its size
    //!
    //! @param msg object to be written to the file
    //!
    //! @return true if successful, otherwise false
    //!
    //--------------------------------------------------------------------------
    inline bool operator()(const ::google::protobuf::Message& msg)
    {
      _CodedOutputStream->WriteVarint32(msg.ByteSize());

      if (!msg.SerializeToCodedStream(_CodedOutputStream))
      {
        std::cerr << "SerializeToCodedStream error " << std::endl;
        return false;
      }

      return true;
    }

  private:

    std::ofstream mFs;                          ///< output file stream
    OstreamOutputStream* _OstreamOutputStream;  ///<
    CodedOutputStream* _CodedOutputStream;      ///<
};


//------------------------------------------------------------------------------
//! Class ProtoReader
//------------------------------------------------------------------------------
class ProtoReader
{

  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file file from which ProtocolBuffer objects are read
    //!
    //--------------------------------------------------------------------------
    ProtoReader(const std::string& file);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~ProtoReader();


    //--------------------------------------------------------------------------
    //! Read next object from file
    //!
    //! @return the ProtocolBuffer object read from the file
    //!
    //--------------------------------------------------------------------------
    template<class T>
    T* ReadNext()
    {
      T* msg = new T();
      uint32_t size;
      bool ret;

      if ((ret = _CodedInputStream->ReadVarint32(&size)))
      {
        CodedInputStream::Limit msgLimit = _CodedInputStream->PushLimit(size);

        if ((ret = msg->ParseFromCodedStream(_CodedInputStream)))
        {
          _CodedInputStream->PopLimit(msgLimit);
        }
        else
        {
          delete msg;
          msg = 0;
        }
      }
      else
      {
        delete msg;
        msg = 0;
      }

      return msg;
    }

  private:

    std::ifstream mFs;                         ///< input file stream
    IstreamInputStream* _IstreamInputStream;   ///<
    CodedInputStream* _CodedInputStream;       ///<
};

EOSBMKNAMESPACE_END

#endif // __EOSBMK_PROTOIO_HH__
