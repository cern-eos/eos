//------------------------------------------------------------------------------
// File: Result.hh
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

#ifndef __EOSBMK_RESULT_HH__
#define __EOSBMK_RESULT_HH__

/*-----------------------------------------------------------------------------*/
#include "Namespace.hh"
#include "test/benchmark/ResultProto.pb.h"
/*-----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Result
//------------------------------------------------------------------------------
class Result
{

  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    Result();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~Result();


    //--------------------------------------------------------------------------
    //! Disable copy constructor
    //--------------------------------------------------------------------------
    Result(const Result&) = delete;


    //--------------------------------------------------------------------------
    //! Disable copy operator
    //--------------------------------------------------------------------------
    Result& operator =(const Result&) = delete;


    //--------------------------------------------------------------------------
    //! Print statistics
    //--------------------------------------------------------------------------
    void Print() const;


    //--------------------------------------------------------------------------
    //! Get transfer speed
    //!
    //! @param size transfer size in bytes
    //! @param duration time duration in miliseconds
    //!
    //! @return transfer speed in MB/s
    //!
    //--------------------------------------------------------------------------
    static float GetTransferSpeed(float size, float duration);


    //--------------------------------------------------------------------------
    //! Get low level result object (reference)
    //!
    //! @return low level result object (ProtoBuf object)
    //!
    //--------------------------------------------------------------------------
    ResultProto& GetPbResult() const;


    //--------------------------------------------------------------------------
    //! Set the low level result object
    //!
    //! @param pbResult low level result object (ProtoBuf object)
    //!
    //--------------------------------------------------------------------------
    void SetPbResult(ResultProto* pbResult);


    //--------------------------------------------------------------------------
    //! Merge result object into the current one
    //!
    //! @param partial partial result object to be merged
    //!
    //--------------------------------------------------------------------------
    void Merge(const Result& partial);

  private:

    ResultProto* mPbResult; ///< pointer to low level result object

    //--------------------------------------------------------------------------
    //! Function used to compute the average for the supplied argument
    //!
    //! @param input container of float values to be averaged
    //!
    //! @return average value
    //!
    //--------------------------------------------------------------------------
    static float Average(const ::google::protobuf::RepeatedField<float>& input);


    //--------------------------------------------------------------------------
    //! Function used to compute the standard deviation for the supplied argument
    //!
    //! @param input container of float values to be averaged
    //! @param mean mean value
    //!
    //! @return standard deviation value
    //!
    //--------------------------------------------------------------------------
    static float StdDev(const ::google::protobuf::RepeatedField<float>& input,
                        float                                           mean);


    //--------------------------------------------------------------------------
    //! Function used to compute the sum of the elements in a container
    //!
    //! @param input container of float values
    //!
    //! @return sum of the values
    //!
    //--------------------------------------------------------------------------
    static float Sum(const ::google::protobuf::RepeatedField<float>& input);


    //--------------------------------------------------------------------------
    //! Compute group statistics like average value and standard deviation
    //--------------------------------------------------------------------------
    void ComputeGroupStatistics();

};

EOSBMKNAMESPACE_END

#endif // __EOSBMK_RESULT_HH__
