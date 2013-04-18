//------------------------------------------------------------------------------
// File: Result.cc
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


/*-----------------------------------------------------------------------------*/
#include <iostream>
#include <iomanip>
#include <cmath>
/*-----------------------------------------------------------------------------*/
#include "Result.hh"
#include "common/StringConversion.hh"
/*-----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

using namespace std;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Result::Result()
{
  mPbResult = new ResultProto();
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Result::~Result()
{
  if (mPbResult)  delete mPbResult;
}


//------------------------------------------------------------------------------
// Set the low level result object
//------------------------------------------------------------------------------
void
Result::SetPbResult(ResultProto* pbResult)
{
  if (mPbResult) delete mPbResult;

  mPbResult = pbResult;
}


//------------------------------------------------------------------------------
// Get low level result object (reference)
//------------------------------------------------------------------------------
ResultProto&
Result::GetPbResult() const
{
  return *mPbResult;
}


//------------------------------------------------------------------------------
// Get transfer speed in MB/s
//------------------------------------------------------------------------------
float
Result::GetTransferSpeed(float size, float duration)
{
  float speed = 0;
  size /= eos::common::MB;
  duration /= 1000.0;

  if (duration)
  {
    speed = size / duration;
  }

  return speed;
}


//------------------------------------------------------------------------------
// Print statistics
//------------------------------------------------------------------------------
void
Result::Print() const
{
  std::stringstream sstr;
  sstr << left << setw(190) << setfill('.') << "" << endl;
  std::string dot_line =  sstr.str();
  sstr.str("");
  sstr << left << setw(190) << setfill('-') << "" << endl;
  std::string minus_line = sstr.str();
  sstr.str("");
  sstr << left << setw(190) << setfill('*') << "" << endl;
  std::string star_line = sstr.str();
  sstr.str("");
  
  cout << star_line
       << setw(110) << right << "I n d i v i d u a l   s t a t i s t i c s" << endl
       << star_line
       << setw(20) << right << "Timestamp"
       << setw(14) << right << "Open time"
       << setw(14) << right << "Read time"
       << setw(16) << right << "Rd wait async"
       << setw(14) << right << "Read total"
       << setw(14) << right << "Read speed"
       << setw(14) << right << "Write time"
       << setw(16) << right << "Wr wait async"
       << setw(14) << right << "Write total"
       << setw(14) << right << "Write speed"
       << setw(14) << right << "Close time"
       << setw(18) << right << "Transaction time"
       << endl
       << minus_line;

  for (int32_t i = 0; i < mPbResult->opentime_size(); i++)
  {
    cout << setw(20) << right << mPbResult->timestamp(i)
         << setw(14) << right << mPbResult->opentime(i)
         << setw(14) << right << mPbResult->readtime(i)
         << setw(16) << right << mPbResult->readwaitasync(i)
         << setw(14) << right
         << eos::common::StringConversion::GetPrettySize(mPbResult->readtotal(i))
         << setw(14) << right << mPbResult->readspeed(i)
         << setw(14) << right << mPbResult->writetime(i)
         << setw(16) << right << mPbResult->writewaitasync(i)
         << setw(14) << right
         << eos::common::StringConversion::GetPrettySize(mPbResult->writetotal(i))
         << setw(14) << right << mPbResult->writespeed(i)
         << setw(14) << right << mPbResult->closetime(i)
         << setw(18) << right << mPbResult->transactiontime(i)
         << endl;
  }

  cout << minus_line << endl;
  cout << endl << star_line
       << setw(105) << "G r o u p   s t a t i s t i c s"  << endl
       << star_line
       << setw(10) << right << ""
       << setw(14) << right << "Open time"
       << setw(14) << right << "Read time"
       << setw(16) << right << "Rd wait async"
       << setw(14) << right << "Read speed"
       << setw(14) << right << "Write time"
       << setw(16) << right << "Wr wait async"
       << setw(14) << right << "Write speed"
       << setw(18) << right << "Transaction time"
       << setw(14) << right << "Close time" << endl
       << minus_line
       << setw(10) << right << "Average"
       << setw(14) << right << mPbResult->avgopentime()
       << setw(14) << right << mPbResult->avgreadtime()
       << setw(16) << right << mPbResult->avgreadwaitasync()
       << setw(14) << right << mPbResult->avgreadspeed()
       << setw(14) << right << mPbResult->avgwritetime()
       << setw(16) << right << mPbResult->avgwritewaitasync()
       << setw(14) << right << mPbResult->avgwritespeed()
       << setw(18) << right << mPbResult->avgtransactiontime()
       << setw(14) << right << mPbResult->avgclosetime() << endl
       << dot_line
       << setw(10) << right << "Std. dev."
       << setw(14) << right << mPbResult->stdopentime()
       << setw(14) << right << mPbResult->stdreadtime()
       << setw(16) << right << mPbResult->stdreadwaitasync()
       << setw(14) << right << mPbResult->stdreadspeed()
       << setw(14) << right << mPbResult->stdwritetime()
       << setw(16) << right << mPbResult->stdwritewaitasync()
       << setw(14) << right << mPbResult->stdwritespeed()
       << setw(18) << right << mPbResult->stdtransactiontime()
       << setw(14) << right << mPbResult->stdclosetime() << endl
       << dot_line << dot_line << endl
       << endl ;
}


//------------------------------------------------------------------------------
// Merge partial result object into the current one
//------------------------------------------------------------------------------
void
Result::Merge(const Result& partial)
{
  const ResultProto& pb_partial = partial.GetPbResult();

  for (int32_t i = 0; i < pb_partial.opentime_size(); i++)
  {
    mPbResult->add_timestamp(pb_partial.timestamp(i));
    mPbResult->add_opentime(pb_partial.opentime(i));
    mPbResult->add_readtime(pb_partial.readtime(i));
    mPbResult->add_readwaitasync(pb_partial.readwaitasync(i));
    mPbResult->add_writetime(pb_partial.writetime(i));
    mPbResult->add_writewaitasync(pb_partial.writewaitasync(i));
    mPbResult->add_closetime(pb_partial.closetime(i));
    mPbResult->add_transactiontime(pb_partial.transactiontime(i));
    mPbResult->add_readspeed(pb_partial.readspeed(i));
    mPbResult->add_writespeed(pb_partial.writespeed(i));
    mPbResult->add_readtotal(pb_partial.readtotal(i));
    mPbResult->add_writetotal(pb_partial.writetotal(i));
  }

  ComputeGroupStatistics();
}


//------------------------------------------------------------------------------
// Compute group statistics like average value and standard deviation
//------------------------------------------------------------------------------
void
Result::ComputeGroupStatistics()
{
  mPbResult->set_avgopentime(Average(mPbResult->opentime()));
  mPbResult->set_stdopentime(StdDev(mPbResult->opentime(),
                                    mPbResult->avgopentime()));

  mPbResult->set_avgreadtime(Average(mPbResult->readtime()));
  mPbResult->set_stdreadtime(StdDev(mPbResult->readtime(),
                                    mPbResult->avgreadtime()));

  mPbResult->set_avgreadwaitasync(Average(mPbResult->readwaitasync()));
  mPbResult->set_stdreadwaitasync(StdDev(mPbResult->readwaitasync(),
                                         mPbResult->avgreadwaitasync()));

  mPbResult->set_avgwritetime(Average(mPbResult->writetime()));
  mPbResult->set_stdwritetime(StdDev(mPbResult->writetime(),
                                     mPbResult->avgwritetime()));

  mPbResult->set_avgwritewaitasync(Average(mPbResult->writewaitasync()));
  mPbResult->set_stdwritewaitasync(StdDev(mPbResult->writewaitasync(),
                                          mPbResult->avgwritewaitasync()));

  mPbResult->set_avgclosetime(Average(mPbResult->closetime()));
  mPbResult->set_stdclosetime(StdDev(mPbResult->closetime(),
                                     mPbResult->avgclosetime()));

  mPbResult->set_avgtransactiontime(Average(mPbResult->transactiontime()));
  mPbResult->set_stdtransactiontime(StdDev(mPbResult->transactiontime(),
                                    mPbResult->avgtransactiontime()));

  mPbResult->set_avgreadspeed(Average(mPbResult->readspeed()));
  mPbResult->set_stdreadspeed(StdDev(mPbResult->readspeed(),
                                     mPbResult->avgreadspeed()));

  mPbResult->set_avgwritespeed(Average(mPbResult->writespeed()));
  mPbResult->set_stdwritespeed(StdDev(mPbResult->writespeed(),
                                      mPbResult->avgwritespeed()));
}


//------------------------------------------------------------------------------
// Function used to compute the average for the supplied argument
//------------------------------------------------------------------------------
float
Result::Average(const ::google::protobuf::RepeatedField< float >& input)
{
  float avg = 0;

  for (int64_t i = 0; i < input.size(); i++)
  {
    avg += input.Get(i);
  }

  avg /= input.size();
  return avg;
}


//------------------------------------------------------------------------------
// Function used to compute the standard deviation for the supplied argument
//------------------------------------------------------------------------------
float
Result::StdDev(const ::google::protobuf::RepeatedField<float>& input,
               float mean)
{
  float std_dev = 0;

  for (int64_t i = 0; i < input.size(); i++)
  {
    std_dev += pow((input.Get(i) - mean), 2);
  }

  std_dev /= input.size();
  std_dev = sqrt(std_dev);
  return std_dev;
}


//------------------------------------------------------------------------------
// Function used to compute the sum of the elements in the container
//------------------------------------------------------------------------------
float
Result::Sum(const ::google::protobuf::RepeatedField<float>& input)
{
  float sum = 0;

  for (int64_t i = 0; i < input.size(); i++)
  {
    sum += input.Get(i);
  }

  return sum;
}

EOSBMKNAMESPACE_END
