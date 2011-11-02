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
// desc:   Manager for change log files
//------------------------------------------------------------------------------

#include "namespace/persistency/LogManager.hh"
#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/persistency/ChangeLogConstants.hh"
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <iomanip>

namespace
{
  //----------------------------------------------------------------------------
  // Record scanner
  //----------------------------------------------------------------------------
  typedef google::dense_hash_map<uint64_t, uint64_t> RecordMap;
  class CompactingScanner: public eos::ILogRecordScanner
  {
  public:
    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    CompactingScanner( RecordMap                   &map,
                       eos::ILogCompactingFeedback *feedback,
                       eos::LogCompactingStats     &stats,
                       time_t                      time ):
      pMap( map ), pFeedback( feedback ), pStats( stats ), pTime( time ) {}

    //------------------------------------------------------------------------
    // Got through the records
    //------------------------------------------------------------------------
    virtual void processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      //----------------------------------------------------------------------
      // Check the buffer
      //----------------------------------------------------------------------
      if( buffer.size() < 8 )
        {
          eos::MDException ex;
          ex.getMessage() << "Record at 0x" << std::setbase(16) << offset;
          ex.getMessage() << " is corrupted. Repair it first.";
          throw ex;
        }

      uint64_t id;
      buffer.grabData( 0, &id, 8 );
      ++pStats.recordsTotal;

      //----------------------------------------------------------------------
      // Update
      //----------------------------------------------------------------------
      if( type == eos::UPDATE_RECORD_MAGIC )
        {
          pMap[id] = offset;
          ++pStats.recordsUpdated;
        }

      //----------------------------------------------------------------------
      // Deleteion
      //----------------------------------------------------------------------
      if( type == eos::DELETE_RECORD_MAGIC )
        {
          pMap.erase( id );
          ++pStats.recordsDeleted;
        }

      //----------------------------------------------------------------------
      // Report progress
      //----------------------------------------------------------------------
      pStats.timeElapsed = time(0) - pTime;
      if( pFeedback )
        pFeedback->reportProgress( pStats,
                                   eos::ILogCompactingFeedback::InitialScan );
    }

  private:
    RecordMap                   &pMap;
    eos::ILogCompactingFeedback *pFeedback;
    eos::LogCompactingStats     &pStats;
    time_t                       pTime;
  };
}

namespace eos
{
  //----------------------------------------------------------------------------
  // Compact the old log and write a new one, this works only for logs
  //----------------------------------------------------------------------------
  void LogManager::compactLog( const std::string      &oldLogName,
                               const std::string      &newLogName,
                               LogCompactingStats     &stats,
                               ILogCompactingFeedback *feedback )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Open the files
    //--------------------------------------------------------------------------
    ChangeLogFile inputFile;
    ChangeLogFile outputFile;
    inputFile.open( oldLogName,  ChangeLogFile::ReadOnly );
    outputFile.open( newLogName, ChangeLogFile::Create, inputFile.getContentFlag() );

    if( inputFile.getContentFlag() != FILE_LOG_MAGIC &&
        inputFile.getContentFlag() != CONTAINER_LOG_MAGIC )
      {
        MDException ex;
        ex.getMessage() << "Cannot repack content: " << std::setbase( 16 );
        ex.getMessage() << inputFile.getContentFlag();
        throw ex;
      }

    //--------------------------------------------------------------------------
    // Scan the file
    //--------------------------------------------------------------------------
    RecordMap         map;
    time_t            startTime = time( 0 );
    CompactingScanner scanner( map, feedback, stats, startTime );
    map.set_deleted_key( 0 );
    map.set_empty_key( 0xffffffffffffffff );
    map.resize(10000000);
    inputFile.scanAllRecords( &scanner );
    stats.recordsKept = map.size();

    if( feedback )
      feedback->reportProgress( stats, ILogCompactingFeedback::CopyPreparation );

    //--------------------------------------------------------------------------
    // Copy the records
    //--------------------------------------------------------------------------
    RecordMap::iterator it;
    Buffer              buffer;
    for( it = map.begin(); it != map.end(); ++it )
      {
        uint8_t type = inputFile.readRecord( it->second, buffer );
        outputFile.storeRecord( type, buffer );
        ++stats.recordsWritten;
        stats.timeElapsed = time(0) - startTime;
        if( feedback )
          feedback->reportProgress( stats,
                                    ILogCompactingFeedback::RecordCopying );
      }

    //--------------------------------------------------------------------------
    // Write a compacting stamp
    //--------------------------------------------------------------------------
    buffer.clear();
    buffer.putData( "DUMMY", 5 );
    outputFile.storeRecord( eos::COMPACT_STAMP_RECORD_MAGIC, buffer );

    //--------------------------------------------------------------------------
    // Cleanup
    //--------------------------------------------------------------------------
    inputFile.close();
    outputFile.close();
  }
}
