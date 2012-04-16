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
// desc:   ChangeLog compacting test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>

#include <cstdlib>
#include <map>
#include <set>
#include <utility>

#include "namespace/persistency/LogManager.hh"
#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/persistency/ChangeLogConstants.hh"
#include "namespace/tests/TestHelpers.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class LogCompactingTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( LogCompactingTest );
    CPPUNIT_TEST( correctnessTest );
    CPPUNIT_TEST_SUITE_END();
    void correctnessTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( LogCompactingTest );

//------------------------------------------------------------------------------
// Generate random change log
//------------------------------------------------------------------------------
static void createRandomLog( const std::string       &path,
                             uint64_t                 numUnique,
                             uint64_t                 numDeleted,
                             int                      maxUpdates,
                             eos::LogCompactingStats &stats )
{
  srandom( time( 0 ) );
  eos::ChangeLogFile file;
  eos::Buffer        buffer;

  std::map<uint64_t, std::pair<int, bool> >           mods;
  std::map<uint64_t, std::pair<int, bool> >::iterator modsIt;

  //----------------------------------------------------------------------------
  // Random number of modifications for each record
  //----------------------------------------------------------------------------
  for( uint64_t i = 1; i <= numUnique; ++i )
  {
    std::pair<int, bool> mod = std::make_pair( 1+random()%maxUpdates, false );
    mods.insert( std::make_pair( i, std::make_pair(1 + random()%5, false ) ) );
  }

  //----------------------------------------------------------------------------
  // Randomize records to be deleted
  //----------------------------------------------------------------------------
  uint64_t toBeDeleted = 0;
  while( toBeDeleted < numDeleted )
  {
    uint64_t id = 1+random()%numUnique;
    if( mods[id].second )
      continue;
    mods[id].second = true;
    ++toBeDeleted;
  }

  //----------------------------------------------------------------------------
  // Write the records
  //----------------------------------------------------------------------------
  file.open( path, eos::ChangeLogFile::Create, eos::FILE_LOG_MAGIC );
  while( !mods.empty() )
  {
    //--------------------------------------------------------------------------
    // Write random records
    //--------------------------------------------------------------------------
    std::set<uint64_t> toBeCleanedUp;

    for( modsIt = mods.begin(); modsIt != mods.end(); ++modsIt )
    {
      uint8_t numBlocks = (random()%25) + 1;
      buffer.clear();
      buffer.reserve( numBlocks*4+8 );

      buffer.putData( &modsIt->first, 8 );

      for( uint16_t j = 0; j < numBlocks; ++j )
      {
        uint32_t block = random();
        buffer.putData( &block, 4 );
      }

      file.storeRecord( eos::UPDATE_RECORD_MAGIC, buffer );
      --modsIt->second.first;

      if( modsIt->second.first == 0 )
        toBeCleanedUp.insert( modsIt->first );

      ++stats.recordsUpdated;
    }

    //--------------------------------------------------------------------------
    // Delete records to be deleted
    //--------------------------------------------------------------------------
    for( std::set<uint64_t>::iterator it = toBeCleanedUp.begin();
         it != toBeCleanedUp.end();
         ++it )
    {
      if( mods[*it].second )
      {
        buffer.clear();
        buffer.putData( &(*it), 8 );
        file.storeRecord( eos::DELETE_RECORD_MAGIC, buffer );
        ++stats.recordsDeleted;
      }
      mods.erase( *it );
    }
  }
  stats.recordsTotal = stats.recordsUpdated + stats.recordsDeleted;
  stats.recordsKept  = numUnique - numDeleted;
  file.close();
}

//------------------------------------------------------------------------------
// Count the compacting stamps
//------------------------------------------------------------------------------
class StampsScanner: public eos::ILogRecordScanner
{
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    StampsScanner():
      pStampCount( 0 ), pStampLast( false ) {}

    //--------------------------------------------------------------------------
    // Go through the records
    //--------------------------------------------------------------------------
    virtual void processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      if( type == eos::COMPACT_STAMP_RECORD_MAGIC )
      {
        ++pStampCount;
        pStampLast = true;
      }
      else
      pStampLast = false;
    }

    //--------------------------------------------------------------------------
    // Accessors
    //--------------------------------------------------------------------------
    bool isStampLast() const { return pStampLast; }
    uint64_t stampCount() const { return pStampCount; }

  private:
    uint64_t pStampCount;
    bool     pStampLast;
};


//------------------------------------------------------------------------------
// compacting correctness test
//------------------------------------------------------------------------------
void LogCompactingTest::correctnessTest()
{
  eos::LogCompactingStats stats;
  eos::LogCompactingStats genStats;
  std::string             fileNameOld       = getTempName( "/tmp", "eosns" );
  std::string             fileNameCompacted = getTempName( "/tmp", "eosns" );

  createRandomLog( fileNameOld, 100000, 10000, 10, genStats );
  CPPUNIT_ASSERT_NO_THROW( eos::LogManager::compactLog( fileNameOld, fileNameCompacted, stats, 0 ) );

  CPPUNIT_ASSERT( stats.recordsTotal   == genStats.recordsTotal );
  CPPUNIT_ASSERT( stats.recordsUpdated == genStats.recordsUpdated );
  CPPUNIT_ASSERT( stats.recordsDeleted == genStats.recordsDeleted );
  CPPUNIT_ASSERT( stats.recordsKept    == genStats.recordsKept );
  CPPUNIT_ASSERT( stats.recordsKept    == stats.recordsWritten );

  eos::ChangeLogFile file;
  StampsScanner      stampScanner;
  CPPUNIT_ASSERT_NO_THROW( file.open( fileNameCompacted, eos::ChangeLogFile::ReadOnly, eos::FILE_LOG_MAGIC ) );
  CPPUNIT_ASSERT_NO_THROW( file.scanAllRecords( &stampScanner ) );
  CPPUNIT_ASSERT( stampScanner.stampCount() == 1 );
  CPPUNIT_ASSERT( stampScanner.isStampLast() );
  file.close();

  unlink( fileNameOld.c_str() );
  unlink( fileNameCompacted.c_str() );
}
