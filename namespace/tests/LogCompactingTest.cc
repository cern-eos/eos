//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog compacting test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <cstdlib>
#include <map>
#include <set>
#include <utility>

#include "namespace/persistency/LogManager.hh"
#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/persistency/ChangeLogConstants.hh"

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
  file.open( path, false, eos::FILE_LOG_MAGIC );
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
// compacting correctness test
//------------------------------------------------------------------------------
void LogCompactingTest::correctnessTest()
{
  eos::LogCompactingStats stats;
  eos::LogCompactingStats genStats;
  std::string             fileNameOld       = tempnam( "/tmp", "eosns" );
  std::string             fileNameCompacted = tempnam( "/tmp", "eosns" );

  createRandomLog( fileNameOld, 100000, 10000, 10, genStats );
  eos::LogManager::compactLog( fileNameOld, fileNameCompacted, stats, 0 );

  CPPUNIT_ASSERT( stats.recordsTotal   == genStats.recordsTotal );
  CPPUNIT_ASSERT( stats.recordsUpdated == genStats.recordsUpdated );
  CPPUNIT_ASSERT( stats.recordsDeleted == genStats.recordsDeleted );
  CPPUNIT_ASSERT( stats.recordsKept    == genStats.recordsKept );
  CPPUNIT_ASSERT( stats.recordsKept    == stats.recordsWritten );

  unlink( fileNameOld.c_str() );
  unlink( fileNameCompacted.c_str() );
}
