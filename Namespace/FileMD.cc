//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the file metadata
//------------------------------------------------------------------------------

#include "FileMD.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  FileMD::FileMD( id_t id ):
    pId( id ),
    pCTime( 0 ),
    pSize( 0 ),
    pContainerId( 0 ),
    pUid( 0 ),
    pGid( 0 ),
    pLayoutId( 0 )
  {
  }

  //----------------------------------------------------------------------------
  // Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void FileMD::serialize( Buffer &buffer ) throw( MDException )
  {
    buffer.putData( &pId,          sizeof( pId ) );
    buffer.putData( &pCTime,       sizeof( pCTime ) );
    buffer.putData( &pSize,        sizeof( pSize ) );
    buffer.putData( &pContainerId, sizeof( pContainerId ) );

    uint16_t len = pName.length()+1;
    buffer.putData( &len,          sizeof( len ) );
    buffer.putData( pName.c_str(), len );

    len = pLocation.size();
    buffer.putData( &len, sizeof( len ) );

    LocationSet::iterator it;
    for( it = pLocation.begin(); it != pLocation.end(); ++it )
    {
      location_t location = *it;
      buffer.putData( &location, sizeof( location_t ) );
    }

    buffer.putData( &pUid,      sizeof( pUid ) );
    buffer.putData( &pGid,      sizeof( pGid ) );
    buffer.putData( &pLayoutId, sizeof( pLayoutId ) );

    uint8_t size = pChecksum.getSize();
    buffer.putData( &size, sizeof( size ) );
    buffer.putData( pChecksum.getDataPtr(), size );
  }

  //----------------------------------------------------------------------------
  // Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void FileMD::deserialize( Buffer &buffer ) throw( MDException )
  {
    uint16_t offset = 0;
    offset = buffer.grabData( offset, &pId,          sizeof( pId ) );
    offset = buffer.grabData( offset, &pCTime,       sizeof( pCTime ) );
    offset = buffer.grabData( offset, &pSize,        sizeof( pSize ) );
    offset = buffer.grabData( offset, &pContainerId, sizeof( pContainerId ) );

    uint16_t len = 0;
    offset = buffer.grabData( offset, &len, 2 );
    char strBuffer[len];
    offset = buffer.grabData( offset, strBuffer, len );
    pName = strBuffer;

    offset = buffer.grabData( offset, &len, 2 );
    for( uint16_t i = 0; i < len; ++i )
    {
      location_t location;
      offset = buffer.grabData( offset, &location, sizeof( location_t ) );
      pLocation.insert( location );
    }

    offset = buffer.grabData( offset, &pUid,      sizeof( pUid ) );
    offset = buffer.grabData( offset, &pGid,      sizeof( pGid ) );
    offset = buffer.grabData( offset, &pLayoutId, sizeof( pLayoutId ) );

    uint8_t size = 0;
    offset = buffer.grabData( offset, &size, sizeof( size ) );
    pChecksum.resize( size );
    offset = buffer.grabData( offset, pChecksum.getDataPtr(), size );
  };
}
