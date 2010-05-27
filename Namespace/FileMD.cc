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
    pChecksum( 0 )
  {
  }

  //----------------------------------------------------------------------------
  // Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void FileMD::serialize( Buffer &buffer )
  {
    buffer.putData( &pId,          sizeof( pId ) );
    buffer.putData( &pCTime,       sizeof( pCTime ) );
    buffer.putData( &pSize,        sizeof( pSize ) );
    buffer.putData( &pContainerId, sizeof( pContainerId ) );
    buffer.putData( &pChecksum,    sizeof( pChecksum ) );

    uint16_t len = pName.length()+1;
    buffer.putData( &len,          2 );
    buffer.putData( pName.c_str(), len );

    len = pLocation.size();

    buffer.putData( &len, 2 );
    buffer.putData( &pLocation[0], len*sizeof( location_t ) );
  }

  //----------------------------------------------------------------------------
  // Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void FileMD::deserialize( Buffer &buffer )
  {
    uint16_t offset = 0;
    offset = buffer.grabData( offset, &pId,          sizeof( pId ) );
    offset = buffer.grabData( offset, &pCTime,       sizeof( pCTime ) );
    offset = buffer.grabData( offset, &pSize,        sizeof( pSize ) );
    offset = buffer.grabData( offset, &pContainerId, sizeof( pContainerId ) );
    offset = buffer.grabData( offset, &pChecksum,    sizeof( pChecksum ) );

    uint16_t len;
    offset = buffer.grabData( offset, &len, 2 );
    char strBuffer[len];
    offset = buffer.grabData( offset, strBuffer, len );
    pName = strBuffer;

    offset = buffer.grabData( offset, &len, 2 );
    pLocation.resize( len );
    for( uint16_t i = 0; i < len; ++i )
      offset = buffer.grabData( offset, &pLocation[i], sizeof( location_t ) );
  };
}
