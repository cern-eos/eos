//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the container metadata
//------------------------------------------------------------------------------

#include "Namespace/ContainerMD.hh"
#include "Namespace/FileMD.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ContainerMD::ContainerMD( id_t id ):
    pId( id ),
    pParentId( 0 ),
    pName( "" )
  {
    pSubContainers.set_deleted_key( "" );
    pFiles.set_deleted_key( "" );
    pCTime.tv_sec = 0;
    pCTime.tv_nsec = 0;
  }

  //----------------------------------------------------------------------------
  // Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void ContainerMD::serialize( Buffer &buffer ) throw( MDException )
  {
    buffer.putData( &pId,       sizeof( pId ) );
    buffer.putData( &pParentId, sizeof( pParentId ) );
    buffer.putData( &pCTime,    sizeof( pCTime ) );

    uint16_t len = pName.length()+1;
    buffer.putData( &len,          2 );
    buffer.putData( pName.c_str(), len );
  }

  //----------------------------------------------------------------------------
  // Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void ContainerMD::deserialize( Buffer &buffer ) throw( MDException )
  {
    uint16_t offset = 0;
    offset = buffer.grabData( offset, &pId,       sizeof( pId ) );
    offset = buffer.grabData( offset, &pParentId, sizeof( pParentId ) );
    offset = buffer.grabData( offset, &pCTime,    sizeof( pCTime ) );

    uint16_t len;
    offset = buffer.grabData( offset, &len, 2 );
    char strBuffer[len];
    offset = buffer.grabData( offset, strBuffer, len );
    pName = strBuffer;
  };

  //----------------------------------------------------------------------------
  // Add file
  //----------------------------------------------------------------------------
  void ContainerMD::addFile( FileMD *file )
  {
    file->setContainerId( pId );
    pFiles[file->getName()] = file;
  }
}
