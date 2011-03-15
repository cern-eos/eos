/*----------------------------------------------------------------------------*/
#include "fst/layout/PlainLayout.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
int          
PlainLayout::open(const char                *path,
			   XrdSfsFileOpenMode   open_mode,
			   mode_t               create_mode,
			   const XrdSecEntity        *client,
			   const char                *opaque)
{
  return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
}


/*----------------------------------------------------------------------------*/
int
PlainLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->readofs(offset, buffer,length);
}

/*----------------------------------------------------------------------------*/
int 
PlainLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->writeofs(offset, buffer, length);
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::truncate(XrdSfsFileOffset offset)
{
  return ofsFile->truncateofs(offset);
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::sync() 
{
  return ofsFile->syncofs();
}


/*----------------------------------------------------------------------------*/
int
PlainLayout::close()
{
  return ofsFile->closeofs();
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
