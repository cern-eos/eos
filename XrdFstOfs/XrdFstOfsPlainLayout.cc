/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsPlainLayout.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsPlainLayout::open(const char                *path,
			   XrdSfsFileOpenMode   open_mode,
			   mode_t               create_mode,
			   const XrdSecEntity        *client,
			   const char                *opaque)
{
  return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsPlainLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->readofs(offset, buffer,length);
}

/*----------------------------------------------------------------------------*/
int 
XrdFstOfsPlainLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->writeofs(offset, buffer, length);
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsPlainLayout::truncate(XrdSfsFileOffset offset)
{
  return ofsFile->truncateofs(offset);
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsPlainLayout::sync() 
{
  return ofsFile->syncofs();
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsPlainLayout::close()
{
  return ofsFile->closeofs();
}

/*----------------------------------------------------------------------------*/
