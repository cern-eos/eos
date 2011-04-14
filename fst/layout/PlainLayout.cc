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
PlainLayout::fallocate(XrdSfsXferSize length)
{
  XrdOucErrInfo error;
  if(ofsFile->fctl(SFS_FCTL_GETFD,0,error)) 
    return -1;
  int fd = error.getErrInfo();
  if (fd>0)
    return posix_fallocate(fd,0,length);
  return -1;
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
