#include "fst/io/exos/ExosIo.hh"

EOSFSTNAMESPACE_BEGIN

//--------------------------------------------------------------------------                                              
//! Open file                                                                                                             
//!                                                                                                                       
//! @param flags open flags                                                                                               
//! @param mode open mode                                                                                                 
//! @param opaque opaque information                                                                                      
//! @param timeout timeout value                                                                                          
//! @return 0 if successful, -1 otherwise and error code is set                                                           
//--------------------------------------------------------------------------                                              
int 
ExosIo::fileOpen(XrdSfsFileOpenMode flags,
	     mode_t mode,
	     const std::string& opaque,
	     uint16_t timeout)
{
  return mEXOS.open(flags);
}

//--------------------------------------------------------------------------                                              
//! Read from file - sync                                                                                                 
//!                                                                                                                       
//! @param offset offset in file                                                                                          
//! @param buffer where the data is read                                                                                  
//! @param length read length                                                                                             
//! @param timeout timeout value                                                                                          
//! @return number of bytes read or -1 if error                                                                           
//--------------------------------------------------------------------------                                              
int64_t 
ExosIo::fileRead(XrdSfsFileOffset offset,
		 char* buffer,
		 XrdSfsXferSize length,
		 uint16_t timeout)
{
  return mEXOS.read(buffer, offset, length);
}

//--------------------------------------------------------------------------                                              
//! Write to file - sync                                                                                                  
//!                                                                                                                       
//! @param offset offset                                                                                                  
//! @param buffer data to be written                                                                                      
//! @param length length                                                                                                  
//! @param timeout timeout value                                                                                          
//! @return number of bytes written or -1 if error                                                                        
//--------------------------------------------------------------------------                                              
int64_t 
ExosIo::fileWrite(XrdSfsFileOffset offset,
		  const char* buffer,
		  XrdSfsXferSize length,
		  uint16_t timeout)
{
  return mEXOS.write(buffer, offset, length);
}

//--------------------------------------------------------------------------                                                  
//! Read from file - async                                                                                                    
//!                                                                                                                           
//! @param offset offset in file                                                                                              
//! @param buffer where the data is read                                                                                      
//! @param length read length                                                                                                 
//! @param readahead set if readahead is to be used                                                                           
//! @param timeout timeout value                                                                                              
//! @return number of bytes read or -1 if error                                                                               
//--------------------------------------------------------------------------                                                  
int64_t 
ExosIo::fileReadAsync(XrdSfsFileOffset offset,
		      char* buffer,
		      XrdSfsXferSize length,
		      bool readahead,
		      uint16_t timeout)
{
  return mEXOS.read(buffer, offset, length);
}

//--------------------------------------------------------------------------                                                  
//! Write to file - async                                                                                                     
//!                                                                                                                           
//! @param offset offset                                                                                                      
//! @param buffer data to be written                                                                                          
//! @param length length                                                                                                      
//! @param timeout timeout value                                                                                              
//! @return number of bytes written or -1 if error                                                                            
//--------------------------------------------------------------------------                                                  
int64_t 
ExosIo::fileWriteAsync(XrdSfsFileOffset offset,
		       const char* buffer,
		       XrdSfsXferSize length,
		       uint16_t timeout)
{
  return mEXOS.aio_write(buffer, offset, length);
}


//--------------------------------------------------------------------------
//! Close file                                                              
//!
//! @param timeout timeout value
//! @return 0 on success, -1 otherwise and error code is set
//--------------------------------------------------------------------------
int 
ExosIo::fileClose(uint16_t timeout)
{
  return 0;
}

//--------------------------------------------------------------------------                                                  
//! Wait for all async IO                                                                                                     
//!                                                                                                                           
//! @return global return code of async IO                                                                                    
//--------------------------------------------------------------------------
int 
ExosIo::fileWaitAsyncIO()
{
  return mEXOS.aio_flush();
}

//--------------------------------------------------------------------------
//! Truncate
//!
//! @param offset truncate file to this value
//! @param timeout timeout value
//! @return 0 if successful, -1 otherwise and error code is set
//--------------------------------------------------------------------------
int 
ExosIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{

  return mEXOS.truncate(offset);
}

//--------------------------------------------------------------------------
//! Allocate file space
//!
//! @param length space to be allocated
//! @return 0 on success, -1 otherwise and error code is set
//--------------------------------------------------------------------------
int 
ExosIo::fileFallocate(XrdSfsFileOffset length)
{
  return 0;
}

//--------------------------------------------------------------------------
//! Deallocate file space
//!
//! @param fromOffset offset start
//! @param toOffset offset end
//! @return 0 on success, -1 otherwise and error code is set
//--------------------------------------------------------------------------
int 
ExosIo::fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  return 0;
}

//--------------------------------------------------------------------------                                                  
//! Remove file                                                                                                               
//!                                                                                                                           
//! @param timeout timeout value                                                                                              
//! @return 0 on success, -1 otherwise and error code is set                                                                  
//--------------------------------------------------------------------------                                                  
int 
ExosIo::fileRemove(uint16_t timeout)
{
  return mEXOS.unlink();
}

//--------------------------------------------------------------------------                                                  
//! Sync file to disk                                                                                                         
//!                                                                                                                           
//! @param timeout timeout value                                                                                              
//! @return 0 on success, -1 otherwise and error code is set                                                                  
//--------------------------------------------------------------------------                                                  
int 
ExosIo::fileSync(uint16_t timeout)
{
  return mEXOS.aio_flush();
}

//--------------------------------------------------------------------------                                                  
//! Check for the existence of a file                                                                                         
//!                                                                                                                           
//! @param path to the file                                                                                                   
//! @return 0 on success, -1 otherwise and error code is set                                                                  
//--------------------------------------------------------------------------                                                  
int 
ExosIo::fileExists()
{
  struct stat buf;
  return mEXOS.stat(&buf);
}

//--------------------------------------------------------------------------                                                  
//! Get stats about the file                                                                                                  
//!                                                                                                                           
//! @param buf stat buffer                                                                                                    
//! @param timeout timeout value                                                                                              
//! @return 0 on success, -1 otherwise and error code is set                                                                  
//--------------------------------------------------------------------------                                                  
int 
ExosIo::fileStat(struct stat* buf, uint16_t timeout)
{
  return mEXOS.stat(buf);
}

EOSFSTNAMESPACE_END
