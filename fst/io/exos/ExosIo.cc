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
  eos_static_debug("");
  // translate the XRootD flags to POSIX flags
  int pflags = 0;

  if (flags & SFS_O_CREAT) {
    pflags |= (O_CREAT | O_RDWR);
  }

  if ((flags & SFS_O_RDWR)) {
    pflags |= (O_RDWR);
  }
  
  if ((flags & SFS_O_WRONLY)) {
    pflags |= (O_WRONLY);
  }

  if (flags & SFS_O_TRUNC) {
    pflags |= (O_TRUNC);
  }
  return Ret2Errno(mEXOS.open(pflags));
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.read(buffer, offset, length));
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.write(buffer, offset, length));
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.read(buffer, offset, length));
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.aio_write(buffer, offset, length));
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
  eos_static_debug("");
  int retc = mEXOS.close();
  return Ret2Errno(retc);
}

//--------------------------------------------------------------------------                                                  
//! Wait for all async IO                                                                                                     
//!                                                                                                                           
//! @return global return code of async IO                                                                                    
//--------------------------------------------------------------------------
int 
ExosIo::fileWaitAsyncIO()
{
  eos_static_debug("");
  return Ret2Errno(mEXOS.aio_flush());
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.truncate(offset));
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
  eos_static_debug("");
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
  eos_static_debug("");
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.unlink());
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.aio_flush());
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
  eos_static_debug("");
  struct stat buf;
  return Ret2Errno(mEXOS.stat(&buf));
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
  eos_static_debug("");
  return Ret2Errno(mEXOS.stat(buf));
}

// ------------------------------------------------------------------------                          
//! Set a binary attribute (name has to start with 'user.' !!!)                                      
//!                                                                                                  
//! @param name attribute name                                                                       
//! @param value attribute value                                                                     
//! @return 0 on success, -1 otherwise and error code is set                                         
// ------------------------------------------------------------------------                          
int 
ExosIo::attrSet(std::string name, std::string value)
{
  std::map<std::string, std::string> xattr;
  xattr[name] = value;
  return Ret2Errno(mEXOS.setxattr(xattr));
}

// ------------------------------------------------------------------------
//! Set a binary attribute (name has to start with 'user.' !!!)                                                               
//!                                                                                                                           
//! @param name attribute name                                                                                                
//! @param value attribute value                                                                                              
//! @param len value length                                                                                                   
//! @return 0 on success, -1 otherwise and error code is set                                                                  
// ------------------------------------------------------------------------                                                   
int 
ExosIo::attrSet(const char* name, const char* value, size_t len)
{
  std::string key(name);
  std::string val(value, len);
  return attrSet(key, val);
}



// ------------------------------------------------------------------------                          
//! Get a binary attribute by name                                                                   
//!                                                                                                  
//! @param name attribute name                                                                       
//! @param value contains attribute value upon success                                               
//! @param size the buffer size, after success the value size                                        
//! @return 0 on success, -1 otherwise and error code is set                                         
// ------------------------------------------------------------------------                          
int 
ExosIo::attrGet(const char* name, char* value, size_t& size)
{
  int retc = 0;
  std::string sname(name);
  std::string val;
  size_t len=0;
  if (!(retc = attrGet(sname, val))) {
    if (val.size() > size) {
      len = size;
    } else {
      len = val.size();
    }
    memcpy(value, val.c_str(), len);
    size = len;
    return 0;
  }
  return retc;
}

// ------------------------------------------------------------------------                          
//! Get a binary attribute by name                                                                   
//!                                                                                                  
//! @param name attribute name                                                                       
//! @param value contains attribute value upon success                                               
//! @return 0 on success, -1 otherwise and error code is set                                         
// ------------------------------------------------------------------------                          
int 
ExosIo::attrGet(std::string name, std::string& value)
{
  int retc=0;
  std::map<std::string,std::string> xattr;
  xattr[name]="";

  if (!(retc = mEXOS.getxattr(xattr))) {
    if (xattr.count(name)) {
      value = xattr[name];
      return 0;
    } else {
      errno = ENOATTR;
      return -1;
    }
  }
  return Ret2Errno(retc);
}

// ------------------------------------------------------------------------                          
//! Delete a binary attribute by name                                                                
//!                                                                                                  
//! @param name attribute name                                                                       
//! @return 0 on success, -1 otherwise and error code is set                                         
// ------------------------------------------------------------------------                          
int 
ExosIo::attrDelete(const char* name)
{
  std::set<std::string> xattr;
  xattr.insert(name);
  return Ret2Errno(mEXOS.rmxattr(xattr));
}

// ------------------------------------------------------------------------                          
//! List all attributes for the associated path                                                      
//!                                                                                                  
//! @param list contains all attribute names for the set path upon success                           
//! @return 0 on success, -1 otherwise and error code is set                                         
// ------------------------------------------------------------------------                          
int 
ExosIo::attrList(std::vector<std::string>& list)
{
  int retc=0;
  std::map<std::string,std::string> xattr;
  if (!(retc = mEXOS.getxattr(xattr))) {
    for (auto it=xattr.begin(); it!=xattr.end(); ++it) {
      list.push_back(it->first);
    }
    return 0;
  }
  return Ret2Errno(retc);
}

//--------------------------------------------------------------------------                         
//! Open a cursor to traverse a storage system                                                       
//!                                                                                                  
//! @return returns implementation dependent handle or 0 in case of error                            
//--------------------------------------------------------------------------                         
FileIo::FtsHandle* 
ExosIo::ftsOpen()
{
  FtsHandle* handle = new ExosIo::FtsHandle(mFilePath.c_str());
  if (!handle)
    return 0;

  void* listing =  mEXOS.objectlist();
  if (!listing) {
    delete handle;
    return 0;
  }
  handle->set(listing);
  return handle;
}

//--------------------------------------------------------------------------                         
//! Return the next path related to a traversal cursor obtained with ftsOpen                         
//!                                                                                                  
//! @param fts_handle cursor obtained by ftsOpen                                                     
//! @return returns implementation dependent handle or 0 in case of error                            
//--------------------------------------------------------------------------                         
std::string 
ExosIo::ftsRead(FileIo::FtsHandle* handle)
{
  void* raw_handle = ((ExosIo::FtsHandle*)handle)->get();
  std::string path = mEXOS.nextobject(raw_handle);

  if (path.length()) {
    XrdCl::URL lURL = mURL;
    lURL.SetPath(mURL.GetPath() + path);
    return lURL.GetURL();
  } else {
    return path;
  }
}

//--------------------------------------------------------------------------                         
//! Close a traversal cursor                                                                         
//!                                                                                                  
//! @param fts_handle cursor to close                                                                
//! @return 0 if fts_handle was an open cursor, otherwise -1                                         
//--------------------------------------------------------------------------                         
int 
ExosIo::ftsClose(FileIo::FtsHandle* handle)
{
  void* raw_handle = ((ExosIo::FtsHandle*)handle)->get();
  if (!raw_handle) {
    errno = EINVAL;
    return -1;
  }
  int retc = mEXOS.closelist(raw_handle);
  return retc;
}

EOSFSTNAMESPACE_END
