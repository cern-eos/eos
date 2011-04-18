
/*----------------------------------------------------------------------------*/
#include "common/Attr.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Attr*
Attr::OpenAttr(const char * file)
{
  //----------------------------------------------------------------
  //! factory function checking if file exists
  //----------------------------------------------------------------
  struct stat buf;
  if (!file)
    return 0;

  if (::stat(file,&buf)) 
    return 0;
  
  return new Attr(file);
};

/*----------------------------------------------------------------------------*/
Attr::Attr(const char* file)
{
  //----------------------------------------------------------------
  //! constructor for attribute access
  //----------------------------------------------------------------
  fName = file;
}

/*----------------------------------------------------------------------------*/
Attr::~Attr() 
{
  //----------------------------------------------------------------
  //! destructor
  //----------------------------------------------------------------
}

/*----------------------------------------------------------------------------*/
bool 
Attr::Set(const char* name, const char* value, size_t len)
{
  if ((!name)|| (!value))
    return false;

  if (!lsetxattr(fName.c_str(), name, value, len,0))
    return true;
  return false;
}

/*----------------------------------------------------------------------------*/
bool 
Attr::Set(std::string key, std::string value)
{
  return Set(key.c_str(), value.c_str(), value.length());
}

/*----------------------------------------------------------------------------*/
bool
Attr::Get(const char* name, char* value, size_t &size)
{
  //----------------------------------------------------------------
  //! retrieves a binary attribute - value buffer is of size <size> and the result is stored there
  //! <buffer> must be large enough to hold the full attribute
  //----------------------------------------------------------------  
  if ((!name) || (!value))
    return -1;

  int retc = lgetxattr (fName.c_str(), name, value,size);
  if (retc!=-1) {
    size = retc;
    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
std::string
Attr::Get(std::string name)
{
  //----------------------------------------------------------------
  //! returns an extended attribute as a string
  //----------------------------------------------------------------
  char buffer[65536];
  size_t size = sizeof(buffer)-1;

  if (!Get(name.c_str(), buffer, size))
    return "";

  buffer[size] = 0;
  return std::string(buffer);
}

EOSCOMMONNAMESPACE_END
