// ----------------------------------------------------------------------
// File: Attr.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/


/*----------------------------------------------------------------------------*/
#include "common/Attr.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN;

/*----------------------------------------------------------------------------*/
/** 
 * Factory function checking if file exists.
 * 
 * @param file path to the file with extended attributes
 * 
 * @return Attr object if file exists otherwise 0
 */

/*----------------------------------------------------------------------------*/
Attr*
Attr::OpenAttr (const char * file)
{
  struct stat buf;
  if (!file)
    return 0;

  if (::stat(file, &buf))
    return 0;

  return new Attr(file);
};

/*----------------------------------------------------------------------------*/
/** 
 * Create an attribute object for file. Better use the factory function
 * OpenAttr!
 * @param file path toe the file with extended attributes
 */

/*----------------------------------------------------------------------------*/
Attr::Attr (const char* file)
{
  mName = file;
}

/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 * 
 */

/*----------------------------------------------------------------------------*/
Attr::~Attr () { }

/*----------------------------------------------------------------------------*/
/** 
 * Set an attribute with name to value
 * 
 * @param name attribute name should start with 'user.'
 * @param value attribute string value
 * @param len length of string value
 * 
 * @return true if successful - false if error
 */

/*----------------------------------------------------------------------------*/
bool
Attr::Set (const char* name, const char* value, size_t len)
{
  if ((!name) || (!value))
    return false;
#ifdef __APPLE__
  if (!setxattr(mName.c_str(), name, value, len, 0, 0))
#else
  if (!lsetxattr(mName.c_str(), name, value, len, 0))
#endif
    return true;
  return false;
}

/*----------------------------------------------------------------------------*/
/** 
 * Set an attribute named by key to value
 * 
 * @param key string of the key should start with 'user.'
 * @param value string value
 * 
 * @return true if successful - false if error
 */

/*----------------------------------------------------------------------------*/
bool
Attr::Set (std::string key, std::string value)
{

  return Set(key.c_str(), value.c_str(), value.length());
}

/*----------------------------------------------------------------------------*/
/** 
 * Retrieve a binary attribute - value buffer is of size 'size' and the result is store there.
 * The value buffer must be large enough to hold the full attribute.
 * 
 * @param name name of the attribute to retrieve
 * @param value pointer to string where to store the value
 * @param size size of the value buffer.
 * 
 * @return true if the attribute has been retrieved - false if error
 */

/*----------------------------------------------------------------------------*/
bool
Attr::Get (const char* name, char* value, size_t &size)
{

  if ((!name) || (!value))
    return false;
#ifdef __APPLE__
  int retc = getxattr(mName.c_str(), name, value, size, 0, 0);
#else
  int retc = lgetxattr(mName.c_str(), name, value, size);
#endif
  if (retc != -1)
  {
    size = retc;
    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return a string for the extended attribute by name
 * 
 * @param name key of the extended attribute
 * 
 * @return string of the value and also empty "" if it does not exist
 */

/*----------------------------------------------------------------------------*/
std::string
Attr::Get (std::string name)
{
  mBuffer[0] = 0;
  size_t size = sizeof (mBuffer) - 1;
  if (!Get(name.c_str(), mBuffer, size))
  {
    return "";
  }

  mBuffer[size] = 0;
  return std::string(mBuffer);
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END;
