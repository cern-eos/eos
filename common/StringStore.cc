/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringStore.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

XrdSysMutex StringStore::StringMutex;

XrdOucHash<XrdOucString> StringStore::theStore; 

char* 
StringStore::Store(const char* charstring , int lifetime) {
  XrdOucString* yourstring;

  if (!charstring) return (char*)"";

  StringMutex.Lock();
  if ((yourstring = theStore.Find(charstring))) {
    StringMutex.UnLock();
    return (char*)yourstring->c_str();
  } else {
    XrdOucString* newstring = new XrdOucString(charstring);
    theStore.Add(charstring,newstring, lifetime);
    StringMutex.UnLock();
    return (char*)newstring->c_str();
  } 
}

EOSCOMMONNAMESPACE_END








