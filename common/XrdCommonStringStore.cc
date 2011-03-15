#include "XrdCommon/XrdCommonStringStore.hh"

XrdSysMutex XrdCommonStringStore::StringMutex;

XrdOucHash<XrdOucString> XrdCommonStringStore::theStore; 

char* 
XrdCommonStringStore::Store(const char* charstring , int lifetime) {
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







