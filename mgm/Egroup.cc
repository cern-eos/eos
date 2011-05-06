/*----------------------------------------------------------------------------*/
#include "mgm/Egroup.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

XrdSysMutex Egroup::Mutex;
std::map < std::string, std::map < std::string, bool > > Egroup::Map;
std::map < std::string, std::map < std::string, time_t > > Egroup::LifeTime;
/*----------------------------------------------------------------------------*/

bool
Egroup::Member(std::string &username, std::string &egroupname)
{
  Mutex.Lock();
  time_t now = time(NULL);

  if (Map.count(egroupname)) {
    if (Map[egroupname].count(username)) {
      // we now that user
      if (LifeTime[egroupname][username] > now) {
        // that is ok, we can return member or not member from the cache
        Mutex.UnLock();
        return Map[egroupname][username];
      }
    }
  }

  std::string cmd = "ldapsearch -LLL -h xldap -x -b 'OU=Users,Ou=Organic Units,DC=cern,DC=ch' 'sAMAccountName=";
  cmd += username;
  cmd += "' memberOf | grep CN=";
  cmd += egroupname;
  cmd += ",";
  cmd += ">& /dev/null";
  int rc = system(cmd.c_str());

  if (!WEXITSTATUS(rc)) {
    Map[egroupname][username] = true;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;
  } else {
    Map[egroupname][username] = false;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;
  }

  Mutex.UnLock();
  return true;
}
 
EOSMGMNAMESPACE_END
