#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdCommon/XrdCommonStringStore.hh"

/*----------------------------------------------------------------------------*/
XrdSysMutex                       XrdCommonMapping::gMapMutex;
XrdCommonMapping::UserRoleMap     XrdCommonMapping::gUserRoleVector;
XrdCommonMapping::GroupRoleMap    XrdCommonMapping::gGroupRoleVector;
XrdCommonMapping::VirtualUserMap  XrdCommonMapping::gVirtualUidMap;
XrdCommonMapping::VirtualGroupMap XrdCommonMapping::gVirtualGidMap;
XrdCommonMapping::SudoerMap       XrdCommonMapping::gSudoerMap;

/*----------------------------------------------------------------------------*/


void 
XrdCommonMapping::IdMap(const XrdSecEntity* client,const char* env, const char* tident, XrdCommonMapping::VirtualIdentity &vid)
{
  

  
}


void
XrdCommonMapping::Print(XrdOucString &stdOut, XrdOucString option)
{
  if ((!option.length()) || ( (option.find("u"))!=STR_NPOS)) {
    UserRoleMap::const_iterator it;
    for ( it = gUserRoleVector.begin(); it != gUserRoleVector.end(); ++it) {
      char iuid[4096];
      sprintf(iuid,"%d", it->first);
      char suid[4096];
      sprintf(suid,"%-6s",iuid);
      stdOut += "membership uid: ";stdOut += suid;
      stdOut += " => uids(";
      for ( unsigned int i=0; i< (it->second).size(); i++) {
	stdOut += (int) (it->second)[i];
	if (i < ((it->second).size()-1))
	  stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ( (option.find("g"))!=STR_NPOS)) {
    UserRoleMap::const_iterator it;
    for ( it = gGroupRoleVector.begin(); it != gGroupRoleVector.end(); ++it) {
      char iuid[4096];
      sprintf(iuid,"%d", it->first);
      char suid[4096];
      sprintf(suid,"%-6s",iuid);
      stdOut += "membership uid: ";stdOut += suid;
      stdOut += " => gids(";
      for ( unsigned int i=0; i< (it->second).size(); i++) {
	stdOut += (int) (it->second)[i];
	if (i < ((it->second).size()-1))
	  stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ( (option.find("s"))!=STR_NPOS)) {
    SudoerMap::const_iterator it;
    // print sudoer line
    stdOut += "sudoer                 => uids(";
    for ( it = gSudoerMap.begin() ;it != gSudoerMap.end(); ++it) {  
      if (it->second) {
	stdOut += (int) (it->first);
	stdOut += ",";
      }
    }
    if (stdOut.endswith(",")) {
      stdOut.erase(stdOut.length()-1);
    }
    stdOut += ")\n";
  }
}
