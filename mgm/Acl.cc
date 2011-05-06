/*----------------------------------------------------------------------------*/
#include "mgm/Acl.hh"
#include "mgm/Egroup.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Acl::Acl(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid)
{
  Set(sysacl, useracl, vid);
}

/*----------------------------------------------------------------------------*/
void
Acl::Set(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid)
{
  std::string acl="";
  if (sysacl.length()) {
    acl += sysacl;
  }

  if (useracl.length()) {
    if (acl.length())
      acl += ",";
    acl += useracl;
  }
    
  hasAcl = false;
  canRead = false;
  canWrite = false;
  canWriteOnce = false;
  hasEgroup = false;

  if (!acl.length()) {
    // no acl definition
    return ;
  }
    
  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(acl,rules,delimiter);
  std::vector<std::string>::const_iterator it;

  XrdOucString sizestring1;
  XrdOucString sizestring2;

  std::string userid  = eos::common::StringConversion::GetSizeString(sizestring1, (unsigned long long)vid.uid);
  std::string groupid = eos::common::StringConversion::GetSizeString(sizestring2, (unsigned long long)vid.gid);

  std::string usertag =  "u:"; usertag  += userid; usertag += ":";
  std::string grouptag = "g:"; grouptag += groupid; grouptag += ":";

  std::string username  = eos::common::Mapping::UidToUserName(vid.uid,errc);

  for (it = rules.begin(); it != rules.end(); it++) {
    bool egroupmatch = false;
    if (!it->compare(0, strlen("egroup:"), "egroup:")) {
      std::vector<std::string> entry;
      std::string delimiter = ":";
      eos::common::StringConversion::Tokenize(*it, entry, delimiter);

      if (entry.size() <3 ) {
        continue; 
      }

      egroupmatch = Egroup::Member(username, entry[1]);
      hasEgroup = egroupmatch;
    }
    
    if ((!it->compare(0, usertag.length(), usertag)) || (!it->compare(0,grouptag.length(), grouptag)) || (egroupmatch)) {
      // that is our rule
      std::vector<std::string> entry;
      std::string delimiter = ":";
      eos::common::StringConversion::Tokenize(*it, entry, delimiter);

      if (entry.size() <3 ) {
        continue; 
      }

      if ((entry[2].find("r"))!= std::string::npos) {
        canRead = true;
        hasAcl = true;
      }
      if ((entry[2].find("w"))!= std::string::npos) {
        canWrite = true;
        hasAcl = true;
      }
      if ((entry[2].find("wo"))!= std::string::npos) {
        canWrite = false;
        canWriteOnce = true;
        hasAcl = true;
      }
    }
  }
}
 
EOSMGMNAMESPACE_END
