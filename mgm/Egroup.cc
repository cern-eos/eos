// ----------------------------------------------------------------------
// File: Egroup.cc
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
#include "mgm/Egroup.hh"
#include "common/Logging.hh"
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
	bool member = Map[egroupname][username];
        Mutex.UnLock();
	return member;
      }
    }
  }
  Mutex.UnLock();
  bool isMember = false;

  // run the command not in the locked section !!!
  {
    eos_static_info("msg=\"lookup\" user=\"%s\" e-group=\"%s\"", username.c_str(), egroupname.c_str());
    // run the LDAP query
    LDAP *ld = NULL;
    int version = LDAP_VERSION3;
    // currently hard coded to CERN
    ldap_initialize ( &ld, "ldap://xldap"  );
    if ( ld == NULL ) {
      fprintf(stderr,"error: failed to initialize LDAP\n");
    } else {
      (void) ldap_set_option( ld, LDAP_OPT_PROTOCOL_VERSION, &version ); 
      // the LDAP base
      std::string sbase  = "OU=Users,Ou=Organic Units,DC=cern,DC=ch";
      // the LDAP filter
      std::string filter = "sAMAccountName="; 
      filter += username;;
      // the LDAP attribute
      std::string attr = "memberOf";
      // the LDAP match
      std::string match= "CN=";
      match += egroupname;
      match += ",";

      char* attrs[2];
      attrs[0] = (char*)attr.c_str();
      attrs[1] = NULL;
      LDAPMessage *res = NULL;
      struct timeval timeout;
      timeout.tv_sec=5;
      timeout.tv_usec=0;
      int rc = ldap_search_ext_s (ld, sbase.c_str(), LDAP_SCOPE_SUBTREE,
				  filter.c_str(), attrs, 0, NULL, NULL, &timeout, LDAP_NO_LIMIT, &res);
      if ( ( rc == LDAP_SUCCESS ) && ( ldap_count_entries( ld, res ) != 0 ) ) {
	LDAPMessage* e = NULL;
    
	for ( e = ldap_first_entry( ld, res ); e != NULL; e = ldap_next_entry( ld, e ) ) {
	  struct berval **v = ldap_get_values_len( ld, e, attr.c_str() );
	  
	  if ( v != NULL ) {
	    int n = ldap_count_values_len( v );
	    int j;
	    
	    for ( j = 0; j < n; j++ ) {
	      std::string result = v[ j ]->bv_val;
	      if ( (result.find(match)) != std::string::npos ) {
		isMember = true;
	      }
	    }
	    ldap_value_free_len( v );
	  }
	}
      }
	
      ldap_msgfree ( res );
      
      if ( ld != NULL ) {
	ldap_unbind_ext( ld, NULL, NULL );
      }
    }
    if (isMember)
      eos_static_info("member=true user=\"%s\" e-group=\"%s\" cachetime=%lu", username.c_str(), egroupname.c_str(), now+EOSEGROUPCACHETIME);
    else
      eos_static_info("member=false user=\"%s\" e-group=\"%s\" cachetime=%lu", username.c_str(), egroupname.c_str(), now+EOSEGROUPCACHETIME);
  }

  Mutex.Lock();

  if (isMember) {
    Map[egroupname][username] = true;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

    Mutex.UnLock();
    return true;
  } else {
    Map[egroupname][username] = false;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

    Mutex.UnLock();
    return false;
  }
}
 
EOSMGMNAMESPACE_END
