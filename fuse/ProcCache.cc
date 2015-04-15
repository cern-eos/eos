// ----------------------------------------------------------------------
// File: ProcCache.cc
// Author: Geoffray Adde - CERN
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

#include "ProcCache.hh"
#include "common/Logging.hh"
#include "globus_common.h"
#include "globus_error.h"
#include "globus_gsi_cert_utils.h"
#include "globus_gsi_system_config.h"
#include "globus_gsi_proxy.h"
#include "globus_gsi_credential.h"
#include "globus_openssl.h"
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>

bool ProcReaderCmdLine::ReadContent (std::vector<std::string> &cmdLine)
{
  std::ifstream file (pFileName.c_str ());
  std::string token;
  while (true)
  {
    token.clear ();
    std::getline (file, token, (char) 0);
    if(file.eof ())
      break;
    // insert the environment variable in the map
    cmdLine.push_back (token);
  }
  return true;
}

bool ProcReaderFsUid::ReadContent (uid_t &fsUid, gid_t &fsGid)
{
  std::ifstream file (pFileName.c_str ());
  std::string line, token;
  bool retval = false;

  while (!file.eof ())
  {
    line.clear ();
    std::getline (file, line);
    istringstream iss (line);
    iss >> token;
    if (token == "Uid:")
    {
      for (int i = 0; i < 3; i++) /// fsUid is the 5th token
        iss >> token;

      iss >> fsUid;
      if (iss.fail ()) return false;
    }
    if (token == "Gid:")
    {
      for (int i = 0; i < 3; i++) /// fsGid is the 5th token
        iss >> token;

      iss >> fsGid;
      if (iss.fail ()) return false;
      retval = true;
      break;
    }
  }
  return retval;
}

krb5_context ProcReaderKrb5UserName::sKcontext;
bool ProcReaderKrb5UserName::sKcontextOk = (!krb5_init_context (&ProcReaderKrb5UserName::sKcontext)) || (!eos_static_crit("error initializing Krb5"));

void ProcReaderKrb5UserName::StaticDestroy()
{
  if(sKcontextOk) krb5_free_context(sKcontext);
}

bool ProcReaderKrb5UserName::ReadUserName (string &userName)
{
  if(!sKcontextOk) return false;

  bool result = false;
  krb5_principal princ=NULL;
  krb5_ccache cache=NULL;
  int retval;
  size_t where;
  char *ptrusername = NULL;

  // get the credential cache
  if( (retval=krb5_cc_resolve (sKcontext, pKrb5CcFile.c_str (), &cache)) )
  {
    eos_static_err("error resolving Krb5 credential cache%s, error code is %d", pKrb5CcFile.c_str (), (int )retval);
    goto cleanup;
  }

  // get the principal of the cache
  if ((retval = krb5_cc_get_principal (sKcontext, cache, &princ)))
  {
    eos_static_err("while getting principal of krb5cc %s, error code is %d", pKrb5CcFile.c_str (), (int )retval);
    goto cleanup;
  }

  // get the name of the principal
  // get the name of the principal
  if ((retval = krb5_unparse_name (sKcontext, princ, &ptrusername)))
  {
    eos_static_err("while getting name of principal of krb5cc %s, error code is %d", pKrb5CcFile.c_str (), (int )retval);
    goto cleanup;
  }
  userName.assign (ptrusername);

  // parse the user name
  where = userName.find ('@');
  if (where == std::string::npos)
  {
    eos_static_err("while parsing username of principal name %s, could not find '@'", userName.c_str ());
    goto cleanup;
  }

  userName.resize (where);

  eos_static_debug("parsed user name  %s", userName.c_str ());

  result = true;

cleanup:
  if(cache) krb5_cc_close(sKcontext, cache);
  if(princ) krb5_free_principal(sKcontext,princ);
  if(ptrusername) krb5_free_unparsed_name(sKcontext,ptrusername);
  return result;
}

time_t ProcReaderKrb5UserName::GetModifTime ()
{
  struct tm* clock;
  struct stat attrib;
  if (pKrb5CcFile.substr (0, 5) != "FILE:")
  {
    eos_static_err("expecting a credential cache file and got %s", pKrb5CcFile.c_str ());
    return 0;
  }
  if (stat (pKrb5CcFile.c_str () + 5, &attrib)) return 0;
  clock = gmtime (&(attrib.st_mtime));     // Get the last modified time and put it into the time structure

  return mktime (clock);
}

bool ProcReaderGsiIdentity::sInitOk =
    ( (globus_module_activate(GLOBUS_OPENSSL_MODULE) == (int)GLOBUS_SUCCESS) &&
    (globus_module_activate(GLOBUS_GSI_PROXY_MODULE) == (int)GLOBUS_SUCCESS) )
    || (!eos_static_crit("error initializing GSI"));

void ProcReaderGsiIdentity::StaticDestroy()
{
  if(sInitOk)
  {
    globus_module_deactivate(GLOBUS_OPENSSL_MODULE);
    globus_module_activate(GLOBUS_GSI_PROXY_MODULE);
  }
}

bool
ProcReaderGsiIdentity::ReadIdentity (string &sidentity)
{
  globus_gsi_cred_handle_t proxy_cred = NULL;
  X509_NAME * x509_identity = NULL;
  char* identity = NULL;
  char* identity2 = NULL;
  BIO * mem = NULL;
  bool result = false;

  if (globus_gsi_cred_handle_init (&proxy_cred, NULL) != GLOBUS_SUCCESS)
  {
    eos_static_err("error initializing GSI credential handle");
    goto cleanup;
  }
  if (globus_gsi_cred_read_proxy (proxy_cred, pGsiProxyFile.c_str ()) != GLOBUS_SUCCESS)
  {
    eos_static_err("error reading GSI proxy file %s",pGsiProxyFile.c_str());
    goto cleanup;
  }

  if (false)
  {
    x509_identity = NULL;
    mem = BIO_new (BIO_s_mem ());
    size_t len;
    if (globus_gsi_cred_get_X509_identity_name (proxy_cred, &x509_identity) != GLOBUS_SUCCESS)
    {
      eos_static_err( "\nERROR: Couldn't get a valid identity "
	  "name from the proxy credential.\n");
      goto cleanup;
    }
    if (X509_NAME_print_ex (mem, x509_identity, 0, XN_FLAG_RFC2253))
    {
      eos_static_err("error printing content of the x509_identity");
      goto cleanup;
    }
    len = BIO_ctrl_pending (mem);
    identity = new char[len + 1];
    identity[len] = 0;
    BIO_read (mem, identity, len);
    sidentity = identity;
  }
  else
  {
    if (globus_gsi_cred_get_identity_name (proxy_cred, &identity2) != GLOBUS_SUCCESS)
    {
      eos_static_err("\nERROR: Couldn't get a valid identity "
	  "name from the proxy credential.\n");
      goto cleanup;
    }
    sidentity = identity2;
  }

  result = true;

  cleanup: if (proxy_cred) globus_gsi_cred_handle_destroy (proxy_cred);
  if (mem) BIO_free (mem);
  if (x509_identity) X509_NAME_free (x509_identity);
  if (identity) delete[] identity;
  if (identity2) free (identity2);

  return result;
}

time_t ProcReaderGsiIdentity::GetModifTime ()
{
  struct tm* clock;
  struct stat attrib;
  if (stat (pGsiProxyFile.c_str (), &attrib)) return 0;
  clock = gmtime (&(attrib.st_mtime));     // Get the last modified time and put it into the time structure

  return mktime (clock);
}

bool ProcReaderEnv::ReadContent (std::map<std::string, std::string> &dict)
{
  std::ifstream file (pFileName.c_str ());
  std::string token;

  while (!file.eof ())
  {
    token.clear ();
    std::getline (file, token, (char) 0);
    if (token.empty ()) break;
    size_t eqidx = token.find ('=');
    if (eqidx == std::string::npos)
    {
      return false;
    }
    // insert the environment variable in the map
    //std::cout << token.substr(0,eqidx) << " = " << token.substr(eqidx+1,std::string::npos) << std::endl;
    dict[token.substr (0, eqidx)] = token.substr (eqidx + 1, std::string::npos);
  }
  return true;
}

time_t ProcCacheEntry::ReadStartingTime () const
{
  std::ifstream statFile ((pProcPrefix + "/stat").c_str ());
  if (!statFile.is_open ())
  {
    eos::common::RWMutexWriteLock lock (pMutex);
    pError = ESRCH;
    pErrMessage = "could not open proc file " + pProcPrefix + "/stat";
    return 0;
  }
  std::string line;
  std::getline (statFile, line);
  // the startup time is at the 22th position
  int cursor = 0, count = 0;
  while (count < 21 && cursor < (int) line.length ())
  {
    if (line[cursor] == ' ') count++;
    cursor++;
  }
  if (count != 21 || cursor >= (int) line.length ())
  {
    eos::common::RWMutexWriteLock lock (pMutex);
    pError = ESRCH;
    pErrMessage = " error parsing " + pProcPrefix + "/stat";
    return 0;
  }
  for (count = cursor + 1; count < (int) line.length (); count++)
  {
    if (line[count] == ' ')
    {
      // SUCCESS
      line[count] = 0;
      int stime = strtol (&line[cursor], NULL, 10);
      return (time_t)stime;
    }
  }
  eos::common::RWMutexWriteLock lock (pMutex);
  pError = ESRCH;
  pErrMessage = " error parsing " + pProcPrefix + "/stat";
  return 0;
}

bool ProcCacheEntry::ReadContentFromFiles ()
{
  eos::common::RWMutexWriteLock lock (pMutex);
  ProcReaderEnv pciEnv (pProcPrefix + "/environ");
  ProcReaderCmdLine pciCmd (pProcPrefix + "/cmdline");
  ProcReaderFsUid pciFsUid (pProcPrefix + "/status");
  pEnv.clear ();
  if (!pciEnv.ReadContent (pEnv))
  {
    pError = ESRCH;
    pErrMessage = "error reading content of proc file " + pProcPrefix + "/environ";
    return false;
  }
  pCmdLineVect.clear ();
  if (!pciCmd.ReadContent (pCmdLineVect))
  {
    pError = ESRCH;
    pErrMessage = "error reading content of proc file " + pProcPrefix + "/cmdline";
    return false;
  }
  pCmdLineStr.clear ();
  for (auto it = pCmdLineVect.begin (); it != pCmdLineVect.end (); it++)
  {
    if (it != pCmdLineVect.begin ()) pCmdLineStr.append (" ");
    pCmdLineStr.append (*it);
  }
  if (!pciFsUid.ReadContent (pFsUid, pFsGid))
  {
    pError = ESRCH;
    pErrMessage = "error reading content of proc file " + pProcPrefix + "/status";
    return false;
  }
  return true;
}

int ProcCacheEntry::UpdateIfPsChanged ()
{
  time_t procStartTime = 0;
  procStartTime = ReadStartingTime ();

  if (procStartTime > pStartTime)
  {
    if (ReadContentFromFiles ())
    {
      pStartTime = procStartTime;
      return 0;
    }

    return ESRCH;
  }
  else
  {
    // it means that the proc start time could not be read : most likely the pid does not exist (anymore)
    if (procStartTime == 0)
    {
      return ESRCH;
    }

    return 0; // just does not need an update
  }
}

int ProcCacheEntry::ResolveKrb5CcFile()
{
  std::string proxyfile;
  if (pEnv.count ("EOS_FUSE_AUTH") && pEnv["EOS_FUSE_AUTH"].substr (0, 5) == "krb5:")
  {
    proxyfile=pEnv["EOS_FUSE_AUTH"].substr (5, std::string::npos);
    pKrb5CcModTime = 0;
    pAuthMethod = "krb5:"+proxyfile;
    if(UpdateIfKrb5Changed()==0)
    {
      eos_static_debug("using EOS_FUSE_AUTH krb5 cc file %s",proxyfile.c_str());
      return 1; // resolve ok
    }
    pKrb5CcModTime = 0;
    pAuthMethod.clear();
    return -1; // resolve ok but bad credential for user defined, stop it!
  }
  if (pEnv.count ("KRB5CCNAME"))
  {
    proxyfile = pEnv["KRB5CCNAME"].substr (5, std::string::npos);
    pKrb5CcModTime = 0;
    pAuthMethod = "krb5:"+proxyfile;
    if (UpdateIfKrb5Changed()==0)
    {
      eos_static_debug("using KRB5CCNAME krb5 cc file %s",proxyfile.c_str());
      return 1; // resolve ok
    }
    pKrb5CcModTime = 0;
    pAuthMethod.clear();
    return 0; // resolve ok but bad credential, try next authentication
  }
  // try default location
  {
    char buffer[64];
    snprintf(buffer,64,"FILE:/tmp/krb5cc_%d",(int)pFsUid);
    proxyfile = buffer;
    pKrb5CcModTime= 0;
    pAuthMethod = "krb5:"+proxyfile;
    if (UpdateIfKrb5Changed() == 0)
    {
      eos_static_debug("using default krb5 cc file %s", proxyfile.c_str ());
      return 1; // resolve ok
    }
    pKrb5CcModTime = 0;
    pAuthMethod.clear();
    eos_static_debug("could not get krb5 CC file location : assume that user does NOT want to use krb5");
    return 0; // not resolved, try next authentication
  }
}

int ProcCacheEntry::UpdateIfKrb5Changed ()
{
  if(pAuthMethod.compare(0,5,"krb5:"))
  {
    eos_static_err("should have krb5: prefix");
    return EINVAL;
  }

  ProcReaderKrb5UserName pkrb (pAuthMethod.c_str()+5);

  time_t krb5ModTime = pkrb.GetModifTime ();

  if (!krb5ModTime)
  {
    eos_static_debug("could not stat krb5 credential cache file %s", pEnv["KRB5CCNAME"].c_str ());
    return EACCES;
  }

  if (krb5ModTime > pKrb5CcModTime)
  {
    if (pkrb.ReadUserName (pKrb5UserName))
    {
      pKrb5CcModTime = krb5ModTime;
      return 0;
    }

    return EACCES;
  }

  return 0;
}

int ProcCacheEntry::ResolveGsiProxyFile()
{
  std::string proxyfile;
  if (pEnv.count ("EOS_FUSE_AUTH") && pEnv["EOS_FUSE_AUTH"].substr (0, 4) == "gsi:")
  {
    proxyfile=pEnv["EOS_FUSE_AUTH"].substr (4, std::string::npos);
    pGsiProxyModTime = 0;
    pAuthMethod = "gsi:"+proxyfile;
    if(UpdateIfGsiChanged()==0)
    {
      eos_static_debug("using EOS_FUSE_AUTH gsi proxy file %s",proxyfile.c_str());
      return 1; // resolve ok
    }
    pGsiProxyModTime = 0;
    pAuthMethod.clear();
    return -1; // resolve ok but bad credential for user defined, stop it!
  }
  if (pEnv.count ("X509_USER_PROXY"))
  {
    proxyfile = pEnv["X509_USER_PROXY"].substr (4, std::string::npos);
    pGsiProxyModTime = 0;
    pAuthMethod = "gsi:"+proxyfile;
    if (UpdateIfGsiChanged () == 0)
    {
      eos_static_debug("using X509_USER_PROXY gsi file %s", proxyfile.c_str ());
      return 1; // resolve ok
    }
    pGsiProxyModTime = 0;
    pAuthMethod.clear();
    return 0; // resolve ok but bad credential, try next authentication
  }
  // try default location
  {
    char buffer[64];
    snprintf(buffer,64,"/tmp/x509up_u%d",(int)pFsUid);
    proxyfile = buffer;
    pGsiProxyModTime = 0;
    pAuthMethod = "gsi:"+proxyfile;
    if (UpdateIfGsiChanged () == 0)
    {
      eos_static_debug("using default gsi proxy file %s", proxyfile.c_str ());
      return 1; // resolve ok
    }
    pGsiProxyModTime = 0;
    pAuthMethod.clear();
    eos_static_debug("could not get GSI proxy file location : assume that user does NOT want to use gsi");
    return 0; // not resolved, try next authentication
  }
}

int ProcCacheEntry::UpdateIfGsiChanged()
{
  if(pAuthMethod.compare(0,4,"gsi:"))
  {
    eos_static_err("should have gsi: prefix");
    return EINVAL;
  }

  ProcReaderGsiIdentity pgsi(pAuthMethod.c_str()+4);

  time_t gsiModTime = pgsi.GetModifTime ();

  if (!gsiModTime)
  {
    eos_static_debug("could not stat GSI proxy file %s", pEnv["X509_USER_PROXY"].c_str ());
    return EACCES;
  }

  if (gsiModTime > pGsiProxyModTime)
  {
    if (pgsi.ReadIdentity(pGsiIdentity))
    {
      pGsiProxyModTime = gsiModTime;
      return 0;
    }

    return EACCES;
  }

  return 0;
}
