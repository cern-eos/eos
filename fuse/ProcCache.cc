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
#include <krb5.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>

bool ProcReaderCmdLine::ReadContent (std::vector<std::string> &cmdLine)
{
  std::ifstream file (pFileName.c_str ());
  std::string token;
  while (!file.eof ())
  {
    token.clear ();
    std::getline (file, token, (char) 0);
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

bool ProcReaderKrb5UserName::ReadUserName (string &userName)
{
  krb5_context kcontext;
  krb5_principal princ;
  krb5_ccache cache;

  // Init krb5
  auto retval = krb5_init_context (&kcontext);
  if (retval)
  {
    eos_static_err("while initializing krb5, error code is %d", (int )retval);
    krb5_free_context (kcontext);
    return false;
  }

  // get the credential cache
  krb5_cc_resolve (kcontext, pKrb5CcName.c_str (), &cache);

  // get the principal of the cache
  if ((retval = krb5_cc_get_principal (kcontext, cache, &princ)))
  {
    eos_static_err("while getting principal of krb5cc %s, error code is %d", pKrb5CcName.c_str (), (int )retval);
    krb5_free_context (kcontext);
    return false;
  }

  // get the name of the principal
  char *ptrusername = NULL;
  // get the name of the principal
  if ((retval = krb5_unparse_name (kcontext, princ, &ptrusername)))
  {
    eos_static_err("while getting name of principal of krb5cc %s, error code is %d", pKrb5CcName.c_str (), (int )retval);
    krb5_free_context (kcontext);
    return false;
  }
  userName.assign (ptrusername);

  // parse the user name
  size_t where = userName.find ('@');
  if (where == std::string::npos)
  {
    eos_static_err("while parsing username of principal name %s, could not find '@'", userName.c_str ());
    krb5_free_context (kcontext);
    return false;
  }

  userName.resize (where);

  eos_static_debug("parsed user name  %s", userName.c_str ());
  krb5_free_context (kcontext);
  return true;
}

time_t ProcReaderKrb5UserName::GetModifTime ()
{
  struct tm* clock;
  struct stat attrib;
  if (pKrb5CcName.substr (0, 5) != "FILE:")
  {
    eos_static_err("expecting a credential cache file and got %s", pKrb5CcName.c_str ());
    return 0;
  }
  if (stat (pKrb5CcName.c_str () + 5, &attrib)) return 0;
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

unsigned int ProcCacheEntry::ReadStartingTime () const
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
  int cursor = -1, count = 0;
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
      return stime;
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
  unsigned int procStartTime = 0;
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

int ProcCacheEntry::UpdateIfKrb5Changed ()
{
  if (!pEnv.count ("KRB5CCNAME") || pEnv["KRB5CCNAME"].substr (0, 5) != "FILE:")
  {
    eos_static_err("could not get krb5 credential cache file location");
    return EACCES;
  }

  ProcReaderKrb5UserName pkrb (pEnv["KRB5CCNAME"]);

  time_t krb5ModTime = pkrb.GetModifTime ();

  if (!krb5ModTime)
  {
    eos_static_err("could not stat krb5 credential cache file %s", pEnv["KRB5CCNAME"].c_str ());
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
