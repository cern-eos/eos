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
#include <signal.h>
#include "ProcCache.hh"
#include "common/Logging.hh"
#include <openssl/x509v3.h>
#include <openssl/bn.h>
#include <openssl/asn1.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <XrdSys/XrdSysAtomics.hh>

int ProcReaderCmdLine::ReadContent (std::vector<std::string> &cmdLine)
{
  int ret = 1;
  int fd = open (pFileName.c_str (), O_RDONLY & O_NONBLOCK);

  if (fd >= 0)
  {
    const int bufsize = 1677216;
    char *buffer = new char[bufsize];
    int r = read (fd, buffer, bufsize);
    int beg=0,end=0;
    if (r >= 0 && r < bufsize)
    {
      while(true)
      {
        while(end<r && buffer[end]!=0)
          end++;
        if(end>beg)
          cmdLine.push_back (std::string(buffer+beg,end-beg));
        if(end>=r)
          break;
        end++;
        beg=end;
      }
      ret = 0;
    }
    else
    {
      // read error or the buffer is too small
      ret = 2;
    }
    close (fd);
    delete[] buffer;
  }

  return ret;
}

int ProcReaderFsUid::ReadContent (uid_t &fsUid, gid_t &fsGid)
{
  int retval = 1;
  int fd = open (pFileName.c_str (), O_RDONLY & O_NONBLOCK);

  if (fd >= 0)
  {
    FILE *file = fdopen(fd,"r");
    std::string token, line;
    const int bufsize = 16384;
    char *buffer = new char[bufsize];

    while (fgets (buffer, bufsize, file))
    {
      line = buffer;
      istringstream iss (line);
      iss >> token;
      if (token == "Uid:")
      {
        for (int i = 0; i < 3; i++) /// fsUid is the 5th token
          iss >> token;

        iss >> fsUid;
        if (iss.fail ())
        {
          retval = 2;
          break;
        }
      }
      if (token == "Gid:")
      {
        for (int i = 0; i < 3; i++) /// fsGid is the 5th token
          iss >> token;

        iss >> fsGid;
        if (iss.fail ())
        {
          retval = 2;
          break;
        }
        retval = 0;
        break;
      }
    }
    if(file) fclose(file);
    delete[] buffer;
  }

  return retval;
}
int ProcReaderPsStat::ReadContent (long long unsigned &startTime, pid_t &ppid, pid_t &sid)
{
  int retval = 1;
  int fd = open (pFileName.c_str (), O_RDONLY & O_NONBLOCK);

  if (fd >= 0)
  {
    FILE *file = fdopen (fd, "r");
    std::string token, line;
    const int bufsize = 16384;
    char buffer[bufsize];
    int size = 0;

    // read the one line of the file
    if (!(fgets ((char*)buffer, bufsize, file))) return 2;
    size=strlen(buffer);

    int tokcount = 0, tokStart = 0;
    bool inParenth = false;
    // read char by char
    retval = 2;
    for (int i = 0; i < size - 1; i++)
    {
      if (buffer[i] == '(')
      {
        inParenth = true;
        continue;
      }

      if (buffer[i] == ')')
      {
        inParenth = false;
        continue;
      }

      if (!inParenth && buffer[i] == ' ')
      {
        // process token
        {
          buffer[i] = 0;
          bool over = false;
          switch (tokcount)
          {
            case 3:
              if (!sscanf (buffer + tokStart, "%u", &ppid))
                // error parsing parent process id
                over = true;
              break;
            case 5:
              if (!sscanf (buffer + tokStart, "%u", &sid))
                // error parsing session id
                over = true;
              break;
            case 21:
              over = true;
              if (sscanf (buffer + tokStart, "%llu", &startTime))
                // we parsed everything
                retval = 0;
              break;
            default:
              break;
          }
          if (over) break;
        }
        tokStart = i + 1;
        tokcount++;
        continue;
      }
    }
    // read char by char
    if (file) fclose (file);
  }

  return retval;
}

krb5_context ProcReaderKrb5UserName::sKcontext;
bool ProcReaderKrb5UserName::sKcontextOk = (!krb5_init_context (&ProcReaderKrb5UserName::sKcontext)) || (!eos_static_crit("error initializing Krb5"));;
eos::common::RWMutex ProcReaderKrb5UserName::sMutex;
bool ProcReaderKrb5UserName::sMutexOk = false;

void ProcReaderKrb5UserName::StaticDestroy()
{
  //if(sKcontextOk) krb5_free_context(sKcontext);
}

bool ProcReaderKrb5UserName::ReadUserName (string &userName)
{
  eos::common::RWMutexWriteLock lock(sMutex);

  if(!sKcontextOk) return false;

  eos_static_debug("starting Krb5 reading");

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
  eos_static_debug("finishing Krb5 reading");
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

bool ProcReaderGsiIdentity::sInitOk = true;

void ProcReaderGsiIdentity::StaticDestroy() {}

bool
ProcReaderGsiIdentity::ReadIdentity (string &sidentity)
{
  bool result = false;
  BIO          *certbio = BIO_new(BIO_s_file());
  X509         *cert = NULL;
  X509_NAME    *certsubject = NULL;
  char         *subj=NULL;

  if(!certbio)
  {
    eos_static_err("error allocating BIO buffer");
    goto gsicleanup;
  }

  BIO_read_filename(certbio, pGsiProxyFile.c_str ());
  if (! (cert = PEM_read_bio_X509(certbio, NULL, 0, NULL))) {
    eos_static_err("error loading cert into memory");
    goto gsicleanup;
  }

  certsubject = X509_NAME_new();
  if(!certsubject)
  {
    eos_static_err("error initializing certsubject");
    goto gsicleanup;
  }

  subj = X509_NAME_oneline(X509_get_subject_name(cert), NULL, 0);
  if(!subj)
  {
    eos_static_err("error reading subject name");
    goto gsicleanup;
  }
  sidentity = subj;

  result = true;

  gsicleanup:
  if(certbio) BIO_free_all(certbio);
  if(cert)    X509_free(cert);
  if(subj)    OPENSSL_free(subj);
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

int ProcCacheEntry::ReadContentFromFiles (ProcCache *procCache)
{
  eos::common::RWMutexWriteLock lock (pMutex);
  ProcReaderCmdLine pciCmd (pProcPrefix + "/cmdline"); // this one does NOT gets locked by the kernel when exeve is called
  ProcReaderFsUid pciFsUid (pProcPrefix + "/status");  // this one does NOT get locked by the kernel when exeve is called
  int retc,finalret=0;

  pCmdLineVect.clear ();
  retc = pciCmd.ReadContent (pCmdLineVect);
  if ( retc>1 )
  {
    pError = ESRCH;
    pErrMessage = "error reading content of proc file " + pProcPrefix + "/cmdline";
    return 2;
  }
  else if(retc==1)
  {
    finalret = 1;
    eos_static_notice("could not read command line for process %d because the proc file is locked, the cache is not updated",(int)this->pPid);
  }

  pCmdLineStr.clear ();
  for (auto it = pCmdLineVect.begin (); it != pCmdLineVect.end (); it++)
  {
    if (it != pCmdLineVect.begin ()) pCmdLineStr.append (" ");
    pCmdLineStr.append (*it);
  }

  retc = pciFsUid.ReadContent (pFsUid, pFsGid);
  if ( retc>1 )
  {
    pError = ESRCH;
    pErrMessage = "error reading content of proc file " + pProcPrefix + "/status";
    return 2;
  }
  else if(retc==1)
  {
    finalret = 1;
    eos_static_notice("could not read fsuid and fsgid for process %d because the proc file is locked, the cache is not updated",(int)this->pPid);
  }

  return finalret;
}

int ProcCacheEntry::UpdateIfPsChanged (ProcCache *procCache)
{
  ProcReaderPsStat pciPsStat (pProcPrefix + "/stat");
  unsigned long long procStartTime = 0;
  pciPsStat.ReadContent(procStartTime,pPPid,pSid);
  if (procStartTime > pStartTime)
  {
    int retc = ReadContentFromFiles (procCache);
    if (retc == 0)
    {
      pStartTime = procStartTime;
      return 0;
    }
    else if (retc ==1)
    {
      pStartTime = 0; // to retry to get the environment on the next call
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
