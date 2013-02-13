// ----------------------------------------------------------------------
// File: proc/admin/Transfer.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/txengine/TransferEngine.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPriv.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Transfer ()
{
 XrdOucString mSubCmd = pOpaque->Get("mgm.mSubCmd") ? pOpaque->Get("mgm.mSubCmd") : "";
 XrdOucString src = pOpaque->Get("mgm.txsrc") ? pOpaque->Get("mgm.txsrc") : "";
 XrdOucString dst = pOpaque->Get("mgm.txdst") ? pOpaque->Get("mgm.txdst") : "";
 src = XrdMqMessage::UnSeal(src);
 dst = XrdMqMessage::UnSeal(dst);
 XrdOucString rate = pOpaque->Get("mgm.txrate") ? pOpaque->Get("mgm.txrate") : "";
 XrdOucString streams = pOpaque->Get("mgm.txstreams") ? pOpaque->Get("mgm.txstreams") : "";
 XrdOucString group = pOpaque->Get("mgm.txgroup") ? pOpaque->Get("mgm.txgroup") : "";
 XrdOucString id = pOpaque->Get("mgm.txid") ? pOpaque->Get("mgm.txid") : "";
 XrdOucString option = pOpaque->Get("mgm.txoption") ? pOpaque->Get("mgm.txoption") : "";
 if ((mSubCmd != "submit") && (mSubCmd != "ls") && (mSubCmd != "cancel") && (mSubCmd != "enable") && (mSubCmd != "disable") && (mSubCmd != "reset") && (mSubCmd != "clear") && (mSubCmd != "resubmit") && (mSubCmd != "kill") && (mSubCmd != "log") && (mSubCmd != "purge"))
 {
   retc = EINVAL;
   stdErr = "error: there is no such sub-command defined for <transfer>";
   return SFS_OK;
 }

 if ((mSubCmd == "submit"))
 {
   // check if we have krb5 credentials for that user
   struct stat krbbuf;
   struct stat gsibuf;
   bool usegsi = false;
   bool usekrb = false;
   XrdOucString krbcred = "/var/eos/auth/krb5#";
   krbcred += (int) pVid->uid;
   XrdOucString gsicred = "/var/eos/auth/gsi#";
   gsicred += (int) pVid->gid;
   std::string credential;
   XrdOucString Credential;
   XrdOucString CredentialB64;

   // -------------------------------------------
   // check that the path names are valid
   XrdOucString scheck = src.c_str();
   scheck.erase(scheck.find("?"));
   XrdOucString dcheck = dst.c_str();
   dcheck.erase(dcheck.find("?"));
   const char* inpath = 0;
   {
     inpath = scheck.c_str();
     NAMESPACEMAP;
     info = 0;
     if (info)info = 0; // for compiler happyness
     if (!path)
     {
       // illegal source path
       retc = EILSEQ;
       stdErr += "error: illegal characters in path name\n";
       return SFS_OK;
     }
   }

   {
     inpath = dcheck.c_str();
     NAMESPACEMAP;
     info = 0;
     if (info)info = 0; // for compiler happyness
     if (!path)
     {
       // illegal destination path
       retc = EILSEQ;
       stdErr += "error: illegal characters in path name\n";
       return SFS_OK;

     }
   }

   // -------------------------------------------
   // modify the the URLs for /eos/ paths
   if (src.beginswith("/eos/"))
   {
     src.insert("root:///", 0);
     src.insert(gOFS->MgmOfsAlias, 7);
   }
   if (dst.beginswith("/eos/"))
   {
     dst.insert("root:///", 0);
     dst.insert(gOFS->MgmOfsAlias, 7);
   }

   // -------------------------------------------
   // check s3 pOpaque information
   if (src.beginswith("as3://"))
   {
     if ((src.find("s3.key=") == STR_NPOS))
     {
       retc = EINVAL;
       stdErr += "error: you have to add the s3.key to the URL as ?s3.key=<>\n";
       mDoSort = false;
       return SFS_OK;
     }
     if ((src.find("s3.key=") == STR_NPOS))
     {
       retc = EINVAL;
       stdErr += "error: you have to add the s3.secretkey to the URL as ?s3.secretkey=<>\n";
       mDoSort = false;
       return SFS_OK;
     }
   }

   if (dst.beginswith("as3://"))
   {
     if ((dst.find("s3.key=") == STR_NPOS))
     {
       retc = EINVAL;
       stdErr += "error: you have to add the s3.key to the URL as ?s3.key=<>\n";
       mDoSort = false;
       return SFS_OK;
     }
     if ((dst.find("s3.key=") == STR_NPOS))
     {
       retc = EINVAL;
       stdErr += "error: you have to add the s3.key to the URL as ?s3.key=<>\n";
       mDoSort = false;
       return SFS_OK;
     }
   }

   // -------------------------------------------
   // add eos pOpaque mapping/application tags
   if ((src.find("//eos/")) != STR_NPOS)
   {
     if ((src.find("?")) == STR_NPOS)
     {
       src += "?";
     }
     src += "&eos.ruid=";
     src += (int) pVid->uid;
     src += "&eos.rgid=";
     src += (int) pVid->gid;
     src += "&eos.app=gw";
     if (group.length())
     {
       src += ".";
       src += group;
     }
   }

   if ((dst.find("//eos/")) != STR_NPOS)
   {
     if ((dst.find("?")) == STR_NPOS)
     {
       dst += "?";
     }
     dst += "&eos.ruid=";
     dst += (int) pVid->uid;
     dst += "&eos.rgid=";
     dst += (int) pVid->gid;
     dst += "&eos.app=gw";
     if (group.length())
     {
       dst += ".";
       dst += group;
     }
   }

   {
     XrdSysPrivGuard priv(pVid->uid, pVid->gid);
     if (!::stat(krbcred.c_str(), &krbbuf))
     {
       usekrb = true;
     }
     if (!::stat(gsicred.c_str(), &gsibuf))
     {
       usegsi = true;
     }
   }


   if (usegsi)
   {
     // put the current running user as the owner before loading
     // load the gsi credential
     {
       XrdSysPrivGuard priv(pVid->uid, pVid->gid);
       eos::common::StringConversion::LoadFileIntoString(gsicred.c_str(), credential);
     }
     Credential = credential.c_str();
     if (eos::common::SymKey::Base64Encode((char*) Credential.c_str(), Credential.length() + 1, CredentialB64))
     {
       Credential = CredentialB64;
     }
   }
   else
   {
     if (usekrb)
     {
       // put the current running user as the owner before loading
       char* krb5buf = (char*) malloc(krbbuf.st_size);
       if (krb5buf)
       {
         XrdSysPrivGuard priv(pVid->uid, pVid->gid);
         int fd = ::open(krbcred.c_str(), 0);
         if (fd >= 0)
         {
           if ((::read(fd, krb5buf, krbbuf.st_size)) == krbbuf.st_size)
           {
             if (eos::common::SymKey::Base64Encode((char*) krb5buf, krbbuf.st_size, CredentialB64))
             {
               Credential = CredentialB64;
             }
           }
           ::close(fd);
         }
         else
         {
           fprintf(stderr, "################# failed to open %s\n", krbcred.c_str());
         }
         free(krb5buf);
       }
     }
     else
     {
       credential = "";
       Credential = credential.c_str();
     }
   }

   if (usegsi)
   {
     Credential.insert("gsi:", 0);
   }
   else
   {
     if (usekrb)
     {
       Credential.insert("krb5:", 0);
     }
   }

   if ((!src.beginswith("as3:")) &&
       (!src.beginswith("root:")) &&
       (!src.beginswith("gsiftp:")) &&
       (!src.beginswith("http:")) &&
       (!src.beginswith("https:")))
   {
     retc = EINVAL;
     stdErr += "error: we support only s3,root,gsiftp,http & https as a source transfer protocol\n";
     mDoSort = false;
     return SFS_OK;
   }

   if ((!dst.beginswith("as3:")) &&
       (!dst.beginswith("root:")) &&
       (!dst.beginswith("gsiftp:")) &&
       (!dst.beginswith("http:")) &&
       (!dst.beginswith("https:")))
   {
     retc = EINVAL;
     stdErr += "error: we support only s3,root,gsiftp,http & https as a destination transfer protocol\n";
     mDoSort = false;
     return SFS_OK;
   }

   if (((src.beginswith("gsiftp:")) ||
        (dst.beginswith("gsiftp:")) ||
        (src.beginswith("https:")) ||
        (dst.beginswith("https:"))) &&
       !usegsi)
   {
     retc = EINVAL;
     stdErr += "error: you need to use a delegated X509 proxy to do a transfer with gsiftp or https\n";
     mDoSort = false;
     return SFS_OK;
   }
   retc = gTransferEngine.Submit(src, dst, rate, streams, group, stdOut, stdErr, *pVid, 86400, Credential, (option.find("s") != STR_NPOS) ? true : false);
 }

 if ((mSubCmd == "enable"))
 {
   if (pVid->uid == 0)
   {
     retc = gTransferEngine.Run();
     if (retc)
     {
       stdErr += "error: transfer engine was already running\n";
     }
     else
     {
       stdOut += "success: enabled transfer engine\n";
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you don't have the required priviledges to execute 'transfer enable'!";
   }
 }

 if ((mSubCmd == "disable"))
 {
   if (pVid->uid == 0)
   {
     retc = gTransferEngine.Stop();
     if (retc)
     {
       stdErr += "error: transfer engine was not running\n";
     }
     else
     {
       stdOut += "success: disabled transfer engine\n";
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you don't have the required priviledges to execute 'transfer disable'!";
   }
 }

 if ((mSubCmd == "reset"))
 {
   retc = gTransferEngine.Reset(option, id, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "ls"))
 {
   retc = gTransferEngine.Ls(id, option, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "clear"))
 {
   retc = gTransferEngine.Clear(stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "cancel"))
 {
   retc = gTransferEngine.Cancel(id, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "resubmit"))
 {
   retc = gTransferEngine.Resubmit(id, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "kill"))
 {
   retc = gTransferEngine.Kill(id, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "log"))
 {
   retc = gTransferEngine.Log(id, group, stdOut, stdErr, *pVid);
 }

 if ((mSubCmd == "purge"))
 {
   retc = gTransferEngine.Purge(option, id, group, stdOut, stdErr, *pVid);
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
