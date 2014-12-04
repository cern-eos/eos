// ----------------------------------------------------------------------
// File: com_file.cc
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "fst/FmdDbMap.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/

using namespace eos::common;

/* File handling */

/* Get file information */
int
com_fileinfo (char* arg1)
{
  XrdOucString savearg = arg1;
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString option = "";
  do
  {
    XrdOucString newoption = subtokenizer.GetToken();
    if (!newoption.length())
    {
      break;
    }
    else
    {
      if (newoption == "s")
      {
        option += "silent";
      }
      else
      {
        option += newoption;
      }
    }
  }
  while (1);

  XrdOucString in = "mgm.cmd=fileinfo&";

  if (wants_help(savearg.c_str()))
    goto com_fileinfo_usage;

  if (!path.length())
  {
    goto com_fileinfo_usage;

  }
  else
  {
    if (path.beginswith("-"))
    {
      goto com_fileinfo_usage;
    }
    if ((!path.beginswith("fid:")) && (!path.beginswith("fxid:")) && (!path.beginswith("pid:")) && (!path.beginswith("pxid:")) && (!path.beginswith("inode:")))
      path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    if (option.length())
    {
      in += "&mgm.file.info.option=";
      in += option;
    }

    if ((option.find("silent") == STR_NPOS))
    {
      global_retc = output_result(client_user_command(in));
    }
    return (0);
  }

com_fileinfo_usage:
  fprintf(stdout, "usage: fileinfo <path> [--path] [--fxid] [--fid] [--size] [--checksum] [--fullpath] [-m] [--silent] [--env] :  print file information for <path>\n");
  fprintf(stdout, "       fileinfo fxid:<fid-hex>                                           :  print file information for fid <fid-hex>\n");
  fprintf(stdout, "       fileinfo fid:<fid-dec>                                            :  print file information for fid <fid-dec>\n");
  fprintf(stdout, "       fileinfo inode:<fid-dec>                                          :  print file information for inode (decimal)>\n");
  fprintf(stdout, "                                                                 --path  :  selects to add the path information to the output\n");
  fprintf(stdout, "                                                                 --fxid  :  selects to add the hex file id information to the output\n");
  fprintf(stdout, "                                                                 --fid   :  selects to add the base10 file id information to the output\n");
  fprintf(stdout, "                                                                 --size  :  selects to add the size information to the output\n");
  fprintf(stdout, "                                                              --checksum :  selects to add the checksum information to the output\n");
  fprintf(stdout, "                                                              --fullpath :  selects to add the full path information to each replica\n");
  fprintf(stdout, "                                                                  -m     :  print single line in monitoring format\n");
  fprintf(stdout, "                                                                  --env  :  print in OucEnv format\n");
  fprintf(stdout, "                                                                  -s     :  silent - used to run as internal command\n");
  return (0);

}

int
com_file (char* arg1)
{
  XrdOucString savearg = arg1;
  XrdOucString arg = arg1;
  eos::common::StringTokenizer subtokenizer(arg1);
  XrdOucString option = "";
  XrdOucString path = "";
  subtokenizer.GetLine();


  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString tmpArg;

  do
  {
    tmpArg = subtokenizer.GetToken();
    if (tmpArg.beginswith("-"))
    {
      while (tmpArg.replace("-", ""))
      {
      }
      option += tmpArg;
    }
    else
    {
      path = tmpArg;
      break;
    }
  }
  while (1);

  XrdOucString fsid1 = subtokenizer.GetToken();
  XrdOucString fsid2 = subtokenizer.GetToken();

  if ((!path.beginswith("fid:")) && (!path.beginswith("fxid:")))
    path = abspath(path.c_str());

  XrdOucString in = "mgm.cmd=file";

  if (wants_help(savearg.c_str()))
    goto com_file_usage;

  if ((cmd != "drop") && (cmd != "move") && (cmd != "touch") && (cmd != "replicate") && (cmd != "check") && (cmd != "adjustreplica") && (cmd != "info") && (cmd != "layout") && (cmd != "verify") && (cmd != "rename") && (cmd != "copy") && (cmd != "convert") && (cmd != "share") && (cmd != "purge") && (cmd != "version") && (cmd != "versions") )
  {
    goto com_file_usage;
  }

  // convenience function
  if (cmd == "info")
  {
    arg.erase(0, arg.find(" ")+1);
    return com_fileinfo((char*) arg.c_str());
  }

  if (cmd == "rename")
  {
    if (!path.length() || !fsid1.length())
      goto com_file_usage;

    fsid1 = abspath(fsid1.c_str());
    in += "&mgm.path=";
    in += path;
    in += "&mgm.subcmd=rename";
    in += "&mgm.file.source=";
    in += path.c_str();
    in += "&mgm.file.target=";
    in += fsid1.c_str();
  }

  if (cmd == "share")
  {
    if (!path.length() )
      goto com_file_usage;

    path = abspath(path.c_str());
    in += "&mgm.path=";
    in += path;
    in += "&mgm.subcmd=share";
    in += "&mgm.file.expires=";
    unsigned long long expires= (time(NULL) + 28*86400);

    if (fsid1.length()) 
    {
      expires = time(NULL) + eos::common::StringConversion::GetSizeFromString(fsid1);
    }
    char sexpires[1024];
    snprintf(sexpires,sizeof(sexpires)-1,"%llu", expires);
    in += sexpires;
  }

  if (cmd == "touch")
  {
    if (!path.length())
      goto com_file_usage;
    in += "&mgm.path=";
    in += path;
    in += "&mgm.subcmd=touch";
  }

  if (cmd == "drop")
  {
    if (!path.length() || !fsid1.length())
      goto com_file_usage;

    in += "&mgm.subcmd=drop";
    in += "&mgm.path=";
    in += path;
    in += "&mgm.file.fsid=";
    in += fsid1;

    if (fsid2 == "-f")
    {
      in += "&mgm.file.force=1";
    }
    else
    {
      if (fsid2.length())
        goto com_file_usage;
    }
  }

  if (cmd == "move")
  {
    if (!path.length() || !fsid1.length() || !fsid2.length())
      goto com_file_usage;
    in += "&mgm.subcmd=move";
    in += "&mgm.path=";
    in += path;
    in += "&mgm.file.sourcefsid=";
    in += fsid1;
    in += "&mgm.file.targetfsid=";
    in += fsid2;
  }

  if (cmd == "copy")
  {
    XrdOucString dest_path = fsid1;
    if (!path.length() || !dest_path.length())
      goto com_file_usage;
    in += "&mgm.subcmd=copy";
    in += "&mgm.path=";
    in += path;
    if (option.length())
    {
      XrdOucString checkoption=option;
      checkoption.replace("f","");
      checkoption.replace("s","");
      checkoption.replace("c","");
      if (checkoption.length())
        goto com_file_usage;

      in += "&mgm.file.option=";
      in += option;
    }
    dest_path = abspath(dest_path.c_str());
    in += "&mgm.file.target=";
    in += dest_path;
  }

  if (cmd == "convert")
  {
    XrdOucString layout = fsid1;
    XrdOucString space = fsid2;
    if (!path.length())
      goto com_file_usage;
    in += "&mgm.subcmd=convert";
    in += "&mgm.path=";
    in += path;
    if (layout.length())
    {
      in += "&mgm.convert.layout=";
      in += layout;
    }
    if (space.length())
    {
      in += "&mgm.convert.space=";
      in += space;
    }
    if (option == "sync")
    {
      fprintf(stderr, "error: --sync is currently not supported\n");
      goto com_file_usage;
    }
    if (option == "rewrite") 
    {
      in += "&mgm.option=rewrite";
    } 
    else 
    {
    if (option.length())
    {
      goto com_file_usage;
    }
  }
  }

  if (cmd == "replicate")
  {
    if (!path.length() || !fsid1.length() || !fsid2.length())
      goto com_file_usage;
    in += "&mgm.subcmd=replicate";
    in += "&mgm.path=";
    in += path;
    in += "&mgm.file.sourcefsid=";
    in += fsid1;
    in += "&mgm.file.targetfsid=";
    in += fsid2;
  }

  if ((cmd == "purge") || (cmd == "version") )
  {
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=";
    in += cmd;
    in += "&mgm.path=";
    in += path;
    in += "&mgm.purge.version=";
    if (fsid1.length())
      in += fsid1;
    else
      in += "-1";
  }

  if (cmd == "versions")
  {
    if (!path.length())
      goto com_file_usage;
    in += "&mgm.subcmd=";
    in += cmd;
    in += "&mgm.path=";
    in += path;
    in += "&mgm.grab.version=";
    if (fsid1.length())
      in += fsid1;
    else
      in += "-1";
  }

  if (cmd == "adjustreplica")
  {
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=adjustreplica";
    in += "&mgm.path=";
    in += path;
    if (fsid1.length())
    {
      in += "&mgm.file.desiredspace=";
      in += fsid1;
      if (fsid2.length())
      {
        in += "&mgm.file.desiredsubgroup=";
        in += fsid2;
      }
    }
  }

  if (cmd == "layout")
  {
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=layout";
    in += "&mgm.path=";
    in += path;
    if (fsid1 != "-stripes")
      goto com_file_usage;
    if (!fsid2.length())
      goto com_file_usage;

    in += "&mgm.file.layout.stripes=";
    in += fsid2;
  }

  if (cmd == "verify")
  {
    if (!path.length())
      goto com_file_usage;

    XrdOucString option[6];

    in += "&mgm.subcmd=verify";
    in += "&mgm.path=";
    in += path;
    if (fsid1.length())
    {
      if ((fsid1 != "-checksum") && (fsid1 != "-commitchecksum") && (fsid1 != "-commitsize") && (fsid1 != "-commitfmd") && (fsid1 != "-rate"))
      {
        if (fsid1.beginswith("-"))
          goto com_file_usage;

        in += "&mgm.file.verify.filterid=";
        in += fsid1;
        if (fsid2.length())
        {
          option[0] = fsid2;
          option[1] = subtokenizer.GetToken();
          option[2] = subtokenizer.GetToken();
          option[3] = subtokenizer.GetToken();
          option[4] = subtokenizer.GetToken();
          option[5] = subtokenizer.GetToken();
        }
      }
      else
      {
        option[0] = fsid1;
        option[1] = fsid2;
        option[2] = subtokenizer.GetToken();
        option[3] = subtokenizer.GetToken();
        option[4] = subtokenizer.GetToken();
        option[5] = subtokenizer.GetToken();
      }
    }

    for (int i = 0; i < 6; i++)
    {
      if (option[i].length())
      {
        if (option[i] == "-checksum")
        {
          in += "&mgm.file.compute.checksum=1";
        }
        else
        {
          if (option[i] == "-commitchecksum")
          {
            in += "&mgm.file.commit.checksum=1";
          }
          else
          {
            if (option[i] == "-commitsize")
            {
              in += "&mgm.file.commit.size=1";
            }
            else
            {
              if (option[i] == "-commitfmd")
              {
                in += "&mgm.file.commit.fmd=1";
              }
              else
              {
                if (option[i] == "-rate")
                {
                  in += "&mgm.file.verify.rate=";
                  if ((i == 5) || (!option[i + 1].length()))
                    goto com_file_usage;
                  in += option[i + 1];
                  i++;
                  continue;
                }
                else
                {
                  goto com_file_usage;
                }
              }
            }
          }
        }
      }
    }
  }

  if (cmd == "check")
  {
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=getmdlocation";
    in += "&mgm.format=fuse";
    in += "&mgm.path=";
    in += path;

    XrdOucString option = fsid1;

    XrdOucEnv* result = client_user_command(in);
    if (!result)
    {
      fprintf(stderr, "error: getmdlocation query failed\n");
      global_retc = EINVAL;
      return (0);
    }
    int envlen = 0;

    XrdOucEnv* newresult = new XrdOucEnv(result->Env(envlen));
    delete result;

    XrdOucString checksumattribute = "NOTREQUIRED";

    bool consistencyerror = false;
    bool down = false;
    if (envlen)
    {

      XrdOucString checksumtype = newresult->Get("mgm.checksumtype");
      XrdOucString checksum = newresult->Get("mgm.checksum");
      XrdOucString size = newresult->Get("mgm.size");

      if ((option.find("%silent") == STR_NPOS) && (!silent))
      {
        fprintf(stdout, "path=\"%s\" fid=\"%4s\" size=\"%s\" nrep=\"%s\" "
                "checksumtype=\"%s\" checksum=\"%s\"\n",
                path.c_str(), newresult->Get("mgm.fid0"),
                size.c_str(), newresult->Get("mgm.nrep"),
                checksumtype.c_str(), newresult->Get("mgm.checksum"));
      }

      int i = 0;
      XrdOucString inconsistencylable = "";
      int nreplicaonline = 0;

      for (i = 0; i < LayoutId::kSixteenStripe; i++)
      {
        XrdOucString repurl = "mgm.replica.url";
        repurl += i;
        XrdOucString repfid = "mgm.fid";
        repfid += i;
        XrdOucString repfsid = "mgm.fsid";
        repfsid += i;
        XrdOucString repbootstat = "mgm.fsbootstat";
        repbootstat += i;
        XrdOucString repfstpath = "mgm.fstpath";
        repfstpath += i;
        if (newresult->Get(repurl.c_str()))
        {
          // Query
          XrdCl::StatInfo* stat_info = 0;
          XrdCl::XRootDStatus status;
          XrdOucString address = "root://";
          address += newresult->Get(repurl.c_str());
          address += "//dummy";
          XrdCl::URL url(address.c_str());

          if (!url.IsValid())
          {
            fprintf(stderr, "error=URL is not valid: %s", address.c_str());
	    global_retc = EINVAL;
	    return (0);
          }

          //.............................................................................
          // Get XrdCl::FileSystem object
          //.............................................................................
          XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

          if (!fs)
          {
            fprintf(stderr, "error=failed to get new FS object");
	    global_retc = ECOMM;
	    return (0);
          }

          XrdOucString bs = newresult->Get(repbootstat.c_str());
          if (bs != "booted")
          {
            down = true;
          }
          else
          {
            down = false;
          }

          struct eos::fst::Fmd fmd;
          int retc = 0;
          int oldsilent = silent;

          if ((option.find("%silent")) != STR_NPOS)
          {
            silent = true;
          }

          if (down && ((option.find("%force")) == STR_NPOS))
          {
            consistencyerror = true;
            inconsistencylable = "DOWN";

            if (!silent)
            {
              fprintf(stderr, "error: unable to retrieve file meta data from %s [ status=%s ]\n",
                      newresult->Get(repurl.c_str()), bs.c_str());
            }
          }
          else
          {
            // fprintf( stderr,"%s %s %s\n",newresult->Get(repurl.c_str()),
            //          newresult->Get(repfid.c_str()),newresult->Get(repfsid.c_str()) );
            if ((option.find("%checksumattr") != STR_NPOS))
            {
              checksumattribute = "";
              if ((retc = eos::fst::gFmdClient.GetRemoteAttribute(
                                                                  newresult->Get(repurl.c_str()),
                                                                  "user.eos.checksum",
                                                                  newresult->Get(repfstpath.c_str()),
                                                                  checksumattribute)))
              {
                if (!silent)
                {
                  fprintf(stderr, "error: unable to retrieve extended attribute from %s [%d]\n",
                          newresult->Get(repurl.c_str()), retc);
                }
              }
            }

            //..................................................................
            // Do a remote stat using XrdCl::FileSystem
            //..................................................................
            uint64_t rsize;

            status = fs->Stat(newresult->Get(repfstpath.c_str()), stat_info);

            if (!status.IsOK())
            {
              consistencyerror = true;
              inconsistencylable = "STATFAILED";
              rsize = -1;
            }
            else
            {
              rsize = stat_info->GetSize();
            }

            //..................................................................
            // Free memory
            //..................................................................
            delete stat_info;
            delete fs;

            if ((retc = eos::fst::gFmdClient.GetRemoteFmdSqlite(
                                                                newresult->Get(repurl.c_str()),
                                                                newresult->Get(repfid.c_str()),
                                                                newresult->Get(repfsid.c_str()), fmd)))
            {
              if (!silent)
              {
                fprintf(stderr, "error: unable to retrieve file meta data from %s [%d]\n",
                        newresult->Get(repurl.c_str()), retc);
              }
              consistencyerror = true;
              inconsistencylable = "NOFMD";
            }
            else
            {
              XrdOucString cx = fmd.checksum().c_str();

              for (unsigned int k = (cx.length() / 2); k < SHA_DIGEST_LENGTH; k++)
              {
                cx += "00";
              }
              if ((option.find("%size")) != STR_NPOS)
              {
                char ss[1024];
                sprintf(ss, "%" PRIu64, fmd.size());
                XrdOucString sss = ss;
                if (sss != size)
                {
                  consistencyerror = true;
                  inconsistencylable = "SIZE";
                }
                else
                {
                  if (fmd.size() != (unsigned long long) rsize)
                  {
                    if (!consistencyerror)
                    {
                      consistencyerror = true;
                      inconsistencylable = "FSTSIZE";
                    }
                  }
                }
              }

              if ((option.find("%checksum")) != STR_NPOS)
              {
                if (cx != checksum)
                {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUM";
                }
              }

              if ((option.find("%checksumattr") != STR_NPOS))
              {
                if ((checksumattribute.length() < 8) || (!cx.beginswith(checksumattribute)))
                {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUMATTR";
                }
              }

              nreplicaonline++;

              if (!silent)
              {
                fprintf(stdout, "nrep=\"%02d\" fsid=\"%s\" host=\"%s\" fstpath=\"%s\" "
                        "size=\"%" PRIu64 "\" statsize=\"%llu\" checksum=\"%s\"",
                        i, newresult->Get(repfsid.c_str()),
                        newresult->Get(repurl.c_str()),
                        newresult->Get(repfstpath.c_str()),
                        fmd.size(),
                        static_cast<long long> (rsize),
                        cx.c_str());
              }
              if ((option.find("%checksumattr") != STR_NPOS))
              {
                if (!silent)fprintf(stdout, " checksumattr=\"%s\"\n", checksumattribute.c_str());
              }
              else
              {
                if (!silent)fprintf(stdout, "\n");
              }
            }
          }

          if ((option.find("%silent")) != STR_NPOS)
          {
            silent = oldsilent;
          }
        }
        else
        {
          break;
        }
      }

      if ((option.find("%nrep")) != STR_NPOS)
      {
        int nrep = 0;
        int stripes = 0;
        if (newresult->Get("mgm.stripes"))
        {
          stripes = atoi(newresult->Get("mgm.stripes"));
        }
        if (newresult->Get("mgm.nrep"))
        {
          nrep = atoi(newresult->Get("mgm.nrep"));
        }
        if (nrep != stripes)
        {
          consistencyerror = true;
          if (inconsistencylable != "NOFMD")
          {
            inconsistencylable = "REPLICA";
          }
        }
      }

      if ((option.find("%output")) != STR_NPOS)
      {
        if (consistencyerror)
          fprintf(stdout, "INCONSISTENCY %s path=%-32s fid=%s size=%s stripes=%s nrep=%s nrepstored=%d nreponline=%d checksumtype=%s checksum=%s\n", inconsistencylable.c_str(), path.c_str(), newresult->Get("mgm.fid0"), size.c_str(), newresult->Get("mgm.stripes"), newresult->Get("mgm.nrep"), i, nreplicaonline, checksumtype.c_str(), newresult->Get("mgm.checksum"));
      }

      delete newresult;
    }
    else
    {
      fprintf(stderr, "error: couldn't get meta data information\n");
      global_retc = EIO;
      return (0);
    }
    return (consistencyerror);
  }


  if (option.length())
  {
    in += "&mgm.file.option=";
    in += option;
  }

  global_retc = output_result(client_user_command(in));
  return (0);

com_file_usage:
  fprintf(stdout, "Usage: file adjustreplica|check|convert|copy|drop|info|layout|move|purge|rename|replicate|verify|version ...\n");
  fprintf(stdout, "'[eos] file ..' provides the file management interface of EOS.\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "file adjustreplica [--nodrop] <path>|fid:<fid-dec>|fxid:<fid-hex> [space [subgroup]] :\n");
  fprintf(stdout, "                                                  tries to bring a files with replica layouts to the nominal replica level [ need to be root ]\n");
  fprintf(stdout, "file check <path> [%%size%%checksum%%nrep%%checksumattr%%force%%output%%silent] :\n");
  fprintf(stdout, "                                                  retrieves stat information from the physical replicas and verifies the correctness\n");
  fprintf(stdout, "       - %%size                                                       :  return with an error code if there is a mismatch between the size meta data information\n");
  fprintf(stdout, "       - %%checksum                                                   :  return with an error code if there is a mismatch between the checksum meta data information\n");
  fprintf(stdout, "       - %%nrep                                                       :  return with an error code if there is a mismatch between the layout number of replicas and the existing replicas\n");
  fprintf(stdout, "       - %%checksumattr                                               :  return with an error code if there is a mismatch between the checksum in the extended attributes on the FST and the FMD checksum\n");
  fprintf(stdout, "       - %%silent                                                     :  suppresses all information for each replic to be printed\n");
  fprintf(stdout, "       - %%force                                                      :  forces to get the MD even if the node is down\n");
  fprintf(stdout, "       - %%output                                                     :  prints lines with inconsitency information\n");
  fprintf(stdout, "file convert [--sync|--rewrite] <path> [<layout>:<stripes> | <layout-id> | <sys.attribute.name>] [target-space]:\n");
  fprintf(stdout, "                                                                         convert the layout of a file\n");
  fprintf(stdout, "        <layout>:<stripes>   : specify the target layout and number of stripes\n");
  fprintf(stdout, "        <layout-id>          : specify the hexadecimal layout id \n");
  fprintf(stdout, "        <conversion-name>    : specify the name of the attribute sys.conversion.<name> in the parent directory of <path> defining the target layout\n");
  fprintf(stdout, "        <target-space>       : optional name of the target space or group e.g. default or default.3\n");
  fprintf(stdout, "        --sync               : run convertion in synchronous mode (by default conversions are asynchronous) - not supported yet\n");
  fprintf(stdout, "        --rewrite            : run convertion rewriting the file as is creating new copies and dropping old\n");
  fprintf(stdout, "file copy [-f] [-s] [-c] <src> <dst>                                   :  synchronous third party copy from <src> to <dst>\n");
  fprintf(stdout, "         <src>                                                         :  source can be a file or a directory\n");
  fprintf(stdout, "         <dst>                                                         :  destination can be a file (if source is a file) or a directory\n");
  fprintf(stdout,"                                                                     -f :  force overwrite\n");
  fprintf(stdout,"                                                                     -c :  clone the file (keep ctime,mtime)\n");
  fprintf(stdout, "file drop <path> <fsid> [-f] :\n");
  fprintf(stdout, "                                                  drop the file <path> from <fsid> - force removes replica without trigger/wait for deletion (used to retire a filesystem) \n");
  fprintf(stdout, "file info <path> :\n");
  fprintf(stdout, "                                                  convenience function aliasing to 'fileinfo' command\n");
  fprintf(stdout, "file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -stripes <n> :\n");
  fprintf(stdout, "                                                  change the number of stripes of a file with replica layout to <n>\n");
  fprintf(stdout, "file move <path> <fsid1> <fsid2> :\n");
  fprintf(stdout, "                                                  move the file <path> from  <fsid1> to <fsid2>\n");
  fprintf(stdout, "file purge <path> [purge-version] :\n");
  fprintf(stdout, "                                                  keep maximumg <purge-version> versions of a file. If not specified apply the attribute definition from sys.versioning.\n");
  fprintf(stdout, "file rename <old> <new> :\n");
  fprintf(stdout, "                                                  rename from <old> to <new> name (works for files and directories!).\n ");
  fprintf(stdout, "file replicate <path> <fsid1> <fsid2> :\n");
  fprintf(stdout, "                                                  replicate file <path> part on <fsid1> to <fsid2>\n");

  fprintf(stdout, "file touch <path> :\n");
  fprintf(stdout, "                                                   create a 0-size/0-replica file if <path> does not exist or update modification time of an existing file to the present time\n");

  fprintf(stdout, "file verify <path>|fid:<fid-dec>|fxid:<fid-hex> [<fsid>] [-checksum] [-commitchecksum] [-commitsize] [-rate <rate>] : \n");
  fprintf(stdout, "                                                  verify a file against the disk images\n");
  fprintf(stdout, "       <fsid>          : verifies only the replica on <fsid>\n");
  fprintf(stdout, "       -checksum       : trigger the checksum calculation during the verification process\n");
  fprintf(stdout, "       -commitchecksum : commit the computed checksum to the MGM\n");
  fprintf(stdout, "       -commitsize     : commit the file size to the MGM\n");
  fprintf(stdout, "       -rate <rate>    : restrict the verification speed to <rate> per node\n");
  fprintf(stdout, "file version <path> [purge-version] :\n");
  fprintf( stdout, "                                                 create a new version of a file by cloning\n");

  fprintf(stdout, "file versions [grab-version] :\n");
  fprintf(stdout, "                                                  list versions of a file\n");
  fprintf(stdout, "                                                  grab a version of a file\n");
  fprintf(stdout, "        <purge-version>: defines the max. number of versions to keep\n");
  fprintf(stdout, "                         if not specified it will add a new version without purging any previous version\n");
  fprintf(stdout, "file share <path> [lifetime] :\n");
  fprintf(stdout, "       <path>          : path to create a share link\n");
  fprintf(stdout,"        <lifetime>      : validity time of the share link like 1, 1s, 1d, 1w, 1mo, 1y, ... default is 28d\n");
  fprintf(stdout, "\n");

  return (0);
}

