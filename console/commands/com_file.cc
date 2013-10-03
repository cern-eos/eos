// ----------------------------------------------------------------------
// File: com_file.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
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
#include "console/ConsoleMain.hh"
#include "fst/FmdSqlite.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include <algorithm>
#include <climits>
/*----------------------------------------------------------------------------*/

using namespace eos::common;

/* File handling */

static XrdOucString
handlePath(const std::string &pathToHandle)
{
  XrdOucString path = pathToHandle.c_str();

  if (path[0] == '"')
    path.erase(0, 1);
  if (path[path.length() - 1] == '"')
    path.erase(path.length() - 1, 1);

  path.replace("\\ ", " ");

  if ((!path.beginswith("fid:")) && (!path.beginswith("fxid:")))
    path = abspath(path.c_str());

  return path;
}

/* Get file information */
int
com_fileinfo (char* arg1)
{
  XrdOucString path("");
  std::string option("");
  XrdOucString in = "mgm.cmd=fileinfo&";

  ConsoleCliCommand *fileInfoCmd =
    new ConsoleCliCommand("fileinfo", "print file information for <path>");
  fileInfoCmd->addOption({"path", "the path or file id; giving fxid:<fid-hex> "
                          "prints the file information for fid <fid-hex>; "
                          "giving fid:<fid-dec> prints the file infomation for "
                          "<fid-dec>", 1, 1, "<path>", true});
  fileInfoCmd->addOptions({{"help", "print help", "-h,--help"},
                           {"path-info", "adds the path information to the "
                            "output", "--path"},
                           {"fxid", "adds the hex file id information to the "
                            "output", "--fxid"},
                           {"fid", "adds the base10 file id information to the output",
                            "--fid"},
                           {"size", "adds the size information to the output",
                            "--size"},
                           {"checksum", "adds the checksum information to the "
                            "output", "--checksum"},
                           {"fullpath", "full path information to each "
                            "replica", "--fullpath"},
                           {"monitor", "print single line in monitoring format",
                             "-m,--monitor"},
                           {"env", "print in OucEnv format", "--env"},
                           {"silent", "suppresses all information for each "
                            "replic to be printed", "-s,--silent"}
                          });
  fileInfoCmd->parse(arg1);

  if (checkHelpAndErrors(fileInfoCmd))
    goto bailout;

  path = handlePath(fileInfoCmd->getValue("path"));
  in += "mgm.path=" + path;

  if (fileInfoCmd->hasValue("silent"))
    goto bailout;

  if (fileInfoCmd->hasValue("path-info"))
    option += "--path";
  if (fileInfoCmd->hasValue("fxid"))
    option += "--fxid";
  if (fileInfoCmd->hasValue("fid"))
    option += "--fid";
  if (fileInfoCmd->hasValue("size"))
    option += "-size";
  if (fileInfoCmd->hasValue("checksum"))
    option += "--checksum";
  if (fileInfoCmd->hasValue("fullpath"))
    option += "--fullpath";
  if (fileInfoCmd->hasValue("monitor"))
    option += "-m";
  if (fileInfoCmd->hasValue("env"))
    option += "--env";

  if (option.length())
  {
    in += "&mgm.file.info.option=";
    in += option.c_str();
  }

  global_retc = output_result(client_user_command(in));

 bailout:
  if (fileInfoCmd->parent())
    delete fileInfoCmd->parent();
  else
    delete fileInfoCmd;

  return (0);
}

int
com_file (char* arg1)
{
  XrdOucString savearg = arg1;
  XrdOucString arg = arg1;
  XrdOucString option = "";
  XrdOucString path("");
  XrdOucString in = "mgm.cmd=file";
  int ret = 0;
  ConsoleCliCommand *parsedCmd, *fileCmd, *touchSubCmd, *copySubCmd,
    *renameSubCmd, *replicateSubCmd, *moveSubCmd, *adjustReplicaSubCmd,
    *dropSubCmd, *layoutSubCmd, *verifySubCmd, *convertSubCmd, *checkSubCmd,
    *infoSubCmd;

  fileCmd = new ConsoleCliCommand("file", "file related tools");

  touchSubCmd = new ConsoleCliCommand("touch", "create a 0-size/0-replica file "
                                      "if <path> does not exist or update "
                                      "modification time of an existing file "
                                      "to the present time");
  touchSubCmd->addOptions({{"path", "", 1, 1, "<path>", true}});
  fileCmd->addSubcommand(touchSubCmd);

  copySubCmd = new ConsoleCliCommand("copy", "synchronous third party copy");
  copySubCmd->addOptions({{"force", "overwrite <dst> if it is an existin file",
                           "-f"},
                          {"silent", "silent mode", "-s"}
                         });
  copySubCmd->addOptions({{"source", "a file or a directory to be copied",
                           1, 1, "<src>", true},
                          {"destination", "the destination of the copy "
                           "(if given a directory, it will copy <source> "
                           "to that directory)",
                           2, 1, "<dst>", true}
                          });
  fileCmd->addSubcommand(copySubCmd);

  renameSubCmd = new ConsoleCliCommand("rename", "rename files and directories. "
                                       "This only works for the 'root' user "
                                       "and the renamed file/directory has to "
                                       "stay in the same parent directory");
  renameSubCmd->addOptions({{"old", "the file or directory to be renamed",
                           1, 1, "<old>", true},
                          {"new", "the new name",
                           2, 1, "<new>", true}
                          });
  fileCmd->addSubcommand(renameSubCmd);

  replicateSubCmd = new ConsoleCliCommand("replicate", "replicate file <path> "
                                          "part on <fsid1> to <fsid2>");
  replicateSubCmd->addOptions({{"path", "", 1, 1, "<path>", true},
                               {"fsid1", "", 2, 1, "<fsid1>", true},
                               {"fsid2", "", 3, 1, "<fsid2>", true}
                              });
  fileCmd->addSubcommand(replicateSubCmd);

  moveSubCmd = new ConsoleCliCommand("move", "move the file <path> from "
                                     "<fsid1> to <fsid2>");
  moveSubCmd->addOptions({{"path", "", 1, 1, "<path>", true},
                          {"fsid1", "", 2, 1, "<fsid1>", true},
                          {"fsid2", "", 3, 1, "<fsid2>", true}
                         });
  fileCmd->addSubcommand(moveSubCmd);

  adjustReplicaSubCmd = new ConsoleCliCommand("adjustreplica", "tries to bring "
                                              "a files with replica layouts to "
                                              "the nominal replica level (needs "
                                              "to be root)");
  adjustReplicaSubCmd->addOptions({{"path", "", 1, 1,
                                    "<path>|fid:<fid-dec>|fxid:<fid-hex>", true},
                                   {"space", "", 2, 1, "<space>", false},
                                   {"subgroup", "", 3, 1, "<subgroup>", false}
                                  });
  fileCmd->addSubcommand(adjustReplicaSubCmd);

  dropSubCmd = new ConsoleCliCommand("drop", "drop the file <path> from "
                                     "<fsid>");
  dropSubCmd->addOption({"force", "removes replica without trigger/wait for "
                         "deletion (used to retire a filesystem)",
                         "-f,--force"});
  dropSubCmd->addOptions({{"path", "", 1, 1, "<path>", true},
                          {"fsid", "", 2, 1, "<fsid>", true}
                         });
  fileCmd->addSubcommand(dropSubCmd);

  layoutSubCmd = new ConsoleCliCommand("layout", "change the number of stripes "
                                       "of a file with replica layout to "
                                       "<stripes>");
  layoutSubCmd->addOption({"path", "", 1, 1,
                           "<path>|<fid:<fid-desc>|<fxid:fid-desc>", true});
  CliPositionalOption stripes("stripes", "", 2, 1, "<stripes>", true);
  stripes.addEvalFunction(optionIsIntegerEvalFunc, 0);
  stripes.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  layoutSubCmd->addOption(stripes);
  fileCmd->addSubcommand(layoutSubCmd);

  verifySubCmd = new ConsoleCliCommand("verify", "verify a file against the "
                                       "disk images");
  verifySubCmd->addOptions({{"path", "", 1, 1,
                             "<path>|<fid:<fid-desc>|<fxid:fid-desc>", true},
                            {"filter", "restrict the verification to replicas "
                             "on the filesystem <fs-id>", 2, 1,
                             "<fs-id>", false}
                           });
  verifySubCmd->addOptions({{"checksum", "trigger the checksum calculation "
                             "during the verification process", "--checksum"},
                            {"commit-checksum", "commit the computed checksum "
                             "to the MGM", "--commit-checksum"},
                            {"commit-size", "commit the file size to the MGM",
                             "--commit-size"},
                            {"commit-fmd", "commit the file metadata to the MGM",
                             "--commit-fmd"}
                           });
  CliOptionWithArgs rate("rate",
                         "restrict the verification speed to <rate> per node",
                         "-r,--rate=", "<rate>", false);
  rate.addEvalFunction(optionIsFloatEvalFunc, 0);
  verifySubCmd->addOption(rate);
  fileCmd->addSubcommand(verifySubCmd);

  convertSubCmd = new ConsoleCliCommand("convert", "convert the layout of a "
                                        "file");
  convertSubCmd->addGroupedOptions({{"sync", "run convertion in synchronous "
                                     "mode (by default conversions are "
                                     "asynchronous)", "--sync"},
                                    {"rewrite", "run convertion rewriting the "
                                     "file as is creating new copies and "
                                     "dropping old", "--rewrite"}
                                   })->setRequired(false);
  convertSubCmd->addGroupedOptions({{"layout", "specify the hexadecimal layout "
                                     "id", "--layout-hex-id=", "<id>", true},
                                    {"layout-stripes", "specify the target "
                                     "layout and number of stripes",
                                     "--layout-with-stripes=",
                                     "<layout>:<stripes>", true},
                                    {"attr", "specify the name of the "
                                     "attribute sys.conversion.<name> in the "
                                     "parent directory of <path> defining the "
                                     "target layout", "--attr-name",
                                     "<sys.attribute.name>", true}
                                   });
  convertSubCmd->addOptions({{"path", "", 1, 1, "<path>", true},
                             {"space", "name of the target space or group e.g. "
                              "default or default.3", 2, 1, "<target-space>",
                              false}
                            });
  fileCmd->addSubcommand(convertSubCmd);

  checkSubCmd = new ConsoleCliCommand("check", "retrieves stat information "
                                      "from the physical replicas and verifies "
                                      "the correctness");
  checkSubCmd->addOption({"path", "", 1, 1, "<path>", true});
  checkSubCmd->addOptions({{"size", "return with an error code if there is a "
                            "mismatch between the size meta data information", "--size"},
                           {"checksum", "return with an error code if there "
                            "is a mismatch between the checksum meta data "
                            "information", "-c,--checksum"},
                           {"nr-rep", "return with an error code if there is "
                            "a mismatch between the layout number of replicas "
                            "and the existing replicas", "-n,--nr-replicas"},
                           {"checksum-attr", "return with an error code if "
                            "there is a mismatch between the checksum in the "
                            "extended attributes on the FST and the FMD "
                            "checksum", "-t,--checksum-attr"},
                           {"silent", "suppresses all information for each "
                            "replic to be printed", "-s,--silent"},
                           {"force", "forces to get the MD even if the node "
                            "is down", "-f,--force"},
                           {"output", "prints lines with inconsitency "
                            "information", "-o,--output"}
                          });
  fileCmd->addSubcommand(checkSubCmd);

  infoSubCmd = new ConsoleCliCommand("info", "alias command of 'fileinfo'");
  fileCmd->addSubcommand(infoSubCmd);

  addHelpOptionRecursively(fileCmd);

  parsedCmd = fileCmd->parse(std::string(arg1));

  if (parsedCmd == fileCmd && !fileCmd->hasValues())
  {
    fileCmd->printUsage();
    goto bailout;
  }

  if (parsedCmd != infoSubCmd && checkHelpAndErrors(parsedCmd))
    goto bailout;

  // convenience function
  if (parsedCmd == infoSubCmd)
  {
    return com_fileinfo(arg1 + infoSubCmd->name().length());
  }

  if (parsedCmd == renameSubCmd)
  {
    path = handlePath(renameSubCmd->getValue("old"));
    in += "&mgm.path=";
    in += path;
    in += "&mgm.subcmd=rename";
    in += "&mgm.file.source=";
    in += path;
    in += "&mgm.file.target=";
    in += handlePath(renameSubCmd->getValue("new"));
  }
  else if (parsedCmd == touchSubCmd)
  {
    path = handlePath(touchSubCmd->getValue("path"));
    in += "&mgm.path=" + path;
    in += "&mgm.subcmd=touch";
  }
  else if (parsedCmd == dropSubCmd)
  {
    path = handlePath(touchSubCmd->getValue("path"));
    in += "&mgm.subcmd=drop";
    in += "&mgm.path=" + path;
    in += "&mgm.file.fsid=";
    in += handlePath(touchSubCmd->getValue("fsid1"));

    if (touchSubCmd->hasValue("force"))
    {
      in += "&mgm.file.force=1";
    }
  }
  else if (parsedCmd == copySubCmd)
  {
    path = handlePath(copySubCmd->getValue("source").c_str());
    in += "&mgm.subcmd=copy";
    in += "&mgm.path=" + path;

    if (copySubCmd->hasValue("force"))
      option += "f";
    if (copySubCmd->hasValue("silent"))
      option += "s";

    if (option.length() > 0)
      in += "&mgm.file.option=" + option;

    in += "&mgm.file.target=" + handlePath(copySubCmd->getValue("destination"));
  }
  else if (parsedCmd == convertSubCmd)
  {
    path = handlePath(convertSubCmd->getValue("path"));
    in += "&mgm.subcmd=convert";
    in += "&mgm.path=" + path;

    in += "&mgm.convert.layout=";
    if (convertSubCmd->hasValue("layout"))
    {
      in += convertSubCmd->getValue("layout").c_str();
    }
    else if (convertSubCmd->hasValue("layout-stripes"))
    {
      in += convertSubCmd->getValue("layout-stripes").c_str();
    }
    else if (convertSubCmd->hasValue("attr"))
    {
      in += convertSubCmd->getValue("attr").c_str();
    }

    if (convertSubCmd->hasValue("space"))
    {
      in += "&mgm.convert.space=";
      in += convertSubCmd->getValue("space").c_str();
    }

    if (convertSubCmd->hasValue("sync"))
    {
      fprintf(stderr, "Error: --sync is currently not supported\n");

      goto bailout;
    }

    if (convertSubCmd->hasValue("rewrite"))
    {
      in += "&mgm.option=rewrite";
    }
  }
  else if (parsedCmd == replicateSubCmd || parsedCmd == moveSubCmd)
  {
    in += "&mgm.subcmd=replicate";

    if (parsedCmd == replicateSubCmd)
      in += "replicate";
    else
      in += "move";

    in += "&mgm.path=";
    in += handlePath(replicateSubCmd->getValue("path"));
    in += "&mgm.file.sourcefsid=";
    in += handlePath(replicateSubCmd->getValue("fsid1"));
    in += "&mgm.file.targetfsid=";
    in += handlePath(replicateSubCmd->getValue("fsid2"));
  }
  else if (parsedCmd == adjustReplicaSubCmd)
  {
    in += "&mgm.subcmd=adjustreplica";
    in += "&mgm.path=";
    in += handlePath(replicateSubCmd->getValue("path"));

    if (replicateSubCmd->hasValue("space"))
    {
      in += "&mgm.file.desiredspace=";
      in += handlePath(replicateSubCmd->getValue("space"));
    }

    if (replicateSubCmd->hasValue("subgroup"))
    {
      in += "&mgm.file.desiredsubgroup=";
      in += handlePath(replicateSubCmd->getValue("subgroup"));
    }
  }
  else if (parsedCmd == layoutSubCmd)
  {
    path = handlePath(layoutSubCmd->getValue("path"));
    in += "&mgm.subcmd=layout";
    in += "&mgm.path=" + path;
    in += "&mgm.file.layout.stripes=";
    in += layoutSubCmd->getValue("stripes").c_str();
  }
  else if (parsedCmd == verifySubCmd)
  {
    path = handlePath(verifySubCmd->getValue("path"));
    in += "&mgm.subcmd=verify";
    in += "&mgm.path=" + path;

    if (verifySubCmd->hasValue("filter"))
    {
      in += "&mgm.file.verify.filterid=";
      in += verifySubCmd->getValue("filter").c_str();
    }
    if (verifySubCmd->hasValue("checksum"))
      in += "&mgm.file.compute.checksum=1";
    if (verifySubCmd->hasValue("commit-checksum"))
      in += "&mgm.file.commit.checksum=1";
    if (verifySubCmd->hasValue("commit-size"))
      in += "&mgm.file.commit.size=1";
    if (verifySubCmd->hasValue("commit-fmd"))
      in += "&mgm.file.commit.fmd=1";
    if (verifySubCmd->hasValue("rate"))
    {
      in += "&mgm.file.verify.rate=";
      in += verifySubCmd->getValue("rate").c_str();
    }
  }
  else if (parsedCmd == checkSubCmd)
  {
    path = handlePath(checkSubCmd->getValue("path"));
    in += "&mgm.subcmd=getmdlocation";
    in += "&mgm.path=" + path;

    XrdOucEnv* result = client_user_command(in);

    if (!result)
    {
      fprintf(stderr, "Error: getmdlocation query failed\n");
      ret = EINVAL;
      goto bailout;
    }

    int envlen = 0;
    XrdOucEnv* newresult = new XrdOucEnv(result->Env(envlen));
    delete result;

    XrdOucString checksumattribute = "NOTREQUIRED";

    bool consistencyerror = false;
    bool down = false;
    if (!newresult->Get("mgm.proc.stderr"))
    {

      XrdOucString checksumtype = newresult->Get("mgm.checksumtype");
      XrdOucString checksum = newresult->Get("mgm.checksum");
      XrdOucString size = newresult->Get("mgm.size");

      if (!checkSubCmd->hasValue("silent"))
      {
        fprintf(stdout, "path=\"%-32s\" fid=\"%4s\" size=\"%s\" nrep=\"%s\" "
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
            ret = EINVAL;
            goto bailout;
          }

          //.............................................................................
          // Get XrdCl::FileSystem object
          //.............................................................................
          XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

          if (!fs)
          {
            fprintf(stderr, "error=failed to get new FS object");
            ret = ECOMM;
            goto bailout;
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

          silent = checkSubCmd->hasValue("silent");

          if (down && checkSubCmd->hasValue("force"))
          {
            consistencyerror = true;
            inconsistencylable = "DOWN";

            if (!silent)
            {
              fprintf(stderr, "Error: unable to retrieve file meta data from %s [ status=%s ]\n",
                      newresult->Get(repurl.c_str()), bs.c_str());
            }
          }
          else
          {
            // fprintf( stderr,"%s %s %s\n",newresult->Get(repurl.c_str()),
            //          newresult->Get(repfid.c_str()),newresult->Get(repfsid.c_str()) );
            if (checkSubCmd->hasValue("checksum-attr"))
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
                  fprintf(stderr, "Error: unable to retrieve extended attribute from %s [%d]\n",
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
                fprintf(stderr, "Error: unable to retrieve file meta data from %s [%d]\n",
                        newresult->Get(repurl.c_str()), retc);
              }
              consistencyerror = true;
              inconsistencylable = "NOFMD";
            }
            else
            {
              XrdOucString cx = fmd.checksum.c_str();

              for (unsigned int k = (cx.length() / 2); k < SHA_DIGEST_LENGTH; k++)
              {
                cx += "00";
              }
              if (checkSubCmd->hasValue("size"))
              {
                char ss[1024];
                sprintf(ss, "%llu", fmd.size);
                XrdOucString sss = ss;
                if (sss != size)
                {
                  consistencyerror = true;
                  inconsistencylable = "SIZE";
                }
                else
                {
                  if (fmd.size != (unsigned long long) rsize)
                  {
                    if (!consistencyerror)
                    {
                      consistencyerror = true;
                      inconsistencylable = "FSTSIZE";
                    }
                  }
                }
              }

              if (checkSubCmd->hasValue("checksum"))
              {
                if (cx != checksum)
                {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUM";
                }
              }

              if (checkSubCmd->hasValue("checksum-attr"))
              {
                if ((checksumattribute.length() < 8) ||
                    (!cx.beginswith(checksumattribute)))
                {
                  consistencyerror = true;
                  inconsistencylable = "CHECKSUMATTR";
                }
              }

              nreplicaonline++;

              if (!silent)
              {
                fprintf(stdout, "nrep=\"%02d\" fsid=\"%s\" host=\"%s\" "
                        "fstpath=\"%s\" size=\"%llu\" statsize=\"%llu\" "
                        "checksum=\"%s\"",
                        i, newresult->Get(repfsid.c_str()),
                        newresult->Get(repurl.c_str()),
                        newresult->Get(repfstpath.c_str()),
                        fmd.size,
                        static_cast<long long> (rsize),
                        cx.c_str());
              }
              if (checkSubCmd->hasValue("checksum-attr"))
              {
                if (!silent)
                  fprintf(stdout, " checksumattr=\"%s\"\n",
                          checksumattribute.c_str());
              }
              else
              {
                if (!silent)fprintf(stdout, "\n");
              }
            }
          }

          if (checkSubCmd->hasValue("silent"))
          {
            silent = oldsilent;
          }
        }
        else
        {
          break;
        }
      }

      if (checkSubCmd->hasValue("nr-rep"))
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

      if (checkSubCmd->hasValue("output"))
      {
        if (consistencyerror)
          fprintf(stdout, "INCONSISTENCY %s path=%-32s fid=%s size=%s "
                  "stripes=%s nrep=%s nrepstored=%d nreponline=%d "
                  "checksumtype=%s checksum=%s\n",
                  inconsistencylable.c_str(), path.c_str(),
                  newresult->Get("mgm.fid0"), size.c_str(),
                  newresult->Get("mgm.stripes"), newresult->Get("mgm.nrep"),
                  i, nreplicaonline, checksumtype.c_str(),
                  newresult->Get("mgm.checksum"));
      }

      delete newresult;
    }
    else
    {
      fprintf(stderr, "Error: %s", newresult->Get("mgm.proc.stderr"));
    }

    ret = consistencyerror;
    goto bailout;
  }

  if (checkSubCmd->hasValue("size"))
    option += "%size";
  if (checkSubCmd->hasValue("checksum"))
    option += "%checksum";
  if (checkSubCmd->hasValue("nr-rep"))
    option += "%nrep";
  if (checkSubCmd->hasValue("checksum-attr"))
    option += "%checksumattr";
  if (checkSubCmd->hasValue("silent"))
    option += "%silent";
  if (checkSubCmd->hasValue("force"))
    option += "%force";
  if (checkSubCmd->hasValue("output"))
    option += "%output";

  if (option.length())
  {
    in += "&mgm.file.option=";
    in += option;
  }

  global_retc = output_result(client_user_command(in));

 bailout:
  delete fileCmd;

  return ret;
}

