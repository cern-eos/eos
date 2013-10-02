// ----------------------------------------------------------------------
// File: com_dropbox.cc
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
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/

/* Dropbox interface */
int
com_dropbox (char *arg)
{
  XrdOucString homedirectory = getenv("HOME");
  XrdOucString configdirectory = homedirectory + "/.eosdropboxd";
  ConsoleCliCommand *parsedCmd, *dropboxCmd, *addSubCmd, *rmSubCmd, *lsSubCmd,
    *stopSubCmd, *startSubCmd;

  if (!getenv("HOME"))
  {
    fprintf(stderr, "Error: your HOME environment variable is not defined - I need that!\n");
    global_retc = -1;
    return (0);
  }

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  dropboxCmd = new ConsoleCliCommand("dropbox", "provides DropBox "
                                     "functionality for EOS");
  dropboxCmd->addOption(helpOption);

  addSubCmd = new ConsoleCliCommand("add", "add DropBox configuration to "
                                    "synchronize from <eos-dir> to "
                                    "<local-dir>");
  addSubCmd->addOption(helpOption);
  addSubCmd->addOptions({{"eos-dir", "", 1, 1, "<eos-dir>", true},
                         {"local-dir", "", 2, 1, "<local-dir>", true}
                        });
  dropboxCmd->addSubcommand(addSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "remove dropbox configuration to "
                                   "synchronize from <eos-dir>");
  rmSubCmd->addOption(helpOption);
  rmSubCmd->addOption({"eos-dir", "", 1, 1, "<eos-dir>", true});
  dropboxCmd->addSubcommand(rmSubCmd);

  lsSubCmd = new ConsoleCliCommand("ls", "list configured DropBox daemons "
                                   "and their status");
  lsSubCmd->addOption(helpOption);
  dropboxCmd->addSubcommand(lsSubCmd);

  stopSubCmd = new ConsoleCliCommand("stop", "stop the DropBox daemon for all "
                                     "configured dropbox directories");
  stopSubCmd->addOption(helpOption);
  dropboxCmd->addSubcommand(stopSubCmd);

  startSubCmd = new ConsoleCliCommand("start", "start the DropBox daemon for "
                                      "all configured dropbox directories");
  startSubCmd->addOption(helpOption);
  startSubCmd->addOption({"resync", "resync the local directory from scratch "
                          "from the remote directory", "--resync,-r"});
  dropboxCmd->addSubcommand(startSubCmd);

  addHelpOptionRecursively(dropboxCmd);

  parsedCmd = dropboxCmd->parse(arg);

  if (parsedCmd == dropboxCmd)
  {
    if (!checkHelpAndErrors(dropboxCmd))
      dropboxCmd->printUsage();
    goto bailout;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == addSubCmd)
  {
    XrdOucString remotedir = addSubCmd->getValue("eos-dir").c_str();
    XrdOucString localdir = addSubCmd->getValue("local-dir").c_str();

    if (localdir.beginswith("/eos"))
    {
      fprintf(stderr, "error: the local directory can not start with /eos!\n");
      global_retc = -1;
      goto bailout;
    }

    if (!remotedir.beginswith("/eos"))
    {
      fprintf(stderr, "error: the remote directory has to start with /eos!\n");
      global_retc = -1;
      goto bailout;
    }

    if ((!remotedir.endswith("/dropbox/") && (!remotedir.endswith("/dropbox"))))
    {
      fprintf(stderr, "error: your remote directory has to be named '/dropbox'\n");
      global_retc = -1;
      goto bailout;
    }

    XrdOucString configdummy = configdirectory + "/dummy";
    eos::common::Path cPath(configdummy.c_str());
    if (!cPath.MakeParentPath(S_IRUSR | S_IWUSR))
    {
      fprintf(stderr, "error: cannot create %s\n", configdirectory.c_str());
      global_retc = -EPERM;
      goto bailout;
    }

    XrdOucString localdirdummy = localdir + "/dummy";
    eos::common::Path lPath(localdirdummy.c_str());
    if ((!lPath.MakeParentPath(S_IRUSR | S_IWUSR)) || (access(localdir.c_str(), W_OK | X_OK)))
    {
      fprintf(stderr, "error: cannot access %s\n", localdirdummy.c_str());
      global_retc = -EPERM;
      goto bailout;
    }

    XrdOucString localdircontracted = localdir;
    while (localdircontracted.replace("/", "::"))
    {
    }

    XrdOucString newconfigentry = configdirectory + "/";
    newconfigentry += localdircontracted;

    struct stat buf;
    if (!stat(newconfigentry.c_str(), &buf))
    {
      fprintf(stderr, "error: there is already a configuration for the local directory %s\n", localdir.c_str());
      global_retc = EEXIST;
      goto bailout;
    }

    // we store the remote dir configuration in the target of a symlink!
    if (symlink(remotedir.c_str(), newconfigentry.c_str()))
    {
      fprintf(stderr, "error: failed to symlink the new configuration entry %s\n", localdir.c_str());
      global_retc = errno;
      goto bailout;
    }

    fprintf(stderr, "success: created dropbox configuration from %s |==> %s\n", remotedir.c_str(), localdir.c_str());
    global_retc = 0;
    goto bailout;
  }

  if (parsedCmd == startSubCmd)
  {
    std::string command("eosdropboxd");

    if (startSubCmd->hasValue("resync"))
      command += " --resync";

    int rc = system(command.c_str());

    if (WEXITSTATUS(rc))
    {
      fprintf(stderr, "error: failed to run %s\n", command.c_str());
    }

    global_retc = 0;
    goto bailout;
  }

  if (parsedCmd == rmSubCmd)
  {

    global_retc = 0;
    goto bailout;
  }

  if (parsedCmd == stopSubCmd)
  {
    int rc = system("pkill -15 eosdropboxd >& /dev/null");
    if (WEXITSTATUS(rc))
    {
      fprintf(stderr, "warning: didn't kill any esodropboxd");
    }
    global_retc = 0;
    goto bailout;
  }

  if (parsedCmd == lsSubCmd)
  {
    DIR* dir = opendir(configdirectory.c_str());
    if (!dir)
    {
      fprintf(stderr, "error: cannot opendir %s\n", configdirectory.c_str());
      global_retc = errno;
      goto bailout;
    }
    struct dirent* entry = 0;
    while ((entry = readdir(dir)))
    {
      XrdOucString sentry = entry->d_name;
      XrdOucString configentry = configdirectory;
      configentry += "/";
      if ((sentry == ".") || (sentry == ".."))
      {
        continue;
      }
      configentry += sentry;
      char buffer[4096];
      ssize_t nr = readlink(configentry.c_str(), buffer, sizeof (buffer));
      if (nr < 0)
      {
        fprintf(stderr, "error: unable to read link %s errno=%d\n", configentry.c_str(), errno);
      }
      else
      {
        buffer[nr] = 0;
        while (sentry.replace("::", "/"))
        {
        }
        fprintf(stdout, "[sync] %32s |==> %-32s\n", buffer, sentry.c_str());
      }
    }
    closedir(dir);
    global_retc = 0;
  }

 bailout:
  delete dropboxCmd;

  return (0);
}
