// ----------------------------------------------------------------------
// File: com_transfers.cc
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
/*----------------------------------------------------------------------------*/

/* Control-C handler for interactive transfers */

bool txcancel = false;

void
txcancel_handler (int)
{
  txcancel = true;
  signal(SIGINT, exit_handler);
}

static bool
checkUrlValidity(const CliOptionWithArgs *option,
                 std::vector<std::string> &args,
                 std::string **error,
                 void *userData)
{
  const std::string &url = args[0];
  const char *validUrlPrefixes[] = {"root://", "as3://", "gsiftp://",
                                    "http://", "https://", "/eos/", 0};
  std::string prefixesRepr("");
  for (int i = 0; validUrlPrefixes[i]; i++)
  {
    const int prefixLength = strlen(validUrlPrefixes[i]);
    if (url.compare(0, prefixLength, validUrlPrefixes[i]) == 0)
      return true;

    if (i != 0)
      prefixesRepr += "|";

    prefixesRepr += validUrlPrefixes[i];
  }

  *error = new std::string("Error: Option " + option->repr() + " needs to start "
                           "with one of these prefixes: " + prefixesRepr);

  return false;
}

/* Transfer Interface */
int
com_transfer (char* argin)
{
  XrdOucString rate = "0";
  XrdOucString streams = "0";
  XrdOucString group = "";
  XrdOucString in = "mgm.cmd=transfer&mgm.subcmd=";
  XrdOucString option = "";
  ConsoleCliCommand *parsedCmd, *transferCmd, *submitSubCmd, *cancelSubCmd,
    *lsSubCmd, *enableSubCmd, *disableSubCmd, *resetSubCmd, *clearSubCmd,
    *resubmitSubCmd, *killSubCmd, *purgeSubCmd;

  transferCmd = new ConsoleCliCommand("transfer", "provides the transfer "
                                      "interface of EOS");

  submitSubCmd = new ConsoleCliCommand("submit", "transfer a file from URL1 to "
                                       "URL2");
  CliOptionWithArgs rateOption("rate", "limit the transfer rate to <rate>",
                               "--rate=", "<rate>", false);
  rateOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  CliOptionWithArgs streamsOption("streams", "use <#> parallel streams",
                                  "--streams=", "<streams>", false);
  streamsOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  submitSubCmd->addOption(rateOption);
  submitSubCmd->addOption(streamsOption);
  submitSubCmd->addOption({"group", "set the group name for this transfer",
                           "--group=", "<group-name>", false});
  submitSubCmd->addOptions(std::vector<CliOption>
                           {{"sync", "transfer synchronously", "--sync"},
                            {"silent", "run in silent mode", "--silent"},
                            {"no-progress", "don't show the progress "
                             "bar", "--no-progress,-n"}
                           });
  CliPositionalOption url1Option("url1", "", 1, 1, "<URL-1>", true);
  url1Option.addEvalFunction(checkUrlValidity, 0);
  CliPositionalOption url2Option("url2", "", 2, 1, "<URL-2>", true);
  url2Option.addEvalFunction(checkUrlValidity, 0);
  submitSubCmd->addOption(url1Option);
  submitSubCmd->addOption(url2Option);
  transferCmd->addSubcommand(submitSubCmd);

  cancelSubCmd = new ConsoleCliCommand("cancel", "cancel transfer with ID <id> "
                                       "or by group <groupname>");
  cancelSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
                                  {{"id", "the transfer ID;\nif it is '*', "
                                    "cancels all transfers (only root can do "
                                    "that)", "--id=", "<id>", false},
                                   {"group", "", "--group=", "<group>", false}
                                  })->setRequired(true);
  transferCmd->addSubcommand(cancelSubCmd);

  enableSubCmd = new ConsoleCliCommand("enable", "start the transfer engine "
                                       "(you have to be root to do that)");
  transferCmd->addSubcommand(enableSubCmd);

  disableSubCmd = new ConsoleCliCommand("disable", "stop the transfer engine "
                                       "(you have to be root to do that)");
  transferCmd->addSubcommand(disableSubCmd);

  clearSubCmd = new ConsoleCliCommand("clear", "clear's the transfer database "
                                      "(you have to be root to do that)");
  transferCmd->addSubcommand(clearSubCmd);

  lsSubCmd = new ConsoleCliCommand("ls", "ls's the transfer database "
                                      "(you have to be root to do that)");
  lsSubCmd->addOptions(std::vector<CliOption>
                       {{"all", "list all transfers not only of the current "
                         "role", "-a"},
                        {"monitor", "list all transfers in monitoring format "
                         "(key-val pairs)", "-m"},
                        {"summary", "print transfer summary", "-s"},
                        {"sync", "follow the transfer in interactive mode "
                         "(like interactive third party 'cp')", "--sync"}
                       });
  lsSubCmd->addOption({"group", "list all transfers for group <group>",
                       "--group=", "<group>", false});
  lsSubCmd->addOption({"id", "id of the transfer to list", 1, 1, "<id>"});
  transferCmd->addSubcommand(lsSubCmd);

  resetSubCmd = new ConsoleCliCommand("reset", "resets transfers to "
                                      "'inserted' state (you have to be root "
                                      "to do that); if no transfer is "
                                      "specified, it acts on all transfers");
  resetSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
                                 {{"id", "", "--id=", "<id>", false},
                                  {"group", "", "--group=", "<group>", false}
                                 });
  transferCmd->addSubcommand(resetSubCmd);

  resubmitSubCmd = new ConsoleCliCommand("resubmit", "resubmits a transfer");
  resubmitSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
                                    {{"id", "", "--id=", "<id>", false},
                                     {"group", "", "--group=", "<group>", false}
                                    })->setRequired(true);
  transferCmd->addSubcommand(resubmitSubCmd);

  killSubCmd = new ConsoleCliCommand("kill", "kills a running transfer");
  killSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
                                {{"id", "", "--id=", "<id>", false},
                                 {"group", "", "--group=", "<group>", false}
                                })->setRequired(true);
  transferCmd->addSubcommand(killSubCmd);

  purgeSubCmd = new ConsoleCliCommand("purge", "remove 'failed' transfers from "
                                      "the transfer queue by id, group or all "
                                      "if not specified");
  purgeSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
                                 {{"id", "", "--id=", "<id>", false},
                                  {"group", "", "--group=", "<group>", false}
                                 });
  transferCmd->addSubcommand(purgeSubCmd);

  addHelpOptionRecursively(transferCmd);

  parsedCmd = transferCmd->parse(argin);

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  in += parsedCmd->name().c_str();

  if (parsedCmd == submitSubCmd)
  {
    bool noprogress = false;
    silent = submitSubCmd->hasValue("silent");
    noprogress = submitSubCmd->hasValue("no-progress");

    XrdOucString url1(submitSubCmd->getValue("url1").c_str());
    XrdOucString url2(submitSubCmd->getValue("url2").c_str());

    in += "&mgm.txsrc=";
    in += XrdMqMessage::Seal(url1);
    in += "&mgm.txdst=";
    in += XrdMqMessage::Seal(url2);

    if (submitSubCmd->hasValue("rate"))
    {
      in += "&mgm.txrate=";
      in += submitSubCmd->getValue("rate").c_str();
    }

    if (submitSubCmd->hasValue("streams"))
    {
      in += "&mgm.txstreams=";
      in += submitSubCmd->getValue("streams").c_str();
    }

    if (submitSubCmd->hasValue("group"))
    {
      in += "&mgm.txgroup=";
      in += submitSubCmd->getValue("group").c_str();
    }

    if (!submitSubCmd->hasValue("sync"))
    {
      global_retc = output_result(client_admin_command(in));
    }
    else
    {
      signal(SIGINT, txcancel_handler);

      time_t starttime = time(NULL);
      in += "&mgm.txoption=s";

      XrdOucEnv* result = client_admin_command(in);
      std::vector<std::string> lines;
      command_result_stdout_to_vector(lines);
      global_retc = output_result(result);
      if (!global_retc)
      {
        // scan for success: submitted transfer id=<#>
        if (lines.size() == 2)
        {
          std::string id;
          if ((lines[1].find(" id=")) != std::string::npos)
          {
            id = lines[1];
            id.erase(0, lines[1].find(" id=") + 4);
          }
          // now poll the state

          errno = 0;
          long lid = strtol(id.c_str(), 0, 10);
          if (errno || (lid == LONG_MIN) || (lid == LONG_MAX))
          {
            fprintf(stderr, "Error: submission of transfer probably failed - "
                    "check with 'transfer ls'\n");
            global_retc = EFAULT;
            goto bailout;
          }
          // prepare the get progress command
          in = "mgm.cmd=transfer&mgm.subcmd=ls&mgm.txoption=mp&mgm.txid=";
          in += id.c_str();
          XrdOucString incp = in;
          while (1)
          {
            lines.clear();
            XrdOucEnv* result = client_admin_command(in);
            in = incp;
            command_result_stdout_to_vector(lines);
            if (result) delete result;
            if (lines.size() == 2)
            {
              // this transfer is in the queue
              XrdOucString info = lines[1].c_str();
              while (info.replace(" ", "&"))
              {
              }
              XrdOucEnv txinfo(info.c_str());

              XrdOucString status = txinfo.Get("tx.status");

              if (!noprogress)
              {
                if ((status != "done") && (status != "failed"))
                {
                  fprintf(stdout, "[eoscp TX] [ %-10s ]\t|", txinfo.Get("tx.status"));
                  int progress = atoi(txinfo.Get("tx.progress"));
                  for (int l = 0; l < 20; l++)
                  {
                    if (l < ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, "=");
                    }
                    if (l == ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, ">");
                    }
                    if (l > ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, ".");
                    }
                  }
                  fprintf(stdout, "| %5s%% : %us\r", txinfo.Get("tx.progress"),
                          (unsigned int) (time(NULL) - starttime));
                  fflush(stdout);
                }
              }

              if ((status == "done") || (status == "failed"))
              {

                if (!noprogress)
                {
                  fprintf(stdout, "[eoscp TX] [ %-10s ]\t|", txinfo.Get("tx.status"));
                  int progress = 0;
                  if (status == "done")
                  {
                    progress = 100;
                  }
                  for (int l = 0; l < 20; l++)
                  {
                    if (l < ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, "=");
                    }
                    if (l == ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, ">");
                    }
                    if (l > ((int) (0.2 * progress)))
                    {
                      fprintf(stdout, ".");
                    }
                  }
                  if (status == "done")
                  {
                    fprintf(stdout, "|  100.0%% : %us\n",
                            (unsigned int) (time(NULL) - starttime));
                  }
                  else
                  {
                    fprintf(stdout, "|    0.0%% : %us\n",
                            (unsigned int) (time(NULL) - starttime));
                  }
                  fflush(stdout);
                }

                if (!silent)
                {
                  // get the log
                  in = "mgm.cmd=transfer&mgm.subcmd=log&mgm.txid=";
                  in += id.c_str();
                  output_result(client_admin_command(in));
                }
                if (status == "done")
                {
                  global_retc = 0;
                }
                else
                {
                  global_retc = EFAULT;
                }
                goto bailout;
              }
              for (size_t i = 0; i < 10; i++)
              {
                usleep(100000);
                if (txcancel)
                {
                  fprintf(stdout, "\n<Control-C>\n");
                  in = "mgm.cmd=transfer&mgm.subcmd=cancel&mgm.txid=";
                  in += id.c_str();
                  output_result(client_admin_command(in));
                  global_retc = ECONNABORTED;
                  goto bailout;
                }
              }
            }
            else
            {
              fprintf(stderr, "Error: transfer has been canceled externnaly!\n");
              global_retc = EFAULT;
              goto bailout;
            }
          }
        }
      }
    }
    return (0);
  }
  else if (parsedCmd == lsSubCmd)
  {
    XrdOucString options("");

    if (lsSubCmd->hasValue("monitor"))
      options += "m";
    if (lsSubCmd->hasValue("all"))
      options += "a";
    if (lsSubCmd->hasValue("summary"))
      options += "s";

    if (options != "")
    {
      in += "&mgm.txoption=";
      in += options;
    }

    if (lsSubCmd->hasValue("group"))
    {
      in += "&mgm.txgroup=";
      in += lsSubCmd->getValue("group").c_str();
    }

    if (lsSubCmd->hasValue("id"))
    {
      in += "&mgm.txid=";
      in += lsSubCmd->getValue("id").c_str();
    }
  }
  else if (parsedCmd == enableSubCmd || parsedCmd == disableSubCmd ||
      parsedCmd == clearSubCmd)
  {
    // We do nothing here because it has no parameters
  }
  else
  {
    if (parsedCmd->hasValue("group"))
    {
      in += "&mgm.txgroup=";
      in += parsedCmd->getValue("group").c_str();
    }
    else if (parsedCmd->hasValue("id"))
    {
      in += "&mgm.txid=";
      in += parsedCmd->getValue("id").c_str();
    }
  }

  global_retc = output_result(client_admin_command(in));

 bailout:
  delete transferCmd;

  return (0);
}
