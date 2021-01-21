// ----------------------------------------------------------------------
// File: com_daemon.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "common/StringTokenizer.hh"
#include "common/Config.hh"
#include "common/Path.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include <sys/wait.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/ptrace.h>
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#define EOS_PTRACE_TRACEME PT_TRACEME
#define EOS_PTRACE_ATTACH   PT_ATTACH
#define EOS_PTRACE_DETACH   PT_DETACH
#else
#define EOS_PTRACE_TRACEME PTRACE_TRACEME
#define EOS_PTRACE_ATTACH   PTRACE_ATTACH
#define EOS_PTRACE_DETACH   PTRACE_DETACH
#endif //__APPLE__

/* Steer a service */
int
com_daemon(char* arg)
{
#ifdef __APPLE__
  fprintf(stderr,"error: daemon command is not support on OSX\n");
  global_retc = EINVAL;
  return (0);
#else
  eos::common::StringTokenizer subtokenizer(arg);
  eos::common::Config cfg;
  XrdOucString option = "";
  XrdOucString name = "";
  XrdOucString service = "";
  std::string chapter;
  std::string executable;
  std::string pidfile;
  std::string envfile;
  std::string cfile;

  bool ok = false;

  subtokenizer.GetLine();

  if (wants_help(arg)) {
    goto com_daemon_usage;
  }


  option = subtokenizer.GetToken();

  if (!option.length()) {
    goto com_daemon_usage;
  }

  if (option == "seal") {
    XrdOucString toseal = subtokenizer.GetToken();
    XrdOucString sealed;
    std::string key;

    if (!toseal.length()) {
      return (0);
    }

    const char* pkey = subtokenizer.GetToken();


    if (!pkey) {
      key = eos::common::StringConversion::StringFromShellCmd("cat /etc/eos.keytab | grep u:daemon | md5sum");
    } else {
      key = pkey;
    }

    std::string shakey = eos::common::SymKey::Sha256(key);
    eos::common::SymKey::SymmetricStringEncrypt(toseal, sealed, (char*)shakey.c_str());
    fprintf(stderr,"enc:%s\n", sealed.c_str());
    return (0);
  }

  if ( (option != "run") &&
       (option != "config" ) &&
       (option != "stack" ) &&
       (option != "stop" ) &&
       (option != "kill") ) {
    goto com_daemon_usage;
  }

  service = subtokenizer.GetToken();

  if ((!service.length()) ||
      (( service != "mgm") &&
       ( service != "mq") &&
       ( service != "fst") &&
       ( service != "qdb")) ){
    goto com_daemon_usage;
  }

  name = subtokenizer.GetToken();

  if (!name.length()) {
    name = service.c_str();
  }

  executable = "eos-"; executable += service.c_str();
  envfile = "/var/run/eos/"; envfile += executable.c_str() ; envfile += "."; envfile += name.c_str(); envfile += ".env";
  pidfile = "/var/run/eos/xrd."; pidfile += service.c_str(); pidfile += ".";
  pidfile += name.c_str();
  pidfile += ".pid";

  cfg.Load("generic","all");
  ok |= cfg.ok();
  cfg.Load(service.c_str(), name.c_str(), false);
  ok |= cfg.ok();

  chapter = service.c_str(); chapter += ":xrootd:"; chapter += name.c_str();

  cfile = "/var/run/eos/xrd.cf.";
  cfile += name.c_str();

  if (option == "config") {
    if ( service == "qdb" ) {
      XrdOucString subcmd = subtokenizer.GetToken();
      if (subcmd == "coup") {
	std::string kline;
	kline = "redis-cli -p `cat "; kline += "/etc/eos/config/qdb/"; kline+= name.c_str(); kline += " |grep xrd.port | cut -d ' ' -f 2` <<< raft-attempt-coup";
	int rc = system(kline.c_str());
	fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
	global_retc = WEXITSTATUS(rc);
	return (0);
      } else if (subcmd == "info") {
	std::string kline;
	kline = "redis-cli -p `cat "; kline += "/etc/eos/config/qdb/"; kline+= name.c_str(); kline += " |grep xrd.port | cut -d ' ' -f 2` <<< raft-info";
	int rc = system(kline.c_str());
	fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
	global_retc = WEXITSTATUS(rc);
	return (0);
      }
    }

    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"# ------------- i n i t -----------------\n");
    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"%s\n", cfg.Dump("init", true).c_str());
    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"# ------------- s y s c o n f i g -------\n");
    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"%s\n", cfg.Dump("sysconfig", true).c_str());
    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"# ------------- x r o o t d  ------------\n");
    fprintf(stderr,"# ---------------------------------------\n");
    fprintf(stderr,"# running config file: %s\n", cfile.c_str());

    fprintf(stderr,"%s\n", cfg.Dump(chapter.c_str(),true).c_str());
    fprintf(stderr,"#########################################\n");
  } else if (option == "stack") {
    std::string kline;
    kline = "test -e "; kline += envfile.c_str(); kline += " && eu-stack -p `cat "; kline += envfile.c_str(); kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);

  } else if (option == "kill") {
    std::string kline;
    kline = "test -e "; kline += envfile.c_str(); kline += " && kill -9 `cat "; kline += envfile.c_str(); kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "stop") {
    std::string kline;
    kline = "test -e "; kline += envfile.c_str(); kline += " && kill -15 `cat "; kline += envfile.c_str(); kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "run") {
    if (!cfg.Has(chapter.c_str())) {
      fprintf(stderr,"error: missing service configuration [%s] in generic config file '/etc/eos/config/generic/all' or '/etc/eos/config/%s/%s'\n",
	      chapter.c_str(), service.c_str(), name.c_str());
      global_retc = EINVAL;
      return (0);
    }

    char **const envv = cfg.Env("sysconfig");
    for (size_t i =0; i< 1024; ++i) {
      if (envv[i]) {
	fprintf(stderr,"%s\n", envv[i]);
      } else {
	break;
      }
    }

    if (cfg.Has("init")) {
      fprintf(stderr,"# ---------------------------------------\n");
      fprintf(stderr,"# ------------- i n i t -----------------\n");
      fprintf(stderr,"# ---------------------------------------\n");
      if (cfg.Has("unshare")) {
	fprintf(stderr,"# ---------------------------------------\n");
	fprintf(stderr,"# ------------- u n s h a r e -----------\n");
	if (unshare(CLONE_NEWNS)) {
	  fprintf(stderr,"warning: failed to unshare mount namespace errno=%d\n", errno);
	}
	if (mount("none", "/", NULL, MS_REC|MS_PRIVATE, NULL)) {
	  fprintf(stderr,"warning: failed none mount / - errno=%d\n", errno);
	}
	fprintf(stderr,"# ---------------------------------------\n");
      }

      for ( auto it : cfg["init"] ) {
	bool exit_on_failure = false;
	fprintf(stderr,"# run: %s\n", it.c_str());
	pid_t pid;
	std::string cline = it;
	if (cline.substr(0,4) == "enc:") {
	  std::string key;
	  const char* pkey = subtokenizer.GetToken();
	  if (!pkey) {
	    key = eos::common::StringConversion::StringFromShellCmd("cat /etc/eos.keytab | grep u:daemon | md5sum");
	  } else {
	    key = pkey;
	  }

	  std::string shakey = eos::common::SymKey::Sha256(key);
	  XrdOucString in = cline.substr(4).c_str();;
	  XrdOucString out;
	  eos::common::SymKey::SymmetricStringDecrypt(in, out, (char*)shakey.c_str());
	  if (!out.c_str()) {
	    fprintf(stderr,"error: encoded init line '%s' cannot be decoded\n", in.c_str());
	    continue;
	  }
	  cline = out.c_str();
	  exit_on_failure = true;
	}

	if (exit_on_failure) {
	  // test that nobody traces us ..
	  if (!(pid=fork())) {
	    pause();
	    exit(0);
	  } else {
	    if (ptrace(EOS_PTRACE_ATTACH, pid, 0, 0)) {
	      kill(pid,SIGKILL);
	      fprintf(stderr,"error: failed to attach to forked process pid=%d errno=%d - we are untraceable\n", pid, errno);
	      if (exit_on_failure) {
		exit(-1);
	      }
	    } else {
	      ptrace(EOS_PTRACE_DETACH, pid, 0, 0);
	      kill(pid,9);
	      waitpid(pid, 0, 0);
	    }
	  }
	}
	if (!(pid=fork())) {
	  int rc = execle("/bin/bash", "eos-bash", "-c", cline.c_str(), NULL, envv);
	  exit(0);
	} else {
	  waitpid(pid, 0, 0);
	}
      }
    }

    if (ok) {
      fprintf(stderr,"# ---------------------------------------\n");
      fprintf(stderr,"# ------------- x r o o t d  ------------\n");
      fprintf(stderr,"# ---------------------------------------\n");
      fprintf(stderr,"# running config file: %s\n", cfile.c_str());
      fprintf(stderr,"# ---------------------------------------\n");
      fprintf(stderr,"%s\n", cfg.Dump(chapter.c_str(),true).c_str());
      fprintf(stderr,"#########################################\n");

      eos::common::Path cPath(cfile.c_str());
      if (!cPath.MakeParentPath(0x1ed)) {
	fprintf(stderr,"error: unable to create run directory '%s'\n", cPath.GetParentPath());
	global_retc = errno;
	return (0);
      }

      if (!eos::common::StringConversion::SaveStringIntoFile(cfile.c_str(), cfg.Dump(chapter.c_str(),true))) {
	fprintf(stderr,"error: unable to create startup config file '%s'\n",cfile.c_str());
	global_retc = errno;
	return (0);
      }

      ::chdir(cPath.GetParentPath());
      std::string logfile = "/var/log/eos/xrdlog."; logfile += service.c_str();

      if ( service == "qdb") {
	execle("/opt/eos/xrootd/bin/xrootd", executable.c_str(), "-n", name.c_str(), "-c", cfile.c_str(),"-l", logfile.c_str(), "-R", "daemon", "-k", "fifo", "-s", pidfile.c_str(), NULL, envv);
      } else {
	execle("/opt/eos/xrootd/bin/xrootd", executable.c_str(), "-n", name.c_str(), "-c", cfile.c_str(),"-l", logfile.c_str(), "-R", "daemon", "-s", pidfile.c_str(), NULL, envv);
      }
      return (0);
    } else {
      fprintf(stderr,"error: rc=%d msg=%s\n", cfg.getErrc(), cfg.getMsg().c_str());
      global_retc = cfg.getErrc();
      return (0);
    }
  }
  return (0);
com_daemon_usage:
  fprintf(stdout,
          "usage: daemon config|kill|run|stack|stop <service> [name] [subcmd]                                     :  \n");

  fprintf(stdout,
	  "                <service> := mq | mgm | fst | qdb\n");
  fprintf(stdout,
          "                config                                                -  configure a service / show configuration\n");
  fprintf(stdout,
	  "                kill                                                  -  kill -9 a given service\n");
  fprintf(stdout,
          "                run                                                   -  run the given service daemon optionally identified by name\n");
  fprintf(stdout,
	  "                stack                                                 -  print an 'eu-stack'\n");
  fprintf(stdout,
	  "                stop                                                  -  kill -15 a given service\n");
  fprintf(stdout,"\n");
  fprintf(stdout,
	  "      examples: eos daemon config qdb qdb coup                        -  try to make instance [qdb] a leader of QDB\n");
  fprintf(stdout,
	  "                eos daemon config qdb qdb1 info                       -  show raft-info for the [qdb1] QDB instance\n");
  fprintf(stdout,
	  "                eos daemon config fst fst.1                           -  show the init,sysconfig and xrootd config for the [fst.1] FST service\n");
  fprintf(stdout,
	  "                eos daemon kill mq                                    -  shoot the MQ service with signal -9\n");
  fprintf(stdout,
	  "                eos daemon stop mq                                    -  gracefully shut down the MQ service with signal -15\n");
  fprintf(stdout,
	  "                eos daemon stack mgm                                  -  take an 'eu-stack' of the MGM service\n");
  fprintf(stdout,
	  "                eos daemon run fst fst.1                              -  run the fst.1 subservice FST\n");
  global_retc = EINVAL;
  return (0);
#endif
}
