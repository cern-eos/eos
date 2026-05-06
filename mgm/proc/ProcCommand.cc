//------------------------------------------------------------------------------
//! @file ProcCommand.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "ProcCommand.hh"
#include "common/Path.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "common/CommentLog.hh"
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "json/json.h"
#include <openssl/rand.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

EOSMGMNAMESPACE_BEGIN

namespace
{
//------------------------------------------------------------------------------
//! Escape a string for safe inclusion as HTML text content or as the value of
//! a double-quoted attribute. Conservative set covers <, >, &, ", '.
//------------------------------------------------------------------------------
std::string
HtmlEscape(const std::string& in)
{
  std::string out;
  out.reserve(in.size());

  for (char c : in) {
    switch (c) {
    case '&':
      out.append("&amp;");
      break;

    case '<':
      out.append("&lt;");
      break;

    case '>':
      out.append("&gt;");
      break;

    case '"':
      out.append("&quot;");
      break;

    case '\'':
      out.append("&#39;");
      break;

    default:
      out.push_back(c);
    }
  }

  return out;
}
} // anonymous namespace

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ProcCommand::ProcCommand():
  pVid(0), mPath(""),  mCmd(""), mSubCmd(""), mArgs(""), mResultStream(""),
  pOpaque(0), ininfo(0),  mDoSort(false), mSelection(0), mOutFormat(""),
  mOutDepth(0), fstdout(0), fstderr(0), fresultStream(0), fstdoutfilename(""),
  fstderrfilename(""), fresultStreamfilename(""), mError(0),
  mLen(0), mAdminCmd(false), mUserCmd(false), mFuseFormat(false),
  mJsonFormat(false), mHttpFormat(false), mClosed(false),  mSendRetc(false),
  mJsonCallback("") {}

//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
ProcCommand::ProcCommand(eos::common::VirtualIdentity& vid):
  ProcCommand()
{
  pVid = &vid;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ProcCommand::~ProcCommand()
{
  if (fstdout) {
    fclose(fstdout);
    fstdout = 0;
    unlink(fstdoutfilename.c_str());
  }

  if (fstderr) {
    fclose(fstderr);
    fstderr = 0;
    unlink(fstderrfilename.c_str());
  }

  if (fresultStream) {
    fclose(fresultStream);
    fresultStream = 0;
    unlink(fresultStreamfilename.c_str());
  }

  if (pOpaque) {
    delete pOpaque;
    pOpaque = 0;
  }
}

//------------------------------------------------------------------------------
// Open temporary output files for results of find commands
//------------------------------------------------------------------------------
bool
ProcCommand::OpenTemporaryOutputFiles()
{
  // Mix the thread id with per-request CSPRNG randomness and the daemon's
  // effective uid so the temp-file basename cannot be predicted from
  // outside. The parent dir is still 0700 daemon:daemon (chown below) but
  // an unguessable basename closes the residual race where a co-located
  // process could pre-create the entry.
  unsigned char rnd[16] = {0};

  if (RAND_bytes(rnd, sizeof(rnd)) != 1) {
    for (size_t i = 0; i < sizeof(rnd); ++i) {
      rnd[i] = static_cast<unsigned char>(i);
    }
  }

  char hex[2 * sizeof(rnd) + 1];

  for (size_t i = 0; i < sizeof(rnd); ++i) {
    snprintf(hex + 2 * i, 3, "%02x", rnd[i]);
  }

  char tmpdir [4096];
  snprintf(tmpdir, sizeof(tmpdir), "%s/%lu.%llu.%s",
           gOFS->TmpStorePath.c_str(),
           static_cast<unsigned long>(geteuid()),
           (unsigned long long) XrdSysThread::ID(),
           hex);
  fstdoutfilename = tmpdir;
  fstdoutfilename += ".stdout";
  fstderrfilename = tmpdir;
  fstderrfilename += ".stderr";
  fresultStreamfilename = tmpdir;
  fresultStreamfilename += ".mResultstream";
  eos::common::Path cPath(fstdoutfilename.c_str());

  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("Unable to create temporary outputfile directory %s", tmpdir);
    return false;
  }

  // own the directory by daemon
  if (::chown(cPath.GetParentPath(), 2, 2)) {
    eos_err("Unable to own temporary outputfile directory %s",
            cPath.GetParentPath());
  }

  // Create with O_EXCL so we never reuse a pre-existing entry.
  auto excl_open_w = [](const std::string & p) -> FILE* {
    int fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
                    S_IRUSR | S_IWUSR);

    if (fd < 0) {
      return nullptr;
    }

    FILE* fp = ::fdopen(fd, "w");

    if (!fp) {
      ::close(fd);
    }

    return fp;
  };
  auto excl_open_wp = [](const std::string & p) -> FILE* {
    int fd = ::open(p.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
                    S_IRUSR | S_IWUSR);

    if (fd < 0) {
      return nullptr;
    }

    FILE* fp = ::fdopen(fd, "w+");

    if (!fp) {
      ::close(fd);
    }

    return fp;
  };
  fstdout = excl_open_w(fstdoutfilename.c_str());
  fstderr = excl_open_w(fstderrfilename.c_str());
  fresultStream = excl_open_wp(fresultStreamfilename.c_str());

  if ((!fstdout) || (!fstderr) || (!fresultStream)) {
    if (fstdout) {
      fclose(fstdout);
      fstdout = nullptr;
    }

    if (fstderr) {
      fclose(fstderr);
      fstderr = nullptr;
    }

    if (fresultStream) {
      fclose(fresultStream);
      fresultStream = nullptr;
    }

    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Open a proc command e.g. call the appropriate user or admin command and
// store the output in a resultstream of in case of find in temporary output
// files.
//------------------------------------------------------------------------------
int
ProcCommand::open(const char* inpath, const char* info,
                  eos::common::VirtualIdentity& vid_in,
                  XrdOucErrInfo* error)
{
  pVid = &vid_in;
  mClosed = false;
  mPath = inpath;
  mDoSort = false;
  mError = error;
  ininfo = info;

  if ((mPath.beginswith("/proc/admin"))) {
    mAdminCmd = true;
  }

  if (mPath.beginswith("/proc/user")) {
    mUserCmd = true;
  }

  // Deal with '&' ... sigh
  XrdOucString sinfo = ininfo;

  for (int i = 0; i < sinfo.length(); i++) {
    if (sinfo[i] == '&') {
      // figure out if this is a real separator or
      XrdOucString follow = sinfo.c_str() + i + 1;

      if (!follow.beginswith("mgm.") && (!follow.beginswith("eos.")) &&
          (!follow.beginswith("xrd.")) && (!follow.beginswith("callback")) &&
          (!follow.beginswith("authz"))) {
        sinfo.erase(i, 1);
        sinfo.insert("#AND#", i);
      }
    }
  }

  pOpaque = new XrdOucEnv(sinfo.c_str());

  if (!pOpaque) {
    // alloc failed
    return SFS_ERROR;
  }

  mOutFormat = "";
  mOutDepth = 0;
  mCmd = pOpaque->Get("mgm.cmd");
  mSubCmd = pOpaque->Get("mgm.subcmd");
  mOutFormat = pOpaque->Get("mgm.outformat");
  long depth = pOpaque->GetInt("mgm.outdepth");

  if (depth > 0) {
    mOutDepth = (unsigned)depth;
  }

  mSelection = pOpaque->Get("mgm.selection");
  mComment = pOpaque->Get("mgm.comment") ? pOpaque->Get("mgm.comment") : "";
  {
    const char* cb = pOpaque->Get("callback");
    mJsonCallback = "";

    if (cb && *cb) {
      // Restrict the JSONP callback name to a safe JavaScript identifier
      // (with optional dotted member access). Anything else is dropped to
      // avoid letting a CGI-controlled string land verbatim in a response
      // body served as application/javascript.
      bool ok = true;
      size_t len = strlen(cb);

      if (len == 0 || len > 128) {
        ok = false;
      } else {
        for (size_t i = 0; i < len; ++i) {
          unsigned char c = static_cast<unsigned char>(cb[i]);
          bool is_alpha = ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'));
          bool is_digit = (c >= '0' && c <= '9');
          bool is_under = (c == '_' || c == '$');
          bool is_dot = (c == '.');

          if (i == 0) {
            if (!(is_alpha || is_under)) {
              ok = false;
              break;
            }
          } else {
            if (!(is_alpha || is_digit || is_under || is_dot)) {
              ok = false;
              break;
            }
          }
        }
      }

      if (ok) {
        mJsonCallback = cb;
      } else {
        eos_static_warning("msg=\"rejecting JSONP callback - invalid identifier\" "
                           "callback=\"%s\"", cb);
      }
    }
  }
  mSendRetc = pOpaque->Get("mgm.retc") ? true : false;
  eos_static_debug("json-callback=%s opaque=%s", mJsonCallback.c_str(),
                   sinfo.c_str());
  int envlen = 0;
  mArgs = pOpaque->Env(envlen);
  mFuseFormat = false;
  mJsonFormat = false;
  mHttpFormat = false;
  // If set to FUSE, don't print the stdout,stderr tags and we guarantee a line
  // feed in the end
  XrdOucString format = pOpaque->Get("mgm.format");

  if (format == "fuse") {
    mFuseFormat = true;
  }

  if (format == "json") {
    mJsonFormat = true;
  }

  if (format == "http") {
    mHttpFormat = true;
  }

  stdOut = "";
  stdErr = "";
  retc = 0;
  mResultStream = "";
  mLen = 0;
  mDoSort = true;

  if (mJsonCallback.length()) {
    mJsonFormat = true;
  }

  // Admin command section
  if (mAdminCmd) {
    if (mCmd == "archive") {
      Archive();
      mDoSort = false;
    } else if (mCmd == "backup") {
      Backup();
      mDoSort = false;
    } else if (mCmd == "geosched") {
      GeoSched();
      mDoSort = false;
    } else if (mCmd == "fusex") {
      Fusex();
      mDoSort = false;
    } else if (mCmd == "vid") {
      Vid();
    } else if (mCmd == "rtlog") {
      Rtlog();
      mDoSort = false;
    } else if (mCmd == "access") { // @todo (faluchet) drop when move to 5.0.0
      Access();
      mDoSort = false;
    } else if (mCmd == "quota") { // @todo (faluchet) drop when move to 5.0.0
      AdminQuota();
      mDoSort = false;
    } else {
      // command is not implemented
      stdErr += "error: no such admin command '";
      stdErr += mCmd;
      stdErr += "'";
      retc = EINVAL;
    }

    MakeResult();
    return SFS_OK;
  }

  // User command section
  if (mUserCmd) {
    if (mCmd == "accounting") {
      Accounting();
      mDoSort = false;
    } else if (mCmd == "archive") {
      Archive();
      mDoSort = false;
    } else if (mCmd == "motd") {
      Motd();
      mDoSort = false;
    } else if (mCmd == "version") {
      Version();
      mDoSort = false;
    } else if (mCmd == "who") {
      Who();
      mDoSort = false;
    } else if (mCmd == "fuse") {
      return Fuse();
    } else if (mCmd == "fuseX") {
      return FuseX();
    } else if (mCmd == "file") {
      File();
      mDoSort = false;
    } else if (mCmd == "fileinfo") {
      Fileinfo();
      mDoSort = false;
    } else if (mCmd == "mkdir") {
      Mkdir();
    } else if (mCmd == "rmdir") {
      Rmdir();
    } else if (mCmd == "cd") {
      Cd();
      mDoSort = false;
    } else if (mCmd == "chown") {
      Chown();
    } else if (mCmd == "ls") {
      Ls();
      mDoSort = false;
    } else if (mCmd == "rm") {
      Rm();
    } else if (mCmd == "whoami") {
      Whoami();
      mDoSort = false;
    } else if (mCmd == "find") {
      Find();
    } else if (mCmd == "map") {
      Map();
    } else if (mCmd == "member") {
      Member();
    } else if (mCmd == "attr") {
      Attr();
      mDoSort = false;
    } else if (mCmd == "chmod") {
      Chmod();
    } else if (mCmd == "quota") { // @todo (faluchet) drop when move to 5.0.0
      UserQuota();
      mDoSort = false;
    } else {
      // Command not implemented
      stdErr += "error: no such user command '";
      stdErr += mCmd;
      stdErr += "'";
      retc = ENOTSUP;
    }

    if (mSendRetc) {
      // client wants return code on open
      if (retc)
        return gOFS->Emsg((const char*) "open", *error, retc,
                          "execute command", ininfo);
      else {
        return SFS_OK;
      }
    } else {
      // client gets result stream
      MakeResult();
      return SFS_OK;
    }
  }

  // If neither admin nor proc command
  return gOFS->Emsg((const char*) "open", *error, EINVAL,
                    "execute command - not implemented ", ininfo);
}

//------------------------------------------------------------------------------
// Read a part of the result stream produced during open
//------------------------------------------------------------------------------
size_t
ProcCommand::read(XrdSfsFileOffset boff, char* buff, XrdSfsXferSize blen)
{
  if (fresultStream) {
    // file based results go here ...
    if ((fseek(fresultStream, boff, 0)) == 0) {
      size_t nread = fread(buff, 1, blen, fresultStream);

      if (nread > 0) {
        return nread;
      }
    } else {
      eos_err("seek to %llu failed\n", boff);
    }

    return 0;
  } else {
    if (mLen - boff <= 0) {
      return 0;
    }

    // Memory based results go here ...
    if (((unsigned int) blen <= (mLen - boff))) {
      memcpy(buff, mResultStream.c_str() + boff, blen);
      return blen;
    } else {
      memcpy(buff, mResultStream.c_str() + boff, (mLen - boff));
      return (mLen - boff);
    }
  }
}

//------------------------------------------------------------------------------
// Return stat information for the result stream to tell the client the size
// of the proc output.
//------------------------------------------------------------------------------
int
ProcCommand::stat(struct stat* buf)
{
  memset(buf, 0, sizeof(struct stat));
  buf->st_size = mLen;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Close the proc stream and store the client's command comment
// in the comments logbook.
//------------------------------------------------------------------------------
int
ProcCommand::close()
{
  if (!mClosed) {
    // Only instance users or sudoers can add to the logbook
    if ((pVid->uid <= 2) || (pVid->sudoer)) {
      if (mComment.length() && gOFS->mCommentLog) {
        if (!gOFS->mCommentLog->Add(mTimestamp, mCmd.c_str(), mSubCmd.c_str(),
                                    mArgs.c_str(), mComment.c_str(), stdErr.c_str(), retc)) {
          eos_err("failed to log to comments logbook");
        }
      }
    }

    mClosed = true;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Build the in-memory result of the stdout, stderr & retc of the proc command.
// Depending on the output format the key-value CGI returned changes => see
// implementation.
//------------------------------------------------------------------------------
void
ProcCommand::MakeResult()
{
  using eos::common::StringConversion;
  mResultStream = "";

  if (!fstdout) {
    if (mDoSort) {
      eos::common::StringConversion::SortLines(stdOut);
    }

    if ((!mFuseFormat && !mJsonFormat && !mHttpFormat)) {
      // The default format
      mResultStream = "mgm.proc.stdout=";
      mResultStream += StringConversion::Seal(stdOut);
      mResultStream += "&mgm.proc.stderr=";
      mResultStream += StringConversion::Seal(stdErr);
      mResultStream += "&mgm.proc.retc=";
      mResultStream += std::to_string(retc);
    }

    if (mFuseFormat || mHttpFormat) {
      if (mFuseFormat) {
        mResultStream += stdOut.c_str();
      } else {
        mResultStream +=
          "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\n";
        mResultStream += "<html>\n";
        mResultStream +=
          "<TITLE>EOS-HTTP</TITLE> <link rel=\"stylesheet\" href=\"http://www.w3.org/StyleSheets/Core/Midnight\"> \n";
        mResultStream += "<meta charset=\"utf-8\"> \n";

        // block cross-site scripting in responses
        if (stdErr.length()) {
          mResultStream +=
            "<meta http-equiv=\"Content-Security-Policy\" content=\"script-src https://code.jquery.com 'self';\">\n";
        }

        // mCmd / mSubCmd come straight from the caller's CGI - escape them
        // before they land in the div id attribute.
        mResultStream += "<div class=\"httptable\" id=\"";
        mResultStream += HtmlEscape(std::string(mCmd.c_str())).c_str();
        mResultStream += "_";
        mResultStream += HtmlEscape(std::string(mSubCmd.c_str())).c_str();
        mResultStream += "\">\n";

        // FUSE format contains only STDOUT
        if (stdOut.length() && KeyValToHttpTable(stdOut)) {
          // KeyValToHttpTable rebuilds stdOut as an escaped HTML table.
          mResultStream += stdOut.c_str();
        } else {
          if (stdErr.length() || retc) {
            mResultStream += HtmlEscape(std::string(stdOut.c_str())).c_str();
            mResultStream += "<h3>&#9888;&nbsp;<font color=\"red\">";
            mResultStream += HtmlEscape(std::string(stdErr.c_str())).c_str();
            mResultStream += "</font></h3>";
          } else {
            if (!stdOut.length()) {
              mResultStream += "<h3>&#10004;&nbsp;";
              mResultStream += "Success!";
              mResultStream += "</h3>";
            } else {
              mResultStream += HtmlEscape(std::string(stdOut.c_str())).c_str();
            }
          }
        }

        mResultStream += "</div>";
      }
    }

    if (mJsonFormat) {
      if (!stdJson.length()) {
        Json::Value json;

        try {
          Json::Value jsonOut;
          json["errormsg"] = stdErr.c_str();
          json["retc"] = std::to_string(retc);
          jsonOut = IProcCommand::ConvertOutputToJsonFormat(stdOut.c_str());

          if (mCmd.length()) {
            if (mSubCmd.length()) {
              json[mCmd.c_str()][mSubCmd.c_str()] = jsonOut;
            } else {
              json[mCmd.c_str()] = jsonOut;
            }
          } else {
            json["result"] = jsonOut;
          }
        } catch (Json::Exception& e) {
          eos_static_err("Json conversion exception cmd=%s subcmd=%s "
                         "emsg=\"%s\"", mCmd.c_str(), mSubCmd.c_str(), e.what());
          json["errormsg"] = "illegal string in json conversion";
          json["retc"] = std::to_string(EFAULT);
        }

        stdJson = SSTR(json).c_str();
      }

      if (mJsonCallback.length()) {
        // JSONP
        mResultStream = mJsonCallback.c_str();
        mResultStream += "([\n";
        mResultStream += stdJson.c_str();
        mResultStream += "\n]);";
      } else {
        // JSON
        if (vid.prot.beginswith("http")) {
          mResultStream = stdJson.c_str();
        } else {
          mResultStream = "mgm.proc.json=";
          mResultStream += StringConversion::Seal(stdJson);
        }
      }
    }

    if (mResultStream.length() && (*(mResultStream.rbegin()) != '\n')) {
      mResultStream += "\n";
    }

    if (retc) {
      eos_static_err("%s (errno=%u)", stdErr.c_str(), retc);
    }

    mLen = mResultStream.length();
  } else {
    // File based results CANNOT be sorted and don't have mFuseFormat
    if (!mFuseFormat) {
      // Create the stdout result
      if (!fseek(fstdout, 0, 0) &&
          !fseek(fstderr, 0, 0) &&
          !fseek(fresultStream, 0, 0)) {
        fprintf(fresultStream, "&mgm.proc.stdout=");
        std::ifstream inStdout(fstdoutfilename.c_str());
        std::ifstream inStderr(fstderrfilename.c_str());
        std::string entry;

        while (std::getline(inStdout, entry)) {
          XrdOucString sentry = entry.c_str();
          sentry += "\n";

          if (!mFuseFormat) {
            StringConversion::Seal(sentry);
          }

          fprintf(fresultStream, "%s", sentry.c_str());
        }

        // Close and remove - if this fails there is nothing to recover anyway
        fclose(fstdout);
        fstdout = 0;
        unlink(fstdoutfilename.c_str());
        // Create the stderr result
        fprintf(fresultStream, "&mgm.proc.stderr=");

        while (std::getline(inStderr, entry)) {
          XrdOucString sentry = entry.c_str();
          sentry += "\n";
          StringConversion::Seal(sentry);
          fprintf(fresultStream, "%s", sentry.c_str());
        }

        // Close and remove - if this fails there is nothing to recover anyway
        fclose(fstderr);
        fstderr = 0;
        unlink(fstderrfilename.c_str());
        fprintf(fresultStream, "&mgm.proc.retc=%d", retc);
        mLen = ftell(fresultStream);
        // Spool the resultstream to the beginning
        fseek(fresultStream, 0, 0);
      } else {
        eos_static_err("cannot seek to position 0 in result files");
      }
    }
  }
}

//------------------------------------------------------------------------------
// Try to detect and convert a monitor output format and convert it into a
// nice http table
//------------------------------------------------------------------------------
bool
ProcCommand::KeyValToHttpTable(XrdOucString& stdOut)
{
  while (stdOut.replace("= ", "=\"\"")) {
  }

  std::string stmp = stdOut.c_str();
  XrdOucTokenizer tokenizer((char*) stmp.c_str());
  const char* line;
  bool ok = true;
  std::vector<std::string> keys;
  std::vector < std::map < std::string, std::string >> keyvaluetable;
  std::string table;

  while ((line = tokenizer.GetLine())) {
    if (strlen(line) <= 1) {
      continue;
    }

    std::map<std::string, std::string> keyval;

    if (eos::common::StringConversion::GetKeyValueMap(line,
        keyval,
        "=",
        " ",
        &keys)) {
      keyvaluetable.push_back(keyval);
    } else {
      ok = false;
      break;
    }
  }

  if (ok) {
    table +=
      R"literal(<style>
table
{
  table-layout:auto;
}
</style>
)literal";

    table += "<table border=\"8\" cellspacing=\"10\" cellpadding=\"20\">\n";
    // build the header
    table += "<tr>\n";
    for (size_t i = 0; i < keys.size(); i++)
    {
      table += "<th>";
      table += "<font size=\"2\">";
      // for keys don't print lengthy strings like a.b.c.d ... just print d
      std::string dotkeys = keys[i];
      size_t pos = dotkeys.rfind(".");
      if (pos != std::string::npos)
        dotkeys.erase(0, pos + 1);
      //table += dotkeys;
      table += HtmlEscape(keys[i]);
      table += "</font>";
      table += "</th>";
      table += "\n";
    }
    table += "</tr>\n";

    // build the rows

    for (size_t i = 0; i < keyvaluetable.size(); i++)
    {
      table += "<tr>\n";
      for (size_t j = 0; j < keys.size(); j++)
      {
        table += "<td nowrap=\"nowrap\">";
        table += "<font size=\"2\">";
        XrdOucString sizestring = keyvaluetable[i][keys[j]].c_str();
        unsigned long long val = eos::common::StringConversion::GetSizeFromString(sizestring);
        if (errno || val == 0 || (!sizestring.isdigit()))
        {
          XrdOucString decodeURI = keyvaluetable[i][keys[j]].c_str();
          // we need to remove URI encoded spaces now
          while (decodeURI.replace("%20", " "))
          {
          }
          table += HtmlEscape(std::string(decodeURI.c_str()));
        }
        else
        {
          eos::common::StringConversion::GetReadableSizeString(sizestring, val, "");
          table += HtmlEscape(std::string(sizestring.c_str()));
        }
        table += "</font>";
        table += "</td>";
      }
      table += "</tr>\n";
      table += "\n";
    }


    table += "</table>\n";
    stdOut = table.c_str();
  }
  return ok;
}

Json::Value ProcCommand::CallJsonFormatter(const std::string& output)
{
  return IProcCommand::ConvertOutputToJsonFormat(output);
}

EOSMGMNAMESPACE_END
