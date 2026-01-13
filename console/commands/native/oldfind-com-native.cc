// ----------------------------------------------------------------------
// File: oldfind-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdPosix/XrdPosixXrootd.hh>
#include <dirent.h>
#include <memory>
#include <sstream>
#include <sys/stat.h>

extern int com_file(char*);

static int
native_com_old_find(char* arg1)
{
  XrdPosixXrootd Xroot;
  XrdOucString oarg = arg1;
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1;
  XrdOucString path;
  XrdOucString option = "";
  XrdOucString attribute = "";
  XrdOucString maxdepth = "";
  XrdOucString olderthan = "";
  XrdOucString youngerthan = "";
  XrdOucString printkey = "";
  XrdOucString filter = "";
  XrdOucString stripes = "";
  XrdOucString versions = "";
  XrdOucString filematch = "";
  XrdOucString in = "mgm.cmd=find&";
  bool valid = false;

  if (wants_help(arg1)) {
    goto com_find_usage;
  }

  while ((s1 = subtokenizer.GetToken()).length() && (s1.beginswith("-"))) {
    valid = false;
    if (s1 == "-j") {
      option += "j";
      continue;
    }
    if (s1 == "-s") {
      option += "s";
      valid = true;
    }
    if (s1 == "-d") {
      option += "d";
      valid = true;
    }
    if (s1 == "-f") {
      option += "f";
      valid = true;
    }
    if (s1 == "-0") {
      option += "f0";
      valid = true;
    }
    if (s1 == "-m") {
      option += "fG";
      valid = true;
    }
    if (s1 == "--size") {
      option += "S";
      valid = true;
    }
    if (s1 == "--fs") {
      option += "L";
      valid = true;
    }
    if (s1 == "--checksum") {
      option += "X";
      valid = true;
    }
    if (s1 == "--ctime") {
      option += "C";
      valid = true;
    }
    if (s1 == "--mtime") {
      option += "M";
      valid = true;
    }
    if (s1 == "--fid") {
      option += "F";
      valid = true;
    }
    if (s1 == "--nrep") {
      option += "R";
      valid = true;
    }
    if (s1 == "--online") {
      option += "O";
      valid = true;
    }
    if (s1 == "--fileinfo") {
      option += "I";
      valid = true;
    }
    if (s1 == "--nunlink") {
      option += "U";
      valid = true;
    }
    if (s1 == "--uid") {
      option += "u";
      valid = true;
    }
    if (s1 == "--gid") {
      option += "g";
      valid = true;
    }
    if (s1 == "--stripediff") {
      option += "D";
      valid = true;
    }
    if (s1 == "--faultyacl") {
      option += "A";
      valid = true;
    }
    if (s1 == "--count") {
      option += "Z";
      valid = true;
    }
    if (s1 == "--hosts") {
      option += "H";
      valid = true;
    }
    if (s1 == "--partition") {
      option += "P";
      valid = true;
    }
    if (s1 == "--childcount") {
      option += "l";
      valid = true;
    }
    if (s1 == "--xurl") {
      option += "x";
      valid = true;
    }
    if (s1 == "-1") {
      option += "1";
      valid = true;
    }
    if (s1.beginswith("-h") || (s1.beginswith("--help"))) {
      goto com_find_usage;
    }
    if (s1 == "-x") {
      valid = true;
      attribute = subtokenizer.GetToken();
      if (!attribute.length())
        goto com_find_usage;
      if ((attribute.find("&")) != STR_NPOS)
        goto com_find_usage;
    }
    if (s1 == "--maxdepth") {
      valid = true;
      maxdepth = subtokenizer.GetToken();
      if (!maxdepth.length())
        goto com_find_usage;
    }
    if ((s1 == "-ctime") || (s1 == "-mtime")) {
      valid = true;
      XrdOucString period = "";
      period = subtokenizer.GetToken();
      if (!period.length())
        goto com_find_usage;
      bool do_olderthan = false, do_youngerthan = false;
      if (period.beginswith("+"))
        do_olderthan = true;
      if (period.beginswith("-"))
        do_youngerthan = true;
      if ((!do_olderthan) && (!do_youngerthan))
        goto com_find_usage;
      period.erase(0, 1);
      time_t now = time(NULL);
      now -= (86400 * strtoul(period.c_str(), 0, 10));
      char snow[1024];
      snprintf(snow, sizeof(snow) - 1, "%lu", now);
      if (do_olderthan) {
        olderthan = snow;
      }
      if (do_youngerthan) {
        youngerthan = snow;
      }
      if (s1 == "-ctime")
        option += "C";
      if (s1 == "-mtime")
        option += "M";
    }
    if (s1 == "-c") {
      valid = true;
      option += "c";
      filter = subtokenizer.GetToken();
      if (!filter.length())
        goto com_find_usage;
      if ((filter.find("%%")) != STR_NPOS)
        goto com_find_usage;
    }
    if (s1 == "--purge") {
      valid = true;
      versions = subtokenizer.GetToken();
      if (!versions.length())
        goto com_find_usage;
    }
    if (s1 == "-name") {
      valid = true;
      filematch = subtokenizer.GetToken();
      option += "f";
      if (!filematch.length())
        goto com_find_usage;
    }
    if (s1 == "-layoutstripes") {
      valid = true;
      stripes = subtokenizer.GetToken();
      if (!stripes.length())
        goto com_find_usage;
    }
    if (s1 == "-p") {
      valid = true;
      option += "p";
      printkey = subtokenizer.GetToken();
      if (!printkey.length())
        goto com_find_usage;
    }
    if (s1 == "-b") {
      valid = true;
      option += "b";
    }
    if (!valid) {
      goto com_find_usage;
    }
  }

  if (s1.length()) {
    path = s1;
  }
  if (path == "help") {
    goto com_find_usage;
  }
  if (!path.endswith("/")) {
    if (!path.endswith(":")) {
      path += "/";
    }
  }

  if (path.beginswith("root://") || path.beginswith("file:")) {
    bool XRootD = path.beginswith("root:");
    std::vector<std::vector<std::string>> found_dirs;
    std::map<std::string, std::set<std::string>> found;
    XrdOucString protocol;
    XrdOucString hostport;
    XrdOucString sPath;
    if (path == "/") {
      fprintf(stderr, "error: I won't do a find on '/'\n");
      global_retc = EINVAL;
      return 0;
    }
    const char* v = 0;
    if (!(v = eos::common::StringConversion::ParseUrl(path.c_str(), protocol,
                                                      hostport))) {
      global_retc = EINVAL;
      return 0;
    }
    sPath = v;
    std::string Path = v;
    if (sPath == "" && (protocol == "file")) {
      sPath = getenv("PWD");
      Path = getenv("PWD");
      if (!sPath.endswith("/")) {
        sPath += "/";
        Path += "/";
      }
    }
    found_dirs.resize(1);
    found_dirs[0].resize(1);
    found_dirs[0][0] = Path.c_str();
    int deepness = 0;
    do {
      struct stat buf;
      found_dirs.resize(deepness + 2);
      for (unsigned int i = 0; i < found_dirs[deepness].size(); i++) {
        Path = found_dirs[deepness][i].c_str();
        XrdOucString url = "";
        eos::common::StringConversion::CreateUrl(
            protocol.c_str(), hostport.c_str(), Path.c_str(), url);
        int rstat = (XRootD) ? XrdPosixXrootd::Stat(url.c_str(), &buf)
                             : stat(url.c_str(), &buf);
        if (!rstat) {
          if (S_ISDIR(buf.st_mode)) {
            DIR* dir = (XRootD) ? XrdPosixXrootd::Opendir(url.c_str())
                                : opendir(url.c_str());
            if (dir) {
              struct dirent* entry;
              while ((entry = (XRootD) ? XrdPosixXrootd::Readdir(dir)
                                       : readdir(dir))) {
                XrdOucString curl = "";
                XrdOucString cpath = Path.c_str();
                cpath += entry->d_name;
                if ((!strcmp(entry->d_name, ".")) ||
                    (!strcmp(entry->d_name, ".."))) {
                  continue;
                }
                eos::common::StringConversion::CreateUrl(
                    protocol.c_str(), hostport.c_str(), cpath.c_str(), curl);
                if (!((XRootD) ? XrdPosixXrootd::Stat(curl.c_str(), &buf)
                               : stat(curl.c_str(), &buf))) {
                  if (S_ISDIR(buf.st_mode)) {
                    curl += "/";
                    cpath += "/";
                    found_dirs[deepness + 1].push_back(cpath.c_str());
                    (void)found[curl.c_str()].size();
                  } else {
                    found[url.c_str()].insert(entry->d_name);
                  }
                }
              }
              (XRootD) ? XrdPosixXrootd::Closedir(dir) : closedir(dir);
            }
          }
        }
      }
      deepness++;
    } while (found_dirs[deepness].size());

    bool show_files = false, show_dirs = false;
    if ((option.find("f") == STR_NPOS) && (option.find("d") == STR_NPOS)) {
      show_files = show_dirs = true;
    } else {
      if (option.find("f") != STR_NPOS)
        show_files = true;
      if (option.find("d") != STR_NPOS)
        show_dirs = true;
    }
    for (auto it = found.begin(); it != found.end(); ++it) {
      if (show_dirs)
        fprintf(stdout, "%s\n", it->first.c_str());
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
        if (show_files)
          fprintf(stdout, "%s%s\n", it->first.c_str(), sit->c_str());
      }
    }
    return 0;
  }

  if (path.beginswith("as3:")) {
    XrdOucString hostport;
    XrdOucString protocol;
    int rc = system("which s3 >&/dev/null");
    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: you miss the <s3> executable provided by libs3 "
                      "in your PATH\n");
      exit(-1);
    }
    if (path.endswith("/")) {
      path.erase(path.length() - 1);
    }
    XrdOucString sPath = path.c_str();
    XrdOucString sOpaque;
    int qpos = 0;
    if ((qpos = sPath.find("?")) != STR_NPOS) {
      sOpaque.assign(sPath, qpos + 1);
      sPath.erase(qpos);
    }
    XrdOucString fPath = eos::common::StringConversion::ParseUrl(
        sPath.c_str(), protocol, hostport);
    XrdOucEnv env(sOpaque.c_str());
    if (env.Get("s3.key")) {
      setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
    }
    if (env.Get("s3.id")) {
      setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
    }
    const char* cstr = getenv("S3_ACCESS_KEY");
    if (cstr) {
      setenv("S3_SECRET_ACCESS_KEY", cstr, 1);
    }
    cstr = getenv("S3_ACESSS_ID");
    if (cstr) {
      setenv("S3_ACCESS_KEY_ID", cstr, 1);
    }
    if (!getenv("S3_ACCESS_KEY_ID") || !getenv("S3_HOSTNAME") ||
        !getenv("S3_SECRET_ACCESS_KEY")) {
      fprintf(stderr, "error: you have to set the S3 environment variables "
                      "S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use a "
                      "URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString s3env;
    s3env = "env S3_ACCESS_KEY_ID=";
    s3env += getenv("S3_ACCESS_KEY_ID");
    s3env += " S3_HOSTNAME=";
    s3env += getenv("S3_HOSTNAME");
    s3env += " S3_SECRET_ACCESS KEY=";
    s3env += getenv("S3_SECRET_ACCESS_KEY");
    XrdOucString cmd = "bash -c \"";
    cmd += s3env;
    cmd += " s3 list ";
    int bpos = fPath.find("/");
    XrdOucString bucket;
    if (bpos != STR_NPOS) {
      bucket.assign(fPath, 0, bpos - 1);
    } else {
      bucket = fPath.c_str();
    }
    XrdOucString match;
    if (bpos != STR_NPOS) {
      match.assign(fPath, bpos + 1);
    } else {
      match = "";
    }
    if ((!bucket.length()) || (bucket.find("*") != STR_NPOS)) {
      fprintf(stderr,
              "error: no bucket specified or wildcard in bucket name!\n");
      global_retc = EINVAL;
      return 0;
    }
    cmd += bucket.c_str();
    cmd += " | awk '{print $1}' ";
    if (match.length()) {
      if (match.endswith("*")) {
        match.erase(match.length() - 1);
        match.insert("^", 0);
      }
      if (match.beginswith("*")) {
        match.erase(0, 1);
        match += "$";
      }
      cmd += " | egrep '";
      cmd += match.c_str();
      cmd += "'";
    }
    cmd += " | grep -v 'Bucket' | grep -v '----------' | grep -v 'Key' | awk "
           "-v prefix='";
    cmd += bucket.c_str();
    cmd += "' '{print \"as3:\"prefix\"/\"$1}'";
    cmd += "\"";
    rc = system(cmd.c_str());
    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: failed to run %s\n", cmd.c_str());
    }
  }

  if ((stripes.length())) {
    XrdOucString subfind = oarg;
    XrdOucString repstripes = " ";
    repstripes += stripes;
    repstripes += " ";
    subfind.replace("-layoutstripes", "");
    subfind.replace(repstripes, " -f -s ");
    int rc = native_com_old_find((char*)subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    unsigned long long cnt = 0, goodentries = 0, badentries = 0;
    for (unsigned int i = 0; i < files_found.size(); i++) {
      if (!files_found[i].length())
        continue;
      XrdOucString cline = "layout ";
      cline += files_found[i].c_str();
      cline += " -stripes ";
      cline += stripes;
      rc = com_file((char*)cline.c_str());
      if (rc)
        badentries++;
      else
        goodentries++;
      cnt++;
    }
    rc = 0;
    if (!silent) {
      fprintf(stderr, "nentries=%llu good=%llu bad=%llu\n", cnt, goodentries,
              badentries);
    }
    return 0;
  }

  if ((option.find("c")) != STR_NPOS) {
    XrdOucString subfind = oarg;
    subfind.replace("-c", "-s -f");
    subfind.replace(filter, "");
    int rc = native_com_old_find((char*)subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    unsigned long long cnt = 0, goodentries = 0, badentries = 0;
    for (unsigned int i = 0; i < files_found.size(); i++) {
      if (!files_found[i].length())
        continue;
      XrdOucString cline = "check ";
      cline += files_found[i].c_str();
      cline += " ";
      cline += filter;
      rc = com_file((char*)cline.c_str());
      if (rc)
        badentries++;
      else
        goodentries++;
      cnt++;
    }
    rc = 0;
    if (!silent) {
      fprintf(stderr, "nentries=%llu good=%llu bad=%llu\n", cnt, goodentries,
              badentries);
    }
    return 0;
  }

  path = abspath(path.c_str());
  if (!s1.length() && (path == "/")) {
    fprintf(stderr, "error: you didnt' provide any path and would query '/' - "
                    "will not do that!\n");
    return EINVAL;
  }
  in += "mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  if (attribute.length()) {
    in += "&mgm.find.attribute=";
    in += attribute;
  }
  if (maxdepth.length()) {
    in += "&mgm.find.maxdepth=";
    in += maxdepth;
  }
  if (olderthan.length()) {
    in += "&mgm.find.olderthan=";
    in += olderthan;
  }
  if (youngerthan.length()) {
    in += "&mgm.find.youngerthan=";
    in += youngerthan;
  }
  if (versions.length()) {
    in += "&mgm.find.purge.versions=";
    in += versions;
  }
  if (filematch.length()) {
    in += "&mgm.find.match=";
    in += filematch;
  }
  if (printkey.length()) {
    in += "&mgm.find.printkey=";
    in += printkey;
  }
  {
    XrdOucEnv* result;
    result = client_command(in);
    if ((option.find("s")) == STR_NPOS) {
      global_retc = output_result(result);
    } else {
      if (result) {
        global_retc = 0;
      } else {
        global_retc = EINVAL;
      }
    }
  }
  return 0;
com_find_usage:
  fprintf(
      stdout,
      "Usage: find [-name <pattern>] [--xurl] [--childcount] [--purge <n> ] "
      "[--count] [-s] [-d] [-f] [-0] [-1] [-ctime +<n>|-<n>] [-m] [-x "
      "<key>=<val>] [-p <key>] [-b] [-c %%tags] [-layoutstripes <n>] <path>\n");
  global_retc = EINVAL;
  return 0;
}

namespace {
class OldfindCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "oldfind";
  }
  const char*
  description() const override
  {
    return "Find files/directories (old implementation)";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    return native_com_old_find((char*)joined.c_str());
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: find [-name <pattern>] [--xurl] [--childcount] "
                    "[--purge <n> ] [--count] [-s] [-d] [-f] [-0] [-1] [-ctime "
                    "+<n>|-<n>] [-m] [-x <key>=<val>] [-p <key>] [-b] [-c "
                    "%%tags] [-layoutstripes <n>] <path>\n");
  }
};
} // namespace

void
RegisterOldfindNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<OldfindCommand>());
}

// Legacy compatibility symbol required by ConsoleMain and other modules
int
com_old_find(char* arg)
{
  return native_com_old_find(arg);
}
