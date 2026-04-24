// ----------------------------------------------------------------------
// File: tui-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"

#include <cerrno>
#include <cstring>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {
class TuiCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "tui";
  }

  const char*
  description() const override
  {
    return "Launch the eos-tui terminal UI";
  }

  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }

  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::vector<std::string> ownedArgs;
    ownedArgs.reserve(args.size() + 1);
    ownedArgs.emplace_back("eos-tui");
    ownedArgs.insert(ownedArgs.end(), args.begin(), args.end());

    std::vector<char*> argv;
    argv.reserve(ownedArgs.size() + 1);

    for (auto& arg : ownedArgs) {
      argv.push_back(arg.data());
    }

    argv.push_back(nullptr);

    pid_t pid = fork();

    if (pid < 0) {
      fprintf(stderr, "error: failed to fork eos-tui launcher: %s\n", strerror(errno));
      return (global_retc = errno ? errno : EIO);
    }

    if (pid == 0) {
      execvp(argv.front(), argv.data());
      const int execErrno = errno;

      if (execErrno == ENOENT) {
        fprintf(stderr, "error: 'eos-tui' is not installed. Install the EOS client "
                        "package with TUI support.\n");
      } else {
        fprintf(stderr, "error: failed to launch eos-tui: %s\n", strerror(execErrno));
      }

      _exit(execErrno == ENOENT ? 127 : 126);
    }

    int status = 0;

    while ((waitpid(pid, &status, 0) < 0) && (errno == EINTR)) {
    }

    if (WIFEXITED(status)) {
      return (global_retc = WEXITSTATUS(status));
    }

    if (WIFSIGNALED(status)) {
      return (global_retc = 128 + WTERMSIG(status));
    }

    return (global_retc = EIO);
  }

  void
  printHelp() const override
  {
    fprintf(stderr, "usage: tui [eos-tui-args]\n"
                    "\n"
                    "Launch the eos-tui terminal UI.\n"
                    "Any additional arguments are passed through to eos-tui.\n");
  }
};
} // namespace

void
RegisterTuiNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TuiCommand>());
}
