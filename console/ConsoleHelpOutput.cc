// ----------------------------------------------------------------------
// File: ConsoleHelpOutput.cc
// ----------------------------------------------------------------------

#include "console/ConsoleCli.hh"
#include "console/ConsoleHelp.hh"
#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <fcntl.h>
#include <memory>
#include <spawn.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace eos::console {
namespace {
bool configured = false;
bool batchOutput = false;
bool jsonOutput = false;
bool pagerDisabled = false;

winsize
terminalSize(int descriptor)
{
  winsize size{};
  if (ioctl(descriptor, TIOCGWINSZ, &size) != 0) {
    size = {};
  }
  return size;
}

bool
pageHelp(std::string_view text, std::FILE* stream)
{
  const auto width = HelpWidth(fileno(stream));
  const char* command = getenv("EOS_PAGER");
  if (!width || pagerDisabled || !isatty(STDIN_FILENO) || !command || !*command) {
    return false;
  }
  const auto size = terminalSize(fileno(stream));
  const size_t rows = size.ws_row ? size.ws_row : 24;
  if (static_cast<size_t>(std::count(text.begin(), text.end(), '\n')) < rows) {
    return false;
  }
  std::vector<std::string> arguments;
  if (!TokenizeArguments(command, arguments) || arguments.empty() ||
      arguments.front() == "cat") {
    return false;
  }
  // A temporary input file avoids SIGPIPE when a pager exits early. No shell
  // interprets EOS_PAGER: quoted arguments are passed directly to the program.
  std::unique_ptr<std::FILE, decltype(&fclose)> input(tmpfile(), fclose);
  if (!input || fwrite(text.data(), 1, text.size(), input.get()) != text.size() ||
      fflush(input.get()) != 0 || fseek(input.get(), 0, SEEK_SET) != 0) {
    return false;
  }
  const int inputDescriptor = fileno(input.get());
  if (fcntl(inputDescriptor, F_SETFD, FD_CLOEXEC) == -1 || fflush(stream) != 0) {
    return false;
  }
  std::vector<char*> argv;
  for (auto& argument : arguments) {
    argv.push_back(argument.data());
  }
  argv.push_back(nullptr);
  posix_spawn_file_actions_t actions;
  if (posix_spawn_file_actions_init(&actions) != 0) {
    return false;
  }
  const int actionError =
      posix_spawn_file_actions_adddup2(&actions, inputDescriptor, STDIN_FILENO) ||
      posix_spawn_file_actions_adddup2(&actions, fileno(stream), STDOUT_FILENO);
  pid_t child = -1;
  const int error = actionError ? actionError
                                : posix_spawnp(&child, argv.front(), &actions, nullptr,
                                               argv.data(), environ);
  posix_spawn_file_actions_destroy(&actions);
  if (error) {
    return false;
  }
  int status = 0;
  pid_t waited;
  do {
    waited = waitpid(child, &status, 0);
  } while (waited == -1 && errno == EINTR);
  return waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
} // namespace

void
ConfigureHelp(bool batch, bool json, bool noPager)
{
  configured = true;
  batchOutput = batch;
  jsonOutput = json;
  pagerDisabled = noPager;
}

void
SetJsonHelpOutput(bool json)
{
  jsonOutput = json;
}

size_t
HelpWidth(int descriptor)
{
  if (!configured || batchOutput || jsonOutput || !isatty(descriptor)) {
    return 0;
  }
  if (const char* widthOverride = getenv("EOS_HELP_WIDTH")) {
    size_t width = 0;
    const std::string_view value(widthOverride);
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), width);
    if (parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size() &&
        width >= 20 && width <= 1000) {
      return width;
    }
  }
  const auto size = terminalSize(descriptor);
  return size.ws_col ? std::min<size_t>(size.ws_col, 120) : 80;
}

void
WriteHelpOutput(std::string_view text, std::FILE* stream)
{
  if (!text.empty() && !pageHelp(text, stream)) {
    fwrite(text.data(), 1, text.size(), stream);
  }
}

void
PrintHelpPage(const HelpPage& page, std::FILE* stream)
{
  const auto width = HelpWidth(fileno(stream));
  PrintLegacyHelp(page.render(width ? width : 80), stream);
}
} // namespace eos::console
