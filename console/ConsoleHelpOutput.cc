// ----------------------------------------------------------------------
// File: ConsoleHelpOutput.cc
// ----------------------------------------------------------------------

#include "console/ConsoleHelp.hh"
#include <algorithm>
#include <sys/ioctl.h>
#include <unistd.h>

namespace eos::console {
namespace {
bool configured = false;
bool batchOutput = false;
bool jsonOutput = false;
} // namespace

void
ConfigureHelp(bool batch, bool json)
{
  configured = true;
  batchOutput = batch;
  jsonOutput = json;
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
  winsize size{};
  if (ioctl(descriptor, TIOCGWINSZ, &size) != 0 || !size.ws_col) {
    return 80;
  }
  return std::min<size_t>(size.ws_col, 120);
}

void
PrintHelpPage(const HelpPage& page, std::FILE* stream)
{
  const auto width = HelpWidth(fileno(stream));
  const auto text = page.render(width ? width : 80, fileno(stream));
  fwrite(text.data(), 1, text.size(), stream);
}
} // namespace eos::console
