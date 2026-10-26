// ----------------------------------------------------------------------
// File: ConsoleColorRequestTest.cc
// ----------------------------------------------------------------------

#include "console/commands/helpers/ICmdHelper.hh"
#include "gtest/gtest.h"

namespace {
class ColorRequest : public ICmdHelper {
public:
  explicit ColorRequest(const GlobalOptions& options)
      : ICmdHelper(options)
  {
  }
  bool
  ParseCommand(const char*) override
  {
    return false;
  }
  const eos::console::RequestProto&
  request() const
  {
    return mReq;
  }
};

TEST(ConsoleColorRequest, ExistingWireFlagFollowsTheOutputPolicy)
{
  using namespace eos::console;
  struct RestoreColor {
    ~RestoreColor() { ConfigureColor(ColorMode::Auto, false, false); }
  } restore;
  GlobalOptions options;
  ConfigureColor(ColorMode::Always, false, false);
  EXPECT_FALSE(ColorRequest(options).request().dontcolor());
  ConfigureColor(ColorMode::Never, false, false);
  EXPECT_TRUE(ColorRequest(options).request().dontcolor());
  ConfigureColor(ColorMode::Always, true, false);
  EXPECT_TRUE(ColorRequest(options).request().dontcolor());
  ConfigureColor(ColorMode::Always, false, false);
  options.mJsonFormat = true;
  ColorRequest json(options);
  EXPECT_TRUE(json.request().dontcolor());
  EXPECT_EQ(json.request().format(), RequestProto::JSON);
}
} // namespace
