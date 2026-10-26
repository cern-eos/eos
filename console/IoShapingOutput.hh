// ----------------------------------------------------------------------
// File: IoShapingOutput.hh
// ----------------------------------------------------------------------

#pragma once

#include "proto/ConsoleRequest.pb.h"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace eos::console {

// Request structured data only for the shaping views rendered locally.
bool PrepareIoShapingHumanOutput(RequestProto& request, bool globalJson);

// An unsupported or malformed reply leaves the existing server output intact.
std::optional<std::string> RenderIoShapingOutput(const IoProto_ShapingProto& request,
                                                 std::string_view payload,
                                                 std::size_t width, int descriptor);

} // namespace eos::console
