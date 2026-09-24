# Native monitoring is not part of XRootD 6.1.1. Probe the API and its library
# symbols, since the bundled development headers alone do not provide it.
set(EOS_XROOTD_METRICS AUTO CACHE STRING "Native XRootD monitoring: AUTO, ON or OFF")
set_property(CACHE EOS_XROOTD_METRICS PROPERTY STRINGS AUTO ON OFF)
if(NOT EOS_XROOTD_METRICS MATCHES "^(AUTO|ON|OFF)$")
  message(FATAL_ERROR "EOS_XROOTD_METRICS must be AUTO, ON or OFF")
endif()

set(HAVE_XROOTD_METRICS FALSE)
if(NOT CLIENT AND NOT PACKAGEONLY AND NOT EOS_XROOTD_METRICS STREQUAL "OFF")
  include(CheckCXXSourceCompiles)
  include(CMakePushCheckState)
  cmake_push_check_state(RESET)
  get_target_property(_xrootd_includes XROOTD::UTILS INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(_xrootd_private_includes XROOTD::PRIVATE INTERFACE_INCLUDE_DIRECTORIES)
  set(CMAKE_REQUIRED_INCLUDES ${_xrootd_includes} ${_xrootd_private_includes})
  # Prefer installed headers. Some monitoring-wip packages export the symbols
  # but omit the headers, so retain the bundled headers for those packages only.
  set(EOS_XROOTD_METRICS_INCLUDE_DIR "")
  if(NOT EXISTS "${_xrootd_includes}/XrdMetrics/XrdMetricsRegistry.hh" AND
     NOT EXISTS "${_xrootd_private_includes}/XrdMetrics/XrdMetricsRegistry.hh")
    set(EOS_XROOTD_METRICS_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/common/xrootd")
    list(APPEND CMAKE_REQUIRED_INCLUDES ${EOS_XROOTD_METRICS_INCLUDE_DIR})
  endif()
  set(CMAKE_REQUIRED_LIBRARIES XROOTD::UTILS)
  # Recheck when the selected installation changes in an existing build tree.
  unset(EOS_XROOTD_METRICS_API CACHE)
  check_cxx_source_compiles([=[
    #include <XrdMetrics/XrdMetricsRegistry.hh>
    int main() {
      XrdMetrics::Collector collector("eos", {{"cluster", "probe"}});
      collector.addTextCollector([](std::string& out) { out += "probe"; });
      auto& registry = XrdMetrics::CollectorRegistry::instance();
      registry.add(collector);
      std::string out;
      for (auto* entry : registry.collectors()) entry->runTextCollectors(out);
      registry.remove(collector);
      return 0;
    }
  ]=] EOS_XROOTD_METRICS_API)
  cmake_pop_check_state()
  set(HAVE_XROOTD_METRICS ${EOS_XROOTD_METRICS_API})
  if(NOT HAVE_XROOTD_METRICS AND EOS_XROOTD_METRICS STREQUAL "ON")
    message(FATAL_ERROR "Native XRootD monitoring requested, but the XrdMetrics API is unavailable")
  endif()
endif()

if(HAVE_XROOTD_METRICS)
  add_compile_definitions(HAVE_XROOTD_METRICS=1)
  message(STATUS "Native XRootD monitoring: enabled")
else()
  message(STATUS "Native XRootD monitoring: disabled (EOS metrics will not be exported)")
endif()
