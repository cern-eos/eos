# Try to find prometheus-cpp (devel)
# Once done, this will define the imported target prometheus-cpp::pull.

find_package(prometheus-cpp CONFIG QUIET)

if (TARGET prometheus-cpp::pull)
  set(PrometheusCpp_FOUND TRUE)
else()
  find_package(PkgConfig QUIET)

  if (PkgConfig_FOUND)
    pkg_check_modules(PROMETHEUS_CPP_PULL QUIET IMPORTED_TARGET prometheus-cpp-pull)
  endif()

  if (TARGET PkgConfig::PROMETHEUS_CPP_PULL)
    add_library(prometheus-cpp::pull INTERFACE IMPORTED)
    target_link_libraries(prometheus-cpp::pull INTERFACE
      PkgConfig::PROMETHEUS_CPP_PULL)
    set(PrometheusCpp_FOUND TRUE)
  endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PrometheusCpp
  REQUIRED_VARS PrometheusCpp_FOUND)
