# FindActiveMQCPP.cmake

# Locate the header
find_path(ACTIVEMQCPP_INCLUDE_DIR
    NAMES cms/Connection.h
    PATH_SUFFIXES
        activemq-cpp
        activemq-cpp-3.9.5
        activemq-cpp-3.9
        activemq-cpp-3
    PATHS
        /usr/include
        /usr/local/include
        /opt/include
)

# Locate the library
find_library(ACTIVEMQCPP_LIBRARY
    NAMES activemq-cpp
    PATHS
        /usr/lib /usr/lib64
        /usr/local/lib /usr/local/lib64
        /opt/lib /opt/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ActiveMQCPP
    REQUIRED_VARS ACTIVEMQCPP_LIBRARY ACTIVEMQCPP_INCLUDE_DIR
)

if (ACTIVEMQCPP_FOUND AND NOT TARGET ActiveMQCPP::ActiveMQCPP)
    add_library(ActiveMQCPP::ActiveMQCPP UNKNOWN IMPORTED)

    set_target_properties(ActiveMQCPP::ActiveMQCPP PROPERTIES
        IMPORTED_LOCATION "${ACTIVEMQCPP_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ACTIVEMQCPP_INCLUDE_DIR}"
    )
endif()
