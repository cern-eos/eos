#-------------------------------------------------------------------------------
# Get version
#-------------------------------------------------------------------------------
function(EOS_GetVersion MAJOR MINOR PATCH RELEASE)
  if(("${MAJOR}" STREQUAL "") OR
     ("${MINOR}" STREQUAL "") OR
     ("${PATCH}" STREQUAL ""))
    execute_process(
      COMMAND ${CMAKE_SOURCE_DIR}/genversion.sh ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE VERSION_INFO
      OUTPUT_STRIP_TRAILING_WHITESPACE)

    string(REPLACE "." ";" VERSION_LIST ${VERSION_INFO})
    list(GET VERSION_LIST 0 MAJOR)
    list(GET VERSION_LIST 1 MINOR)
    list(GET VERSION_LIST 2 PATCH)
  endif()

  set(VERSION_MAJOR ${MAJOR} PARENT_SCOPE)
  set(VERSION_MINOR ${MINOR} PARENT_SCOPE)
  set(VERSION_PATCH ${PATCH} PARENT_SCOPE)
  set(VERSION "${MAJOR}.${MINOR}.${PATCH}" PARENT_SCOPE)

  if("${RELEASE}" STREQUAL "")
    set(RELEASE "head")
  endif()

  set(RELEASE ${RELEASE} PARENT_SCOPE)
endfunction()

#-------------------------------------------------------------------------------
# Detect the operating system and define variables
#-------------------------------------------------------------------------------
function(EOS_DefineOperatingSystem)
  # Nothing detected yet
  set(Linux FALSE PARENT_SCOPE)
  set(MacOSX FALSE PARENT_SCOPE)
  set(Windows FALSE PARENT_SCOPE)
  set(OSDEFINE "")

  # Check if we are on Linux
  if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    set(Linux TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__LINUX__=1" PARENT_SCOPE)
  endif()

  # Check if we are on MacOSX
  if(APPLE)
    set(MacOSX TRUE PARENT_SCOPE)
    set(CLIENT TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__APPLE__=1" PARENT_SCOPE)
  endif(APPLE)

  # Check if we are on Windows
  if("${CMAKE_SYSTEM_NAME}" STREQUAL "Windows")
    set(Windows TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__WINDOWS__=1" PARENT_SCOPE)
  endif()
endfunction()

#-------------------------------------------------------------------------------
# Detect in source builds
#-------------------------------------------------------------------------------
macro(EOS_CheckOutOfSourceBuild)
  
  #Check if previous in-source build failed
  if(EXISTS ${CMAKE_SOURCE_DIR}/CMakeCache.txt OR EXISTS ${CMAKE_SOURCE_DIR}/CMakeFiles)
    message(FATAL_ERROR "CMakeCache.txt or CMakeFiles exists in source directory! Please remove them before running cmake .")
  endif(EXISTS ${CMAKE_SOURCE_DIR}/CMakeCache.txt OR EXISTS ${CMAKE_SOURCE_DIR}/CMakeFiles)
  
  #Get Real Paths of the source and binary directories
  get_filename_component(srcdir "${CMAKE_SOURCE_DIR}" REALPATH)
  get_filename_component(bindir "${CMAKE_BINARY_DIR}" REALPATH)
  
  #Check for in-source builds
  if(${srcdir} STREQUAL ${bindir})
    message(FATAL_ERROR "EOS cannot be built in-source! Please run cmake <src-dir> outside the source directory")
  endif(${srcdir} STREQUAL ${bindir})

endmacro(EOS_CheckOutOfSourceBuild)
