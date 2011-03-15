#-------------------------------------------------------------------------------
# Detect the operating system and define variables
#-------------------------------------------------------------------------------
function(EOS_defineOperatingSystem)
	# nothing detected yet
	set(Linux FALSE PARENT_SCOPE)
	set(MacOSX FALSE PARENT_SCOPE)
	set(Windows FALSE PARENT_SCOPE)
	set(OSDEFINE "")
	# check if we are on Linux
	if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
				set(Linux TRUE PARENT_SCOPE)
                                set(OSDEFINE "-D__LINUX__=1" PARENT_SCOPE)
				endif(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
				# check if we are on MacOSX
				if(APPLE)
					set(MacOSX TRUE PARENT_SCOPE)
                                        set(OSDEFINE "-D__APPLE__=1" PARENT_SCOPE)
					endif(APPLE)

					# check if we are on Windows
					if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
								set(Windows TRUE PARENT_SCOPE)
                                                                set(OSDEFINE "-D__WINDOWS__=1" PARENT_SCOPE)
								endif(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
endfunction(EOS_defineOperatingSystem)
#-------------------------------------------------------------------------------


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
    message(FATAL_ERROR "AliRoot cannot be built in-source! Please run cmake $ALICE_ROOT outside the source directory")
  endif(${srcdir} STREQUAL ${bindir})

endmacro(EOS_CheckOutOfSourceBuild)
