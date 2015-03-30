include_directories(${KINETIC_INCLUDE_DIR})

add_executable(kinetic-test
     KineticTestMain.cc
     KineticChunkTest.cc
     KineticIoTest.cc
     ../io/KineticChunk.cc
     ../io/KineticIo.cc
)
add_dependencies(kinetic-test kinetic_cpp_client)
target_link_libraries(kinetic-test 
        eosCommon
        ${KINETIC_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        )