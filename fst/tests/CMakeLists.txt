include_directories(${KINETIC_INCLUDE_DIR})

add_executable(kinetic-test
     KineticTestMain.cc
     KineticChunkTest.cc
     KineticIoTest.cc
     KineticDriveMapTest.cc
     ../io/KineticChunk.cc
     ../io/KineticIo.cc
     ../io/KineticDriveMap.cc
)
target_link_libraries(kinetic-test 
        eosCommon
        ${JSONC_LIBRARIES}
        ${KINETIC_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        )