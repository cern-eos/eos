#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

/* Tests assume that location and security json file(s) exists and they contain information for 
 * serial numbers SN1 and SN2, where SN1 is correct and SN2 is incorrect. 
 * Preset is correct when project is built in a build folder in-source and a simulator is started on 
 * localhost. */
#define KINETIC_DRIVE_LOCATION "../../../fst/tests/localhost.json"
#define KINETIC_DRIVE_SECURITY KINETIC_DRIVE_LOCATION

int main( int argc, char* const argv[] )
{
    // Set environment variables so that KineticIo can find the simulator. 
    std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
    setenv("KINETIC_DRIVE_LOCATION", KINETIC_DRIVE_LOCATION, 1);
    
    std::string security(getenv("KINETIC_DRIVE_SECURITY") ? getenv("KINETIC_DRIVE_SECURITY") : "" );
    setenv("KINETIC_DRIVE_SECURITY", KINETIC_DRIVE_SECURITY, 1);

    int result = Catch::Session().run( argc, argv );
    
    // Reset environment variables back to the initial values. 
    setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1); 
    setenv("KINETIC_DRIVE_SECURITY", security.c_str(), 1); 
    return result;
}