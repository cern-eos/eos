#include "catch.hpp"
#include "fst/io/KineticDriveMap.hh"
#include <exception>


SCENARIO("KineticDriveMap Public API.", "[DriveMap]"){
    GIVEN("An invalid path"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "nonExistingFileName", 1);
    
        KineticDriveMap kdm;
        THEN("DriveMap is empty"){
            REQUIRE(kdm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }
    
    GIVEN("A path to a non json file"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "kinetic-test", 1);
        
        KineticDriveMap kdm;
        THEN("DriveMap is empty"){
            REQUIRE(kdm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }
    
    GIVEN("A valid path."){
        KineticDriveMap kdm;
        std::shared_ptr<kinetic::BlockingKineticConnectionInterface> con; 
        
        THEN("DriveMap size equals number of drive entries."){
            REQUIRE(kdm.getSize() == 2);
        }
        THEN("An existing drive id to a running device returns a working connection."){
            REQUIRE(kdm.getConnection("SN1", con) == 0);
            REQUIRE(con->NoOp().ok()); 
        }
        THEN("An existing drive id with an unreachable IP returns ENXIO"){
            REQUIRE(kdm.getConnection("SN2", con) == ENXIO);   
        }
        THEN("A nonexisting drive id returns ENODEV."){
            REQUIRE(kdm.getConnection("nonExistingID", con) == ENODEV);    
        }
    }
}
