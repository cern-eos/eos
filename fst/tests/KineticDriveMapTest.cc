#include "catch.hpp"
#include "fst/io/KineticDriveMap.hh"
#include <exception>


SCENARIO("KineticDriveMap Public API.", "[DriveMap]"){
    GIVEN("An invalid path"){
        KineticDriveMap kdm("thisPathDoesNotExist");
        THEN("DriveMap is empty"){
            REQUIRE(kdm.getSize() == 0);
        }
    }
    GIVEN("A path to a non json file"){
        KineticDriveMap kdm("kinetic-test");
        THEN("DriveMap is empty"){
            REQUIRE(kdm.getSize() == 0);
        }
    }
    GIVEN("The path set in the environment variable"){
        KineticDriveMap kdm("");
        std::shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> con; 
        
        THEN("DriveMap size equals number of drive entries."){
            REQUIRE(kdm.getSize() == 2);
        }
        THEN("An existing wwn to a running device returns a working connection."){
            REQUIRE(kdm.getConnection("wwn1", con) == 0);
            REQUIRE(con->NoOp().ok()); 
        }
        THEN("An existing wwn with an unreachable IP returns ENXIO"){
            REQUIRE(kdm.getConnection("wwn2", con) == ENXIO);   
        }
        THEN("A nonexisting wwn returns ENODEV."){
            REQUIRE(kdm.getConnection("nonExistingWWN", con) == ENODEV);    
        }
    }
}
