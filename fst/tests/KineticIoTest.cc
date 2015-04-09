#include "catch.hpp"
#include "fst/io/KineticIo.hh"

SCENARIO("KineticIo handles incorrect input.", "[KineticIo]"){
    
    GIVEN("An empty KineticIo object created with a nullpointer connection"){
        eos::fst::KineticIo kio(NULL);
        THEN("All operations fail with ENXIO error code"){
            char buffer[64];
            REQUIRE(kio.Open("path", SFS_O_CREAT) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Read(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.ReadAsync(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Write(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.WriteAsync(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Truncate(0) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Fallocate(64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Remove() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Sync() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Close() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            struct statfs sfs;
            REQUIRE(kio.Statfs("path", &sfs) == ENXIO);
        }
        THEN("Encapsulated attr functions fail"){
            eos::fst::KineticIo::Attr a ("path", kio);
            char buffer[64]; size_t size;
            REQUIRE(a.Get("name",buffer,size) == false);
            REQUIRE(a.Set("name",buffer,size) == false);
        }
        
    }
}

SCENARIO("KineticIo public API test.", "[KineticIo]"){

	kinetic::ConnectionOptions options;
	options.host = "localhost";
	options.port = 8443;
	options.use_ssl = true;
	options.user_id = 1;
	options.hmac_key = "asdfasdf";

	kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
	std::shared_ptr<kinetic::BlockingKineticConnection> con;

	REQUIRE(factory.NewBlockingConnection(options, con, 30).ok() == true);
	REQUIRE(con->InstantErase("NULL").ok());

	GIVEN ("An empty KineticIo object and an empty drive."){
		eos::fst::KineticIo kio(con);

		THEN("Open fails without create flag."){
			REQUIRE(kio.Open("path",0) == SFS_ERROR);
		}
		WHEN("Open succeeds"){
			REQUIRE(kio.Open("path", SFS_O_CREAT) == SFS_OK);

			THEN("Writing is possible from object start."){
				REQUIRE(kio.Write(0, "123",3) == 3);
			}
			THEN("Writing is possible from any offset."){
				REQUIRE(kio.Write(1000000,"123",3) == 3);
			}
			THEN("Reading is possible from object start."){
				char buf[10];
				REQUIRE(kio.Read(0,buf,10) == 10);
			}
			THEN("Reading is possible from any offset."){
				char buf[10];
				REQUIRE(kio.Read(1000000,buf,10) == 10);
			}

			AND_WHEN("Flushing written data."){
				int buf_size = 64;
				char buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
				for(off_t o=0; o <= 1024*1024; o+=buf_size)
					REQUIRE(kio.Write(o, buf, buf_size) == buf_size);
				REQUIRE(kio.Sync() == SFS_OK);

				THEN("Stat will return the filesize."){
					struct stat stbuf;
					REQUIRE(kio.Stat(&stbuf) == SFS_OK);
					REQUIRE(stbuf.st_blocks == 2);
					REQUIRE(stbuf.st_blksize == 1024*1024);
					REQUIRE(stbuf.st_size == stbuf.st_blksize+buf_size);
				}
				THEN("Open fails with create flag."){
					REQUIRE(kio.Close() == SFS_OK);
					REQUIRE(kio.Open("path", SFS_O_CREAT) == SFS_ERROR);
				}
				THEN("The file can can be removed again."){
					REQUIRE(kio.Remove() == SFS_OK);
					REQUIRE(kio.Close() == SFS_OK);
					REQUIRE(kio.Open("path", SFS_O_CREAT) == SFS_OK);
				}
			}
		}
	}
}
