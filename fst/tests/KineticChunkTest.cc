#include "catch.hpp"
#include <errno.h>
#include <unistd.h>
#include "fst/io/KineticChunk.hh"



SCENARIO("Single chunk public API test.", "[Chunk]"){
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

	GIVEN ("An empty chunk."){
		KineticChunk c(con, "key");

		THEN("Illegal writes to the chunk fail."){
			char buf[10];
			REQUIRE(c.write(NULL, 0, 0) == EINVAL);
			REQUIRE(c.write(buf,1024*1024,1) == EINVAL);
		}

		THEN("The chunk is not dirty."){
			REQUIRE(c.dirty() == false);
		}

		WHEN("Something is written to the chunk."){
			char in[] = "0123456789";
			REQUIRE(c.write(in, 0, 10) == 0);

			THEN("It can be read again from memory."){
				char out[10];
				REQUIRE(c.read(out,0,10) == 0);
				REQUIRE(memcmp(in,out,10) == 0);
			}

			THEN("It is dirty"){
				REQUIRE(c.dirty() == true);
			}

			AND_WHEN("It is truncated to size 0."){
				REQUIRE(c.truncate(0) == 0);

				THEN("Reading from the chunk returns 0s."){
					char out[] = "0123456789";
					char compare[10];
					memset(compare,0,10);
					REQUIRE(c.read(out,0,10)==0);
					REQUIRE(memcmp(compare,out,10) == 0);
				}
			}

			AND_WHEN("It is flushed."){
				REQUIRE(c.flush() == 0 );

				THEN("It can be read again from the drive."){
					KineticChunk x(con, "key");
					char out[10];
					REQUIRE(x.read(out,0,10) == 0);
					REQUIRE(memcmp(in,out,10) == 0);
				}

				THEN("It is no longer dirty."){
					REQUIRE(c.dirty() == false);
				}

				AND_WHEN("The on-drive value is manipulated by someone else."){
					KineticChunk x(con, "key");
					REQUIRE(x.write("99",0,2) == 0);
					REQUIRE(x.flush() == 0);

					THEN("The change is not visible immediately."){
						char out[10];
						REQUIRE(c.read(out,0,10) == 0);
						REQUIRE(memcmp(in,out,10) == 0);

						AND_THEN("It will become visible after expiration time has run out."){
							usleep(c.expiration_time * 1000);
							REQUIRE(c.read(out,0,10) == 0);
							REQUIRE(memcmp(in,out,10) != 0);
						}
					}
				}
			}
		}
	}
}
