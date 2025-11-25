//  File: testMain.cc
//  Author: Ilkay Yanar - 42Lausanne / CERN
//  ----------------------------------------------------------------------

/*************************************************************************
 *  EOS - the CERN Disk Storage System                                   *
 *  Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                       *
 *  This program is free software: you can redistribute it and/or modify *
 *  it under the terms of the GNU General Public License as published by *
 *  the Free Software Foundation, either version 3 of the License, or    *
 *  (at your option) any later version.                                  *
 *                                                                       *
 *  This program is distributed in the hope that it will be useful,      *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 *  GNU General Public License for more details.                         *
 *                                                                       *
 *  You should have received a copy of the GNU General Public License    *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 *************************************************************************/

#include <gtest/gtest.h>
#include "tester.hh"

TEST(IoStat, CleanData) {
	EXPECT_EQ(testIoStatCleaning(), 0);
}

TEST(IoStat, FillData) {
	EXPECT_EQ(testIoStatFillData(), 0);
}

TEST(IoStat, exactValue) {
	ASSERT_FALSE(testIoStatIOPS());
	ASSERT_FALSE(testIoStatExactValue());
	EXPECT_EQ(testIoStatCopy(), 0);
}

TEST(IoMap, FillData) {
	EXPECT_EQ(testIoMapData(), 0);
}

TEST(IoMap, exactValue) {
	ASSERT_FALSE(testIoMapSpecificCase());
	ASSERT_FALSE(testIoMapExactValue());
	ASSERT_FALSE(testIoMapSummary());
	EXPECT_EQ(testIoMapBigVolume(), 0);
	EXPECT_EQ(testIoMapIds(), 0);
	EXPECT_EQ(testIoMapCopy(), 0);
}

TEST(IoAggregateMap, exactValue) {
	ASSERT_FALSE(testIoAggregateMapWindow());
	EXPECT_EQ(testIoAggregateMap(), 0);
	EXPECT_EQ(testIoAggregateMapCopy(), 0);
}

/// Only for debugging, interaction mode with command line
// TEST(IoMap, testWithInteraction) {
// 	EXPECT_EQ(testInteractiveIoMap(), 0);
// }

/// Only for debugging, interaction mode with command line
// TEST(IoMap, testWithInteraction) {
// 	EXPECT_EQ(testIoAggregateMapInteract(), 0);
// 

int main(int ac, char **av) {
	srand(static_cast<unsigned int>(time(NULL)));
	int code = 0;
	(void)ac;
	(void)av;
	(void)code;
	
	// code = testIoStatExactValue();
	// code = testIoStatIOPS();
	// code = testIoStatCopy();

	// code = testIoMapSpecificCase();
	// code = testIoMapSummary();
	// code = testIoMapBigVolume();
	// code = testIoMapExactValue();
	// code = testIoMapIds();
	// code = testInteractiveIoMap();
	// code = testIoMapCopy();

	// code = testIoAggregateMap();
	// code = testIoAggregateMapWindow();
	// code = testIoAggregateMapDelete();
	code = testIoAggregateMapInteract();
	// code = testIoAggregateMapCopy();

	// code = testIoBuffer();
	std::cout << "code: " << code << std::endl;
	google::protobuf::ShutdownProtobufLibrary();
 //    ::testing::InitGoogleTest(&ac, av);
    // return RUN_ALL_TESTS();
}
