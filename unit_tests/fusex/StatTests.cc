#include "fmt/printf.h"
#include "gtest/gtest.h"
#include "fusex/stat/Stat.hh"

// This file only tests parts of the output, consider adding more test cases
// when you touch the code to validate that we don't break outputs
TEST(fusex_stats, default_header)
{
  double avg {0};
  double sig {0};
  size_t ops  {0};
  double TotalExec {0};
  std::string outline = fmt::sprintf("%-7s %-32s %3.02f +- %3.02f = %.02fs (%lu ops)\n", "ALL",
                                     "Execution Time", avg, sig,
                                     TotalExec/1000.0,
                                     ops);
  EXPECT_EQ(outline,
            "ALL     Execution Time                   0.00 +- 0.00 = 0.00s (0 ops)\n");

}

TEST(fusex_stats, header)
{
  double avg = 1.232;
  double sig= 1.658;
  size_t ops = 1<<20;
  double TotalExec = 3.14;
  std::string outline = fmt::sprintf("%-7s %-32s %3.02f +- %3.02f = %.02fs (%lu ops)\n", "ALL",
                                     "Execution Time", avg, sig,
                                     TotalExec/1000.0,
                                     ops);
  EXPECT_EQ(outline,
            "ALL     Execution Time                   1.23 +- 1.66 = 0.00s (1048576 ops)\n");



  avg = 12878.34;
  sig = 2167081.76;
  TotalExec = 36651521.02;
  ops = 1375618;
  outline = fmt::sprintf("%-7s %-32s %3.02f +- %3.02f = %.02fs (%lu ops)\n", "ALL",
                         "Execution Time", avg, sig, TotalExec/1000.0, ops);
  std::string expected = "ALL     Execution Time                   12878.34 +- "
                         "2167081.76 = 36651.52s (1375618 ops)\n";
  EXPECT_EQ(expected, outline);
}

TEST(fusex_stats, simple_float_printf)
{
  Stat s;
  const char* tag="list";

  for (int i = 0; i < 5; i++) {
    s.Add(tag,0, 0, 0);
  }
  constexpr std::string_view TOTAL_AVG_FMT = "%3.02f";

  char c_a5[1024];
  sprintf(c_a5, "%3.02f", s.GetTotalAvg5(tag));
  std::string a5 = fmt::sprintf(TOTAL_AVG_FMT, s.GetTotalAvg5(tag));
  EXPECT_STREQ(c_a5, a5.c_str());

  double pi = 3.142857;
  sprintf(c_a5, "%3.02f", pi);
  EXPECT_STREQ(c_a5, fmt::sprintf(TOTAL_AVG_FMT, pi).c_str());
}


TEST(fusex_stats, cmd_stats)
{
  Stat s;
  const char* tag="list";

  for (int i = 0; i < 5; i++) {
    s.Add(tag,0, 0, 0);
  }

  constexpr std::string_view TOTAL_AVG_FMT = "%3.02f";
  std::string a5 = fmt::sprintf(TOTAL_AVG_FMT, s.GetTotalAvg5(tag));
  std::string a60 = fmt::sprintf(TOTAL_AVG_FMT, s.GetTotalAvg60(tag));
  std::string a300 = fmt::sprintf(TOTAL_AVG_FMT, s.GetTotalAvg300(tag));
  std::string a3600 = fmt::sprintf(TOTAL_AVG_FMT, s.GetTotalAvg3600(tag));

  float avg{0}, sig{0}, total{0};
  uint64_t total_s {0};
  std::string outline = fmt::sprintf("uid=all gid=all cmd=%s total=%llu 5s=%s "
                                     "60s=%s 300s=%s "
                                     "3600s=%s exec=%f execsig=%f cumulated=%f\n",
                                     tag, total_s, a5, a60, a300, a3600, avg, sig,
                                     total);
  EXPECT_EQ(outline,
            "uid=all gid=all cmd=list total=0 5s=0.00 60s=0.00 300s=0.00"
            " 3600s=0.00 exec=0.000000 execsig=0.000000 cumulated=0.000000\n");

  std::string aexec = fmt::sprintf("%3.05f",0.0);
  std::string aexecsig = fmt::sprintf("%3.05f", 0.0);
  std::string atotal = fmt::sprintf("%04.02f",0.0);
  std::string out2 = fmt::sprintf("ALL     %-32s %12llu %8s %8s %8s %8s %8s +- %-10s = %-10s\n",
                                  tag, total_s, a5, a60, a300, a3600, aexec, aexecsig, atotal);
  EXPECT_EQ(out2,
            "ALL     list                                        "
            "0     0.00     0.00     0.00     0.00  0.00000 +- 0.00000    = 0.00      \n");
}
