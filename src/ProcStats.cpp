#include "ProcStats.h"
#include "yagpcc_metrics.pb.h"
#include <string>
#include <fstream>
#include <unistd.h>

extern "C"
{
#include "postgres.h"
#include "utils/elog.h"
}

namespace {
#define FILL_IO_STAT(stat_name)             \
    uint64_t stat_name;                     \
    proc_stat >> tmp >> stat_name;          \
    stats->set_##stat_name(stat_name);

void fill_io_stats(yagpcc::SystemStat *stats)
{
    std::ifstream proc_stat("/proc/self/io");
    std::string tmp;
    FILL_IO_STAT(rchar);
    FILL_IO_STAT(wchar);
    FILL_IO_STAT(syscr);
    FILL_IO_STAT(syscw);
    FILL_IO_STAT(read_bytes);
    FILL_IO_STAT(write_bytes);
    FILL_IO_STAT(cancelled_write_bytes);
}

void fill_cpu_stats(yagpcc::SystemStat *stats)
{
    static const int UTIME_ID = 13;
    static const int STIME_ID = 14;
    static const int STARTTIME_ID = 21;
    static const int VSIZE_ID = 22;
    static const int RSS_ID = 23;
    static const double tps = sysconf(_SC_CLK_TCK);

    double uptime;
    {
        std::ifstream proc_stat("/proc/uptime");
        proc_stat >> uptime;
    }

    std::ifstream proc_stat("/proc/self/stat");
    std::string trash;
    double start_time = 0;
    for (int i = 0; i <= RSS_ID; ++i)
    {
        switch (i)
        {
        case UTIME_ID:
            double utime;
            proc_stat >> utime;
            stats->set_usertimeseconds(utime / tps);
            break;
        case STIME_ID:
            double stime;
            proc_stat >> stime;
            stats->set_kerneltimeseconds(stime / tps);
            break;
        case STARTTIME_ID:
            uint64_t starttime;
            proc_stat >> starttime;
            start_time = static_cast<double>(starttime) / tps;
            break;
        case VSIZE_ID:
            uint64_t vsize;
            proc_stat >> vsize;
            stats->set_vsize(vsize);
            break;
        case RSS_ID:
            uint64_t rss;
            proc_stat >> rss;
            // NOTE: this is a double AFAIU, need to double-check
            stats->set_rss(rss);
            break;
        default:
            proc_stat >> trash;
        }
        stats->set_runningtimeseconds(uptime - start_time);
    }
}

void fill_status_stats(yagpcc::SystemStat *stats)
{
    std::ifstream proc_stat("/proc/self/status");
    std::string key, measure;
    while (proc_stat >> key)
    {
        if (key == "VmPeak:")
        {
            uint64_t value;
            proc_stat >> value;
            stats->set_vmpeakkb(value);
            proc_stat >> measure;
            if (measure != "kB")
                elog(FATAL, "Expected memory sizes in kB, but got in %s", measure.c_str());
        }
        else if (key == "VmSize:")
        {
            uint64_t value;
            proc_stat >> value;
            stats->set_vmsizekb(value);
            if (measure != "kB")
                elog(FATAL, "Expected memory sizes in kB, but got in %s", measure.c_str());
        }
    }
}
} // namespace

void fill_self_stats(yagpcc::SystemStat *stats)
{
    fill_io_stats(stats);
    fill_cpu_stats(stats);
    fill_status_stats(stats);
}