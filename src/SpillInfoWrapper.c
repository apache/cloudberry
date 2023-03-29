#include "postgres.h"
#include "utils/workfile_mgr.h"

void get_spill_info(int ssid, int ccid, int32_t* file_count, int64_t* total_bytes);

void get_spill_info(int ssid, int ccid, int32_t* file_count, int64_t* total_bytes)
{
    int count = 0;
    int i = 0;
    workfile_set *workfiles = workfile_mgr_cache_entries_get_copy(&count);
    workfile_set *wf_iter = workfiles;
    for (i = 0; i < count; ++i, ++wf_iter)
    {
        if (wf_iter->active && wf_iter->session_id == ssid && wf_iter->command_count == ccid)
        {
            *file_count += wf_iter->num_files;
            *total_bytes += wf_iter->total_bytes;
        }
    }
    pfree(workfiles);
}