#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern void hooks_init();
extern void hooks_deinit();
extern void yagp_functions_reset();
extern Datum yagp_functions_get(FunctionCallInfo fcinfo);

extern void init_log();
extern void truncate_log();

extern void test_uds_start_server(const char *path);
extern int64_t test_uds_receive(int timeout_ms);
extern void test_uds_stop_server();

#ifdef __cplusplus
}
#endif