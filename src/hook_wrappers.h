#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern void hooks_init();
extern void hooks_deinit();
extern void yagp_functions_reset();
extern Datum yagp_functions_get(FunctionCallInfo fcinfo);

#ifdef __cplusplus
}
#endif