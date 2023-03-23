#pragma once

#ifdef __cplusplus
extern "C"
{
#endif

extern void stat_statements_parser_init(void);
extern void stat_statements_parser_deinit(void);

#ifdef __cplusplus
}
#endif

uint64_t get_plan_id(QueryDesc *queryDesc);