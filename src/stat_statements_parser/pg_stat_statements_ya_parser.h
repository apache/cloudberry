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

StringInfo gen_normplan(const char *executionPlan);
char *gen_normquery(const char *query);