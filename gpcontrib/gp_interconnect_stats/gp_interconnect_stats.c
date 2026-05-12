/*--------------------------------------------------------------------------
 *
 * gp_interconnect_stats.c
 *	  Routine for getting UDPIFC interconnect stats
 *
 **--------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "cdb/ic_udpifc.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gp_interconnect_get_stats);

Datum
gp_interconnect_get_stats(PG_FUNCTION_ARGS)
{
	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDPIFC)
		return GpInterconnectGetStatsUDPIFC(fcinfo);
	ereport(WARNING,
		(errcode(ERRCODE_WARNING_GP_INTERCONNECTION),
		errmsg("Interconnect statistics are collected only for UDPIFC protocol")));
	PG_RETURN_NULL();
}