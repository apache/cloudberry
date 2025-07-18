#include "greengage_cluster_info.h"
#include "greengage_cluster_info_internal.h"
#include "pg_upgrade_greengage.h"

char *
greengage_extra_pg_ctl_flags(GreengageClusterInfo *info)
{
	int gp_dbid;
	int gp_content_id;

	if (is_greengage_dispatcher_mode())
	{
		gp_dbid       = 1;
		gp_content_id = -1;
	}
	else
	{
		gp_dbid       = get_gp_dbid(info);
		gp_content_id = 0;
	}

	return psprintf("--gp_dbid=%d --gp_contentid=%d ", gp_dbid, gp_content_id);
}
