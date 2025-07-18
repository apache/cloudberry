#include "postgres_fe.h"
#include "greengage_cluster_info_internal.h"

static int gp_dbid_not_set = -1;

struct GreengageClusterInfoData
{
	int gp_dbid;
};

GreengageClusterInfo * make_cluster_info(void)
{
	GreengageClusterInfo *info = palloc0(sizeof(GreengageClusterInfo));
	info->gp_dbid = gp_dbid_not_set;
	return info;
}

int
get_gp_dbid(GreengageClusterInfo *info)
{
	return info->gp_dbid;
}

void
set_gp_dbid(GreengageClusterInfo *info, int gp_dbid)
{
	info->gp_dbid = gp_dbid;
}

bool
is_gp_dbid_set(GreengageClusterInfo *info)
{
	return info->gp_dbid != gp_dbid_not_set;
}
