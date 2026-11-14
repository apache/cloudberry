#ifndef __PIPE__
#define __PIPE__

#define LOCALMSGSZ (8*1024)

/*
 * Orafce can use shared memory. For security reasons, the size of
 * used shared memory is restricted. When orafce is loaded by
 * shared_proload_library, the limit is 3MB, in other cases, the
 * limit is 30kB. The SHMEMMSGSZ should be enough for execution
 * of regresion tests, and for some basic work and usage pipes and
 * alerts for notification.
 *
 * Attention, 30kB is a minimum, and this limit can be early
 * exhausted, but 30kB is safe limit and this shared memory
 * always available without necessity of preallocation.
 */
#define SHMEMMSGSZ_MIN		(30*1024)
#define SHMEMMSGSZ_DEFAULT	(3*1024*1024)
#define MAX_PIPES  30
#define MAX_EVENTS 30
#define MAX_LOCKS  256

typedef struct _message_item
{
	char	   *message;
	float8		timestamp;
	struct _message_item *next_message;
	struct _message_item *prev_message;
	unsigned char message_id;
	int		   *receivers;		/* copy of array all registered receivers */
	int			receivers_number;
} message_item;

typedef struct _message_echo
{
	struct _message_item *message;
	unsigned char message_id;
	struct _message_echo *next_echo;
} message_echo;

typedef struct
{
	char	   *event_name;
	int			max_receivers;
	int		   *receivers;
	int			receivers_number;
	struct _message_item *messages;
} alert_event;

typedef struct
{
	int			sid;
	int			pid;
	message_echo *echo;
} alert_lock;

bool		ora_lock_shmem(size_t size, int max_pipes, int max_events, int max_locks, bool reset);

#define ERRCODE_ORA_PACKAGES_LOCK_REQUEST_ERROR        MAKE_SQLSTATE('3','0', '0','0','1')

#define LOCK_ERROR() \
	ereport(ERROR, \
	(errcode(ERRCODE_ORA_PACKAGES_LOCK_REQUEST_ERROR), \
	 errmsg("lock request error"), \
	 errdetail("Failed exclusive locking of shared memory."), \
	 errhint("Restart PostgreSQL server.")));
#endif

extern alert_event *events;
extern alert_lock *locks;

extern int	sid;
extern LWLockId shmem_lockid;

extern size_t orafce_shmemmsgsz;

#include "storage/condition_variable.h"

extern ConditionVariable *alert_cv;
