/*-------------------------------------------------------------------------
 *
 * ps_status.h
 *
 * Declarations for backend/utils/misc/ps_status.c
 *
 * src/include/utils/ps_status.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PS_STATUS_H
#define PS_STATUS_H

extern bool update_process_title;

extern char **save_ps_display_args(int argc, char **argv);

extern void init_ps_display(const char *fixed_part);

extern void set_ps_display(const char *activity);

extern const char *get_ps_display(int *displen);

/* CDB: Get the "username" string saved by init_ps_display().  */
extern const char *get_ps_display_username(void);
extern const char *get_real_act_ps_display(int *displen);
extern void set_ps_display_username(const char* username);

#endif							/* PS_STATUS_H */
