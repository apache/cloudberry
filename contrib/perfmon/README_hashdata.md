1. gp_elog and guc:'gpperfmon_log_alert_level' have been
removed in hashdata-lightning
	- disable check_disk_space
	- disable message_main
	- disable gpdb_import_alert_log
2. load gpmon as a shared library
	- disable parse_command_line.
	- get opt.port and opt.conf_file by xx
	- modify the Makefile and gpperfmon_install
