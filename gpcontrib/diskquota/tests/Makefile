.PHONY: installcheck-regress
installcheck-regress:
	$(MAKE) -C regress installcheck

## Check whether we are able to run isolation2 tests.
## If the gp_inject_fault extension is not enabled, we emit a warning message for that.
.PHONY: installcheck-isolation2
installcheck-isolation2:
ifneq (,$(findstring '--enable-debug-extensions',$(shell pg_config --configure)))
	$(MAKE) -C isolation2 installcheck
else
	@echo -e "\033[0;33mThe gp_inject_fault extension is not enabled in the current build of Greenplum, isolation2 tests will not run."
	@echo -e "To enable gp_inject_fault extension, append --enable-debug-extensions option to ./configure\033[0m"
endif
