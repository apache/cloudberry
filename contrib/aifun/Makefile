EXTENSION = aifun
DATA = aifun--1.0.sql
CONTROL = aifun.control

# Python 3 executable
PYTHON3=$(shell which python3)

# Python code directories
PYTHON_DIR = $(srcdir)/aifun
PG_PYTHON_DIR = $(libdir)/python/aifun

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# ------------------------------------------------------------------
# Custom install section for Python handler files
# ------------------------------------------------------------------

install: python-install

python-install:
	@echo "Installing Python handler files to $(DESTDIR)$(PG_PYTHON_DIR)"
	mkdir -p '$(DESTDIR)$(PG_PYTHON_DIR)'
	cp -a $(PYTHON_DIR)/*.py '$(DESTDIR)$(PG_PYTHON_DIR)/'
	cp -a $(PYTHON_DIR)/providers '$(DESTDIR)$(PG_PYTHON_DIR)/'
	cp $(srcdir)/requirements.txt '$(DESTDIR)$(PG_PYTHON_DIR)/'
	$(PYTHON3) -m pip install --user --upgrade -r '$(DESTDIR)$(PG_PYTHON_DIR)/requirements.txt'

uninstall: python-uninstall

python-uninstall:
	@echo "Removing Python handler files from $(DESTDIR)$(PG_PYTHON_DIR)"
	rm -rf '$(DESTDIR)$(PG_PYTHON_DIR)'
