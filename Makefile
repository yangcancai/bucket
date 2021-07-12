# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd modules && $(MAKE) $@

install:
	cd modules && $(MAKE) $@

.PHONY: install
