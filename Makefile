#!/usr/bin/make -f

all:
	npm install --production
	
install:
	mkdir -p $(DESTDIR)/usr/lib/node_modules/gearman
	cp -a lib test node_modules package.json $(DESTDIR)/usr/lib/node_modules/gearman