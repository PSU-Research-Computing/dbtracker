.PHONY: run clean reload init

DEFAULT_DB_NAME=dbtracker

PYTHON=python3
export PATH:=.env/bin:/usr/pgsql-9.3/bin:$(PATH)

init:
	rm -rf env
	$(MAKE) env
	psql postgres -c "CREATE DATABASE $(DEFAULT_DB_NAME);"
	psql $(DEFAULT_DB_NAME) < sql.sql

env:
	virtualenv -p python3 env
