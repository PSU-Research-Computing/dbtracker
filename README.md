# dbtracker

[![Code Climate](https://codeclimate.com/github/PSU-OIT-ARC/dbtracker/badges/gpa.svg)](https://codeclimate.com/github/PSU-OIT-ARC/dbtracker)


`dbtracker` quieries the mysql and postgresql databases at ARC for stats, and saves them into postgresql.

## Prerequisites

`dbtracker` was developed inside of [PSU-OIT-ARC/vagrant-manifest](https://github.com/PSU-OIT-ARC/vagrant-manifest) and uses the following packages:

- python3
- postgresql

## Install

```sh
$ make init
```

This will set up a virtualenv and create a database for dbtracker to save to inside of postgresql.  Running this command will likely require root as it creates a postgres database.

Inside of vagrant, this will just work.  For production make sure `psql` is running aginst the correct postgres server.

```sh
$ source env/bin/activate
$ python setup.py install
# or
$ python setup.py develop
```

This provides a `dbtracker` command that queries the ARCs databases and optionally saves these stats to postgres.

```sh
$ dbtracker -h
```

## Configure

`dbtracker` expects a `dbtracker.ini` inside `~/.config/`.  This repository has an example `dbtracker.ini` file which can be duplicated and configured by hand.  An alternate path can be specified using the `--config /path/to/config.ini` flag.
