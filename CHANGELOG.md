# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.1] - 2020-08-10

### Added

* Build using CMake.

### Changed

* Fixed bug in prepared statements after reconnection to the DB.
* Updated etc/hdb++_mysql_migrate_array_json.sql
* Updated README.md.

### Removed

## [1.2.0] - 2018-04-04

### Added

* etc/create_hdb++_mysql_innodb_json.sql file to create the MySQL schema using InnoDB engine and time-range partitioning and JSON field for arrays.
* etc/hdb++_mysql_migrate_array_json.sql to migrate data from the tables with multiple rows per array element to the tables with JSON fields

### Changed

* LibHdb++MySQL.cpp: added json_array configuration key to store array elements in a JSON field of a single row
* Updated README.md.

### Removed

## [1.1.0] - 2018-03-02

### Added

* etc/create_hdb++_mysql_innodb.sql file to create the MySQL schema using InnoDB engine and time-range partitioning.

### Changed

* LibHdb++MySQL.*: added ignore_duplicates configuration key to ignore duplicated key (att_conf_id,data_time) when inserting data in InnoDB schema
* Updated README.md.

### Removed


## [1.0.0] - 2017-09-28

### Added

* CHANGELOG.md file.
* Debian Package build files under debian/

### Changed

* Makefile: Added install rules, clean up paths etc
* libhdb++ include paths to match new install location.
* Updated README.md.

### Removed

* libhdbpp submodule. This now has to be installed manually.
