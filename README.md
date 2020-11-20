# liphdbpp-mysql

[![TangoControls](https://img.shields.io/badge/-Tango--Controls-7ABB45.svg?style=flat&logo=%20data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAACAAAAAkCAYAAADo6zjiAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAALEwAACxMBAJqcGAAAAsFJREFUWIXtl01IFVEYht9zU%2FvTqOxShLowlOgHykWUGEjUKqiocB1FQURB0KJaRdGiaFM7gzZRLWpTq2olhNQyCtpYCP1gNyIoUTFNnxZzRs8dzvw4Q6564XLnfOf73vedc2a%2BmZEKALgHrC3CUUR8CxZFeEoFalsdM4uLmMgFoIlZLJp3A9ZE4S2oKehhlaR1BTnyg2ocnW%2FxsxEDhbYij4EPVncaeASMAavnS%2FwA8NMaqACNQCew3f4as3KZOYh2SuqTVJeQNiFpn6QGSRVjTH9W%2FiThvcCn6H6n4BvQDvQWFT%2BSIDIFDAKfE3KOAQeBfB0XGPeQvgE67P8ZoB44DvTHmFgJdOQRv%2BUjc%2BavA9siNTWemgfA3TwGquCZ3w8szFIL1ALngIZorndvgJOR0GlP2gtJkzH%2Bd0fGFxW07NqY%2FCrx5QRXcYjbCbmxF1dkBSbi8kpACah3Yi2Sys74cVyxMWY6bk5BTwgRe%2BYlSzLmxNpU3aBeJogk4XWWpJKUeiap3RJYCpQj4QWZDQCuyIAk19Auj%2BAFYGZZjTGjksaBESB8P9iaxUBIaJzjZcCQcwHdj%2BS2Al0xPOeBYYKHk4vfmQ3Y8YkIwRUb7wQGU7j2ePrA1URx93ayd8UpD8klyPbSQfCOMIO05MbI%2BDvwBbjsMdGTwlX21AAMZzEerkaI9zFkP4AeYCPBg6gNuEb6I%2FthFgN1KSQupqzoRELOSed4DGiJala1UmOMr2U%2Bl%2FTWEy9Japa%2Fy41IWi%2FJ3d4%2FkkaAw0Bz3AocArqApwTvet3O3GbgV8qqjAM7bf4N4KMztwTodcYVyelywKSCD5V3xphNXoezuTskNSl4bgxJ6jPGVJJqbN0aSV%2Bd0M0aO7FCs19Jo2lExphXaTkxdRVgQFK7DZVDZ8%2BcpdmQh3wuILh7ut3AEyt%2B51%2BL%2F0cUfwFOX0t0StltmQAAAABJRU5ErkJggg%3D%3D)](http://www.tango-controls.org) [![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0) [![GitHub release](https://img.shields.io/github/release/tango-controls-hdbpp/libhdbpp-mysql.svg)](https://github.com/tango-controls-hdbpp/libhdbpp-mysql/releases)   [![Download](https://api.bintray.com/packages/tango-controls/debian/libhdb%2B%2Bmysql6/images/download.svg)](https://bintray.com/tango-controls/debian/libhdb%2B%2Bmysql6/_latestVersion)


- [liphdbpp-mysql](#liphdbpp-mysql)
  - [Version](#Version)
  - [Documentation](#Documentation)
  - [Building](#Building)
    - [Dependencies](#Dependencies)
    - [Build](#Build)
    - [Build Flags](#Build-Flags)
  - [Installation](#Installation)
  - [DB Schema](#DB-Schema)
    - [Partition management](#Partition-management)
    - [MyIsam to InnoDB schema migration](#MyIsam-to-InnoDB-schema-migration)
    - [JSON arrays schema migration](#JSON-arrays-schema-migration)
    - [TTL: per attribute old data deletion](#TTL:-per-attribute-old-data-deletion)
  - [Configuration](#Configuration)
    - [Library Configuration Parameters](#Library-Configuration-Parameters)
    - [Configuration Example](#Configuration-Example)
  - [License](#License)



Library for HDB++ implementing MySQL schema

## Version

The current release version is 1.2.1

## Documentation

* See the Tango documentation [here](http://tango-controls.readthedocs.io/en/latest/administration/services/hdbpp/index.html#hdb-an-archiving-historian-service) for broader information about the HDB++ archiving system and its integration into Tango Controls
* libhdbpp-mysql [CHANGELOG.md](https://github.com/tango-controls-hdbpp/libhdbpp/blob/master/CHANGELOG.md) contains the latest changes both released and in development.

## Building

### Dependencies

Ensure the development version of the dependencies are installed. These are as follows:

* HDB++ library libhdbpp (downloaded automatically by cmake if not found)
* Tango Controls 9 or higher - either via debian package or source install.
* omniORB release 4 - libomniorb4 and libomnithread.
* libzmq - libzmq3-dev or libzmq5-dev.
* MySQL client library - libmysqlclient-dev or libmariadb-dev

### Build

The build system uses pkg-config to find some dependencies, for example Tango. If Tango is not installed to a standard location, set PKG_CONFIG_PATH, i.e.

```bash
export PKG_CONFIG_PATH=/non/standard/tango/install/location
```

Then to build the library:

```bash
mkdir -p build
cd build
cmake ..
make
```

If HDB++ library libhdbpp is not installed in system paths, CMAKE_PREFIX_PATH can be used to point to non standard install location, or CMAKE_INCLUDE_PATH should be set to point to the sources location. If neither of the previous condition is true, cmake will automatically fetch the dependency from the github repository.
CMake flags can be set on the command line at configuration time, i.e.:

```bash
cmake -DCMAKE_INCLUDE_PATH=/path/to/local/install/of/libhdbpp/headers .. 
```

The pkg-config path can also be set with the cmake argument CMAKE_PREFIX_PATH. This can be set on the command line at configuration time, i.e.:

```bash
cmake -DCMAKE_PREFIX_PATH=/non/standard/dependency/install/location .. 
```

### Build Flags

The following is a list of common useful CMake flags and their use:

| Flag | Setting | Description |
|------|-----|-----|
| CMAKE_INSTALL_PREFIX | PATH | Standard CMake flag to modify the install prefix. |
| CMAKE_PREFIX_PATH | PATH[S] | Standard CMake flag to add paths where to look for installed dependencies. |
| CMAKE_INCLUDE_PATH | PATH[S] | Standard CMake flag to add include paths to the search path. |
| CMAKE_LIBRARY_PATH | PATH[S] | Standard CMake flag to add paths to the library search path |
| CMAKE_BUILD_TYPE | Debug/Release | Build type to produce |


## Installation

Once built simply run `make install`. The install can be passed a PREFIX variable, this is set to /usr/local by default. It is also possible to use DESTDIR. Install path is constructed as DESTDIR/PREFIX.

## DB Schema

Three files are provided to create the DB schema:

* etc/create_hdb++_mysql.sql
* etc/create_hdb++_mysql_innodb.sql
* etc/create_hdb++_mysql_innodb_json.sql

In the first file MyISAM is used as the DB engine and 3 TIMESTAMP columns (data_time, recv_time, insert_time) are created for every data table.
MyIsam is deprecated and partitioning on MyIsam tables is no longer supported since Mysql 8.
In the second file InnoDB is used as the DB engine and 2 DATETIME columns (data_time, recv_time) are created for every data table.
A primary key (att_conf_id, data_time) is created for every data table, with this key maximum performances are obtained by partitioning tables by range on the data_time column.
At the same time with this primary key duplicated of data_time with the same att_conf_id are not allowed. To ignore primary key duplication errors, the 'ignore_duplicates' configuration key can be set to 1.
The third file, as the second one, uses InnoDB as the DB engine, 2 DATETIME columns and partitioning. But for array attributes, instead of storing every element of the array in a single row, stores all the elements of the array in a JSON field.
The use of JSON arrays needs to be enabled with json_array=1 in the LibConfiguration device/class property.
DB Schema, user creation script and partitioning procedure can be found in [hdbpp-mysql-project](https://github.com/tango-controls-hdbpp/hdbpp-mysql-project) under resources/schema.

### Partition management

Partitions defined in etc/create_hdb++_mysql_innodb.sql should be adjusted depending on the amount of data really present in the tables. Partitions 'p000' and 'future' should always be present to catch events arriving with the wrong (in the past or in the future) timestamps.
Older partitions can be dropped with the 'DROP PARTITION' command or moved to another database with the 'ALTER TABLE ... EXCHANGE PARTITION' command.
When approaching the end of the range of the current partition, the 'future' partition should be split into two parts to add the new partition with the 'ALTER TABLE ... REORGANIZE PARTITION' command.

### MyIsam to InnoDB schema migration

To migrate from the schema using MyIsam to the schema with InnoDB and partitiong, the suggested sequence is the following:

* rename existing tables (e.g. 'RENAME TABLE att_scalar_devboolean_ro TO att_scalar_devboolean_ro_old;')
* dump to file the renamed tables (e.g. 'SELECT ad.att_conf_id,ad.data_time,ad.recv_time, ad.value_r, ad.quality, ad.att_error_desc_id INTO OUTFILE '/tmp/att_scalar_devboolean_ro.csv' FROM hdbpp.att_scalar_devboolean_ro ad;')
* optimize MySQL configuration for InnoDB adjusting key_buffer_size, innodb_buffer_pool_size and many more parameters
* create new schema with 'etc/create_hdb++_mysql_innodb.sql'
* bulk load data from files into created tables (e.g. 'LOAD DATA LOCAL INFILE '/tmp/att_scalar_devboolean_ro.csv' INTO TABLE att_scalar_devboolean_ro;'). If the table size is big (in the order of tens of million rows) then the file to be loaded should be split into chunks of around 1 million rows: this can be easily done with scripts like 'pt-fifo-split' from Percona Tools.
* if everything went OK, old renamed tables can be dropped (e.g. 'DROP TABLE att_scalar_devboolean_ro_old;')

### JSON arrays schema migration

To migrate existing data from the schema using InnoDB and partitiong but multiple rows to store arrays elements to the schema using JSON for arrays, the suggested sequence is the following:

* rename existing array tables (e.g. 'RENAME TABLE att_array_devboolean_ro TO att_array_devboolean_ro_bk;')
* create the new array tables using the definitions in etc/create_hdb++_mysql_innodb_json.sql
* migrate using the appropriate queries (e.g. 'INSERT IGNORE INTO att_array_devboolean_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devboolean_ro_bk GROUP BY att_conf_id,data_time;')

The file etc/hdb++_mysql_migrate_array_json.sql can be used for these operations.

### TTL: per attribute old data deletion

In HDB++ a TimeToLive value in hours can be specified for each attribute. It is meant to let data older than the TTL value specified to be deleted. MySQL does not support this feature in a native way so something ad hoc needs to be implemented and run periodically.

The file etc/create_hdb++_mysql_delete_attr_ttl_procedure.sql can be used to create two stored procedure:
 * delete_attr_ttl which takes as parameters the attribute name and the ttl value
 * delete_ttl which takes no parameters and call delete_attr_ttl for all the attributes which ttl is greater than 0
 
 Note that it is preferable to use partitioning and drop the whole old partition. In fact deleting single values in MySQL can have a bad inpact on performances for the following reasons:

* deleting values in MySQL is always a heavy operation
* if MyISAM is used as DB engine, holes are left in tables after deletion. Tables with holes needs to be optimized (which can take quite a long time), otherwise performances degrade and an exclusive lock on the whole table needs to be taken for every operation (also for inserts).

## Configuration

This is the configuration to use for [event subscribers](https://github.com/tango-controls-hdbpp/hdbpp-es) to use this backend

### Library Configuration Parameters

Configuration parameters are as follows:

| Parameter | Mandatory | Default | Description |
|------|-----|-----|-----|
| libname | true | None | Must be "libhdb++mysql.so", a specific version or the full path can be specified |
| host | true | None | MySQL/MariaDB hostname or IP address |
| user | true | None | MySQL/MariaDB username |
| password | true | None | MySQL/MariaDB password |
| dbname | true | None | MySQL/MariaDB db name (e.g. hdbpp) |
| port | true | None | MySQL/MariaDB port (e.g. 3306) |
| lightschema | false | 0 | When set to 1 (true) insert_time and rcv_time are not stored. When set to 0 (false) presence of insert_time, rcv_time, quality, error_desc_id columns are auto-detected at library startup per table |
| ignore_duplicates | false | 0 | Schema optimized for InnoDB has the primary key on (att_conf_id,data_time), set this property to 1 to not have duplicates on (att_conf_id,data_time) signalled as storing errors. |
| json_array | false | 0 | When set to 1 use a JSON array to store Spectrum/Image data types instead of one row per array element. |


### Configuration Example

Short example LibConfiguration property value on an EventSubscriber. You will HAVE to change the various parts to match your system:

```
host=mysql_hostname
user=hdbpprw
password=hdbpprw_pwd
dbname=hdbpp
port=3306
ignore_duplicates=1
json_array=1
libname=libhdb++mysql.so
```` 

## License

The code is released under the LGPL3 license and a copy of this license is provided with the code. 
