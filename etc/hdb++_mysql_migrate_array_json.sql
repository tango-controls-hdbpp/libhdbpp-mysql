-----------------------
---rename old tables---
-----------------------

RENAME TABLE att_array_devboolean_ro TO att_array_devboolean_ro_bk;
RENAME TABLE att_array_devboolean_rw TO att_array_devboolean_rw_bk;
RENAME TABLE att_array_devuchar_ro TO att_array_devuchar_ro_bk;
RENAME TABLE att_array_devuchar_rw TO att_array_devuchar_rw_bk;
RENAME TABLE att_array_devshort_ro TO att_array_devshort_ro_bk;
RENAME TABLE att_array_devshort_rw TO att_array_devshort_rw_bk;
RENAME TABLE att_array_devushort_ro TO att_array_devushort_ro_bk;
RENAME TABLE att_array_devushort_rw TO att_array_devushort_rw_bk;
RENAME TABLE att_array_devlong_ro TO att_array_devlong_ro_bk;
RENAME TABLE att_array_devlong_rw TO att_array_devlong_rw_bk;
RENAME TABLE att_array_devulong_ro TO att_array_devulong_ro_bk;
RENAME TABLE att_array_devulong_rw TO att_array_devulong_rw_bk;
RENAME TABLE att_array_devlong64_ro TO att_array_devlong64_ro_bk;
RENAME TABLE att_array_devlong64_rw TO att_array_devlong64_rw_bk;
RENAME TABLE att_array_devulong64_ro TO att_array_devulong64_ro_bk;
RENAME TABLE att_array_devulong64_rw TO att_array_devulong64_rw_bk;
RENAME TABLE att_array_devfloat_ro TO att_array_devfloat_ro_bk;
RENAME TABLE att_array_devfloat_rw TO att_array_devfloat_rw_bk;
RENAME TABLE att_array_devdouble_ro TO att_array_devdouble_ro_bk;
RENAME TABLE att_array_devdouble_rw TO att_array_devdouble_rw_bk;
RENAME TABLE att_array_devstring_ro TO att_array_devstring_ro_bk;
RENAME TABLE att_array_devstring_rw TO att_array_devstring_rw_bk;
RENAME TABLE att_array_devstate_ro TO att_array_devstate_ro_bk;
RENAME TABLE att_array_devstate_rw TO att_array_devstate_rw_bk;
RENAME TABLE att_array_devencoded_ro TO att_array_devencoded_ro_bk;
RENAME TABLE att_array_devencoded_rw TO att_array_devencoded_rw_bk;
RENAME TABLE att_array_devenum_ro TO att_array_devenum_ro_bk;
RENAME TABLE att_array_devenum_rw TO att_array_devenum_rw_bk;

-----------------------
---create new tables---
-----------------------

CREATE TABLE IF NOT EXISTS att_array_devboolean_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Boolean ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devboolean_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Boolean ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devuchar_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array UChar ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devuchar_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array UChar ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devshort_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Short ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devshort_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Short ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devushort_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array UShort ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devushort_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array UShort ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devlong_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Long ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devlong_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Long ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devulong_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array ULong ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devulong_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array ULong ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devlong64_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Long64 ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devlong64_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Long64 ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devulong64_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array ULong64 ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devulong64_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array ULong64 ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devfloat_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Float ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devfloat_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Float ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devdouble_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Double ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devdouble_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Double ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devstring_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array String ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devstring_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array String ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devstate_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array State ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devstate_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array State ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devencoded_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Encoded ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devencoded_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Encoded ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devenum_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Enum ReadOnly Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS att_array_devenum_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time DATETIME(6) NOT NULL,
recv_time DATETIME(6) NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r JSON DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w JSON DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
PRIMARY KEY(att_conf_id, data_time)
) ENGINE=InnoDB COMMENT='Array Enum ReadWrite Values Table'
PARTITION BY RANGE COLUMNS(data_time) (
	PARTITION p000 VALUES LESS THAN ('2015-01-01'),
        PARTITION p2015 VALUES LESS THAN ('2016-01-01'),
        PARTITION p2016 VALUES LESS THAN ('2017-01-01'),
        PARTITION p2017 VALUES LESS THAN ('2018-01-01'),
        PARTITION p2018 VALUES LESS THAN ('2019-01-01'),
        PARTITION future       VALUES LESS THAN MAXVALUE
);

------------------
---migrate data---
------------------

INSERT IGNORE INTO att_array_devboolean_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devboolean_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devboolean_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devboolean_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devuchar_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devuchar_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devuchar_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devuchar_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devshort_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devshort_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devshort_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devshort_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devushort_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devushort_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devushort_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devushort_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devlong_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devlong_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devlong_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devlong_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devulong_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devulong_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devulong_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devulong_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devlong64_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devlong64_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devlong64_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devlong64_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devulong64_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devulong64_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devulong64_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devulong64_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devfloat_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devfloat_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devfloat_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devfloat_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devdouble_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devdouble_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devdouble_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devdouble_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devstring_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devstring_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devstring_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devstring_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devstate_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devstate_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devstate_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devstate_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devencoded_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devencoded_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devencoded_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devencoded_rw_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devenum_ro (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), quality, att_error_desc_id from att_array_devenum_ro_bk GROUP BY att_conf_id,data_time;

INSERT IGNORE INTO att_array_devenum_rw (att_conf_id,data_time,recv_time,dim_x_r, dim_y_r,value_r,dim_x_w, dim_y_w,value_w,quality,att_error_desc_id) SELECT att_conf_id, data_time, recv_time, dim_x_r, dim_y_r, CONCAT('[',GROUP_CONCAT(value_r SEPARATOR ','),']'), dim_x_w, dim_y_w, CONCAT('[',GROUP_CONCAT(value_w SEPARATOR ','),']'),quality, att_error_desc_id from att_array_devenum_rw_bk GROUP BY att_conf_id,data_time;
