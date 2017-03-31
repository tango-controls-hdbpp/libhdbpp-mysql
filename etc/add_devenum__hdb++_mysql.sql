-- ---------------------------------------------------------------
-- ------------------Add DevEnum support--------------------------
-- ---------------------------------------------------------------

DROP TABLE IF EXISTS att_conf_data_type;
CREATE TABLE IF NOT EXISTS att_conf_data_type
(
att_conf_data_type_id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
data_type VARCHAR(255) NOT NULL,
tango_data_type TINYINT(1) NOT NULL
) ENGINE=MyISAM COMMENT='Attribute types description';

INSERT INTO att_conf_data_type (data_type, tango_data_type) VALUES
('scalar_devboolean_ro', 1),('scalar_devboolean_rw', 1),('array_devboolean_ro', 1),('array_devboolean_rw', 1),
('scalar_devuchar_ro', 22),('scalar_devuchar_rw', 22),('array_devuchar_ro', 22),('array_devuchar_rw', 22),
('scalar_devshort_ro', 2),('scalar_devshort_rw', 2),('array_devshort_ro', 2),('array_devshort_rw', 2),
('scalar_devushort_ro', 6),('scalar_devushort_rw', 6),('array_devushort_ro', 6),('array_devushort_rw', 6),
('scalar_devlong_ro', 3),('scalar_devlong_rw', 3),('array_devlong_ro', 3),('array_devlong_rw', 3),
('scalar_devulong_ro', 7),('scalar_devulong_rw', 7),('array_devulong_ro', 7),('array_devulong_rw', 7),
('scalar_devlong64_ro', 23),('scalar_devlong64_rw', 23),('array_devlong64_ro', 23),('array_devlong64_rw', 23),
('scalar_devulong64_ro', 24),('scalar_devulong64_rw', 24),('array_devulong64_ro', 24),('array_devulong64_rw', 24),
('scalar_devfloat_ro', 4),('scalar_devfloat_rw', 4),('array_devfloat_ro', 4),('array_devfloat_rw', 4),
('scalar_devdouble_ro', 5),('scalar_devdouble_rw', 5),('array_devdouble_ro', 5),('array_devdouble_rw', 5),
('scalar_devstring_ro', 8),('scalar_devstring_rw', 8),('array_devstring_ro', 8),('array_devstring_rw', 8),
('scalar_devstate_ro', 19),('scalar_devstate_rw', 19),('array_devstate_ro', 19),('array_devstate_rw', 19),
('scalar_devencoded_ro', 28),('scalar_devencoded_rw', 28),('array_devencoded_ro', 28),('array_devencoded_rw', 28),
('scalar_devenum_ro', 29),('scalar_devenum_rw', 29),('array_devenum_ro', 29),('array_devenum_rw', 29);

CREATE TABLE IF NOT EXISTS att_scalar_devenum_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time TIMESTAMP(6) DEFAULT 0,
recv_time TIMESTAMP(6) DEFAULT 0,
insert_time TIMESTAMP(6) DEFAULT 0,
value_r SMALLINT DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
INDEX att_conf_id_data_time (att_conf_id,data_time)
) ENGINE=MyISAM COMMENT='Scalar Enum ReadOnly Values Table';

CREATE TABLE IF NOT EXISTS att_scalar_devenum_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time TIMESTAMP(6) DEFAULT 0,
recv_time TIMESTAMP(6) DEFAULT 0,
insert_time TIMESTAMP(6) DEFAULT 0,
value_r SMALLINT DEFAULT NULL,
value_w SMALLINT DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
INDEX att_conf_id_data_time (att_conf_id,data_time)
) ENGINE=MyISAM COMMENT='Scalar Enum ReadWrite Values Table';

CREATE TABLE IF NOT EXISTS att_array_devenum_ro
(
att_conf_id INT UNSIGNED NOT NULL,
data_time TIMESTAMP(6) DEFAULT 0,
recv_time TIMESTAMP(6) DEFAULT 0,
insert_time TIMESTAMP(6) DEFAULT 0,
idx INT UNSIGNED NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r SMALLINT DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
INDEX att_conf_id_data_time (att_conf_id,data_time)
) ENGINE=MyISAM COMMENT='Array Enum ReadOnly Values Table';

CREATE TABLE IF NOT EXISTS att_array_devenum_rw
(
att_conf_id INT UNSIGNED NOT NULL,
data_time TIMESTAMP(6) DEFAULT 0,
recv_time TIMESTAMP(6) DEFAULT 0,
insert_time TIMESTAMP(6) DEFAULT 0,
idx INT UNSIGNED NOT NULL,
dim_x_r INT UNSIGNED NOT NULL,
dim_y_r INT UNSIGNED NOT NULL DEFAULT 0,
value_r SMALLINT DEFAULT NULL,
dim_x_w INT UNSIGNED NOT NULL,
dim_y_w INT UNSIGNED NOT NULL DEFAULT 0,
value_w SMALLINT DEFAULT NULL,
quality TINYINT(1) DEFAULT NULL,
att_error_desc_id INT UNSIGNED NULL DEFAULT NULL,
INDEX att_conf_id_data_time (att_conf_id,data_time)
) ENGINE=MyISAM COMMENT='Array Enum ReadWrite Values Table';

