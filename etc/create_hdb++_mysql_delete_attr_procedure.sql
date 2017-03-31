delimiter //
CREATE PROCEDURE delete_attr (IN attr VARCHAR(255))
DETERMINISTIC
BEGIN
DECLARE table_name VARCHAR(255);
DECLARE att_id INT;
SELECT att_conf_id, CONCAT('att_',data_type) FROM att_conf c JOIN att_conf_data_type d ON c.att_conf_data_type_id = d.att_conf_data_type_id WHERE att_name = attr INTO att_id, table_name;

IF table_name = 'att_scalar_devboolean_ro' THEN
DELETE FROM att_scalar_devboolean_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devboolean_rw' THEN
DELETE FROM att_scalar_devboolean_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devboolean_ro' THEN
DELETE FROM att_array_devboolean_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devboolean_rw' THEN
DELETE FROM att_array_devboolean_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devuchar_ro' THEN
DELETE FROM att_scalar_devuchar_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devuchar_rw' THEN
DELETE FROM att_scalar_devuchar_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devuchar_ro' THEN
DELETE FROM att_array_devuchar_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devuchar_rw' THEN
DELETE FROM att_array_devuchar_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devshort_ro' THEN
DELETE FROM att_scalar_devshort_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devshort_rw' THEN
DELETE FROM att_scalar_devshort_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devshort_ro' THEN
DELETE FROM att_array_devshort_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devshort_rw' THEN
DELETE FROM att_array_devshort_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devushort_ro' THEN
DELETE FROM att_scalar_devushort_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devushort_rw' THEN
DELETE FROM att_scalar_devushort_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devushort_ro' THEN
DELETE FROM att_array_devushort_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devushort_rw' THEN
DELETE FROM att_array_devushort_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devlong_ro' THEN
DELETE FROM att_scalar_devlong_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devlong_rw' THEN
DELETE FROM att_scalar_devlong_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devlong_ro' THEN
DELETE FROM att_array_devlong_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devlong_rw' THEN
DELETE FROM att_array_devlong_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devulong_ro' THEN
DELETE FROM att_scalar_devulong_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devulong_rw' THEN
DELETE FROM att_scalar_devulong_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devulong_ro' THEN
DELETE FROM att_array_devulong_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devulong_rw' THEN
DELETE FROM att_array_devulong_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devlong64_ro' THEN
DELETE FROM att_scalar_devlong64_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devlong64_rw' THEN
DELETE FROM att_scalar_devlong64_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devlong64_ro' THEN
DELETE FROM att_array_devlong64_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devlong64_rw' THEN
DELETE FROM att_array_devlong64_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devulong64_ro' THEN
DELETE FROM att_scalar_devulong64_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devulong64_rw' THEN
DELETE FROM att_scalar_devulong64_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devulong64_ro' THEN
DELETE FROM att_array_devulong64_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devulong64_rw' THEN
DELETE FROM att_array_devulong64_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devfloat_ro' THEN
DELETE FROM att_scalar_devfloat_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devfloat_rw' THEN
DELETE FROM att_scalar_devfloat_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devfloat_ro' THEN
DELETE FROM att_array_devfloat_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devfloat_rw' THEN
DELETE FROM att_array_devfloat_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devdouble_ro' THEN
DELETE FROM att_scalar_devdouble_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devdouble_rw' THEN
DELETE FROM att_scalar_devdouble_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devdouble_ro' THEN
DELETE FROM att_array_devdouble_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devdouble_rw' THEN
DELETE FROM att_array_devdouble_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devstring_ro' THEN
DELETE FROM att_scalar_devstring_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devstring_rw' THEN
DELETE FROM att_scalar_devstring_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devstring_ro' THEN
DELETE FROM att_array_devstring_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devstring_rw' THEN
DELETE FROM att_array_devstring_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devstate_ro' THEN
DELETE FROM att_scalar_devstate_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devstate_rw' THEN
DELETE FROM att_scalar_devstate_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devstate_ro' THEN
DELETE FROM att_array_devstate_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devstate_rw' THEN
DELETE FROM att_array_devstate_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devencoded_ro' THEN
DELETE FROM att_scalar_devencoded_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devencoded_rw' THEN
DELETE FROM att_scalar_devencoded_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devencoded_ro' THEN
DELETE FROM att_array_devencoded_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devencoded_rw' THEN
DELETE FROM att_array_devencoded_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devenum_ro' THEN
DELETE FROM att_scalar_devenum_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_scalar_devenum_rw' THEN
DELETE FROM att_scalar_devenum_rw WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devenum_ro' THEN
DELETE FROM att_array_devenum_ro WHERE att_conf_id = att_id;
ELSEIF table_name = 'att_array_devenum_rw' THEN
DELETE FROM att_array_devenum_rw WHERE att_conf_id = att_id;
END IF;

DELETE FROM att_history WHERE att_conf_id = att_id;
DELETE FROM att_parameter WHERE att_conf_id = att_id;
DELETE FROM att_conf WHERE att_conf_id = att_id;
END
//
delimiter ;
