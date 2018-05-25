delimiter //
USE hdbpp//
CREATE PROCEDURE delete_attr_ttl (IN attr VARCHAR(255), IN ttl INT)
proc_label: BEGIN
DECLARE table_name VARCHAR(255);
DECLARE att_id INT;
DECLARE a DATETIME;
DECLARE z DATETIME;
DECLARE countRow INT;
DECLARE totCountRow INT;
DECLARE foundData INT;

DECLARE att_conf_count INT;
SET att_conf_count = (SELECT count(*) FROM att_conf WHERE att_name = attr);
IF att_conf_count != 1 THEN
	SELECT CONCAT('Attribute not found: ',attr);
	LEAVE proc_label;
END IF;

SELECT att_conf_id, CONCAT('att_',data_type) FROM att_conf c JOIN att_conf_data_type d ON c.att_conf_data_type_id = d.att_conf_data_type_id WHERE att_name = attr INTO att_id, table_name;

SET a = '2010-01-01';
SET totCountRow = 0;

IF table_name = 'att_scalar_devboolean_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devboolean_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devboolean_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devboolean_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devboolean_ro' THEN
	SET a = (SELECT data_time FROM att_array_devboolean_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devboolean_rw' THEN
	SET a = (SELECT data_time FROM att_array_devboolean_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devuchar_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devuchar_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devuchar_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devuchar_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devuchar_ro' THEN
	SET a = (SELECT data_time FROM att_array_devuchar_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devuchar_rw' THEN
	SET a = (SELECT data_time FROM att_array_devuchar_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devshort_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devshort_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devshort_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devshort_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devshort_ro' THEN
	SET a = (SELECT data_time FROM att_array_devshort_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devshort_rw' THEN
	SET a = (SELECT data_time FROM att_array_devshort_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devushort_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devushort_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devushort_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devushort_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devushort_ro' THEN
	SET a = (SELECT data_time FROM att_array_devushort_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devushort_rw' THEN
	SET a = (SELECT data_time FROM att_array_devushort_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devlong_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devlong_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devlong_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devlong_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devlong_ro' THEN
	SET a = (SELECT data_time FROM att_array_devlong_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devlong_rw' THEN
	SET a = (SELECT data_time FROM att_array_devlong_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devulong_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devulong_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devulong_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devulong_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devulong_ro' THEN
	SET a = (SELECT data_time FROM att_array_devulong_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devulong_rw' THEN
	SET a = (SELECT data_time FROM att_array_devulong_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devlong64_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devlong64_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devlong64_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devlong64_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devlong64_ro' THEN
	SET a = (SELECT data_time FROM att_array_devlong64_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devlong64_rw' THEN
	SET a = (SELECT data_time FROM att_array_devlong64_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devulong64_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devulong64_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devulong64_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devulong64_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devulong64_ro' THEN
	SET a = (SELECT data_time FROM att_array_devulong64_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devulong64_rw' THEN
	SET a = (SELECT data_time FROM att_array_devulong64_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devfloat_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devfloat_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devfloat_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devfloat_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devfloat_ro' THEN
	SET a = (SELECT data_time FROM att_array_devfloat_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devfloat_rw' THEN
	SET a = (SELECT data_time FROM att_array_devfloat_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devdouble_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devdouble_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devdouble_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devdouble_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devdouble_ro' THEN
	SET a = (SELECT data_time FROM att_array_devdouble_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devdouble_rw' THEN
	SET a = (SELECT data_time FROM att_array_devdouble_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devstring_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devstring_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devstring_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devstring_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devstring_ro' THEN
	SET a = (SELECT data_time FROM att_array_devstring_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devstring_rw' THEN
	SET a = (SELECT data_time FROM att_array_devstring_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devstate_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devstate_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devstate_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devstate_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devstate_ro' THEN
	SET a = (SELECT data_time FROM att_array_devstate_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devstate_rw' THEN
	SET a = (SELECT data_time FROM att_array_devstate_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devencoded_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devencoded_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devencoded_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devencoded_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devencoded_ro' THEN
	SET a = (SELECT data_time FROM att_array_devencoded_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devencoded_rw' THEN
	SET a = (SELECT data_time FROM att_array_devencoded_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devenum_ro' THEN
	SET a = (SELECT data_time FROM att_scalar_devenum_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_scalar_devenum_rw' THEN
	SET a = (SELECT data_time FROM att_scalar_devenum_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devenum_ro' THEN
	SET a = (SELECT data_time FROM att_array_devenum_ro WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
ELSEIF table_name = 'att_array_devenum_rw' THEN
	SET a = (SELECT data_time FROM att_array_devenum_rw WHERE att_conf_id = att_id ORDER BY data_time LIMIT 1);
END IF;
SET foundData = FOUND_ROWS();
IF foundData < 1 THEN
	SELECT CONCAT_WS(' ',table_name,'att_conf_id=',att_id,'Not found, looking for data returned ->',foundData,'in table name');
	LEAVE proc_label;
END IF;

SELECT CONCAT_WS(' ',table_name,'att_conf_id=',att_id,'Starting to delete from time:',a);

WHILE a < DATE_SUB(CURRENT_DATE(), INTERVAL ttl HOUR) DO
	SET z = DATE_ADD(a, INTERVAL 10 DAY);
	REPEAT
	IF table_name = 'att_scalar_devboolean_ro' THEN
		DELETE FROM att_scalar_devboolean_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devboolean_rw' THEN
		DELETE FROM att_scalar_devboolean_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devboolean_ro' THEN
		DELETE FROM att_array_devboolean_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devboolean_rw' THEN
		DELETE FROM att_array_devboolean_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devuchar_ro' THEN
		DELETE FROM att_scalar_devuchar_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devuchar_rw' THEN
		DELETE FROM att_scalar_devuchar_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devuchar_ro' THEN
		DELETE FROM att_array_devuchar_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devuchar_rw' THEN
		DELETE FROM att_array_devuchar_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devshort_ro' THEN
		DELETE FROM att_scalar_devshort_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devshort_rw' THEN
		DELETE FROM att_scalar_devshort_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devshort_ro' THEN
		DELETE FROM att_array_devshort_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devshort_rw' THEN
		DELETE FROM att_array_devshort_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devushort_ro' THEN
		DELETE FROM att_scalar_devushort_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devushort_rw' THEN
		DELETE FROM att_scalar_devushort_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devushort_ro' THEN
		DELETE FROM att_array_devushort_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devushort_rw' THEN
		DELETE FROM att_array_devushort_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devlong_ro' THEN
		DELETE FROM att_scalar_devlong_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devlong_rw' THEN
		DELETE FROM att_scalar_devlong_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devlong_ro' THEN
		DELETE FROM att_array_devlong_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devlong_rw' THEN
		DELETE FROM att_array_devlong_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devulong_ro' THEN
		DELETE FROM att_scalar_devulong_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devulong_rw' THEN
		DELETE FROM att_scalar_devulong_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devulong_ro' THEN
		DELETE FROM att_array_devulong_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devulong_rw' THEN
		DELETE FROM att_array_devulong_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devlong64_ro' THEN
		DELETE FROM att_scalar_devlong64_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devlong64_rw' THEN
		DELETE FROM att_scalar_devlong64_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devlong64_ro' THEN
		DELETE FROM att_array_devlong64_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devlong64_rw' THEN
		DELETE FROM att_array_devlong64_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devulong64_ro' THEN
		DELETE FROM att_scalar_devulong64_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devulong64_rw' THEN
		DELETE FROM att_scalar_devulong64_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devulong64_ro' THEN
		DELETE FROM att_array_devulong64_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devulong64_rw' THEN
		DELETE FROM att_array_devulong64_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devfloat_ro' THEN
		DELETE FROM att_scalar_devfloat_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devfloat_rw' THEN
		DELETE FROM att_scalar_devfloat_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devfloat_ro' THEN
		DELETE FROM att_array_devfloat_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devfloat_rw' THEN
		DELETE FROM att_array_devfloat_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devdouble_ro' THEN
		DELETE FROM att_scalar_devdouble_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devdouble_rw' THEN
		DELETE FROM att_scalar_devdouble_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devdouble_ro' THEN
		DELETE FROM att_array_devdouble_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devdouble_rw' THEN
		DELETE FROM att_array_devdouble_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devstring_ro' THEN
		DELETE FROM att_scalar_devstring_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devstring_rw' THEN
		DELETE FROM att_scalar_devstring_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devstring_ro' THEN
		DELETE FROM att_array_devstring_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devstring_rw' THEN
		DELETE FROM att_array_devstring_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devstate_ro' THEN
		DELETE FROM att_scalar_devstate_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devstate_rw' THEN
		DELETE FROM att_scalar_devstate_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devstate_ro' THEN
		DELETE FROM att_array_devstate_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devstate_rw' THEN
		DELETE FROM att_array_devstate_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devencoded_ro' THEN
		DELETE FROM att_scalar_devencoded_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devencoded_rw' THEN
		DELETE FROM att_scalar_devencoded_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devencoded_ro' THEN
		DELETE FROM att_array_devencoded_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devencoded_rw' THEN
		DELETE FROM att_array_devencoded_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devenum_ro' THEN
		DELETE FROM att_scalar_devenum_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_scalar_devenum_rw' THEN
		DELETE FROM att_scalar_devenum_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devenum_ro' THEN
		DELETE FROM att_array_devenum_ro WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	ELSEIF table_name = 'att_array_devenum_rw' THEN
		DELETE FROM att_array_devenum_rw WHERE att_conf_id = att_id AND data_time < z ORDER BY data_time LIMIT 10000;
	END IF;
	SET countRow = ROW_COUNT();
	IF countRow > 0 THEN
		SELECT CONCAT_WS(' ',table_name,'att_conf_id=',att_id,'Deleted',countRow,'+',totCountRow,'rows up to:',z);
		DO SLEEP(1);
	END IF;
	IF countRow >= 1000 THEN
		DO SLEEP(3);
	ELSEIF countRow >= 5000 THEN
		DO SLEEP(7);
	ELSEIF countRow >= 10000 THEN
		DO SLEEP(14);
	END IF;
	SET totCountRow = totCountRow + countRow;
	DO SLEEP(1);
	UNTIL countRow <= 0 
	END REPEAT;

	SET a = z;
	DO SLEEP(1);
END WHILE;

SELECT CONCAT_WS(' ',table_name,'att_conf_id=',att_id,'TOTAL Deleted rows=',totCountRow);

END

CREATE PROCEDURE delete_ttl ()
proc_label_ttl: BEGIN
DECLARE bDone INT;
DECLARE ttl INT;
DECLARE name VARCHAR(1024);
DECLARE foundData INT;
DECLARE curs CURSOR FOR SELECT att_name, att_ttl FROM att_conf WHERE att_ttl > 0;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = TRUE;

OPEN curs;
	SET bDone = FALSE;
	read_loop: LOOP
		FETCH curs INTO name,ttl;
		IF bDone THEN
      		LEAVE read_loop;
      	END IF;
		SELECT CONCAT_WS(' ','FOUND',name,'with ttl',ttl);
		CALL delete_attr_ttl(name,ttl);
		DO SLEEP(1);
	END LOOP;
CLOSE curs;

END

//
delimiter ;
