ALTER TABLE att_parameter ADD COLUMN enum_labels JSON NOT NULL;
UPDATE att_parameter SET enum_labels='[]';

