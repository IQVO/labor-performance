-- Reverse 0006_standard_version_and_one_open.
DROP INDEX idx_labor_standards_one_open_per_task_type;
ALTER TABLE labor_standards DROP COLUMN version;
