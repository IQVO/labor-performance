-- Reverse 0007_handling_unit_standards. Handling-unit standards cannot exist
-- under the old one-open-per-task_type index, so they are removed first.
DELETE FROM labor_standards WHERE handling_unit <> '';
ALTER TABLE task_performances DROP COLUMN team_lift;
ALTER TABLE task_performances DROP COLUMN handling_unit;
DROP INDEX idx_labor_standards_one_open_per_task_type;
CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
    ON labor_standards (task_type)
    WHERE effective_to IS NULL;
ALTER TABLE labor_standards DROP CONSTRAINT labor_standards_team_lift_case_only;
ALTER TABLE labor_standards DROP CONSTRAINT labor_standards_team_lift_complete;
ALTER TABLE labor_standards DROP COLUMN team_lift_expected_seconds;
ALTER TABLE labor_standards DROP COLUMN team_lift_threshold_g;
ALTER TABLE labor_standards DROP COLUMN handling_unit;
