-- Reverse 0008_standard_site_scope. Site-scoped standards cannot exist under
-- the (task_type, handling_unit) one-open index, so they are removed first;
-- the site-less standards (every standard before 0008) are kept unchanged.
DELETE FROM labor_standards WHERE site_code <> '';
DROP INDEX idx_labor_standards_one_open_per_task_type;
CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
    ON labor_standards (task_type, handling_unit)
    WHERE effective_to IS NULL;
ALTER TABLE labor_standards DROP COLUMN site_code;
