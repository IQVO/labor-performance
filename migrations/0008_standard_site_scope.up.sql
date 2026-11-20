-- Optional site scope on a labor standard
-- (see docs/docs/adr/0038-labor-standards-on-the-integration-topic-and-site-scope.md).
--
-- site_code '' (the default and every pre-existing row) is the site-less
-- standard that applies to every site; a non-empty code scopes the standard
-- to one facility: 1-32 characters, no whitespace (the same rule the domain
-- enforces in shared.NewSiteCode).
ALTER TABLE labor_standards ADD COLUMN site_code TEXT NOT NULL DEFAULT ''
    CONSTRAINT labor_standards_site_code_shape
    CHECK (char_length(site_code) <= 32 AND site_code !~ '[[:space:]]');

-- "One open standard per (task_type, handling_unit)" (migration 0007)
-- widens to one open standard per (task_type, handling_unit, site_code). The
-- index keeps its name, which the Postgres adapter maps to
-- ports.ErrOpenStandardConflict.
DROP INDEX idx_labor_standards_one_open_per_task_type;
CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
    ON labor_standards (task_type, handling_unit, site_code)
    WHERE effective_to IS NULL;
