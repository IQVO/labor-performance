-- Engineered standards per handling unit and the heavy-case team lift
-- (see docs/docs/adr/0035-handling-unit-standards-and-team-lift.md).
--
-- handling_unit '' (the default and every pre-existing row) is the
-- task-type-only standard; EACH, CASE or PALLET is the more specific
-- standard for that unit. The team-lift columns declare, on a CASE
-- standard only, the weight (grams) ABOVE which a case needs two operators
-- and the engineered elapsed seconds of that team lift.
ALTER TABLE labor_standards ADD COLUMN handling_unit TEXT NOT NULL DEFAULT ''
    CHECK (handling_unit IN ('', 'EACH', 'CASE', 'PALLET'));
ALTER TABLE labor_standards ADD COLUMN team_lift_threshold_g BIGINT
    CHECK (team_lift_threshold_g IS NULL OR team_lift_threshold_g > 0);
ALTER TABLE labor_standards ADD COLUMN team_lift_expected_seconds BIGINT
    CHECK (team_lift_expected_seconds IS NULL OR team_lift_expected_seconds > 0);
ALTER TABLE labor_standards ADD CONSTRAINT labor_standards_team_lift_complete
    CHECK ((team_lift_threshold_g IS NULL) = (team_lift_expected_seconds IS NULL));
ALTER TABLE labor_standards ADD CONSTRAINT labor_standards_team_lift_case_only
    CHECK (team_lift_threshold_g IS NULL OR handling_unit = 'CASE');

-- "One open standard per task_type" (migration 0006) widens to one open
-- standard per (task_type, handling_unit). The index keeps its name, which
-- the Postgres adapter maps to ports.ErrOpenStandardConflict.
DROP INDEX idx_labor_standards_one_open_per_task_type;
CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
    ON labor_standards (task_type, handling_unit)
    WHERE effective_to IS NULL;

-- The frozen facts of a scored completion: the handling unit its event
-- carried ('' = none) and whether the team-lift standard scored it.
ALTER TABLE task_performances ADD COLUMN handling_unit TEXT NOT NULL DEFAULT '';
ALTER TABLE task_performances ADD COLUMN team_lift BOOLEAN NOT NULL DEFAULT FALSE;
