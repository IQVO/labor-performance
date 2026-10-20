/** Mirrors labor-performance's own openapi.yaml schemas exactly (see
 *  apis/openapi.yaml's components.schemas) -- this is the frontend's copy
 *  of that same boundary contract. */

export type TaskType = "PICK" | "PACK" | "SLAM";

export interface Standard {
  taskType: TaskType;
  expectedSeconds: number;
  /** Optional declaration of how much of expectedSeconds is travel
   *  between locations (TravelComponentSeconds in the spec). Omitted
   *  entirely -- never zero-defaulted -- when this standard never
   *  declared one; the screen must render that absence as "not broken
   *  out", not as 0s. */
  travelComponentSeconds?: number | null;
  effectiveFrom: string;
  /** Present only on a superseded (closed) standard. */
  effectiveTo?: string | null;
}

export interface TaskTypeBreakdown {
  taskCount: number;
  meanEfficiencyPct: number | null;
}

export type Trend = "IMPROVING" | "DECLINING" | "STABLE" | "INSUFFICIENT_DATA";

export interface Scorecard {
  associateId: string;
  taskCount: number;
  meanEfficiencyPct: number | null;
  byTaskType: Record<string, TaskTypeBreakdown>;
  trend: Trend;
  /** True iff this associate's 3 most recent SCORED tasks were all below
   *  the coaching floor (85% efficiency). Visibility only -- this
   *  service never automates coaching, pay, or task-claim decisions
   *  from this signal (see apis/openapi.yaml's own doc comment). */
  coachingFlag: boolean;
}

export interface TaskTypePerformance {
  taskType: TaskType;
  taskCount: number;
  meanEfficiencyPct: number | null;
  meanActualSeconds: number | null;
}
