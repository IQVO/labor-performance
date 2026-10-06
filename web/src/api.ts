import { LABOR_API_BASE } from "./config";

/** RFC 7807 problem+json body every error response from labor-performance
 *  returns (see its own dto.go's problemDetails type). Mirrors
 *  process-path-mfe's own ApiError shape byte-for-byte. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export class ApiError extends Error {
  problem: ProblemDetails | null;
  status: number;

  constructor(status: number, problem: ProblemDetails | null, fallbackMessage: string) {
    super(problem?.detail || problem?.title || fallbackMessage);
    this.status = status;
    this.problem = problem;
  }
}

async function parseProblemOrThrow(res: Response): Promise<void> {
  let problem: ProblemDetails | null = null;
  try {
    problem = (await res.json()) as ProblemDetails;
  } catch {
    // non-JSON error body -- fall through with problem = null
  }
  throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`);
}

/**
 * POST call for defining (or revising) the engineered labor standard for
 * a TaskType (POST /standards). This is labor-performance's ONLY write
 * endpoint over REST -- TaskPerformance rows are exclusively created by
 * its inbound Kafka consumer of fulfillment-execution's TaskCompleted
 * event, never via this API (see apis/openapi.yaml's own doc comment).
 * Parses an RFC 7807 problem+json body on failure so the form can
 * surface the exact domain-error detail instead of a generic "request
 * failed".
 */
export async function apiPost<TResponse>(
  path: string,
  body: unknown,
): Promise<TResponse> {
  const res = await fetch(`${LABOR_API_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      // POST /standards is wrapped by RequireIdempotencyKey (ADR 0016) when
      // the service runs with Postgres; one fresh key per submission.
      "Idempotency-Key": crypto.randomUUID(),
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) await parseProblemOrThrow(res);
  if (res.status === 204) return undefined as TResponse;
  return (await res.json()) as TResponse;
}
