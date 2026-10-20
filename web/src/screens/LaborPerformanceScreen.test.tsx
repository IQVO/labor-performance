import { describe, expect, it } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse, server } from "../test/mocks/server";
import { LABOR_API_BASE } from "../config";
import { LaborPerformanceScreen } from "./LaborPerformanceScreen";

/** Fixture for the active-standard card's mount-time fetch. Like the
 *  fleet-wide performance card, it fetches its default PICK selection on
 *  mount, and MSW runs with onUnhandledRequest: "error" -- so every test
 *  that does not override /standards/PICK itself needs this handler.
 *  Values chosen to collide with no assertion in this file. */
const activePickStandard = {
  taskType: "PICK",
  expectedSeconds: 38,
  effectiveFrom: "2026-01-05T08:00:00Z",
};

/** Three "Task type" selects live on this screen (define form, the
 *  read-only active-standard card, fleet-wide performance), in card
 *  order; index 1 is the active-standard card's own. */
function activeStandardSelect(): HTMLElement {
  return screen.getAllByLabelText("Task type")[1];
}

describe("LaborPerformanceScreen", () => {
  it("defines a standard and shows a success message", async () => {
    server.use(
      http.post(`${LABOR_API_BASE}/standards`, async ({ request }) => {
        const body = (await request.json()) as { taskType: string; expectedSeconds: number };
        return HttpResponse.json(
          {
            taskType: body.taskType,
            expectedSeconds: body.expectedSeconds,
            effectiveFrom: "2026-09-06T00:00:00Z",
          },
          { status: 201 },
        );
      }),
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);
    await userEvent.type(screen.getByLabelText("Expected seconds *"), "45");
    await userEvent.click(screen.getByRole("button", { name: "Set standard" }));

    await waitFor(() =>
      expect(screen.getByText("Standard for PICK set to 45s.")).toBeInTheDocument(),
    );
  });

  it("shows the RFC 7807 problem detail when defining a standard fails", async () => {
    server.use(
      http.post(`${LABOR_API_BASE}/standards`, () =>
        HttpResponse.json(
          {
            type: "https://errors.labor-performance.warehouse-systems.dev/invalid-expected-seconds",
            title: "Expected seconds must be greater than zero",
            status: 422,
            detail: "expected seconds must be greater than zero",
          },
          { status: 422 },
        ),
      ),
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);
    await userEvent.type(screen.getByLabelText("Expected seconds *"), "0");
    await userEvent.click(screen.getByRole("button", { name: "Set standard" }));

    expect(
      await screen.findByText("expected seconds must be greater than zero"),
    ).toBeInTheDocument();
  });

  it("looks up an associate's scorecard and shows its breakdown", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/associates/assoc-42/scorecard`, () =>
        HttpResponse.json({
          associateId: "assoc-42",
          taskCount: 12,
          meanEfficiencyPct: 91.5,
          byTaskType: {
            PICK: { taskCount: 10, meanEfficiencyPct: 92.0 },
            PACK: { taskCount: 2, meanEfficiencyPct: 88.0 },
          },
          trend: "IMPROVING",
          coachingFlag: false,
        }),
      ),
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);
    await userEvent.type(screen.getByLabelText("Associate ID *"), "assoc-42");
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));

    expect(await screen.findByText("12")).toBeInTheDocument();
    expect(screen.getByText("91.5%")).toBeInTheDocument();
    expect(screen.getByText("IMPROVING")).toBeInTheDocument();
  });

  it("shows a not-found message for an associate with no recorded tasks", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/associates/assoc-unknown/scorecard`, () =>
        HttpResponse.json(
          {
            type: "https://errors.labor-performance.warehouse-systems.dev/associate-not-found",
            title: "No task performance recorded for this associate",
            status: 404,
            detail: "no task performance recorded for this associate",
          },
          { status: 404 },
        ),
      ),
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);
    await userEvent.type(screen.getByLabelText("Associate ID *"), "assoc-unknown");
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));

    expect(
      await screen.findByText("No task performance recorded for assoc-unknown."),
    ).toBeInTheDocument();
  });

  it("shows fleet-wide task-type performance for the selected task type", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 340,
          meanEfficiencyPct: 96.2,
          meanActualSeconds: 42.8,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("340")).toBeInTheDocument();
    expect(screen.getByText("96.2%")).toBeInTheDocument();
    expect(screen.getByText("42.8s")).toBeInTheDocument();
  });

  it("shows the active standard for the selected task type with every field", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json({
          taskType: "PICK",
          expectedSeconds: 45,
          travelComponentSeconds: 15,
          effectiveFrom: "2026-09-06T00:00:00Z",
        }),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("45s")).toBeInTheDocument();
    expect(screen.getByText("15s")).toBeInTheDocument();
    expect(screen.getByText("2026-09-06T00:00:00Z")).toBeInTheDocument();
    expect(screen.getByText("Active (open-ended)")).toBeInTheDocument();
    // taskType renders as its own stat tile alongside the three selects'
    // PICK options (3 selects + this tile = 4 "PICK" texts on screen).
    expect(screen.getAllByText("PICK")).toHaveLength(4);
  });

  it("renders an undeclared travel component as an explicit state, not zero", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("38s")).toBeInTheDocument();
    expect(screen.getByText("Not broken out")).toBeInTheDocument();
    expect(screen.queryByText("0s")).not.toBeInTheDocument();
  });

  it("shows an explicit empty state when the selected task type has no active standard", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/standards/PACK`, () =>
        HttpResponse.json(
          {
            type: "https://errors.labor-performance.warehouse-systems.dev/no-active-standard",
            title: "No active standard for this task type",
            status: 404,
            detail: "no active standard for task type PACK",
          },
          { status: 404 },
        ),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("38s")).toBeInTheDocument();
    await userEvent.selectOptions(activeStandardSelect(), "PACK");

    expect(await screen.findByText("No active standard for PACK.")).toBeInTheDocument();
  });

  it("fetches each task type's own standard when the selector changes", async () => {
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () =>
        HttpResponse.json(activePickStandard),
      ),
      http.get(`${LABOR_API_BASE}/standards/SLAM`, () =>
        HttpResponse.json({
          taskType: "SLAM",
          expectedSeconds: 90,
          travelComponentSeconds: 30,
          effectiveFrom: "2026-02-02T09:30:00Z",
        }),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("38s")).toBeInTheDocument();
    await userEvent.selectOptions(activeStandardSelect(), "SLAM");

    expect(await screen.findByText("90s")).toBeInTheDocument();
    expect(screen.getByText("30s")).toBeInTheDocument();
    expect(screen.queryByText("38s")).not.toBeInTheDocument();
  });

  it("refreshes the active standard after a successful define", async () => {
    let standardCalls = 0;
    server.use(
      http.get(`${LABOR_API_BASE}/standards/PICK`, () => {
        standardCalls += 1;
        if (standardCalls === 1) {
          return HttpResponse.json(
            {
              type: "https://errors.labor-performance.warehouse-systems.dev/no-active-standard",
              title: "No active standard for this task type",
              status: 404,
              detail: "no active standard for task type PICK",
            },
            { status: 404 },
          );
        }
        return HttpResponse.json({
          taskType: "PICK",
          expectedSeconds: 45,
          travelComponentSeconds: 15,
          effectiveFrom: "2026-10-17T00:00:00Z",
        });
      }),
      http.post(`${LABOR_API_BASE}/standards`, () =>
        HttpResponse.json(
          {
            taskType: "PICK",
            expectedSeconds: 45,
            effectiveFrom: "2026-10-17T00:00:00Z",
          },
          { status: 201 },
        ),
      ),
      http.get(`${LABOR_API_BASE}/task-types/PICK/performance`, () =>
        HttpResponse.json({
          taskType: "PICK",
          taskCount: 0,
          meanEfficiencyPct: null,
          meanActualSeconds: null,
        }),
      ),
    );

    render(<LaborPerformanceScreen />);

    expect(await screen.findByText("No active standard for PICK.")).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Expected seconds *"), "45");
    await userEvent.click(screen.getByRole("button", { name: "Set standard" }));

    expect(await screen.findByText("45s")).toBeInTheDocument();
    expect(screen.getByText("15s")).toBeInTheDocument();
  });
});
