---
id: ubiquitous-language
title: Ubiquitous Language
sidebar_label: Ubiquitous Language
sidebar_position: 7
description: The Labor Performance glossary — every term mapped to the code identifier that implements it, with the terms whose code name differs flagged.
---

# Ubiquitous Language

Every term below maps to a code identifier. The **Code name differs?**
column flags terms whose identifier in code is not the business word, so
a reader grepping for the business word knows what to search for instead.

## Core terms

| Term | Meaning | Code identifier | Code name differs? |
|---|---|---|---|
| **LaborStandard** | The engineered expected duration for one task type, with an effective range. | `standard.LaborStandard` (`internal/domain/standard`) | no |
| **Expected seconds** | How long a task of that type should take. | `LaborStandard.ExpectedSeconds()`; JSON `expectedSeconds`; column `expected_seconds` | no |
| **Travel component** | Optional part of the expected seconds attributable to travel, supplied by the caller (ADR 0015). | `LaborStandard.TravelComponentSeconds()`; `travelComponentSeconds` | no |
| **Effective range** | `[effectiveFrom, effectiveTo)` during which a standard is in force; open while `effectiveTo` is null. | `EffectiveFrom()`, `EffectiveTo()`, `IsActiveAt(t)` | no |
| **Revision** | Replacing the open standard: close the old one, open a new one at the same instant. | `LaborStandard.Close(at)` inside `usecases.DefineStandard`; event `LaborStandardRevised` | **yes** — no `Revise` method; the use case is `DefineStandard` for both first definition and revision |
| **TaskType** | The kind of task a standard applies to: `PICK`, `PACK`, `SLAM` (mirrors `fulfillment-execution`'s `task.Type`). | `shared.TaskType`, constants `shared.Pick`, `shared.Pack`, `shared.Slam` | no |
| **Unclassified** | A completion whose task type is absent or not one of the three. Recorded, never scored. | `""` from `shared.ParseTaskTypeLenient`; `UNCLASSIFIED` in the analytics rollup (`report.NormalizeTaskType`) | **yes** — empty string in OLTP, `UNCLASSIFIED` label in analytics |
| **TaskPerformance** | One completed task, scored and frozen. | `performance.TaskPerformance` (`internal/domain/performance`); table `task_performances` | no |
| **Actual seconds** | How long the task actually took (claim → completion). | `TaskPerformance.ActualSeconds()`; wire field `duration_seconds` on `TaskCompleted` | **yes** — upstream calls it `duration_seconds` |
| **StandardSecondsAtCompletion** | The expected seconds of the standard active *as of* completion, copied onto the row and never recomputed (ADR 0004). | `TaskPerformance.StandardSecondsAtCompletion()`; column `standard_seconds_at_completion` | no |
| **EfficiencyPct** | `100 × StandardSecondsAtCompletion ÷ ActualSeconds`; null when either is not positive. | `TaskPerformance.EfficiencyPct()` (`*float64`), `computeEfficiencyPct` | no |
| **Scored / Unscored** | Whether a task has an EfficiencyPct. | `EfficiencyPct() != nil`; `tasks_scored` / `TasksUnscored()` in analytics | **yes** — no named type; a nil check |
| **Measured** | A task whose actual seconds is positive (counts towards mean actual seconds). | `tasks_measured`; `ActualSeconds > 0` filter in `TaskTypePerformanceFor` | **yes** — only named in analytics |
| **Associate** | The person who completed a task; empty for a station with no checked-in occupant (e.g. a robot). | `shared.AssociateId` | no |
| **Kafka event id** | The CloudEvents `id` of the consumed `TaskCompleted`; the dedupe key. | `RecordTaskPerformanceRequest.KafkaEventId`, `TaskPerformance.EventId()`, `processed_events.event_id` | **yes** — called `KafkaEventId`/`eventId`, it is the CloudEvents `id` |
| **Idle Gap** | One associate's wait from finishing a task to claiming the next. | `idleness.IdlePeriod`; table `idle_periods` | **yes** — `IdlePeriod` |
| **Claim instant** | When the next task was claimed, derived as completion time − actual seconds. | `claimedAt` in `RecordTaskPerformance.recordIdleGap` | no |
| **Capped** | The idle gap exceeded `IDLE_GAP_CAP_SECONDS` and was clipped. | `IdlePeriod.Capped()`; column `capped` | no |
| **Open Gap** | Idle time still running right now; computed at read time, never stored. Per associate only: the task-type utilization result always reports 0. Decided 2026-10-06: kept per associate; at task-type scope `openGapSeconds` is always 0 by design (an open gap is an Associate-level concept, ADR 0014; attributing it to a task type would invent semantics). | `openGapSeconds` in `usecases/get_utilization.go`; JSON `openGapSeconds` | no |
| **Utilization** | Task time ÷ (task time + idle time) over a trailing window, as a percent; null when nothing was observed. | `idleness.UtilizationPct`, `usecases.GetUtilization`, `UtilizationResult.UtilizationPct` | no |
| **Window** | The trailing period utilization is measured over (default 1 h). | REST query `window` (Go duration), MCP `windowSeconds`; `defaultUtilizationWindow` | no |
| **Idle seconds before** | The idle gap preceding a task, carried on the published event. | `TaskPerformanceRecorded.IdleSecondsBefore`; wire `idle_seconds_before` | no |

## Read-model terms

| Term | Meaning | Code identifier | Code name differs? |
|---|---|---|---|
| **Scorecard** | Per-associate summary: task count, mean efficiency, per-task-type breakdown, trend, coaching flag. | `ports.Scorecard`, `ports.TaskTypeBreakdown`, `usecases.GetAssociateScorecard` | no |
| **Trend** | `IMPROVING`, `DECLINING`, `STABLE` or `INSUFFICIENT_DATA`: recent mean vs all-time mean, 5-point threshold, at least 3 scored recent tasks (ADR 0005). | `performance.TrendDirection`, `performance.ClassifyTrend` | no |
| **Coaching flag** | The last 3 scored tasks were all under 85 % efficiency — a conversation prompt, never an action (ADR 0005). | `Scorecard.CoachingFlag`, `performance.DetectCoachingFlag` | no |
| **Task-type performance** | Fleet-wide task count, mean efficiency and mean actual seconds for one task type. | `ports.TaskTypePerformance`, `usecases.GetTaskTypePerformance` | no |
| **Mean actual seconds** | Real measured pace, independent of any standard (ADR 0006). | `TaskTypePerformance.MeanActualSeconds` | no |
| **Labor Performance Report** | Hourly analytical rollup per task type, with totals. | `report.LaborPerformanceReport`, `report.Row`, `report.TaskTypeBar`, `report.Totals` | no |
| **Hour bucket** | The UTC hour a fact is folded into. | `report.HourBucket`; column `hour_bucket` | no |
| **Freshness lag** | How far the analytical projection trails the newest applied event. | `ReportStore.FreshnessLag`; JSON `lagSeconds` | no |

## Event names

| Event (business) | Code type | CloudEvents type |
|---|---|---|
| Labor standard defined | `shared.LaborStandardDefined` | `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` |
| Labor standard revised | `shared.LaborStandardRevised` | `com.warehouse.wes.labor-performance.standard.LaborStandardRevised` |
| Task performance recorded | `shared.TaskPerformanceRecorded` | `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` |
| Task completed (consumed) | `cloudevents.TypeFulfillmentTaskCompleted` (decoded into `taskCompletedData`) | `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` |

Full payloads: [Domain events](./domain-events.md).

## Words this context deliberately does not use

- **Shift** — shifts belong to `workforce-management`; the idle-gap cap
  stands in for a shift boundary (ADR 0014).
- **Assignment / labor allocation** — `workforce-management`'s language.
- **Task claim / lease** — `fulfillment-execution` owns the task
  lifecycle; this context only reads the completion.
