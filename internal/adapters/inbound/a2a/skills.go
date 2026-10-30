package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans,
// mirroring the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/labor-performance/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card). Both are reads.
const (
	skillAssociateScorecard = "associate_scorecard"
	skillTaskTypeStatus     = "task_type_status"
)

// skillRequest is the wire contract of every skill call: the first message
// part carries (as a Data part, or a Text part holding the same JSON) the
// skill id and its arguments. Strict on purpose -- an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "windowSecond" must not silently fall back to the
// default window. (Go's encoding/json matches field names case-insensitively,
// so only genuinely unknown names are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

type associateScorecardArgs struct {
	AssociateId string `json:"associateId"`
}

type taskTypeStatusArgs struct {
	TaskType string `json:"taskType"`
	// WindowSeconds is the optional trailing utilization window; omitted or
	// zero applies GetUtilization's default (1 hour).
	WindowSeconds int64 `json:"windowSeconds"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer.
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, prob := parseSkillRequest(ec)
	if prob != nil {
		return reply(ec, envelope{Problem: prob}, prob.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, prob := run(ctx, deps, req)
	if prob != nil {
		span.SetStatus(codes.Error, prob.Detail)
		span.SetAttributes(attribute.Int("a2a.problem.status", prob.Status))
		return reply(ec, envelope{Skill: req.Skill, Problem: prob}, prob.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.summary)
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillAssociateScorecard:
		data, prob := deps.associateScorecard(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{AssociateScorecard: &data, summary: data.oneLine()}, nil
	case skillTaskTypeStatus:
		data, prob := deps.taskTypeStatus(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{TaskTypeStatus: &data, summary: data.oneLine()}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first part.
// A Data part (or a Text part holding the same JSON object) is accepted;
// anything else is a request-shape problem, answered -- not raised -- because
// a malformed delegation is a business answer for the calling agent, not a
// transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// associateScorecard answers "how is this associate doing" through the same
// GetAssociateScorecard use case REST and MCP call.
func (d Deps) associateScorecard(ctx context.Context, rawArgs json.RawMessage) (scorecardDTO, *problem) {
	var args associateScorecardArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return scorecardDTO{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if strings.TrimSpace(args.AssociateId) == "" {
		return scorecardDTO{}, problemInvalidArgument("associateId is required")
	}
	sc, err := d.GetAssociateScorecard.Execute(ctx, shared.AssociateId(args.AssociateId))
	if err != nil {
		return scorecardDTO{}, problemFromUseCase(err)
	}
	return toScorecardDTO(sc), nil
}

// taskTypeStatus composes the three task-type reads (fleet-wide performance,
// the active standard and utilization) into one goal-shaped answer. A task
// type with no active standard is a valid, answerable state -- the standard is
// reported as null (efficiency then cannot be scored) rather than failing the
// whole read, unlike the standalone REST/MCP standard lookup which answers 404.
func (d Deps) taskTypeStatus(ctx context.Context, rawArgs json.RawMessage) (taskTypeState, *problem) {
	var args taskTypeStatusArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return taskTypeState{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if args.WindowSeconds < 0 {
		return taskTypeState{}, problemInvalidArgument("windowSeconds must not be negative")
	}
	taskType, err := shared.NewTaskType(args.TaskType)
	if err != nil {
		return taskTypeState{}, problemFromUseCase(err)
	}

	perf, err := d.GetTaskTypePerformance.Execute(ctx, taskType)
	if err != nil {
		return taskTypeState{}, problemFromUseCase(err)
	}
	std, err := d.GetStandard.Execute(ctx, taskType)
	if err != nil && !errors.Is(err, usecases.ErrStandardNotFound) {
		return taskTypeState{}, problemFromUseCase(err)
	}
	util, err := d.GetUtilization.ForTaskType(ctx, taskType, time.Duration(args.WindowSeconds)*time.Second)
	if err != nil {
		return taskTypeState{}, problemFromUseCase(err)
	}
	return toTaskTypeState(perf, std, util), nil
}

// problemFromUseCase maps a use case error to a problem answer, using the
// SAME classification as the REST adapter (internal/adapters/inbound/http
// statusFor) for the errors a read can raise, so a delegating agent sees the
// outcome a REST caller would: 404 for a missing associate/standard, 400 for
// an unknown task type, 500 otherwise. Domain rejections are EXPECTED
// business outcomes and are answered as RFC 7807-shaped problems; anything
// unrecognised is an internal failure, still answered as a problem, so the
// calling agent always receives a structured, machine-readable result.
func problemFromUseCase(err error) *problem {
	switch {
	case errors.Is(err, usecases.ErrAssociateNotFound):
		return &problem{Type: problemTypeBase + "/associate-not-found", Title: "No task performance recorded for this associate", Detail: err.Error(), Status: 404}
	case errors.Is(err, usecases.ErrStandardNotFound):
		return &problem{Type: problemTypeBase + "/standard-not-found", Title: "No active labor standard for this task type", Detail: err.Error(), Status: 404}
	case errors.Is(err, shared.ErrUnknownTaskType):
		return &problem{Type: problemTypeBase + "/unknown-task-type", Title: "Task type must be PICK, PACK, or SLAM", Detail: err.Error(), Status: 400}
	default:
		slog.Error("a2a skill failed", "error", err)
		return &problem{Type: problemTypeBase + "/internal-error", Title: "Unexpected failure executing the skill", Detail: "the skill failed; see server logs (trace id in the response metadata)", Status: 500}
	}
}
