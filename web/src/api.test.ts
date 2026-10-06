import { describe, expect, it, vi, afterEach } from "vitest";
import { apiPost, ApiError } from "./api";

describe("apiPost", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("returns the parsed JSON body on success", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({ taskType: "PICK", expectedSeconds: 45 }),
    }) as unknown as typeof fetch;

    const result = await apiPost("/standards", { taskType: "PICK", expectedSeconds: 45 });
    expect(result).toEqual({ taskType: "PICK", expectedSeconds: 45 });
  });

  it("sends a fresh UUID Idempotency-Key header on every call", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({}),
    });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await apiPost("/standards", { taskType: "PICK", expectedSeconds: 45 });
    await apiPost("/standards", { taskType: "PICK", expectedSeconds: 45 });

    const keys = fetchMock.mock.calls.map(
      ([, init]) => (init as RequestInit).headers as Record<string, string>,
    );
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    expect(keys[0]["Idempotency-Key"]).toMatch(uuid);
    expect(keys[1]["Idempotency-Key"]).toMatch(uuid);
    expect(keys[0]["Idempotency-Key"]).not.toBe(keys[1]["Idempotency-Key"]);
    expect(keys[0]["Content-Type"]).toBe("application/json");
  });

  it("throws ApiError with the parsed RFC 7807 problem detail on failure", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 422,
      statusText: "Unprocessable Entity",
      json: async () => ({
        type: "https://errors.labor-performance.warehouse-systems.dev/invalid-expected-seconds",
        title: "Expected seconds must be greater than zero",
        status: 422,
        detail: "expected seconds must be greater than zero",
      }),
    }) as unknown as typeof fetch;

    await expect(apiPost("/standards", { taskType: "PICK", expectedSeconds: 0 })).rejects.toThrow(
      ApiError,
    );
    await expect(apiPost("/standards", { taskType: "PICK", expectedSeconds: 0 })).rejects.toThrow(
      "expected seconds must be greater than zero",
    );
  });

  it("falls back to statusText when the error body is not JSON", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: async () => {
        throw new Error("not json");
      },
    }) as unknown as typeof fetch;

    await expect(apiPost("/standards", {})).rejects.toThrow("500 Internal Server Error");
  });
});
