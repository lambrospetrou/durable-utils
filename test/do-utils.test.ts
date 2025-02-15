import { describe, expect, it } from "vitest";
import { isErrorRetryable } from "../src/do-utils";

describe("do-utils:isErrorRetryable", async () => {
    it("should recognize DO errors properly", async () => {
        expect(isErrorRetryable({ retryable: true, overloaded: false })).toBe(true);
        expect(isErrorRetryable({ retryable: true, overloaded: null })).toBe(true);
        expect(isErrorRetryable({ retryable: true })).toBe(true);
        expect(isErrorRetryable({ retryable: false, overloaded: false })).toBe(false);
        expect(isErrorRetryable({ retryable: false, overloaded: true })).toBe(false);
        expect(isErrorRetryable({ retryable: true, overloaded: true })).toBe(false);
    });
});