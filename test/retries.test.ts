import { describe, expect, it } from "vitest";
import { jitterBackoff, tryN } from "../src/retries";

describe("retries", async () => {
    it("jitterBackoff", async () => {
        const baseDelayMs = 100;
        const maxDelayMs = 3000;
        const attempts = 10000;

        const delays = new Set();
        for (let i = 0; i < attempts; i++) {
            const delay = jitterBackoff(i, baseDelayMs, maxDelayMs);
            delays.add(delay);
            expect(delay).toBeGreaterThanOrEqual(0);
            expect(delay).toBeLessThanOrEqual(maxDelayMs);
        }
        // Might be flaky, so keep an eye, but we should have a good spread of delays.
        // Testing that we have at least 50% of the possible delays.
        expect(delays.size).toBeGreaterThan(maxDelayMs * 0.5);
    });

    it("tryN", async () => {
        // Custom options.
        let attempts = 0;
        const result = await tryN(
            10,
            async () => {
                attempts++;
                if (attempts < 5) {
                    throw new Error("retry");
                }
                return "ok";
            },
            (err) => (err as any).message === "retry",
            { baseDelayMs: 1, maxDelayMs: 10 },
        );
        expect(result).toEqual("ok");
        expect(attempts).toEqual(5);

        // Default options.
        let attempts2 = 0;
        const result2 = await tryN(
            10,
            async () => {
                attempts2++;
                if (attempts2 < 3) {
                    throw new Error("retry");
                }
                return "ok";
            },
            (err) => (err as any).message === "retry",
        );
        expect(result2).toEqual("ok");
        expect(attempts2).toEqual(3);

        // Retry all attempts.
        let attempts3 = 0;
        await expect(
            tryN(
                5,
                async () => {
                    attempts3++;
                    throw new Error("retry");
                },
                (err) => (err as any).message === "retry",
                { baseDelayMs: 1, maxDelayMs: 10 },
            ),
        ).rejects.toThrow("retry");
        expect(attempts3).toEqual(5);
    });

    it("tryN invalid inputs", async () => {
        const doer = async () => 11;
        const shouldRetry = (err: unknown) => (err as any).message === "retry";
        await expect(tryN(0, doer, shouldRetry)).rejects.toThrow("n must be greater than 0");
        await expect(tryN(1, doer, shouldRetry, { baseDelayMs: 0 })).rejects.toThrow(
            "baseDelayMs and maxDelayMs must be greater than 0",
        );
        await expect(tryN(1, doer, shouldRetry, { maxDelayMs: 0 })).rejects.toThrow(
            "baseDelayMs and maxDelayMs must be greater than 0",
        );
    });
});
