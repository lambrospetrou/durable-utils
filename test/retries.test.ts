import { describe, expect, it } from "vitest";
import { defaultPlatform, jitterBackoff, Platform, tryN, tryWhile } from "../src/retries";

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

    it("jitterBackoff with custom platform", async () => {
        expect(jitterBackoff(3, 100, 3000, { random: () => 0 })).toEqual(0);
        expect(jitterBackoff(3, 100, 3000, { random: () => 0.5 })).toEqual(400);
        expect(jitterBackoff(10, 100, 3000, { random: () => 0.5 })).toEqual(1500);
    });

    describe("custom platform", async () => {
        function fakePlatform(randomValue: number) {
            const waits: number[] = [];
            const platform: Platform = {
                random: () => randomValue,
                wait: async (ms: number) => {
                    waits.push(ms);
                },
            };
            return { platform, waits };
        }

        it("tryN uses the platform for delays", async () => {
            const { platform, waits } = fakePlatform(0.5);
            let attempts = 0;
            await expect(
                tryN(
                    4,
                    async () => {
                        attempts++;
                        throw new Error("retry");
                    },
                    { baseDelayMs: 100, maxDelayMs: 1000, platform },
                ),
            ).rejects.toThrow("retry");
            expect(attempts).toEqual(4);
            // Delays for next attempts 2, 3, 4: floor(0.5 * min(2^attempt * 100, 1000)).
            expect(waits).toEqual([200, 400, 500]);
        });

        it("tryWhile uses the platform for delays", async () => {
            const { platform, waits } = fakePlatform(0.25);
            const result = await tryWhile(
                async (attempt) => {
                    if (attempt < 3) {
                        throw new Error("retry");
                    }
                    return "ok";
                },
                () => true,
                { baseDelayMs: 100, maxDelayMs: 1000, platform },
            );
            expect(result).toEqual("ok");
            expect(waits).toEqual([100, 200]);
        });
    });

    describe("abort signal", async () => {
        it("defaultPlatform.wait resolves when the signal does not abort", async () => {
            const controller = new AbortController();
            await expect(defaultPlatform.wait(1, { signal: controller.signal })).resolves.toBeUndefined();
        });

        it("defaultPlatform.wait rejects at once if the signal is already aborted", async () => {
            const reason = new Error("stop");
            await expect(defaultPlatform.wait(10000, { signal: AbortSignal.abort(reason) })).rejects.toBe(reason);
        });

        it("tryN does not call fn if the signal is already aborted", async () => {
            const reason = new Error("stop");
            let attempts = 0;
            await expect(tryN(3, async () => attempts++, { signal: AbortSignal.abort(reason) })).rejects.toBe(reason);
            expect(attempts).toEqual(0);
        });

        it("tryN stops the default wait early when the signal aborts", async () => {
            const controller = new AbortController();
            const reason = new Error("stop");
            // A random value near 1 makes the wait close to maxDelayMs.
            const platform: Platform = { random: () => 0.99, wait: defaultPlatform.wait };
            let attempts = 0;
            const start = Date.now();
            await expect(
                tryN(
                    3,
                    async () => {
                        attempts++;
                        setTimeout(() => controller.abort(reason), 10);
                        throw new Error("retry");
                    },
                    { baseDelayMs: 10000, maxDelayMs: 20000, platform, signal: controller.signal },
                ),
            ).rejects.toBe(reason);
            expect(attempts).toEqual(1);
            expect(Date.now() - start).toBeLessThan(5000);
        });

        it("tryWhile stops if a custom wait ignores the signal", async () => {
            const controller = new AbortController();
            const reason = new Error("stop");
            const platform: Platform = {
                random: () => 0.5,
                wait: async () => {
                    controller.abort(reason);
                },
            };
            let attempts = 0;
            await expect(
                tryWhile(
                    async () => {
                        attempts++;
                        throw new Error("retry");
                    },
                    () => true,
                    { platform, signal: controller.signal },
                ),
            ).rejects.toBe(reason);
            expect(attempts).toEqual(1);
        });
    });

    describe("tryN", async () => {
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
                { baseDelayMs: 1, maxDelayMs: 10, isRetryable: (err) => (err as any).message === "retry" },
            );
            expect(result).toEqual("ok");
            expect(attempts).toEqual(5);

            // Default options.
            let attempts2 = 0;
            const result2 = await tryN(10, async () => {
                attempts2++;
                if (attempts2 < 3) {
                    throw new Error("retry");
                }
                return "ok";
            });
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
                    { baseDelayMs: 1, maxDelayMs: 10 },
                ),
            ).rejects.toThrow("retry");
            expect(attempts3).toEqual(5);
        });

        it("tryN - DEPRECATED", async () => {
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
            await expect(tryN(0, doer)).rejects.toThrow("n must be greater than 0");
            await expect(tryN(1, doer, { baseDelayMs: 0 })).rejects.toThrow(
                "baseDelayMs and maxDelayMs must be greater than 0",
            );
            await expect(tryN(1, doer, { maxDelayMs: 0 })).rejects.toThrow(
                "baseDelayMs and maxDelayMs must be greater than 0",
            );
        });
    });
});
