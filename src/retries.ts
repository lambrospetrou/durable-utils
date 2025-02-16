/**
 * Returns the number of milliseconds to wait before retrying a request.
 * See the "Full Jitter" approach in https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/.
 * @param attempt The number of attempts so far.
 * @param baseDelayMs Number of milliseconds to use as multiplier for the exponential backoff.
 * @param maxDelayMs Maximum number of milliseconds to wait.
 * @returns Milliseconds to wait before retrying.
 */
export function jitterBackoff(attempt: number, baseDelayMs: number, maxDelayMs: number): number {
    const attemptUpperBoundMs = Math.min(2 ** attempt * baseDelayMs, maxDelayMs);
    return Math.floor(Math.random() * attemptUpperBoundMs);
}

/**
 * 
 * @param n Number of total attempts to make.
 * @param fn The function to call for each attempt. Receives the attempt number.
 * @param isRetryable The function to call to determine if the error is retryable. Receives the error and the next attempt number.
 * @param options The options for the retry strategy.
 * @param options.baseDelayMs Number of milliseconds to use as multiplier for the exponential backoff.
 * @param options.maxDelayMs Maximum number of milliseconds to wait.
 * @param options.verbose If true, logs the error and attempt number to the console.
 * @returns The result of the `fn` function or propagates the last error thrown once `isRetryable` returns false or all retries failed.
 */
export async function tryN<T>(
    n: number,
    fn: (attempt: number) => Promise<T>,
    isRetryable: (err: unknown, attempt: number) => boolean,
    options?: {
        baseDelayMs?: number;
        maxDelayMs?: number;

        verbose?: boolean;
    },
): Promise<T> {
    if (n <= 0) {
        throw new Error("n must be greater than 0");
    }
    n = Math.floor(n);
    const baseDelayMs = options?.baseDelayMs ?? 100;
    const maxDelayMs = options?.maxDelayMs ?? 3000;
    if (baseDelayMs <= 0 || maxDelayMs <= 0) {
        throw new Error("baseDelayMs and maxDelayMs must be greater than 0");
    }
    let attempt = 1;
    while (true) {
        try {
            return await fn(attempt);
        } catch (err) {
            if (options?.verbose) {
                console.info({
                    message: "tryN",
                    attempt,
                    error: String(err),
                    errorProps: err,
                });
            }
            attempt += 1;
            if (!isRetryable(err, attempt) || attempt > n) {
                throw err;
            }
            const delay = jitterBackoff(attempt, baseDelayMs, maxDelayMs);
            await new Promise((resolve) => setTimeout(resolve, delay));
        }
    }
}
