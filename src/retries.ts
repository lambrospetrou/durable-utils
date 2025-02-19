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

export type TryNOptions = {
    /**
     * @param err Error thrown by the function.
     * @param nextAttempt Number of next attempt to make.
     * @returns Returns true if the error and nextAttempt number is retryable.
     */
    isRetryable?: (err: unknown, nextAttempt: number) => boolean;

    /**
     * Number of milliseconds to use as multiplier for the exponential backoff.
     */
    baseDelayMs?: number;
    /**
     * Maximum number of milliseconds to wait.
     */
    maxDelayMs?: number;

    /**
     * If true, logs the error and attempt number to the console.
     */
    verbose?: boolean;
};

/**
 * @param n Number of total attempts to make.
 * @param fn The function to call for each attempt. Receives the attempt number.
 * @param options The options for the retry strategy.
 * @param options.isRetryable The function to call to determine if the error is retryable. Receives the error and the next attempt number.
 * @param options.baseDelayMs Number of milliseconds to use as multiplier for the exponential backoff.
 * @param options.maxDelayMs Maximum number of milliseconds to wait.
 * @param options.verbose If true, logs the error and attempt number to the console.
 * @returns The result of the `fn` function or propagates the last error thrown once `isRetryable` returns false or all retries failed.
 */
export async function tryN<T>(n: number, fn: (attempt: number) => Promise<T>, options?: TryNOptions): Promise<T>;
/**
 * @deprecated Use the overload with 3rd argument being `options`. This function overload will be removed in the next version.
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
    isRetryable: (err: unknown, nextAttempt: number) => boolean,
    /**
     * @deprecated Use the `options` 3rd argument instead.
     */
    options?: TryNOptions,
): Promise<T>;
export async function tryN<T>(
    n: number,
    fn: (attempt: number) => Promise<T>,
    optionsOrFunction?: TryNOptions | ((err: unknown, nextAttempt: number) => boolean),
    optionsOld?: TryNOptions,
): Promise<T> {
    if (n <= 0) {
        throw new Error("n must be greater than 0");
    }
    n = Math.floor(n);

    let options: TryNOptions;
    if (typeof optionsOrFunction !== "function") {
        options = optionsOrFunction ?? {};
    } else {
        // Deprecated overload with required `isRetryable`.
        // TODO Remove this in a later version.
        options = {
            ...optionsOld,
            isRetryable: optionsOrFunction,
        };
    }

    return await tryWhile(
        fn,
        (err: unknown, nextAttempt: number) => {
            return nextAttempt <= n && (options.isRetryable?.(err, nextAttempt) ?? true);
        },
        options,
    );
}

/**
 * @param fn The function to call for each attempt. Receives the attempt number.
 * @param isRetryable The function to call to determine if the error is retryable. Receives the error and the next attempt number.
 * @param options The options for the retry strategy.
 * @param options.baseDelayMs Number of milliseconds to use as multiplier for the exponential backoff.
 * @param options.maxDelayMs Maximum number of milliseconds to wait.
 * @param options.verbose If true, logs the error and attempt number to the console.
 * @returns The result of the `fn` function or propagates the last error thrown once `isRetryable` returns false or all retries failed.
 */
export async function tryWhile<T>(
    fn: (attempt: number) => Promise<T>,
    isRetryable: (err: unknown, nextAttempt: number) => boolean,
    options?: {
        baseDelayMs?: number;
        maxDelayMs?: number;

        verbose?: boolean;
    },
): Promise<T> {
    const baseDelayMs = Math.floor(options?.baseDelayMs ?? 100);
    const maxDelayMs = Math.floor(options?.maxDelayMs ?? 3000);
    if (baseDelayMs <= 0 || maxDelayMs <= 0) {
        throw new Error("baseDelayMs and maxDelayMs must be greater than 0");
    }
    if (baseDelayMs >= maxDelayMs) {
        throw new Error("baseDelayMs must be less than maxDelayMs");
    }
    let attempt = 1;
    while (true) {
        try {
            return await fn(attempt);
        } catch (err) {
            if (options?.verbose) {
                console.info({
                    message: "tryWhile",
                    attempt,
                    error: String(err),
                    errorProps: err,
                });
            }
            attempt += 1;
            if (!isRetryable(err, attempt)) {
                throw err;
            }
            const delay = jitterBackoff(attempt, baseDelayMs, maxDelayMs);
            await new Promise((resolve) => setTimeout(resolve, delay));
        }
    }
}
