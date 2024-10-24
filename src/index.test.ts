import { expect, test } from "vitest";
import { add } from "./index.ts";

test("adds two numbers", () => {
    expect(add(1, 2)).toBe(3);
});
