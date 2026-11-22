import { describe, expect, it } from "vitest";
import { GOLDEN_RATIO, hash32 } from "../../src/internal/xxhash";

describe("hash32", () => {
    it("never changes the hash of a key", () => {
        // `StaticShardedDO` and `ShardedReducerDO` select shards with these values, and earlier releases stored data
        // with them (computed with the pure JS `js-xxhash` package). A change moves keys away from their data.
        const pinned: Array<[string, number]> = [
            ["", 917998311],
            ["test", 2848015416],
            ["hello-key", 19224802],
            ["test-02-hashed-differently", 4049537902],
            ["key-0", 360824288],
            ["key-99", 2130062150],
            ["campaign:42", 3850138463],
            ["λάμπρος", 777724318],
            ["😀🚀", 3566604557],
        ];
        expect(GOLDEN_RATIO).toBe(0x9e3779b1);
        for (const [key, expected] of pinned) {
            expect(hash32(key), key).toBe(expected);
        }
    });
});
