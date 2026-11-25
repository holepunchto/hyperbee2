# Import benchmark

Compares the chunked `tryPut` import strategy used before this change with the
packed loader that `destination.import()` now uses.

- `batched` replays the previous importer loop: read the pinned source stream,
  buffer up to 1,024 entries or 1 MiB, then `destination.write()` + `tryPut` +
  `batch.flush()`. It uses the real writer, including splits and block encoding.
- `import` calls the production `destination.import(source)` path, which builds
  the tree from the sorted stream with `lib/bulk-load.js`.

Seeding the source, choosing/pinning its version and verifying the destination
are excluded from the measured import time. Every measured run uses a fresh
destination core/store. The cases are:

| case          | entries | keys                              | values                                                                                            |
| ------------- | ------- | --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `sorted-40k`  | 40,000  | 4-byte ascending counter          | 1 byte                                                                                            |
| `sorted-200k` | 200,000 | 4-byte ascending counter          | 1 byte                                                                                            |
| `mixed-20k`   | 20,000  | 4-byte counter + 0-22 byte suffix | null, empty, inline, external (> 1 KiB) and oversized (> 4 KiB) values, including one 2 MiB value |

## Reproduce

```sh
npm install
node bench/import.js --runs 5
```

Each implementation gets one discarded warm-up and then five measured runs; the
table reports medians. Other options:

```sh
node bench/import.js --cases sorted-40k --impl import --runs 5 --json
node bench/import.js --impl batched,import --cases mixed-20k
```

`--impl` accepts `batched` and/or `import`; `--cases` accepts a comma-separated
subset of the table above; `--json` prints the raw medians.

The benchmark also re-reads the destination and compares a SHA-256 digest of
every key and value (including the null/empty distinction) against the source.
A mismatch is reported as `FAIL` and exits non-zero.

## Environment

- Node v22.22.1, linux x64, 8 vCPU QEMU virtual CPU, 30 GiB RAM.
- Source and destination stores in the system temporary directory.
- `peak` is the maximum sampled `process.memoryUsage().heapUsed` above the
  pre-import baseline, sampled every 2 ms. It is an approximation: GC timing
  adds noise, especially for the 40,000-entry case.
- `leaf keys` is minimum/median/maximum keys per leaf. Tree shape is measured by
  walking the imported tree with the normal inflation path.

## Results

Medians of five measured runs (`node bench/import.js --runs 5`):

| case        | impl    | time (ms) |          bytes |    blocks | peak heap (MiB) | levels |  leaves |   leaf keys |
| ----------- | ------- | --------: | -------------: | --------: | --------------: | -----: | ------: | ----------: |
| sorted-40k  | batched |       222 |        528,023 |       313 |            14.4 |      3 |     312 | 127/127/192 |
| sorted-40k  | import  |    **87** |    **483,195** |   **158** |            14.9 |  **2** | **157** | 159/255/255 |
| sorted-200k | batched |     1,153 |      2,969,576 |     1,572 |            48.8 |      3 |   1,562 | 127/127/192 |
| sorted-200k | import  |   **448** |  **2,690,149** |   **787** |        **47.3** |      3 | **782** | 159/255/255 |
| mixed-20k   | batched |       349 |     11,916,008 |     1,330 |             7.3 |      2 |     156 | 127/127/160 |
| mixed-20k   | import  |   **298** | **11,894,389** | **1,257** |         **6.4** |      2 |  **79** | 143/255/255 |

Relative change (import versus batched, medians):

| case        |         time | bytes | blocks | leaves | levels |
| ----------- | -----------: | ----: | -----: | -----: | -----: |
| sorted-40k  | 2.54x faster | -8.5% | -49.5% | -49.7% | 3 -> 2 |
| sorted-200k | 2.57x faster | -9.4% | -49.9% | -50.0% | 3 -> 3 |
| mixed-20k   | 1.17x faster | -0.2% |  -5.5% | -49.4% | 2 -> 2 |

The 40,000-entry case reaches the plan's target: two levels instead of three,
157 densely packed leaves instead of 312 half-full leaves, and 158 blocks
instead of 313. `sorted-200k` also halves the block and leaf count, but its
higher level count is inherent: a default-degree tree holds at most
255 + 256 x 255 = 65,535 entries in two levels.

`mixed-20k` is value-bound: roughly 1,170 of its blocks hold external values,
which both strategies must write. The packed path still halves the tree nodes
and removes about 15% of the elapsed time without increasing space or peak
memory. The memory comparison is roughly flat for the sorted cases (the 40k
difference is within sampling noise) and lower for the mixed and 200k runs.

## Decision: keep the packed loader

The packed path is used by `destination.import()`. It:

1. produces the target two-level shape for the 40,000-entry case and packs every
   larger case to the same occupancy rules,
2. preserves all keys and values byte-for-byte (including null versus empty,
   inline, external and oversized values) across close/reopen, and keeps the
   destination writable with a single genesis change batch,
3. wins on elapsed time in every measured case, by about 2.5x for the sorted
   workloads, while reducing bytes, blocks, levels and/or leaves and not
   regressing peak memory.

The chunked `tryPut` loop is not kept as a production fallback. It remains in
this benchmark as the reproducible baseline.

Rejected alternatives:

- **Keeping the chunked importer.** It is about 2.5x slower on the sorted cases
  and produces a 3-level, half-full tree for 40,000 entries because each sorted
  insert splits a leaf at half occupancy.
- **Building the complete tree in memory and flushing it at the end.** That
  would hold every key and large value at once. The shipped loader instead
  streams: only the open node per level, one closed node per level for the final
  tail rebalance, at most 32 queued append entries and one pending value group
  (about 4 KiB, or one oversized value) are buffered. Values above the 1 KiB
  inline limit are written to shared value blocks as their group fills.

`lib/bulk-load.js` uses the writer's internal `appendBlocks` hook exported from
`lib/write.js`. The hook is the same serialization, pointer fix-up, checkpoint
and append path used by normal `_flush`; normal inserts are unchanged.

## Caveats

- `batched` reproduces only the former writer loop, not the importer's source
  pinning and destination validation. Before this change the real production
  importer and this replay measured the same 313 blocks and 312 leaves with
  comparable time, so the wrapper is not hiding a strategy difference.
- The numbers are from a single machine and a single median run. Re-run
  `node bench/import.js` to compare on another machine.
- The benchmark deliberately does not time seeding or verification, and it does
  not include replication or network I/O.
