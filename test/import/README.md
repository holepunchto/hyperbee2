# Import benchmark

Compares the chunked `tryPut` import strategy used before this change with the
bounded sorted right-edge writer that `destination.import()` now uses.

- `batched` replays the previous importer loop: read the pinned source stream,
  buffer up to 1,024 entries or 1 MiB, then `destination.write()` + `tryPut` +
  `batch.flush()`. It uses the real writer, including halfway splits and block
  encoding.
- `import` calls the production `destination.import(source)` path. It uses the
  same 1,024-entry / 1 MiB batches, but each batch is marked as a sorted
  right-edge write: nodes are allowed to fill before they split, so every
  committed batch is a legal tree with packed leaves and a valid root. The
  importer commits one change batch per flush instead of one genesis batch for
  the whole import.

Seeding the source, choosing/pinning its version and verifying the destination
are excluded from the measured import time. Every measured run uses a fresh
destination core/store. The cases are:

| case            | entries | keys                              | values                                                                                            |
| --------------- | ------- | --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `sorted-40k`    | 40,000  | 4-byte ascending counter          | 1 byte                                                                                            |
| `sorted-200k`   | 200,000 | 4-byte ascending counter          | 1 byte                                                                                            |
| `mixed-20k`     | 20,000  | 4-byte counter + 0-22 byte suffix | null, empty, inline, external (> 1 KiB) and oversized (> 4 KiB) values, including one 2 MiB value |
| `wide-keys-400` | 400     | ascending 64 KiB keys             | null (a full 255-key node would be ~16.7 MiB, above the block limit)                              |

## Reproduce

```sh
npm install
node test/import/bench.js --runs 5
```

Each implementation gets one discarded warm-up and then five measured runs; the
table reports medians. Other options:

```sh
node test/import/bench.js --cases sorted-40k --impl import --runs 5 --json
node test/import/bench.js --impl batched,import --cases mixed-20k
node test/import/bench.js --cases sorted-200k --impl import --runs 1
```

`--impl` accepts `batched` and/or `import`; `--cases` accepts a comma-separated
subset of the table above; `--json` prints the raw medians. Use `--runs 1` for a
single fresh-process memory sample.

The benchmark also re-reads the destination and compares a SHA-256 digest of
every key and value (including the null/empty distinction) against the source.
A mismatch is reported as `FAIL` and exits non-zero.

## Environment

- Node v22.22.1, linux x64, 8 vCPU QEMU virtual CPU, 30 GiB RAM.
- Source and destination stores in the system temporary directory.
- `heap`, `ext` and `rss` are the maximum sampled
  `process.memoryUsage()` deltas above the pre-import sample, sampled every
  2 ms. They include `external` and `arrayBuffers`, not only the V8 heap, but
  they are still approximations: GC timing and RSS high-water marks add noise.
  Sequential runs in one process can report a near-zero RSS delta because the
  process already grew during the warm-up and earlier cases. For a clean RSS
  sample, run one case and implementation per process with `--runs 1`.
- `leaf keys` is minimum/median/maximum keys per leaf. Tree shape is measured by
  walking the imported tree with the normal inflation path.

## Results

Medians of five measured runs (`node test/import/bench.js --runs 5`):

| case          | impl    | time (ms) |         bytes |    blocks | heap (MiB) | ext (MiB) | rss (MiB) | levels |  leaves |   leaf keys |
| ------------- | ------- | --------: | ------------: | --------: | ---------: | --------: | --------: | -----: | ------: | ----------: |
| sorted-40k    | batched |       217 |       528,023 |       313 |       16.2 |       0.1 |       8.4 |      3 |     312 | 127/127/192 |
| sorted-40k    | import  |   **196** |   **515,458** |   **157** |   **15.7** |       0.1 |   **7.3** |  **2** | **157** | 128/255/255 |
| sorted-200k   | batched |     1,141 |     2,969,576 |     1,572 |       41.4 |       0.1 |      45.3 |      3 |   1,562 | 127/127/192 |
| sorted-200k   | import  | **1,083** | **2,913,604** |   **791** |       46.4 |       0.1 |       3.4 |      3 | **782** | 128/255/255 |
| mixed-20k     | batched |       354 |    11,916,008 |     1,330 |        7.3 |       5.8 |      12.5 |      2 |     156 | 127/127/160 |
| mixed-20k     | import  |   **341** |    11,918,637 | **1,245** |    **7.4** |       5.9 |   **8.5** |      2 |  **79** | 128/255/255 |
| wide-keys-400 | batched |       102 |    26,221,057 |        27 |        0.8 |      13.0 |      11.1 |      2 |       3 | 127/127/144 |
| wide-keys-400 | import  |       103 |    26,221,125 |        27 |        1.0 |      15.0 |      19.0 |      2 |       3 | 127/128/143 |

Relative change (import versus batched, medians):

| case          |         time | bytes | blocks | leaves | levels |
| ------------- | -----------: | ----: | -----: | -----: | -----: |
| sorted-40k    | 1.11x faster | -2.4% | -49.8% | -49.7% | 3 -> 2 |
| sorted-200k   | 1.05x faster | -1.9% | -49.7% | -50.0% | 3 -> 3 |
| mixed-20k     | 1.04x faster | +0.0% |  -6.4% | -49.4% | 2 -> 2 |
| wide-keys-400 |        ~flat | +0.0% |   0.0% |   0.0% | 2 -> 2 |

The RSS column from the five-run table is not a useful comparison: the runs
share a process and RSS is not returned after the warm-up. Fresh-process,
single-run samples (no warm-up) gave:

| case        | impl    | heap (MiB) | ext (MiB) | rss (MiB) |
| ----------- | ------- | ---------: | --------: | --------: |
| sorted-40k  | batched |       10.6 |       0.1 |      11.4 |
| sorted-40k  | import  |       10.9 |       0.1 |       9.8 |
| sorted-200k | batched |       51.8 |       0.2 |      48.1 |
| sorted-200k | import  |       47.9 |       0.2 |      45.2 |
| mixed-20k   | batched |       10.4 |       6.4 |      25.5 |
| mixed-20k   | import  |       13.5 |       7.3 |      25.3 |

Those samples are noisy (GC timing and allocator high-water marks), but they do
not show the whole-tree retention or block-size blow-up of the rejected
prototype; the importer now uses the normal node cache, which evicts nodes.

`wide-keys-400` is the regression test for the old packed loader: writing all
keys of a 255-key node into one block produced a 16,713,993-byte block and
Hypercore rejected it. Both implementations now stay at 27 blocks of about
1 MiB, because each import batch only contains about 1 MiB of new key material.

## Decision: keep the bounded sorted writer

`destination.import()` uses the bounded right-edge writer. It:

1. produces the target two-level shape for the 40,000-entry case (157 packed
   leaves instead of 312 half-full leaves),
2. preserves every key/value byte (including null versus empty, 64 KiB keys,
   external values and a 15 MiB - 1 KiB near-limit value) across close/reopen,
3. commits bounded change batches whose roots, `batch.start`/`batch.end` and
   `previous` chain are consistent, so changes and `checkout` work per batch,
4. reduces node and block space by roughly half on sorted workloads at a modest
   time win, without a measured memory regression.

The measured win is much smaller than the rejected `4fbc7a4` prototype claimed
(about 2.5x): that prototype bypassed the per-entry tree walk, but it was unsafe
(unbounded decoded pointers, 16.7 MiB wide-key node blocks, one genesis batch for
the whole import, and value grouping that could exceed 15 MiB). The bounded
writer reuses the normal `_put`/`_flush` path, so its win is the packed shape and
roughly half the node blocks, not a different insertion algorithm.

Rejected alternatives:

- **Shipping the `4fbc7a4` packed loader.** It fails the wide-key, near-limit
  value, memory/buffering and change-batch requirements above.
- **Keeping only the chunked importer.** It is safe for the tested data, but it
  leaves the sorted 40,000-entry destination at three levels and 312 leaves
  (313 blocks). It is marginally slower than the bounded writer and uses about
  2% more bytes in the sorted cases.
- **Building the complete tree in memory and flushing it once.** That would
  retain every key and large value and reproduce the change-batch problem.

## Caveats

- `batched` reproduces only the former writer loop, not the importer's source
  pinning and destination validation. Before this change the real production
  importer and this replay measured the same 313 blocks and 312 leaves with
  comparable time, so the wrapper is not hiding a strategy difference.
- The numbers are from a single machine and a single median run. Re-run
  `node test/import/bench.js` to compare on another machine.
- The benchmark deliberately does not time seeding or verification, and it does
  not include replication or network I/O.
- A single value above Hypercore's 15 MiB block limit is still written alone and
  rejected by Hypercore; the importer does not split values.
