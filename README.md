# hyperbee2

```
npm install hyperbee2
```

Next major version for [hyperbee](https://github.com/holepunchto/hyperbee).
Will be merged in there and released a new major when fully done.

An append-only B-tree on top of a [Hypercore][hypercore].

## Usage

```js
import Hyperbee2 from 'hyperbee2'
import Corestore from 'corestore'

// Create/open tree
const tree = new Hyperbee2(new Corestore('./mystore'))

// Wait for tree to become available
await tree.ready()

// Write some values to tree
const batch = tree.write()
batch.tryPut(Buffer.from('name'), Buffer.from('example'))
batch.tryPut(Buffer.from('email'), Buffer.from('example@example.com'))
await batch.flush()

// Read values from tree
const name = await tree.get(Buffer.from('name'))
const email = await tree.get(Buffer.from('email'))

// Print values
console.log(name.value.toString()) // example
console.log(email.value.toString()) // example@example.com
```

## License

Apache-2.0

## API

### Hyperbee

#### `const db = new Hyperbee(store, [options])`

Make a new Hyperbee2 instance. The `store` argument expects a
[Corestore][corestore].

Options include:

```js
{
  key: null,               // Buffer or String. Key of Hypercore to load via Corestore
  maxCacheSize: 4096,      // Max number of nodes to keep in NodeCache
  core: Hypercore(...),    // Hypercore within the Corestore to use (defaults to loading key, or using name='bee')
  view: false,             // Is this a view of an open Hyperbee? (i.e. do not close underlying store)
  writable: true,          // Is append / truncate allowed on the underlying Hypercore?
  unbatch: 0,              // Number of write batches to rollback during bootstrap
  autoUpdate: false,       // Reload root node when underlying Hypercore is appended to?
  preload: null,           // A function called by ready() after the Hypercore is ready. Can be async.
  wait: true,              // Wait for Hypercore to download blocks
}
```

#### `await db.ready()`

Ensures the underlying [Hypercore][hypercore] is ready and prepares the Hyperbee
for use. If `autoUpdate` was set to true in the constructor's options,
this will start watching the [Hypercore][hypercore] for new writes.

Calling `get()` will call this automatically for you.

#### `await db.close()`

Fully close the Hyperbee. If it is not a view on another Hyperbee,
this will also close it's [Hypercore][hypercore].

#### `db.head()`

Returns an object with the following properties:

```
{
  length,  // Number of blocks from the start of Hypercore that apply to this tree.
  key,     // Buffer or null. The key of the underlying Hypercore.
}
```

If the Hyperbee is not ready, this will return null.

#### `db.cache`

Read only. The NodeCache used by this Hyperbee.

#### `db.core`

Read only. The [Hypercore][hypercore] used by this Hyperbee.

#### `db.opening`

Read only. A Promise that resolves to `undefined` once the [Corestore][corestore]
is ready.

#### `db.closing`

Read only. Initially null. When `db.close()` is called, this is set to
a Promise that resolves to `undefined` when the close completes.

#### `db.opened`

Read only. Boolean indicating whether the [Corestore][corestore] has opened.

#### `db.closed`

Read only. Boolean indicating whether the [Corestore][corestore] has closed.

#### `db.replicate()`

Calls `replicate` on underlying [Corestore][corestore].

#### `db.checkout([options])`

Returns a new Hyperbee as a view on the underlying [Hypercore][hypercore].

Options:

```js
{
  writable: false,           // Boolean. Will the new tree be writable?
  length: this.core.length,  // Number. Length of blocks used from the Hypercore
  key: null,                 // Buffer or null. Key of the Hypercore
}
```

#### `db.move([options])`

Replaces the root of the tree with a new entry, potentially using a new
[Hypercore][hypercore].

Options:

```js
{
  writable: this.writable,   // Boolean. Is this tree writable after the move?
  length: this.core.length,  // Number. Length of blocks used from the Hypercore
  key: null,                 // Buffer or null. Key of the Hypercore
}
```

#### `db.snapshot()`

Returns a new Hyperbee that is a read only view of the current tree.

#### `db.undo(n)`

Returns a new Hyperbee that is a writable view of the current tree,
with the last `n` write batches ignored.

#### `db.write([options])`

Returns a [WriteBatch](#writebatch) object through which the tree can be updated.

Options:

```js
{
  length: -1,                     // Length of blocks used from the Hypercore
                                  // (i.e. what point in the hypercore is the write
                                  // going to extend when flush() is called?).
                                  // If -1, the length will be calculated on flush()
                                  // using root.seq + 1.
  key: null,                      // Buffer or null. Key of the Hypercore.
  autoUpdate: true,               // Boolean .Should Hyperbee automatically reflect updates
                                  // after each flush()?
  compat: false,                  // Boolean. Write blocks compatible with Hyperbee 1?
  type: encoding.TYPE_LATEST,     // Number. Block format to use.
  inlineValueSize: 1024,          // Values smaller than this byte length are
                                  // written inline in the node. Larger values
                                  // are referenced via a pointer into the block.
  preferredBlockSize: 4096,       // Try to write blocks of approximately this size
                                  // when flushing updates.
}
```

Errors:

- If the tree is not writable this will throw an Error

#### `db.createReadStream([options])`

Returns a [streamx][streamx] Readable Stream. This is async iterable.

Options:

```js
{
  prefetch: true,        // Prefetch future blocks after yielding an entry?
  reverse: false,        // Iterate backwards over keys?
  limit: -1,             // Max number of entries to yield (-1 means no limit)
  gte: undefined,        // Buffer. Key lower bound (inclusive)
  gt: undefined,         // Buffer. Key lower bound (exclusive)
  lte: undefined,        // Buffer. Key upper bound (inclusive)
  lt: undefined,         // Buffer. Key upper bound (exclusive)
  highWaterMark: 16384,  // Size of read ahead buffer calculated
                         // as: number of entries * 1024
}
```

Iterating over the stream will yield the following properties:

```js
{
  core,    // Hypercore: the hypercore the entry is stored in
  offset,  // Number: the index of the entry in the block
  seq,     // Number: the sequence number of the block in the hypercore
  key,     // Buffer: the key of the entry
  value,   // Buffer: the value of the entry
}
```

Example:

```js
for await (const data of b.createReadStream()) {
  console.log(data.key, '-->', data.value)
}
```

#### `db.createDiffStream(right, [options])`

Returns a [streamx][streamx] Readable Stream that provides
synchronized iteration over two trees. This is async iterable.

Options: Same as options for `createReadStream()`.

Iterating over the stream will yield the same properties as
`createReadStream()` split into left (`db` in this case) and right
(`right` parameter).

```js
{
  left: {core, offset, seq, key, value},   // see createReadStream()
  right: {core, offset, seq, key, value},  // see createReadStream()
}
```

Example:

```js
import Hyperbee from './index.js'
import Corestore from 'corestore'

const b1 = new Hyperbee(new Corestore('./store1'))
const b2 = new Hyperbee(new Corestore('./store2'))

await b1.ready()
await b2.ready()

const w1 = b1.write()
w1.tryPut(Buffer.from('A'), Buffer.from('A'))
w1.tryPut(Buffer.from('B'), Buffer.from('B'))
w1.tryPut(Buffer.from('C'), Buffer.from('C'))
await w1.flush()

const w2 = b2.write()
w2.tryPut(Buffer.from('A'), Buffer.from('A'))
w2.tryPut(Buffer.from('C'), Buffer.from('C'))
w2.tryPut(Buffer.from('E'), Buffer.from('E'))
await w2.flush()

for await (const data of b1.createDiffStream(b2)) {
  console.log(data.left?.key.toString(), data.right?.key.toString())
}
```

Example output:

```js
A A
B undefined
C C
undefined E
```

#### `db.createChangesStream([options])`

Returns a [streamx][streamx] Readable Stream for iterating over all
batches previously written to the tree. This is async iterable.

Options:

```js
{
  head: null,            // null means use this.tree.head()
  highWaterMark: 16384,  // Size of read ahead buffer calculated
                         // as: number of entries * 1024
  timeout: 0,            // Wait at most this many milliseconds (0 means no timeout).
                         // Defaults to the value of the Hyperbee's timeout option.
  wait: true,            // Wait for Hypercore to download blocks
                         // Defaults to the value of the Hyperbee's wait option.
}
```

Iterating over the stream will yield:

```js
{
  head: {
    length,  // Number: number of blocks from the start of Hypercore
             // that apply to this version of the tree
    key,     // Buffer or null: the key of the Hypercore for this version
  },
  tail: {
    length,  // Number: number of blocks from the start of Hypercore
             // that apply to the previous version of the tree
    key,     // Buffer or null: the key of the Hypercore for the previous
             // version
  },
  batch: [   // Blocks written in this batch
    {tree, keys, values, cohorts, metadata, ...},
    ...
  ],
}
```

#### `await db.peek([range])`

Attempts to get the first entry within the given range.

Returns `null` if no entry exists in the range, or an object with
the following properties on success:

```js
{
  core,    // Hypercore: the hypercore the entry is stored in
  offset,  // Number: the index of the entry in the block
  seq,     // Number: the sequence number of the block in the hypercore
  key,     // Buffer: the key of the entry
  value,   // Buffer: the value of the entry
}
```

The `range` argument accepts the same properties as the options for
[`createReadStream()`](#dbcreatereadstreamoptions).

#### `await db.download([range])`

Fetches all entries in the given range. The promise resolves once
all matching entries have been fetched.

The `range` argument accepts the same properties as the options for
[`createReadStream()`](#dbcreatereadstreamoptions).

#### `await db.get(key, [options])`

Attempt to find an entry by its key.

Returns `null` if no entry exists in the range, or an object with
the following properties on success:

```js
{
  core,    // Hypercore: the hypercore the entry is stored in
  offset,  // Number: the index of the entry in the block
  seq,     // Number: the sequence number of the block in the hypercore
  key,     // Buffer: the key of the entry
  value,   // Buffer: the value of the entry
}
```

Options:

```js
{
  timeout,  // Number: wait at most this many milliseconds (0 means no timeout)
  wait,     // Boolean: wait for Hypercore to download blocks
}
```

#### `b.on('ready', listener)`

Emitted once the Hyperbee is ready for use.

##### `b.on('update', listener)`

Emitted in the following scenarios:

- When a `WriteBatch` is flushed and its `autoUpdate` option is `true`
  (the default).
- When the underlying core is appended to (locally or remotely) and the
  `Hyperbee`'s `autoUpdate` option is `true`.
- After a `move()` call on the `Hyperbee`.
- After a rollback completes because the `unbatch` option to the
  `Hyperbee` constructor was greater than `0`.

Warning: this event may be emitted multiple times for the same
update if `autoUpdate` is set on the `Hyperbee` and on `WriteBatch`:

```js
const b = new Hyperbee(store, { autoUpdate: true })
b.on('update', () => console.log('New update'))

const w = b.write(/* autoUpdate is true by default */)
w.tryPut(Buffer.from('key'), Buffer.from('value'))
await w.flush()

// The following will be printed:
// New udpate
// New udpate
```

##### `b.on('close', listener)`

Emitted one the `close()` method completes.

### WriteBatch

A WriteBatch can be constructed via Hyperbee's [write()](#dbwriteoptions)
method. Using a batch, multiple updates can be queued then applied together.

```js
const batch = b.write();
batch.tryPut(Buffer.from('key'), Buffer.from('value');
await batch.flush();
```

**Warning:** WriteBatch does not hold an exclusive lock on the database
while queuing operations unless you call `lock()`. By default, the
lock is only acquired when the operations are flushed. So be careful
about building concurrent batches:

```js
import Hyperbee from './index.js'
import Corestore from 'corestore'

const b = new Hyperbee(new Corestore('./store'))
await b.ready()

const w1 = b.write()
w1.tryPut(Buffer.from('name'), Buffer.from('Sneezy'))

const w2 = b.write()
w2.tryPut(Buffer.from('name'), Buffer.from('Sleepy'))
w2.tryPut(Buffer.from('email'), Buffer.from('sleepy@example.com'))

// Be careful about application order vs. order the operations
// were queued when building batches concurrently.
await w2.flush()
await w1.flush()

for await (const data of b.createReadStream(b)) {
  console.log(data.key.toString(), '-->', data.value.toString())
}

// Output: (email and name mismatch)
//
// email --> sleepy@example.com
// name --> Sneezy
```

### `await batch.lock()`

Aquires an exclusive write lock now instead of waiting for `flush()`
to be called. No writes will occur until this batch is closed allowing
you to keep a consistent view of the database while building the batch.

#### `batch.tryPut(key, value)`

Queues an operation to associate `key` with `value`. Any existing entry
for `key` will be overwritten.

#### `batch.tryDelete(key)`

Queues an operation to remove the entry with `key`, if it exists. If it
does not exist, this method does nothing (and will not throw).

#### `batch.tryClear()`

Queues an operation to clear all entries from the tree.

#### `await batch.flush()`

Aquires an exclusive write lock and applies the operations queued in this
batch to the tree, clearing the queue.

**Warning:** continuing to use the batch after flushing can cause unpredictable
behavior. Batches applied after the first flush will be 'unapplied' if you
flush again later. This can lead to accidentally removing data from the tree.

```js
import Hyperbee from './index.js'
import Corestore from 'corestore'

const b = new Hyperbee(new Corestore('./store'))
await b.ready()

const w1 = b.write()
w1.tryPut(Buffer.from('name'), Buffer.from('Sneezy'))

const w2 = b.write()
w2.tryPut(Buffer.from('name'), Buffer.from('Sleepy'))
w2.tryPut(Buffer.from('email'), Buffer.from('sleepy@example.com'))

await w1.flush()
await w2.flush()

// Warning: flushing w1 again will unapply w2!
w1.tryPut(Buffer.from('active'), Buffer.from('false'))
await w1.flush()

for await (const data of b.createReadStream(b)) {
  console.log(data.key.toString(), '-->', data.value.toString())
}

// Output: (name has reverted to 'Sneezy', email is missing)
//
// active --> false
// name --> Sneezy
```

#### `batch.close()`

Closes the batch without flushing operations. Subsequent attempts
to flush the batch will result in an error.

[hypercore]: https://github.com/holepunchto/hypercore
[corestore]: https://github.com/holepunchto/corestore
[streamx]: https://github.com/mafintosh/streamx
