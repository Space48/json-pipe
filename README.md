# @space48/json-pipe

A powerful TypeScript library for building JSON processing pipelines with async iterables. This library makes it easy to create efficient command-line programs that process JSON data through a series of transformations, with support for streaming and backpressure.

## Features

- Process JSON data streams using async iterables
- Rich set of transformation operators:
  - `map` and `mapAsync` with configurable concurrency
  - `flatMap` and `flatMapAsync` for nested transformations
  - `filter` for conditional processing
  - `distinct` for deduplication
  - `groupBy` and `groupWhile` for data aggregation
  - `batch` for processing items in chunks
  - `take` and `takeWhile` for limiting output
  - `reduce` for aggregating results
- Built-in support for reading/writing JSON Lines format
- Composable transformations using `pipe` and `compose`
- Efficient streaming with backpressure handling
- Written in TypeScript with full type safety
- Node.js 8+ compatible

## Installation

```bash
npm install @space48/json-pipe
```

### Beta Versions

To install the latest beta version:

```bash
npm install @space48/json-pipe@beta
```

Beta versions are automatically published:
- When code is pushed to the `develop` branch (version format: `x.y.z-beta.commit-hash`)
- When a pull request is opened against `master` (version format: `x.y.z-beta.pr.number`)

## Usage

### Basic Example

```typescript
import { pipe, readJsonLinesFrom, writeJsonLinesTo, map } from '@space48/json-pipe';

// Read JSON Lines from stdin, transform them, and write to stdout
await pipe(
  readJsonLinesFrom(process.stdin),
  map(data => {
    // Your transformation logic here
    return transformedData;
  }),
  writeJsonLinesTo(process.stdout)
);
```

### Parallel Processing

```typescript
import { pipe, mapAsync } from '@space48/json-pipe';

// Process items concurrently
const transform = mapAsync(
  { concurrency: 5 }, // Process 5 items at a time
  async item => {
    // Async transformation logic
    return processedItem;
  }
);
```

### Data Aggregation

```typescript
import { pipe, groupBy, batch } from '@space48/json-pipe';

// Group items by a key
const groupedTransform = pipe(
  input,
  groupBy(item => item.category),
  // Process in batches of 100
  batch(100)
);
```

## API Reference

### Core Functions

- `pipe(...fns)`: Compose multiple transformations
- `compose(...fns)`: Compose functions from right to left
- `map(fn)`: Transform each item
- `mapAsync(options, fn)`: Transform items asynchronously with concurrency control
- `flatMap(fn)`: Transform and flatten results
- `flatMapAsync(options, fn)`: Transform and flatten asynchronously
- `filter(predicate)`: Filter items based on a predicate
- `distinct(compare?)`: Remove duplicates
- `groupBy(getKeyFn)`: Group items by key
- `batch(size)`: Process items in batches
- `take(n)`: Limit number of items
- `reduce(fn, initialValue?)`: Aggregate items

### I/O Functions

- `readJsonLinesFrom(source)`: Read JSON Lines from a readable stream
- `writeJsonLinesTo(destination)`: Write JSON Lines to a writable stream
- `readLinesFrom(source)`: Read lines from a readable stream

## Requirements

- Node.js >= 8
- TypeScript (for development)

## Development

1. Clone the repository:
```bash
git clone https://github.com/Space48/json-pipe.git
```

2. Install dependencies:
```bash
npm install
```

3. Build the project:
```bash
npm run build
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For bug reports and feature requests, please open an issue on the [GitHub repository](https://github.com/Space48/json-pipe/issues).

---
Maintained by [Space48](https://github.com/Space48)
