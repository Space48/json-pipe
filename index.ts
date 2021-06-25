import { EventEmitter } from "events";
import * as readline from "readline";

// node 8 compat
if (typeof Symbol === undefined || !(Symbol as any).asyncIterator) {
  (Symbol as any).asyncIterator = Symbol.for("Symbol.asyncIterator");
}

export type Transform<I = any, O = any> = (input: AsyncIterable<I>) => AsyncIterable<O>;

export function readJsonLinesFrom<T = unknown>(source: NodeJS.ReadableStream): AsyncIterable<T> {
  return pipe(
    readLinesFrom(source),
    map(JSON.parse),
  );
}

export function writeJsonLinesTo(destination: NodeJS.WritableStream): <T>(source: Source<T>) => Promise<void> {
  return source => pipe(
    typeof source === 'function' ? source() : source,
    map(element => `${JSON.stringify(element)}\n`),
    writeTo(destination),
  );
}

export type Source<T = any> = AsyncIterable<T> | (() => AsyncIterable<T>);

type Fn<I = any, O = any> = (arg: I) => O;

export function pipe<T>(input: T): T;
export function pipe<T1, T2>(input: T1, f1: Fn<T1, T2>): T2;
export function pipe<T1, T2, T3>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>): T3;
export function pipe<T1, T2, T3, T4>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>): T4;
export function pipe<T1, T2, T3, T4, T5>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>): T5;
export function pipe<T1, T2, T3, T4, T5, T6>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>): T6;
export function pipe<T1, T2, T3, T4, T5, T6, T7>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>): T7;
export function pipe<T1, T2, T3, T4, T5, T6, T7, T8>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>): T8;
export function pipe<T1, T2, T3, T4, T5, T6, T7, T8, T9>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>): T9;
export function pipe<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>, f9: Fn<T9, T10>): T10;
export function pipe<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(input: T1, f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>, f9: Fn<T9, T10>, f10: Fn<T10, T11>): T11;
export function pipe<T = any>(input: T, ...fns: Fn<T, T>[]): T;
//export function pipe(...transforms: Fn[]): Fn;
export function pipe(input: unknown, ...fns: Fn[]): unknown {
  return compose(...fns)(input);
}

export function compose<T>(): Fn<T, T>;
export function compose<T1, T2>(f1: Fn<T1, T2>): Fn<T1, T2>;
export function compose<T1, T2, T3>(f1: Fn<T1, T2>, f2: Fn<T2, T3>): Fn<T1, T3>;
export function compose<T1, T2, T3, T4>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>): Fn<T1, T4>;
export function compose<T1, T2, T3, T4, T5>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>): Fn<T1, T5>;
export function compose<T1, T2, T3, T4, T5, T6>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>): Fn<T1, T6>;
export function compose<T1, T2, T3, T4, T5, T6, T7>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>): Fn<T1, T7>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>): Fn<T1, T8>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>): Fn<T1, T9>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>, f9: Fn<T9, T10>): Fn<T1, T10>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(f1: Fn<T1, T2>, f2: Fn<T2, T3>, f3: Fn<T3, T4>, f4: Fn<T4, T5>, f5: Fn<T5, T6>, f6: Fn<T6, T7>, f7: Fn<T7, T8>, f8: Fn<T8, T9>, f9: Fn<T9, T10>, f10: Fn<T10, T11>): Fn<T1, T11>;
export function compose<T = any>(...fns: Fn<T, T>[]): Fn<T, T>;
//export function compose(...transforms: Fn[]): Fn;
export function compose(...fns: Fn[]): Fn {
  return input => fns.reduce((previous, t) => t(previous), input);
}

export function map<T, R>(mapper: (input: T) => Promise<R> | R): Transform<T, R> {
  return async function* (input) {
    for await (const element of input) {
      yield mapper(element);
    }
  };
}

type AsyncMapper<T, R> = (input: T) => Promise<R>;
export type MapAsyncOptions = {
  concurrency?: number,     // Default: `1`. Max number of concurrent `mapper()` calls.
  preserveOrder?: boolean,  // Deprecated. Order is always preserved now.
};
export function mapAsync<T, R>(mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(options: MapAsyncOptions, mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(mapperOrOptions: AsyncMapper<T, R>|MapAsyncOptions, maybeMapper?: AsyncMapper<T, R>): Transform<T, R> {
  const mapper = maybeMapper || mapperOrOptions as AsyncMapper<T, R>;
  const options = maybeMapper && mapperOrOptions as MapAsyncOptions;
  const maxConcurrency = options?.concurrency ?? 1;

  if (maxConcurrency < 1) {
    throw new Error(`Concurrency must be greater than 0, but ${maxConcurrency} was specified.`);
  }

  if (maxConcurrency === 1) {
    return map(mapper);
  }

  const maxInputOutputDelta = 2 * maxConcurrency;

  return input => {
    const counters = {
      input: 0,
      mapped: 0,
      output: 0,
    };

    const bufferController = new BufferController();

    function increment(key: keyof typeof counters) {
      return () => {
        counters[key]++;
        let numItemsBeingMapped = counters.input - counters.mapped;
        let inputOutputDelta = counters.input - counters.output;
        if (numItemsBeingMapped >= maxConcurrency || inputOutputDelta >= maxInputOutputDelta) {
          bufferController.pause();
        } else {
          bufferController.resume();
        }
      };
    }

    return pipe(
      input,
      tap(increment('input')),
      map(value => [mapper(value)] as const),
      tap(([promise]) => {
        promise.then(increment('mapped'));
      }),
      controllableBuffer(bufferController),
      map(([promise]) => promise),
      tap(increment('output')),
    );
  };
}

export function flatMap<T, R>(mapper: (input: T) => Iterable<R>): Transform<T, R> {
  return compose(map(mapper), flatten());
}

type AsyncFlatMapper<T, R> = (input: T) => AsyncIterable<R>;
export type FlatMapAsyncOptions = {
  concurrency?: number,       // Default: `1`. Max number of concurrent `mapper()` calls.
  preserveOrder?: boolean,    // Deprecated. Order is always preserved now.
  innerPreloadLimit?: number, // Default: `5`. How many items to preload from each inner iterable.
};
export function flatMapAsync<T, R>(mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(options: FlatMapAsyncOptions, mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(mapperOrOptions: AsyncFlatMapper<T, R>|FlatMapAsyncOptions, maybeMapper?: AsyncFlatMapper<T, R>): Transform<T, R> {
  const mapper = maybeMapper || mapperOrOptions as AsyncFlatMapper<T, R>;
  const options = maybeMapper && mapperOrOptions as FlatMapAsyncOptions;
  const maxConcurrency = options?.concurrency ?? 1;

  if (maxConcurrency < 1) {
    throw new Error(`Concurrency must be greater than 0, but ${maxConcurrency} was specified`);
  }

  if (maxConcurrency === 1) {
    return async function* (input) {
      for await (const element of input) {
        yield* mapper(element);
      }
    };
  }
  
  const maxInputOutputDelta = 2 * maxConcurrency;
  const innerPreloadLimit = options?.innerPreloadLimit ?? 5;

  return input => {
    const counters = {
      input: 0,
      mapped: 0,
      output: 0,
    };

    const bufferController = new BufferController();

    function increment(key: keyof typeof counters) {
      return () => {
        counters[key]++;
        let numItemsBeingMapped = counters.input - counters.mapped;
        let inputOutputDelta = counters.input - counters.output;
        if (numItemsBeingMapped >= maxConcurrency || inputOutputDelta >= maxInputOutputDelta) {
          bufferController.pause();
        } else {
          bufferController.resume();
        }
      };
    }

    return pipe(
      input,
      tap(increment('input')),
      map(compose(mapper, onEnd(increment('mapped')), buffer(innerPreloadLimit))),
      controllableBuffer(bufferController),
      tap(increment('output')),
      flatten(),
    );
  };
}

/**
 * Merge iteratables without preserving order
 */
export async function* mergeUnordered<T>(...inputs: AsyncIterable<T>[]) {
  if (inputs.length === 0) {
    return;
  }
  
  if (inputs.length === 1) {
    yield* inputs[0];
  }
  
  yield* buffer<T>(inputs.length)(...inputs);
}

export function collectArray<T>(): Fn<AsyncIterable<T>, Promise<T[]>> {
  return reduce(
    (result, element) => {
      result.push(element);
      return result;
    },
    [] as T[]
  )
}

export function first<T>(options?: {}): Fn<AsyncIterable<T>, Promise<T>>;
export function first<T, D>(options: {default: D}): Fn<AsyncIterable<T>, Promise<T|D>>;
export function first<T, D>(options?: {default?: D}): Fn<AsyncIterable<T>, Promise<T|D>> {
  const hasDefault = Boolean(options && 'default' in options);
  return async (input) => {
    for await (const element of input) {
      return element;
    }
    if (hasDefault) {
      return options!.default!;
    }
    throw new Error('first() was called with an empty iterator and no default value.');
  };
}

export function reduce<T>(callbackfn: (previousValue: T, currentValue: T) => T): Fn<AsyncIterable<T>, Promise<T>>;
export function reduce<T, U>(callbackfn: (previousValue: U, currentValue: T) => U, initialValue: U): Fn<AsyncIterable<T>, Promise<U>>;
export function reduce<T>(callbackfn: (previousValue: T, currentValue: T) => T, initialValue: T): Fn<AsyncIterable<T>, Promise<T>>;
export function reduce<T>(callbackfn: (previousValue: any, currentValue: T) => any, ...optionalArgs: any[]): Fn<AsyncIterable<T>, Promise<any>> {
  if (optionalArgs.length === 0) {
    return async (input) => {
      let firstElement = true;
      let currentValue: any;
      for await (const element of input) {
        if (firstElement) {
          currentValue = element;
          firstElement = false;
        } else {
          currentValue = callbackfn(currentValue, element);
        }
      }
      if (firstElement) {
        throw new Error('reduce() was called with an empty iterator and no default value.');
      }
      return currentValue;
    };
  } else {
    return async (input) => {
      let currentValue: any = optionalArgs[0];
      for await (const element of input) {
        currentValue = callbackfn(currentValue, element);
      }
      return currentValue;
    };
  }
}

export function tap<T>(observer: (element: T) => Promise<any>|any): Transform<T, T> {
  return map(async el => {
    await observer(el);
    return el;
  });
}

export function onEnd<T>(observer: () => Promise<any>|any): Transform<T, T> {
  return async function* (input) {
    for await (const element of input) {
      yield element;
    }
    await observer();
  };
}

export function flatten<T>(): Transform<Iterable<T> | AsyncIterable<T>, T> {
  return async function* (input) {
    for await (const element of input) {
      yield* element;
    }
  };
}

export function filter<T>(predicate: (element: T) => any): Transform<T, T> {
  return flatMap(el => predicate(el) ? [el] : []);
}

export function distinct<T>(): Transform<T, T>;
export function distinct<T>(compare: (element1: T, element2: T) => boolean): Transform<T, T>;
export function distinct<T>(compare: ((element1: T, element2: T) => boolean) = Object.is): Transform<T, T> {
  return async function* (input) {
    let previousElement: T;
    let firstEl = true;

    for await (const element of input) {
      if (firstEl) {
        firstEl = false;
        previousElement = element;
        yield element;
      } else if (!compare(element, previousElement!)) {
        previousElement = element;
        yield element;
      }
    }
  };
}

export function groupWhile<T>(predicate: (element: T, group: readonly T[]) => boolean): Transform<T, T[]> {
  return async function* (input) {
    let currentGroup: T[] = [];
    
    for await (const element of input) {
      if (currentGroup.length === 0 || predicate(element, currentGroup)) {
        currentGroup.push(element);
      } else {
        yield currentGroup;
        currentGroup = [element];
      }
    }

    if (currentGroup.length) {
      yield currentGroup;
      currentGroup = [];
    }
  };
}

export function groupBy<T, K>(getKeyFn: (element: T) => K): Transform<T, T[]> {
  return groupWhile((el, [groupHead]) => getKeyFn(el) === getKeyFn(groupHead));
}

export function batch<T>(size: number): Transform<T, T[]> {
  return groupWhile((_, group) => group.length < size);
}

export function take<T>(n: number): Transform<T, T> {
  if (n < 0) {
    throw new Error('take(n) was called with a negative number. n must be non-negative.');
  }

  return async function* (input) {
    if (n === 0) {
      return;
    }
    let numConsumed = 0;
    for await (const element of input) {
      yield element;
      if (++numConsumed === n) {
        break;
      }
    }
  };
}

export function takeWhile<T>(predicate: (element: T) => any): Transform<T, T> {
  return async function* (input) {
    for await (const element of input) {
      if (!predicate(element)) {
        break;
      }
      yield element;
    }
  };
}

export function forEach<T>(action: (element: T) => void): Fn<AsyncIterable<T>, Promise<void>> {
  return async input => {
    for await (const element of input) {
      action(element);
    }
  };
}

export async function * range(from: number, to: number, step: number = 1): AsyncIterable<number> {
  for (let n = from; n <= to; n += step) {
    yield n;
  }
}

function readLinesFrom(source: NodeJS.ReadableStream): AsyncIterable<string> {
  const rl = readline.createInterface({input: source});
  return (rl as any)[Symbol.asyncIterator] ? rl as any : iterateReadLine(rl);
}

// node 8 compat
async function* iterateReadLine(rl: readline.ReadLine): AsyncIterable<string> {
  let closed = false;
  rl.once('close', () => closed = true);

  let buffer: string[] = [];
  rl.on('line', line => buffer.push(line));
  
  do {
    await new Promise<string|void>(resolve => {
      if (closed) {
        resolve();
        return;
      }
      rl.once('line', () => {
        rl.pause();
        rl.removeListener('close', resolve);
        resolve();
      });
      rl.once('close', resolve);
      rl.resume();
    });

    const output = buffer;
    buffer = [];
    yield* output;
  } while (!closed);
}

// node 8 compat. Treats EPIPE as a success.
export const writeTo = (destination: NodeJS.WritableStream) => (source: AsyncIterable<string>): Promise<void> => {
  let error: any = undefined;
  const captureError = (e: any) => error = e;
  destination.once('error', captureError);
  const interruptOnError = <T>(promise: Promise<T>) => new Promise<T>((resolve, reject) => {
    if (error) {
      reject(error);
      return;
    }

    destination.once('error', reject);
    promise.then(
      value => (destination.removeListener('error', reject), resolve(value)),
      reason => (destination.removeListener('error', reject), reject(reason)),
    );
  });

  return new Promise(async (resolve, reject) => {
    try {
      let numInFlight = 0;

      let sourceFinished = false;
      const sourceIterator = source[Symbol.asyncIterator]();
      do {
        const value = await interruptOnError(sourceIterator.next());
        if (value.done) {
          sourceFinished = true;
        } else {
          numInFlight++;
          const stdoutReadyForMore = destination.write(value.value, (error?: any) => {
            if (!error) {
              numInFlight--;
              if (numInFlight === 0 && sourceFinished) {
                resolve();
              }
            }
          });
          if (!stdoutReadyForMore) {
            await interruptOnError(new Promise(resolve => destination.once('drain', resolve)));
          }
        }
      } while (!sourceFinished);

      if (numInFlight === 0) {
        resolve();
      }
    } catch (e) {
      if (e?.code === 'EPIPE') {
        resolve();
      } else {
        reject(e);
      }
    } finally {
      destination.removeListener('error', captureError);
    }
  });
}

/**
 * @deprecated Use groupBy instead
 */
 export function group<T, K>(getKeyFn: (element: T) => K): Transform<T, T[]> {
  return groupBy(getKeyFn);
}

/** 
 * Reads JSON lines from stdin, transforms them via `transform`, and writes the output to stdout as JSON lines
 * 
 * @deprecated Use pipe(readJsonLinesFrom(process.stdin), transform, writeJsonLinesTo(process.stdout))
 */
export function transformJson(transform: Transform): Promise<void> {
  return pipe(
    readJsonLinesFrom(process.stdin),
    transform,
    writeJsonLinesTo(process.stdout),
  );
}

/**
 * Iterates `source` and writes the output to stdout as JSON lines
 * 
 * @deprecated Use writeJsonLinesTo(process.stdout)
 */
export function streamJson<T>(source: Source<T>): Promise<void> {
  const iterable = typeof source === 'function' ? source() : source;
  const outputLines = map(element => `${JSON.stringify(element)}\n`)(iterable);
  return writeTo(process.stdout)(outputLines);
}

function buffer<T>(size: number) {
  return (...inputs: AsyncIterable<T>[]) => {
    let numItemsInBuffer = 0;
    let bufferController = new BufferController();

    return pipe(
      inputs.map(
        tap(() => {
          numItemsInBuffer++;
          if (numItemsInBuffer >= size) {
            bufferController.pause();
          }
        })
      ),

      preparedInputs => controllableBuffer<T>(bufferController)(...preparedInputs),

      tap(() => {
        numItemsInBuffer--;
        if (numItemsInBuffer <= size) {
          bufferController.resume();
        }
      }),
    );
  };
}

function controllableBuffer<T>(controller: BufferController) {
  return (...inputs: AsyncIterable<T>[]): AsyncIterable<T> => {
    const bufferedItems: T[] = [];

    let numInputsEnded = 0;
    let error: any = undefined;

    let notifyInputActivity = () => {};

    inputs.forEach(input => {
      (async () => {
        try {
          await controller.whenNotPaused();
          for await (const item of input) {
            bufferedItems.push(item);
            notifyInputActivity();
            await controller.whenNotPaused();
          }
        } catch (e) {
          error = e;
        } finally {
          numInputsEnded++;
          notifyInputActivity();
        }
      })();
    });

    return (async function* () {
      while (!error && !(numInputsEnded === inputs.length && bufferedItems.length === 0)) {
        if (bufferedItems.length === 0) {
          await new Promise<void>(resolve => {
            notifyInputActivity = resolve;
          });
        }

        while (bufferedItems.length > 0 && !error) {
          yield bufferedItems.shift()!;
        }
      }

      if (error) {
        throw error;
      }
    })();
  }
}

class BufferController {
  private paused = false;
  private emitter = new EventEmitter;

  pause() {
    this.paused = true;
    return this;
  }

  resume(): this {
    if (this.paused) {
      this.paused = false;
      this.emitter.emit('resumed'); 
    }
    return this;
  }

  async whenNotPaused(): Promise<void> {
    if (this.paused) {
      await new Promise<void>(resolve => this.emitter.once('resumed', resolve));
    }
  }
}
