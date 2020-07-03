import * as readline from "readline";

// node 8 compat
if (typeof Symbol === undefined || !(Symbol as any).asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("Symbol.asyncIterator");
}

export type Transform<I = any, O = any> = (input: AsyncIterable<I>) => AsyncIterable<O>;

// reads JSON lines from stdin, transforms them via `transform`, and writes the output to stdout as JSON lines
export function transformJson(transform: Transform): Promise<void> {
    const f = compose(map(JSON.parse), transform);
    const output = f(readLinesFromStdin());
    return streamJson(output);
}

export type Source<T = any> = AsyncIterable<T> | (() => AsyncIterable<T>);

// iterates `source` and writes the output to stdout as JSON lines
export function streamJson<T>(source: Source<T>): Promise<void> {
    const iterable = typeof source === 'function' ? source() : source;
    const outputLines = map(element => `${JSON.stringify(element)}\n`)(iterable);
    return writeToStdout(outputLines);
}

type Fn<I = any, O = any> = (arg: I) => O;

export function compose<T>(): Fn<T, T>;
export function compose<T1, T2>(t1: Fn<T1, T2>): Fn<T1, T2>;
export function compose<T1, T2, T3>(t1: Fn<T1, T2>, t2: Fn<T2, T3>): Fn<T1, T3>;
export function compose<T1, T2, T3, T4>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>): Fn<T1, T4>;
export function compose<T1, T2, T3, T4, T5>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>): Fn<T1, T5>;
export function compose<T1, T2, T3, T4, T5, T6>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>): Fn<T1, T6>;
export function compose<T1, T2, T3, T4, T5, T6, T7>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>, t6: Fn<T6, T7>): Fn<T1, T7>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>, t6: Fn<T6, T7>, t7: Fn<T7, T8>): Fn<T1, T8>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>, t6: Fn<T6, T7>, t7: Fn<T7, T8>, t8: Fn<T8, T9>): Fn<T1, T9>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>, t6: Fn<T6, T7>, t7: Fn<T7, T8>, t8: Fn<T8, T9>, t9: Fn<T9, T10>): Fn<T1, T10>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(t1: Fn<T1, T2>, t2: Fn<T2, T3>, t3: Fn<T3, T4>, t4: Fn<T4, T5>, t5: Fn<T5, T6>, t6: Fn<T6, T7>, t7: Fn<T7, T8>, t8: Fn<T8, T9>, t9: Fn<T9, T10>, t10: Fn<T10, T11>): Fn<T1, T11>;
export function compose<T = any>(...fns: Fn<T, T>[]): Fn<T, T>;
//export function compose(...transforms: Fn[]): Fn;
export function compose(...transforms: Fn[]): Fn {
    return input => transforms.reduce((previous, t) => t(previous), input);
}

export function map<T, R>(mapper: (input: T) => R): Transform<T, R> {
    return async function* (input) {
        for await (const element of input) {
            yield mapper(element);
        }
    };
}

type AsyncMapper<T, R> = (input: T) => Promise<R>;
export type MapAsyncOptions = {
    concurrency?: number,       // Default: `1`. Max number of concurrent `mapper()` calls. When `concurrency == 1`, element order is always preserved.
    preserveOrder?: boolean,    // Default: `false`. Ensure output order matches input order. Very slow `mapper()` calls will reduce throughput. (If `concurrency > 1`.)
    outputBufferSize?: number,  // Default: `concurrency`. Max number of output elements to buffer before pausing input. (If `preserverOrder == true`.)
};
export function mapAsync<T, R>(mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(options: MapAsyncOptions, mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(mapperOrOptions: AsyncMapper<T, R>|MapAsyncOptions, maybeMapper?: AsyncMapper<T, R>): Transform<T, R> {
    const mapper = maybeMapper || mapperOrOptions as AsyncMapper<T, R>;
    const options = maybeMapper && mapperOrOptions as MapAsyncOptions;
    const maxConcurrency = options?.concurrency ?? 1;
    const preserveOrder = options?.preserveOrder ?? false;
    if (maxConcurrency < 1) {
        throw new Error(`Concurrency must be greater than 0, but ${maxConcurrency} was specified.`);
    }
    if (maxConcurrency === 1) {
        return async function* (input) {
            for await (const element of input) {
                yield await mapper(element);
            }
        };
    } else if (preserveOrder) {
        const maxOutputBufferSize = options?.outputBufferSize ?? maxConcurrency;
        const outputQueueMaxSize = maxConcurrency + maxOutputBufferSize;
        const pending = Symbol();
        return async function* (input) {
            let readInputJob: Promise<void>|null;
            let mapJobs: Promise<void>[] = [];
            const outputQueue: Array<Symbol|R> = [];
            let inputFinished = false;
            let nextInputIndex = 0;
            let nextOutputIndex = 0;

            const inputIterator = input[Symbol.asyncIterator]();

            async function* generateOutput() {
                await Promise.race(readInputJob ? [readInputJob, ...mapJobs] : mapJobs);
                while (outputQueue.length > 0 && outputQueue[0] !== pending) {
                    const outputEl = outputQueue.shift() as R;
                    nextOutputIndex++;
                    yield outputEl;
                }
            }

            do {
                readInputJob = inputIterator.next().then(({done, value}) => {
                    if (done) {
                        inputFinished = true;
                    } else {
                        let index = nextInputIndex++;
                        const mapJob = mapper(value).then(output => {
                            mapJobs = mapJobs.filter(job => job !== mapJob);
                            const queueIndex = index - nextOutputIndex;
                            outputQueue[queueIndex] = output;
                        });
                        outputQueue.push(pending);
                        mapJobs.push(mapJob);
                    }
                    readInputJob = null;
                });

                do {
                    yield* generateOutput();
                } while (readInputJob || mapJobs.length === maxConcurrency || outputQueue.length === outputQueueMaxSize);
            } while (!inputFinished);

            while (mapJobs.length > 0) {
                yield* generateOutput();
            }
        };
    } else {
        return async function* (input) {
            let readInputJob: Promise<void>|null;
            let mapJobs: Promise<void>[] = [];
            let outputBuffer: R[] = [];
            let inputFinished = false;

            const inputIterator = input[Symbol.asyncIterator]();

            async function* generateOutput() {
                await Promise.race(readInputJob ? [readInputJob, ...mapJobs] : mapJobs);
                const output = outputBuffer;
                outputBuffer = [];
                yield* output;
            }

            do {
                readInputJob = inputIterator.next().then(({done, value}) => {
                    if (done) {
                        inputFinished = true;
                    } else {
                        const mapJob = mapper(value).then(output => {
                            outputBuffer.push(output);
                            mapJobs = mapJobs.filter(job => job !== mapJob);
                        });
                        mapJobs.push(mapJob);
                    }
                    readInputJob = null;
                });

                do {
                    yield* generateOutput();
                } while (readInputJob || mapJobs.length === maxConcurrency);
            } while (!inputFinished);

            while (mapJobs.length > 0) {
                yield* generateOutput();
            }

            yield* outputBuffer;
        };
    }
}

export function flatMap<T, R>(mapper: (input: T) => Iterable<R>): Transform<T, R> {
    return compose(map(mapper), flatten());
}

type AsyncFlatMapper<T, R> = (input: T) => AsyncIterable<R>;
export type FlatMapAsyncOptions = {
    concurrency?: number,       // Default: `1`. Max number of concurrent `mapper()` calls. When `concurrency == 1`, element order is always preserved.
};
export function flatMapAsync<T, R>(mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(options: FlatMapAsyncOptions, mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(mapperOrOptions: AsyncFlatMapper<T, R>|FlatMapAsyncOptions, maybeMapper?: AsyncFlatMapper<T, R>): Transform<T, R> {
    const mapper = maybeMapper || mapperOrOptions as AsyncFlatMapper<T, R>;
    const options = maybeMapper && mapperOrOptions as MapAsyncOptions;
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
    } else {
        return async function* (input) {
            let readInputJob: Promise<void>|null;
            let readOutputJobs: Promise<void>[] = [];
            let outputBuffer: R[] = [];
            let inputFinished = false;

            const inputIterator = input[Symbol.asyncIterator]();

            async function* generateOutput() {
                await Promise.race(readInputJob ? [readInputJob, ...readOutputJobs] : readOutputJobs);
                const output = outputBuffer;
                outputBuffer = [];
                yield* output;
            }

            do {
                readInputJob = inputIterator.next().then(({done, value}) => {
                    if (done) {
                        inputFinished = true;
                    } else {
                        const outputIterator = mapper(value)[Symbol.asyncIterator]();
                        function advance() {
                            const readOutputJob = outputIterator.next().then(({done, value}) => {
                                if (!done) {
                                    outputBuffer.push(value);
                                    advance();
                                }
                                readOutputJobs = readOutputJobs.filter(job => job !== readOutputJob);
                            });
                            readOutputJobs.push(readOutputJob);   
                        }
                        advance();
                    }
                    readInputJob = null;
                });

                do {
                    yield* generateOutput();
                } while (readInputJob || readOutputJobs.length === maxConcurrency);
            } while (!inputFinished);

            while (readOutputJobs.length > 0) {
                yield* generateOutput();
            }

            yield* outputBuffer;
        };
    }
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

export function tap<T>(observer: (element: T) => void): Transform<T, T> {
    return async function* (input) {
        for await (const element of input) {
            observer(element);
            yield element;
        }
    };
}

export function flatten<T>(): Transform<Iterable<T>, T> {
    return async function* (input) {
        for await (const element of input) {
            yield* element;
        }
    };
}

export function filter<T>(predicate: (element: T) => any): Transform<T, T> {
    return async function* (input) {
        for await (const element of input) {
            if (predicate(element)) {
                yield element;
            }
        }
    };
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

export function group<T, K>(getKeyFn: (element: T) => K): Transform<T, T[]> {
    return async function* (input) {
        let currentKey: K|undefined = undefined;
        let buffer: T[] = [];
        
        for await (const element of input) {
            const key = getKeyFn(element);
            if (key === currentKey) {
                buffer.push(element);
            } else {
                currentKey = key;
                if (buffer.length) {
                    yield buffer;
                }
                buffer = [element];
            }
        }

        if (buffer.length) {
            yield buffer;
            currentKey = undefined;
            buffer = [];
        }
    };
}

export function batch<T>(size: number): Transform<T, T[]> {
    return async function* (input) {
        let buffer: T[] = [];
        
        for await (const element of input) {
            buffer.push(element);
            if (buffer.length === size) {
                yield buffer;
                buffer = [];
            }
        }

        if (buffer.length) {
            yield buffer;
            buffer = [];
        }
    };
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

function readLinesFromStdin(): AsyncIterable<string> {
    const rl = readline.createInterface({input: process.stdin});
    return (rl as any)[Symbol.asyncIterator] ? rl as any : iterateReadLine(rl);
}

// node 8 compat
async function* iterateReadLine(rl: readline.ReadLine): AsyncIterable<string> {
    let closed = false;
    rl.once('close', () => closed = true);
    
    do {
        const lineOrClosed = await new Promise<string|void>(resolve => {
            if (closed) {
                resolve();
                return;
            }
            rl.once('line', line => {
                rl.removeListener('close', resolve);
                resolve(line);
            })
            rl.once('close', resolve);
        });
        if (typeof lineOrClosed === 'string') {
            yield lineOrClosed;
        }
    } while (!closed);
}

// node 8 compat. Treats EPIPE as a success.
function writeToStdout(source: AsyncIterable<string>): Promise<void> {
    let error: any = undefined;
    process.stdout.once('error', e => error = e);
    const interruptOnError = <T>(promise: Promise<T>) => new Promise<T>((resolve, reject) => {
        if (error) {
            reject(error);
            return;
        }

        process.stdout.once('error', reject);
        promise.then(
            value => (process.stdout.removeListener('error', reject), resolve(value)),
            reject
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
                    const stdoutReadyForMore = process.stdout.write(value.value, (error?: any) => {
                        if (!error) {
                            numInFlight--;
                            if (numInFlight === 0 && sourceFinished) {
                                resolve();
                            }
                        }
                    });
                    if (!stdoutReadyForMore) {
                        await interruptOnError(new Promise(resolve => process.stdout.once('drain', resolve)));
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
        }
    });
}
