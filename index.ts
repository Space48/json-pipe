import * as stream from "stream";
import * as readline from "readline";

export type Transform<I = any, O = any> = (input: AsyncIterable<I>) => AsyncIterable<O>;

// reads JSON lines from stdin, transforms them via `transform`, and writes the output to stdout as JSON lines
export function jsonTransform(transform: Transform): Promise<void> {
    const f = compose(map(JSON.parse), transform);
    const output = f((readline.createInterface({input: process.stdin})));
    return jsonSource(output);
}

export type Source<T = any> = AsyncIterable<T> | (() => AsyncIterable<T>);

// iterates `source` and writes the output to stdout as JSON lines
export function jsonSource<T>(source: Source<T>): Promise<void> {
    const iterable = typeof source === 'function' ? source() : source;
    const outputLines = map(element => `${JSON.stringify(element)}\n`)(iterable);
    return writeToStdout(stream.Readable.from(outputLines));
}

export function compose<T>(): Transform<T, T>;
export function compose<T1, T2>(t1: Transform<T1, T2>): Transform<T1, T2>;
export function compose<T1, T2, T3>(t1: Transform<T1, T2>, t2: Transform<T2, T3>): Transform<T1, T3>;
export function compose<T1, T2, T3, T4>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>): Transform<T1, T4>;
export function compose<T1, T2, T3, T4, T5>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>): Transform<T1, T5>;
export function compose<T1, T2, T3, T4, T5, T6>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>): Transform<T1, T6>;
export function compose<T1, T2, T3, T4, T5, T6, T7>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>, t6: Transform<T6, T7>): Transform<T1, T7>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>, t6: Transform<T6, T7>, t7: Transform<T7, T8>): Transform<T1, T8>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>, t6: Transform<T6, T7>, t7: Transform<T7, T8>, t8: Transform<T8, T9>): Transform<T1, T9>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>, t6: Transform<T6, T7>, t7: Transform<T7, T8>, t8: Transform<T8, T9>, t9: Transform<T9, T10>): Transform<T1, T10>;
export function compose<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(t1: Transform<T1, T2>, t2: Transform<T2, T3>, t3: Transform<T3, T4>, t4: Transform<T4, T5>, t5: Transform<T5, T6>, t6: Transform<T6, T7>, t7: Transform<T7, T8>, t8: Transform<T8, T9>, t9: Transform<T9, T10>, t10: Transform<T10, T11>): Transform<T1, T11>;
export function compose(...transforms: any[]): Transform;
export function compose(...transforms: any[]): Transform {
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
        };
    }
}

export function flatMap<T, R>(mapper: (input: T) => Iterable<R>): Transform<T, R> {
    return compose(map(mapper), flatten());
}

type AsyncFlatMapper<T, R> = (input: T) => AsyncIterable<R>;
export function flatMapAsync<T, R>(mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(concurrency: number, mapper: AsyncFlatMapper<T, R>): Transform<T, R>;
export function flatMapAsync<T, R>(mapperOrConcurrency: AsyncFlatMapper<T, R>|number, maybeMapper?: AsyncFlatMapper<T, R>): Transform<T, R> {
    const mapper = maybeMapper || mapperOrConcurrency as AsyncFlatMapper<T, R>;
    const maxConcurrency = maybeMapper && mapperOrConcurrency as number || 1;
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
        };
    }
}

export function reduce<T, U>(initialValue: U, callbackfn: (previousValue: U, currentValue: T) => U): Transform<T, U> {
    return async function* (input) {
        let currentValue = initialValue;
        for await (const element of input) {
            currentValue = callbackfn(currentValue, element);
        }
        yield currentValue;
    };
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

// pipe source to stdout, only resolving once everything is flushed to stdout. Treats EPIPE as a success.
function writeToStdout(source: stream.Readable): Promise<void> {
    return new Promise((resolve, reject) => {
        source.once('error', reject);

        process.stdout.once('error', err => {
            if (err?.code === 'EPIPE') {
                source.destroy();
                resolve();
            } else {
                source.destroy(err);
            }
        });

        let numInFlight = 0;
        let sourceEnded = false;
        source.once('end', () => {
            sourceEnded = true;
            numInFlight === 0 && resolve();
        });
        const write = function (el: any, callback?: any) {
            numInFlight++;
            return process.stdout.write(el, e => {
                if (e) {
                    callback && callback(e);
                } else {
                    numInFlight--;
                    callback && callback();
                    sourceEnded && numInFlight === 0 && resolve();
                }
            });
        };

        source.pipe(new Proxy(process.stdout, {
            get(stdout, prop) {
                return prop === 'write' ? write : stdout[prop as keyof typeof stdout];
            }
        }));
    });
}
