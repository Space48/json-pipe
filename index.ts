import * as stream from "stream";
import * as readline from "readline";
import multipipe from "multipipe";

// reads JSON lines from stdin, transforms them via `transform`, and writes the output to stdout as JSON lines
export function jsonTransform(transform: Transform): Promise<void> {
    return writeToStdout(multipipe(
        // todo: stream.Readable.from was added in Node 10 -- consider removing for Node 8 compat
        stream.Readable.from(readline.createInterface({input: process.stdin})),
        map(JSON.parse),
        transform,
        map(element => `${JSON.stringify(element)}\n`),
    ));
}

// iterates `source` and writes the output to stdout as JSON lines
export function jsonSource(source: Iterable<any>|AsyncIterable<any>): Promise<void> {
    return writeToStdout(multipipe(
        // todo: stream.Readable.from was added in Node 10 -- consider removing for Node 8 compat
        stream.Readable.from(source),
        map(element => `${JSON.stringify(element)}\n`),
    ));
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
export function compose(t1: Transform, t2: Transform, ...rest: Transform[]): Transform;
export function compose(...transforms: any[]): Transform {
    switch (transforms.length) {
        case 0:
            return new stream.PassThrough();
        case 1:
            return transforms[0];
        default:
            return multipipe(...transforms);
    }
}

export function map<T, R>(mapper: (input: T) => R): Transform<T, R> {
    return transform(function (input, callback) {
        callback(null, mapper(input));
    });
}

type AsyncMapper<T, R> = (input: T) => Promise<R>;
export function mapAsync<T, R>(mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(concurrency: number, mapper: AsyncMapper<T, R>): Transform<T, R>;
export function mapAsync<T, R>(mapperOrConcurrency: AsyncMapper<T, R>|number, maybeMapper?: AsyncMapper<T, R>): Transform<T, R> {
    const mapper = maybeMapper || mapperOrConcurrency as AsyncMapper<T, R>;
    const concurrency = maybeMapper && mapperOrConcurrency as number || 1;
    if (concurrency === 1) {
        return transform(function (input, callback) {
            try {
                mapper(input).then(
                    output => callback(null, output),
                    callback,
                );
            } catch (e) {
                callback(null, e);
            }
        });
    } else {
        let numInFlight = 0;
        let fail: (error: Error) => void;
        let push: (value: R) => void;
        let currentCallback: ((error: null, value: R) => void) | null = null;
        let flushCallback: ((error: null, value: R) => void) | null = null;
        return transform({
            init() {
                fail = error => this.emit('error', error);
                
                push = value => {
                    numInFlight--;
                    if (currentCallback) {
                        const cb = currentCallback;
                        currentCallback = flushCallback;
                        cb(null, value);
                    } else {
                        this.push(value);
                    }
                };
            },

            transform(input, callback) {
                numInFlight++;
                const pause = numInFlight === concurrency;
                if (pause) {
                    currentCallback = callback;
                }
                mapper(input).then(push, fail);
                if (!pause) {
                    callback();
                }
            },
    
            flush(callback) {
                if (numInFlight === 0) {
                    callback();
                } else {
                    currentCallback = flushCallback = (_, value) => {
                        if (numInFlight === 0) {
                            callback(null, value);
                        } else {
                            this.push(value);
                        }
                    };
                }
            },
        });
    }
}

export function reduce<T, U>(initialValue: U, callbackfn: (previousValue: U, currentValue: T) => U): Transform<T, U> {
    let currentValue = initialValue;

    return transform({
        transform(element, callback) {
            currentValue = callbackfn(currentValue, element);
            callback();
        },

        flush(callback) {
            callback(null, currentValue);
        },
    })
}

export function tap<T>(observer: (element: T) => void): Transform<T, T> {
    return transform(function (element, callback) {
        observer(element);
        callback(null, element);
    });
}

export function flatMap<T, R>(mapper: (input: T) => Iterable<R>): Transform<T, R> {
    return compose(map(mapper), flatten());
}

export function flatten<T>(): Transform<Iterable<T>, T> {
    return transform(function (elements, callback) {
        for (const element of elements) {
            this.push(element);
        }
        callback();
    });
}

export function filter<T>(predicate: (element: T) => any): Transform<T, T> {
    return transform(function (element, callback) {
        predicate(element) ? callback(null, element) : callback();
    });
}

export function distinct<T>(): Transform<T, T>;
export function distinct<T>(compare: (element1: T, element2: T) => boolean): Transform<T, T>;
export function distinct<T>(compare: ((element1: T, element2: T) => boolean) = Object.is): Transform<T, T> {
    let previousElement: T;
    let firstEl = true;

    return transform(function (element, callback) {
        if (firstEl) {
            firstEl = false;
            previousElement = element;
            callback(null, element);
        } else if (compare(element, previousElement)) {
            callback();
        } else {
            previousElement = element;
            callback(null, element);
        }
    });
}

export function group<T, K>(getKeyFn: (element: T) => K): Transform<T, T[]> {
    let currentKey: K|undefined = undefined;
    let buffer: T[] = [];

    return transform({
        transform(element, callback) {
            const key = getKeyFn(element);
            if (key === currentKey) {
                buffer.push(element);
                callback();
            } else {
                const output = buffer.length && buffer;
                currentKey = key;
                buffer = [element];
                output ? callback(null, output) : callback();
            }
        },

        flush(callback) {
            if (buffer.length) {
                const output = buffer;
                currentKey = undefined;
                buffer = [];
                callback(null, output);
            } else {
                callback();
            }
        },
    });
}

export function batch<T>(size: number): Transform<T, T[]> {
    let buffer: T[] = [];

    return transform({
        transform(element, callback) {
            buffer.push(element);
            if (buffer.length === size) {
                const batch = buffer;
                buffer = [];
                callback(null, batch);
            } else {
                callback();
            }
        },

        flush(callback) {
            if (buffer.length) {
                const batch = buffer;
                buffer = [];
                callback(null, batch);
            } else {
                callback();
            }
        },
    });
}

export function take<T>(n: number): Transform<T, T> {
    if (n < 0) {
        throw new Error('take(n) was called with a negative number. n must be non-negative.');
    }

    if (n === 0) {
        return takeWhile(() => false);
    }

    const sources: stream.Readable[] = [];
    let numConsumed = 0;

    return transform({
        init() {
            this.once('pipe', source => sources.push(source));
        },

        transform(element, callback) {
            if (++numConsumed === n) {
                sources.forEach(source => source.unpipe(this));
                this.end(element);
                callback();
            } else {
                callback(null, element);
            }
        },
    });
}

export function takeWhile<T>(predicate: (element: T) => any): Transform<T, T> {
    const sources: stream.Readable[] = [];

    return transform({
        init() {
            this.once('pipe', source => sources.push(source));
        },

        transform(element, callback) {
            if (predicate(element)) {
                callback(null, element);
            } else {
                sources.forEach(source => source.unpipe(this));
                this.end();
                callback();
            }
        },
    });
}

type TransformFn<I, O> = (this: Transform<I, O>, element: I, callback: TransformCallback<O>) => void;
type TransformCallback<T> = (error?: Error | null, element?: T) => void;
interface TransformOptions<I, O> {
    init?(this: Transform<I, O>): void;
    transform: TransformFn<I, O>;
    final?(this: Transform<I, O>, callback: (error?: Error | null) => void): void;
    flush?(this: Transform<I, O>, callback: TransformCallback<O>): void;
}
export interface Transform<I = any, O = any> extends stream.Duplex {
    push(element: O): boolean;
}
export function transform<I, O>(options: TransformOptions<I, O>): Transform<I, O>;
export function transform<I, O>(fn: TransformFn<I, O>): Transform<I, O>;
export function transform<I, O>(fnOrOptions: TransformOptions<I, O>|TransformFn<I, O>): Transform<I, O> {
    const options = typeof fnOrOptions === 'function' ? {transform: fnOrOptions} : fnOrOptions;
    const {init, ...rest} = options;
    const t = new stream.Transform({
        ...rest,

        objectMode: true,

        transform(element, _, callback) {
            options.transform.call(this, element, callback);
        },
    });
    if (init) {
        init.call(t);
    }
    return t;
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
