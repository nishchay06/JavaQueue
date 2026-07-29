#!/usr/bin/env bash
#
# Runs the JMH benchmark suite.
#
#   ./benchmarks.sh                          # everything, ~10 min
#   ./benchmarks.sh RoundTripBenchmark       # one class
#   ./benchmarks.sh RoundTrip -f 1 -wi 1 -i 2 -r 1s     # quick check
#
# Any JMH command-line flag can be appended. Results land in
# target/jmh-result.json unless -rff says otherwise.
#
# JMH takes a single -t, so a scaling curve means one run per thread count:
#
#   for t in 1 2 4 8; do
#     ./benchmarks.sh ConcurrencyBenchmark -t "$t" -rff "target/concurrency-t$t.json"
#   done
#
# JMH forks a fresh JVM per trial for isolation, which is why this builds an
# explicit classpath rather than using exec:java -- the forked process needs a
# real java.class.path, and Maven's classloader does not provide one.

set -euo pipefail

cd "$(dirname "$0")"

CLASSPATH_FILE=target/benchmark-classpath.txt

echo "==> Compiling"
mvn -q test-compile

echo "==> Resolving classpath"
mvn -q dependency:build-classpath \
    -Dmdep.outputFile="$CLASSPATH_FILE" \
    -Dmdep.includeScope=test

echo "==> Running benchmarks"
exec java \
    -cp "target/classes:target/test-classes:$(cat "$CLASSPATH_FILE")" \
    com.javaqueue.bench.BenchmarkRunner \
    "$@"
