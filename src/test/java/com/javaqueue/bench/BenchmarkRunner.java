package com.javaqueue.bench;

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Entry point for the benchmark suite.
 *
 * <pre>
 *   mvn -q test-compile exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=com.javaqueue.bench.BenchmarkRunner
 * </pre>
 *
 * Accepts the full JMH command line via {@code -Dexec.args}, so filters,
 * thread counts and iteration counts work as documented upstream. With no
 * arguments it runs every benchmark and writes JSON to
 * {@code target/jmh-result.json}.
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        CommandLineOptions commandLine = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder().parent(commandLine);

        if (commandLine.getIncludes().isEmpty()) {
            options.include("com\\.javaqueue\\.bench\\..*");
        }

        if (!commandLine.getResult().hasValue()) {
            options.resultFormat(ResultFormatType.JSON).result("target/jmh-result.json");
        }

        new Runner(options.build()).run();
    }
}
