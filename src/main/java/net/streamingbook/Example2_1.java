/*
 * Copyright 2018 Tyler Akidau
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.streamingbook;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Standalone executable version of Example 2-1. Can be run locally over small test data from the root repo dir via:
 *
 *   mvn compile exec:java -Dexec.mainClass=net.streamingbook.Example2_1 -Dexec.args="--inputFile=src/main/java/net/streamingbook/inputs.txt --output=output" -Pdirect-runner
 *   cat output-*
 *
 * which will create and then dump one or more files in the root repo named "output-NNNNN-of-MMMMMM".
 */
public class Example2_1 {
    static class ParseFn extends DoFn<String, KV<String, Integer>> {
	@ProcessElement
	public void processElement(@Element String input, OutputReceiver<KV<String, Integer>> output) {
	    String[] parts = input.split(",");
	    String team = parts[1].trim();
	    Integer score = Integer.parseInt(parts[2].trim());
	    Instant eventTime = Instant.parse(parts[3].trim());
	    output.outputWithTimestamp(KV.of(team, score), eventTime);
	}
    }

    public interface Options extends PipelineOptions {
	@Description("Path of the file to read from")
	@Default.String("src/main/java/net/streamingbook/inputs.txt")
	String getInputFile();
	void setInputFile(String value);

	@Description("Path of the file to write to")
	@Required
	String getOutput();
	void setOutput(String value);
    }

    public static void main(String[] args) {
	Options options = PipelineOptionsFactory
	    .fromArgs(args)
	    .withValidation()
	    .as(Options.class);
	Pipeline pipeline = Pipeline.create(options);

	pipeline
	    .apply("Read", TextIO.read().from(options.getInputFile()))
	    .apply("Parse", ParDo.of(new ParseFn()))
	    .apply("Example 2-1", new BeamModel.Example2_1())
	    .apply("Write", TextIO.write().to(options.getOutput()));

	pipeline.run().waitUntilFinish();
    }
}
