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

import static net.streamingbook.Utils.parseTime;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.Callable;

@RunWith(JUnit4.class)
public class BeamModelTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    private static final Instant BASE_TIME = parseTime("12:00:00");
    private Instant lastProcTime = BASE_TIME;

    private Duration to(String time) {
        Instant newProcTime = parseTime(time);
        Duration result = new Duration(lastProcTime, newProcTime);
        lastProcTime = newProcTime;
        return result;
    }

    private static TimestampedValue<KV<String, Integer>> score(String team, Integer score, String eventTime) {
        return TimestampedValue.of(KV.of(team, score), parseTime(eventTime));
    }
    
    // Creates test inputs as a bounded set of values with batch processing semantics.
    Create.TimestampedValues<KV<String, Integer>> createBatch() {
        return Create.timestamped(
            score("TeamX", 5, "12:00:26"),
            score("TeamX", 7, "12:02:26"),
            score("TeamX", 3, "12:03:39"),
            score("TeamX", 4, "12:04:19"),
            score("TeamX", 8, "12:03:06"),
            score("TeamX", 3, "12:06:39"),
            score("TeamX", 9, "12:01:26"),
            score("TeamX", 8, "12:07:26"),
            score("TeamX", 1, "12:07:46"));
    }

    enum WatermarkType {
        BATCH,
        PERFECT,
        HEURISTIC;
    }

    // Creates test inputs as a bounded set of values with stream processing semantics.
    TestStream<KV<String, Integer>> createStream(WatermarkType watermark) {
        return createStream(watermark, false/*extraSix*/);
    }

    // The extraSix parameter is for inserting the extra value of 6 needed for the
    // allowed lateness example.
    TestStream<KV<String, Integer>> createStream(WatermarkType watermark, boolean extraSix) {
        TestStream.Builder<KV<String, Integer>> stream = TestStream.create(
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .advanceWatermarkTo(BASE_TIME)  // Test assumes processing time begins there as well.
            .advanceProcessingTime(to("12:04:59"))
            .addElements(score("TeamX", 5, "12:00:26"))
            .advanceProcessingTime(to("12:05:39"))
            .addElements(score("TeamX", 7, "12:02:24")) // Fudge the event time to 2:24 to make sessions connect
            .advanceProcessingTime(to("12:06:00"));
        if (watermark == WatermarkType.HEURISTIC)
            stream = stream.advanceWatermarkTo(parseTime("12:02:00"));
        stream = stream
            .advanceProcessingTime(to("12:06:13"))
            .addElements(score("TeamX", 3, "12:03:39"))
            .advanceProcessingTime(to("12:06:39"))
            .addElements(score("TeamX", 4, "12:04:19"));
        if (extraSix)
            stream = stream.addElements(score("TeamX", 6, "12:00:53"));
        stream = stream
            .advanceProcessingTime(to("12:07:06"))
            .addElements(score("TeamX", 8, "12:03:06"))
            .advanceProcessingTime(to("12:07:19"))
            .addElements(score("TeamX", 3, "12:06:39"))
            .advanceProcessingTime(to("12:07:30"));
        if (watermark == WatermarkType.HEURISTIC)
            stream = stream
                .advanceWatermarkTo(parseTime("12:04:00"))
                .advanceProcessingTime(to("12:07:40"))
                .advanceWatermarkTo(parseTime("12:06:00"));
        stream = stream
            .advanceProcessingTime(to("12:08:19"))
            .addElements(score("TeamX", 9, "12:01:25")) // Fudge the event time to 1:25 to make sessions connect
            .advanceProcessingTime(to("12:08:39"));
        if (watermark == WatermarkType.PERFECT)
            stream = stream.advanceWatermarkTo(parseTime("12:02:00"));
        stream = stream
            .addElements(score("TeamX", 8, "12:07:26"))
            .advanceProcessingTime(to("12:09:00"))
            .addElements(score("TeamX", 1, "12:07:46"));
        if (watermark == WatermarkType.PERFECT) {
            stream = stream
                .advanceWatermarkTo(parseTime("12:04:00"))
                .advanceProcessingTime(to("12:09:10"))
                .advanceWatermarkTo(parseTime("12:06:00"))
                .advanceProcessingTime(to("12:09:20"))
                .advanceWatermarkTo(parseTime("12:08:00"));
        }
        stream = stream.advanceProcessingTime(to("12:09:30"));
        if (watermark == WatermarkType.HEURISTIC)
          stream = stream.advanceWatermarkTo(parseTime("12:08:30"));
        stream = stream.advanceProcessingTime(to("12:10:00"));
        return stream.advanceWatermarkToInfinity();
    }

    private void runTest(PTransform<PBegin, PCollection<KV<String, Integer>>> createInput, BeamModel.ExampleTransform example) {
        PCollection<String> teamScores = p.apply(createInput)
	    .apply(example);

        PAssert.that(teamScores)
            .containsInAnyOrder(example.getExpectedResults());

        p.run().waitUntilFinish();
    }

    @Test
    public void example2_1_figure2_3_unwindowedBatch() {
	runTest(createBatch(), new BeamModel.Example2_1());
    }

    @Test
    public void example2_2_figure2_5_windowedBatch() {
	runTest(createBatch(),
		new BeamModel.Example2_2());
    }

    @Test
    public void example2_3_figure2_6_perRecordStreaming() {
	runTest(createStream(WatermarkType.BATCH),
		new BeamModel.Example2_3());
    }

    @Test
    public void example2_4_figure2_7_alignedDelayMicrobatch() {
	runTest(createStream(WatermarkType.BATCH),
		new BeamModel.Example2_4());
    }

    @Test
    public void example2_5_figure2_8_unalignedDelay() {
	runTest(createStream(WatermarkType.BATCH),
		new BeamModel.Example2_5());
    }

    @Test
    public void example2_6_figure2_10left_perfectWatermark() {
	runTest(createStream(WatermarkType.PERFECT),
		new BeamModel.Example2_6left());
    }

    @Test
    public void example2_6_figure2_10right_heuristictWatermark() {
	runTest(createStream(WatermarkType.HEURISTIC),
		new BeamModel.Example2_6right());
    }

    @Test
    public void example2_7_figure2_11left_earlyOnTimeLate_perfectWatermark() {
	runTest(createStream(WatermarkType.PERFECT),
		new BeamModel.Example2_7left());
    }

    @Test
    public void example2_7_figure2_11right_earlyOnTimeLate_heuristictWatermark() {
	runTest(createStream(WatermarkType.HEURISTIC),
		new BeamModel.Example2_7right());
    }

    @Test
    public void example2_8_figure2_11_allowedLateness() {
	runTest(createStream(WatermarkType.HEURISTIC, true/*extraSix*/),
		new BeamModel.Example2_8());
    }

    @Test
    public void example2_9_figure2_13_earlyOnTimeLate_heuristictWatermark_discarding() {
	runTest(createStream(WatermarkType.HEURISTIC),
		new BeamModel.Example2_9());
    }

    @Test
    public void example4_3_figure4_7_sessions() {
	runTest(createStream(WatermarkType.HEURISTIC),
		new BeamModel.Example4_3());
    }

    @Test
    public void validityWindowsTest() {
	runTest(createStream(WatermarkType.PERFECT),
		new BeamModel.ValidityWindowsTest());
    }
}
