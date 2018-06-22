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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BeamModel {
    static final Duration ONE_MINUTE = Duration.standardMinutes(1);
    static final Duration TWO_MINUTES = Duration.standardMinutes(2);

    // Converts each Team/Score pair into a string with window and pane information,
    // for verfying output in the unit tests.
    static class FormatAsStrings extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Integer> kv,
                                   @Timestamp Instant timestamp,
                                   BoundedWindow window,
                                   PaneInfo pane,
                                   OutputReceiver<String> output) {
            StringBuilder builder = new StringBuilder(String.format(
                "%s: %s:%-2d %s %-7s index=%d",
                Utils.formatWindow(window), kv.getKey(), kv.getValue(),
                Utils.formatTime(timestamp), pane.getTiming(), pane.getIndex()));
            if (pane.getNonSpeculativeIndex() > -1)
                builder.append(" onTimeIndex=" + pane.getNonSpeculativeIndex());
            if (pane.isFirst())
                builder.append(" isFirst");
            if (pane.isLast())
                builder.append(" isLast");
            output.output(builder.toString());
        }
    }

    public abstract static class ExampleTransform extends PTransform<PCollection<KV<String, Integer>>, PCollection<String>> {
        public abstract String[] getExpectedResults();
    }

    // Example 2-1 / Figure 2-3: Classic batch.
    public static class Example2_1 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] { "[global window]: TeamX:48 END_OF_GLOBAL_WINDOW ON_TIME index=0 onTimeIndex=0 isFirst isLast" };
        }
    }

    // Example 2-2 / Figure 2-5: Windowed batch.
    public static class Example2_2 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES)))
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] { "[11:00:00, 11:02:00): TeamX:14 11:01:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                                  "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                                  "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                                  "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast" };
        }
    }

    // Example 2-3 / Figure 2-6: Per-record streaming.
    public static class Example2_3 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
             .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                          .withAllowedLateness(Duration.standardDays(1000))
                          .accumulatingFiredPanes())
             .apply(Sum.integersPerKey())
             .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 EARLY   index=1",
                "[11:02:00, 11:04:00): TeamX:7  11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:10 11:03:59 EARLY   index=1",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 EARLY   index=2",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:3  11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:11 11:07:59 EARLY   index=1",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 EARLY   index=2"
            };
        }
    }

    // Example 2-4 / Figure 2-7: Aligned delay, i.e. micro-batch.
    public static class Example2_4 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                       .triggering(Repeatedly.forever(AfterProcessingTime
                                                      .pastFirstElementInPane()
                                                      .alignedTo(TWO_MINUTES, Utils.parseTime("12:05:00"))))
                       .withAllowedLateness(Duration.standardDays(1000))
                       .accumulatingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        // These panes are kind of funky relative to what's presented in the book, and I'm
        // not 100% sure why yet (it would help if Beam gave access to the processing time
        // at which a given pane was materialized). For now, I wouldn't pay too much attention
        // to this one. :-)
        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=1 onTimeIndex=0 isLast"
            };
        }
    }

    // Example 2-5 / Figure 2-8: Unaligned delay.
    public static class Example2_5 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                       .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TWO_MINUTES)))
                       .withAllowedLateness(Duration.standardDays(1000))
                       .accumulatingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=1 onTimeIndex=0 isLast",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=1 onTimeIndex=0 isLast"
            };
        }
    }

    public static abstract class Example2_6 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES)))
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }
    }

    // Example 2-6 / Figure 2-10 (left side): Watermark trigger with perfect watermark.
    public static class Example2_6left extends Example2_6 {
        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast"
            };
        }
    }

    // Example 2-6 / Figure 2-10 (right side): Watermark trigger with heuristic watermark.
    public static class Example2_6right extends Example2_6 {
        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=0 onTimeIndex=0 isFirst isLast"
            };
        }
    }

    public static abstract class Example2_7 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                       .triggering(AfterWatermark.pastEndOfWindow()
                                   .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(ONE_MINUTE))
                                   .withLateFirings(AfterPane.elementCountAtLeast(1)))
                       .withAllowedLateness(Duration.standardDays(1000))
                       .accumulatingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }
    }

    // Example 2-7 / Figure 2-11 (left side): Early/on-time/late trigger with perfect watermark.
    public static class Example2_7left extends Example2_7 {
        @Override
        public String[] getExpectedResults() {
            // Note the lack of "isLast" markers, which is an indication that the perfect watermark is what
            // is triggering the ON_TIME panes, and not the final advancement of the watermark to infinity
            // (which would also mark the pane as last).
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 ON_TIME index=1 onTimeIndex=0",
                "[11:02:00, 11:04:00): TeamX:10 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 EARLY   index=1",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=2 onTimeIndex=0",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=1 onTimeIndex=0",
                "[11:06:00, 11:08:00): TeamX:3  11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=1 onTimeIndex=0"
            };
        }
    }

    // Example 2-7 / Figure 2-11 (right side): Early/on-time/late trigger with perfect watermark.
    public static class Example2_7right extends Example2_7 {
        @Override
        public String[] getExpectedResults() {
            // Note the lack of "isLast" markers, which is an indication that the perfect watermark is what
            // is triggering the ON_TIME panes, and not the final advancement of the watermark to infinity
            // (which would also mark the pane as last).
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 ON_TIME index=1 onTimeIndex=0",
                "[11:00:00, 11:02:00): TeamX:14 11:01:59 LATE    index=2 onTimeIndex=1",
                "[11:02:00, 11:04:00): TeamX:10 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=1 onTimeIndex=0",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=1 onTimeIndex=0",
                "[11:06:00, 11:08:00): TeamX:3  11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=1 onTimeIndex=0"
            };
        }
    }

    // Example 2-8 / Figure 2-12: Allowed lateness.
    public static class Example2_8 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                       .triggering(AfterWatermark.pastEndOfWindow()
                                   .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(ONE_MINUTE))
                                   .withLateFirings(AfterPane.elementCountAtLeast(1)))
                       .withAllowedLateness(TWO_MINUTES)
                       .accumulatingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 ON_TIME index=1 onTimeIndex=0",
                "[11:00:00, 11:02:00): TeamX:11 11:01:59 LATE    index=2 onTimeIndex=1",
                "[11:02:00, 11:04:00): TeamX:10 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:18 11:03:59 ON_TIME index=1 onTimeIndex=0",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 ON_TIME index=1 onTimeIndex=0",
                "[11:06:00, 11:08:00): TeamX:3  11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:12 11:07:59 ON_TIME index=1 onTimeIndex=0"
            };
        }
    }

    // Example 2-9 / Figure 2-13: Discarding mode version of early/on-time/late trigger with heuristic watermark.
    public static class Example2_9 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTES))
                       .triggering(AfterWatermark.pastEndOfWindow()
                                   .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(ONE_MINUTE))
                                   .withLateFirings(AfterPane.elementCountAtLeast(1)))
                       .withAllowedLateness(Duration.standardDays(1000))
                       .discardingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:00, 11:02:00): TeamX:5  11:01:59 EARLY   index=0 isFirst",
                "[11:00:00, 11:02:00): TeamX:0  11:01:59 ON_TIME index=1 onTimeIndex=0",
                "[11:00:00, 11:02:00): TeamX:9  11:01:59 LATE    index=2 onTimeIndex=1",
                "[11:02:00, 11:04:00): TeamX:10 11:03:59 EARLY   index=0 isFirst",
                "[11:02:00, 11:04:00): TeamX:8  11:03:59 ON_TIME index=1 onTimeIndex=0",
                "[11:04:00, 11:06:00): TeamX:4  11:05:59 EARLY   index=0 isFirst",
                "[11:04:00, 11:06:00): TeamX:0  11:05:59 ON_TIME index=1 onTimeIndex=0",
                "[11:06:00, 11:08:00): TeamX:3  11:07:59 EARLY   index=0 isFirst",
                "[11:06:00, 11:08:00): TeamX:9  11:07:59 ON_TIME index=1 onTimeIndex=0"
            };
        }
    }

    // Example 4-3 / Figure 4-7: Session windows.
    public static class Example4_3 extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.<KV<String, Integer>>into(Sessions.withGapDuration(ONE_MINUTE))
                       .triggering(AfterWatermark.pastEndOfWindow()
                                   .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(ONE_MINUTE))
                                   .withLateFirings(AfterPane.elementCountAtLeast(1)))
                       .withAllowedLateness(Duration.standardDays(1000))
                       .accumulatingFiredPanes())
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:26, 11:01:26): TeamX:5  11:01:25 EARLY   index=0 isFirst",
                "[11:00:26, 11:01:26): TeamX:5  11:01:25 ON_TIME index=1 onTimeIndex=0",
                "[11:02:24, 11:03:24): TeamX:7  11:03:23 EARLY   index=0 isFirst",
                "[11:02:24, 11:05:19): TeamX:22 11:05:18 EARLY   index=0 isFirst",
                "[11:02:24, 11:05:19): TeamX:22 11:05:18 ON_TIME index=1 onTimeIndex=0",
                "[11:00:26, 11:05:19): TeamX:36 11:05:18 LATE    index=0 onTimeIndex=0 isFirst",
                "[11:06:39, 11:07:39): TeamX:3  11:07:38 EARLY   index=0 isFirst",
                "[11:06:39, 11:08:46): TeamX:12 11:08:45 EARLY   index=0 isFirst",
		"[11:06:39, 11:08:46): TeamX:12 11:08:45 ON_TIME index=1 onTimeIndex=0 isLast"
            };
        }
    }

    // Verifies that the ValidityWindows implementation works. This specific example isn't
    // in the book, it's just sending the same input set as all of the above tests through
    // the ValidityWindows transform and verifying that each element ends up in a window
    // that extends from its original event time until the event time of the next successive
    // element.
    public static class ValidityWindowsTest extends ExampleTransform {
        @Override
        public PCollection<String> expand(PCollection<KV<String, Integer>> input) {
            return input
                .apply(Window.into(new ValidityWindows()))
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new FormatAsStrings()));
        }

        @Override
        public String[] getExpectedResults() {
            return new String[] {
                "[11:00:26, 11:01:25): TeamX:5  11:01:24 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:01:25, 11:02:24): TeamX:9  11:02:23 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:02:24, 11:03:06): TeamX:7  11:03:05 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:03:06, 11:03:39): TeamX:8  11:03:38 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:03:39, 11:04:19): TeamX:3  11:04:18 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:04:19, 11:06:39): TeamX:4  11:06:38 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:06:39, 11:07:26): TeamX:3  11:07:25 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:07:26, 11:07:46): TeamX:8  11:07:45 ON_TIME index=0 onTimeIndex=0 isFirst isLast",
                "[11:07:46, END_OF_GLOBAL_WINDOW): TeamX:1  04:00:54 ON_TIME index=0 onTimeIndex=0 isFirst isLast"
            };
        }
    }
}
