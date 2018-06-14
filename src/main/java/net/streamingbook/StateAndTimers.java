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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateAndTimers {
    private static final Logger LOG = LoggerFactory.getLogger(StateAndTimers.class);

    @DefaultCoder(AvroCoder.class)
    static class Visit {
        @Nullable private String url;
        @Nullable private Instant timestamp;
        // The referring URL. Recall that we’ve constrained the problem in this
        // example to assume every page on our website has exactly one possible
        // referring URL, to allow us to solve the problem for simple trees
        // rather than more general DAGs.
        @Nullable private String referer;
        @Nullable private boolean isGoal;

        @SuppressWarnings("unused")
        public Visit() {
        }

        public Visit(String url, Instant timestamp, String referer, boolean isGoal) {
            this.url = url;
            this.timestamp = timestamp;
            this.referer = referer;
            this.isGoal = isGoal;
        }

        public String url() { return url; }
        public Instant timestamp() { return timestamp; }
        public String referer() { return referer; }
        public boolean isGoal() { return isGoal; }

        @Override
        public String toString() {
            return String.format("{ %s %s from:%s%s }", url, timestamp, referer, isGoal ? " isGoal" : "");
        }
    }

    @DefaultCoder(AvroCoder.class)
    static class Impression {
        @Nullable private Long id;
        @Nullable private String sourceUrl;
        @Nullable private String targetUrl;
        @Nullable private Instant timestamp;

        public static String sourceAndTarget(String source, String target) { return source + ":" + target; }

        @SuppressWarnings("unused")
        public Impression() {
        }

        public Impression(Long id, String sourceUrl, String targetUrl, Instant timestamp) {
            this.id = id;
            this.sourceUrl = sourceUrl;
            this.targetUrl = targetUrl;
            this.timestamp = timestamp;
        }

        public Long id() { return id; }
        public String sourceUrl() { return sourceUrl; }
        public String targetUrl() { return targetUrl; }
        public String sourceAndTarget() { return sourceAndTarget(sourceUrl, targetUrl); }
        public Instant timestamp() { return timestamp; }

        @Override
        public String toString() {
            return String.format("{ %s source:%s target:%s %s }", id, sourceUrl, targetUrl, timestamp);
        }
    }

    @DefaultCoder(AvroCoder.class)
    static class VisitOrImpression {
        @Nullable private Visit visit;
        @Nullable private Impression impression;

        @SuppressWarnings("unused")
        public VisitOrImpression() {
        }

        public VisitOrImpression(Visit visit, Impression impression) {
            this.visit = visit;
            this.impression = impression;
        }

        public Visit visit() { return visit; }
        public Impression impression() { return impression; }
    }

    @DefaultCoder(AvroCoder.class)
    static class Attribution {
        @Nullable private Impression impression;
        @Nullable private List<Visit> trail;
        @Nullable private Visit goal;

        @SuppressWarnings("unused")
        public Attribution() {
        }

        public Attribution(Impression impression, List<Visit> trail, Visit goal) {
            this.impression = impression;
            this.trail = trail;
            this.goal = goal;
        }

        public Impression impression() { return impression; }
        public List<Visit> trail() { return trail; }
        public Visit goal() { return goal; }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("imp=" + impression.id() + " " + impression.sourceUrl());
            for (Visit visit : trail) {
                builder.append(" → " + visit.url());
            }
            builder.append(" → " + goal.url());
            return builder.toString();
        }
    }

    static class FormatAttributionAsString extends DoFn<Attribution, String> {
        @ProcessElement
        public void processElement(@Element Attribution attribution,
				   @Timestamp Instant timestamp,
				   BoundedWindow window,
				   PaneInfo pane,
				   OutputReceiver<String> output) {
            StringBuilder builder = new StringBuilder(String.format(
                "%s: %s %s %-7s", Utils.formatWindow(window), attribution,
                Utils.formatTime(timestamp), pane.getTiming()));
            if (pane.getTiming() != PaneInfo.Timing.UNKNOWN) {
                builder.append(String.format(" index=%d", pane.getIndex()));
                if (pane.getNonSpeculativeIndex() > -1)
                    builder.append(" onTimeIndex=" + pane.getNonSpeculativeIndex());
                if (pane.isFirst())
                    builder.append(" isFirst");
                if (pane.isLast())
                    builder.append(" isLast");
            }
            output.output(builder.toString());
        }
    }

    static class AttributionFn extends DoFn<KV<String, VisitOrImpression>, Attribution> {
        @StateId("visits")
        private final StateSpec<MapState<String, Visit>> visitsSpec =
            StateSpecs.map(StringUtf8Coder.of(), AvroCoder.of(Visit.class));

        // Impressions are keyed by both sourceUrl (i.e., the query) and targetUrl (i.e., the click),
        // since a single query can result in multiple impressions. The source and target are encoded
        // together into a single string by the Impression.sourceAndTarget method.
        @StateId("impressions")
        private final StateSpec<MapState<String, Impression>> impSpec =
            StateSpecs.map(StringUtf8Coder.of(), AvroCoder.of(Impression.class));

        @StateId("goals")
        private final StateSpec<SetState<Visit>> goalsSpec =
            StateSpecs.set(AvroCoder.of(Visit.class));

        @StateId("minGoal")
        private final StateSpec<ValueState<Instant>> minGoalSpec =
            StateSpecs.value(InstantCoder.of());

        @TimerId("attribution")
        private final TimerSpec timerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement
        public void processElement(@Element KV<String, VisitOrImpression> kv,
                                   @StateId("visits") MapState<String, Visit> visitsState,
                                   @StateId("impressions") MapState<String, Impression> impressionsState,
                                   @StateId("goals") SetState<Visit> goalsState,
                                   @StateId("minGoal") ValueState<Instant> minGoalState,
                                   @TimerId("attribution") Timer attributionTimer) {
            Visit visit = kv.getValue().visit();
            Impression impression = kv.getValue().impression();

            if (visit != null) {
                if (!visit.isGoal()) {
                    LOG.info("Adding visit: {}", visit);
                    visitsState.put(visit.url(), visit);
                } else {
                    LOG.info("Adding goal (if absent): {}", visit);
                    goalsState.addIfAbsent(visit);
                    Instant minTimestamp = minGoalState.read();
                    if (minTimestamp == null || visit.timestamp().isBefore(minTimestamp)) {
                        LOG.info("Setting timer from {} to {}", Utils.formatTime(minTimestamp), Utils.formatTime(visit.timestamp()));
                        attributionTimer.set(visit.timestamp());
                        minGoalState.write(visit.timestamp());
                    }
                    LOG.info("Done with goal");
                }
            }
            if (impression != null) {
                // Dedup logical impression duplicates with the same source and target URL. In
                // this case, first one to arrive (in processing time) wins. A more robust approach
                // might be to pick the first one in event time, but that would require an extra read
                // before commit, so the processing-time approach may be slightly more performant.
                LOG.info("Adding impression (if absent): {} → {}", impression.sourceAndTarget(), impression);
                impressionsState.putIfAbsent(impression.sourceAndTarget(), impression);
            }
        }

        private Impression attributeGoal(Visit goal,
                                         Map<String, Visit> visits,
                                         Map<String, Impression> impressions,
                                         List<Visit> trail) {
            Impression impression = null;
            Visit visit = goal;
            while (true) {
                String sourceAndTarget = Impression.sourceAndTarget(visit.referer(), visit.url());
                LOG.info("attributeGoal: visit={} sourceAndTarget={}", visit, sourceAndTarget);
                if (impressions.containsKey(sourceAndTarget)) {
                    LOG.info("attributeGoal: impression={}", impression);
                    // Walked entire path back to impression. Return success.
                    return impressions.get(sourceAndTarget);
                } else if (visits.containsKey(visit.referer())) {
                    // Found another visit in the path, continue searching.
                    visit = visits.get(visit.referer());
                    trail.add(0, visit);
                } else {
                    LOG.info("attributeGoal: not found");
                    // Referer not found, trail has gone cold. Return failure.
                    return null;
                }
            }
        }

        private Visit findGoal(Instant timestamp, Iterable<Visit> goals) {
            for (Visit goal : goals) {
                if (timestamp.equals(goal.timestamp())) {
                    return goal;
                }
            }
            return null;
        }

        private Instant minTimestamp(Iterable<Visit> goals, Visit goalToSkip) {
            Instant min = null;
            for (Visit goal : goals) {
                if (goal != goalToSkip && (min == null || goal.timestamp().isBefore(min))) {
                    min = goal.timestamp();
                }
            }
            return min;
        }

        private <K, V> Map<K, V> buildMap(Iterable<Map.Entry<K, V>> entries) {
            HashMap<K, V> map = new HashMap<>();
            for (Map.Entry<K, V> entry : entries) {
                map.put(entry.getKey(), entry.getValue());
            }
            return map;
        }

        @OnTimer("attribution")
        public void attributeGoal(@Timestamp Instant timestamp,
                                  @StateId("visits") MapState<String, Visit> visitsState,
                                  @StateId("impressions") MapState<String, Impression> impressionsState,
                                  @StateId("goals") SetState<Visit> goalsState,
                                  @StateId("minGoal") ValueState<Instant> minGoalState,
                                  @TimerId("attribution") Timer attributionTimer,
                                  OutputReceiver<Attribution> output) {
            LOG.info("Processing timer: {}", Utils.formatTime(timestamp));

            // Batch state reads together via futures.
            ReadableState<Iterable<Map.Entry<String, Visit> > > visitsFuture = visitsState.entries().readLater();
            ReadableState<Iterable<Map.Entry<String, Impression> > > impressionsFuture = impressionsState.entries().readLater();
            ReadableState<Iterable<Visit>> goalsFuture = goalsState.readLater();

            // Accessed the fetched state.
            Map<String, Visit> visits = buildMap(visitsFuture.read());
            Map<String, Impression> impressions = buildMap(impressionsFuture.read());
            Iterable<Visit> goals = goalsFuture.read();

            // Find the matching goal
            Visit goal = findGoal(timestamp, goals);

            // Attribute the goal
            List<Visit> trail = new ArrayList<>();
            Impression impression = attributeGoal(goal, visits, impressions, trail);
            if (impression != null) {
                output.output(new Attribution(impression, trail, goal));
                impressions.remove(impression.sourceAndTarget());
            }
            goalsState.remove(goal);

            // Set the next timer, if any.
            Instant minGoal = minTimestamp(goals, goal);
            if (minGoal != null) {
                LOG.info("Setting new timer at {}", Utils.formatTime(minGoal));
                minGoalState.write(minGoal);
                attributionTimer.set(minGoal);
            } else {
                minGoalState.clear();
            }
        }
    }
}
