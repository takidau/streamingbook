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
import static net.streamingbook.StateAndTimers.Attribution;
import static net.streamingbook.StateAndTimers.Impression;
import static net.streamingbook.StateAndTimers.Visit;
import static net.streamingbook.StateAndTimers.VisitOrImpression;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StateAndTimersTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    private static TimestampedValue<KV<String, VisitOrImpression>> visitOrImpression(Visit visit, Impression impression) {
        checkArgument((visit == null) != (impression == null), "Either visit or impression must be null");
        VisitOrImpression voi = new VisitOrImpression(visit, impression);
        // Note that for this example we always use the same user, since the test
        // only looks at one user's data.
        return TimestampedValue.of(KV.of("UserX", voi), visit != null ? visit.timestamp() : impression.timestamp());
    }
    
    private static TestStream<KV<String, VisitOrImpression>> createStream() {
        // Impressions and visits, in event-time order, for two attributable goals.
        Impression signupImpression = new Impression(123L, "http://search.com?q=xyz", "http://xyz.com/", Utils.parseTime("12:01:00"));
        Visit signupVisit = new Visit("http://xyz.com/", Utils.parseTime("12:01:10"), "http://search.com?q=xyz", false/*isGoal*/);
        Visit signupGoal = new Visit("http://xyz.com/join-mailing-list", Utils.parseTime("12:01:30"), "http://xyz.com/", true/*isGoal*/);

        Impression shoppingImpression = new Impression(456L, "http://search.com?q=thing", "http://xyz.com/thing", Utils.parseTime("12:02:00"));
        Impression shoppingImpressionDup = new Impression(789L, "http://search.com?q=thing", "http://xyz.com/thing", Utils.parseTime("12:02:10"));
        Visit shoppingVisit1 = new Visit("http://xyz.com/thing", Utils.parseTime("12:02:30"), "http://search.com?q=thing", false/*isGoal*/);
        Visit shoppingVisit2 = new Visit("http://xyz.com/thing/add-to-cart", Utils.parseTime("12:03:00"), "http://xyz.com/thing", false/*isGoal*/);
        Visit shoppingVisit3 = new Visit("http://xyz.com/thing/purchase", Utils.parseTime("12:03:20"), "http://xyz.com/thing/add-to-cart", false/*isGoal*/);
        Visit shoppingGoal = new Visit("http://xyz.com/thing/receipt", Utils.parseTime("12:03:45"), "http://xyz.com/thing/purchase", true/*isGoal*/);

        Impression unattributedImpression = new Impression(000L, "http://search.com?q=thing", "http://xyz.com/other-thing", Utils.parseTime("12:04:00"));
        Visit unattributedVisit = new Visit("http://xyz.com/other-thing", Utils.parseTime("12:04:20"), "http://search.com?q=other thing", false/*isGoal*/);

        // Create a stream of visits and impressions, with data arriving out of order.
        return TestStream.create(
            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(VisitOrImpression.class)))
            .advanceWatermarkTo(Utils.parseTime("12:00:00"))
            .addElements(visitOrImpression(shoppingVisit2, null))
            .addElements(visitOrImpression(shoppingGoal, null))
            .addElements(visitOrImpression(shoppingVisit3, null))
            .addElements(visitOrImpression(signupGoal, null))
            .advanceWatermarkTo(Utils.parseTime("12:00:30"))
            .addElements(visitOrImpression(null, signupImpression))
            .advanceWatermarkTo(Utils.parseTime("12:01:00"))
            .addElements(visitOrImpression(null, shoppingImpression))
            .addElements(visitOrImpression(signupVisit, null))
            .advanceWatermarkTo(Utils.parseTime("12:01:30"))
            .addElements(visitOrImpression(null, shoppingImpressionDup))
            .addElements(visitOrImpression(shoppingVisit1, null))
            .advanceWatermarkTo(Utils.parseTime("12:03:45"))
            .addElements(visitOrImpression(null, unattributedImpression))
            .advanceWatermarkTo(Utils.parseTime("12:04:00"))
            .addElements(visitOrImpression(unattributedVisit, null))
            .advanceWatermarkToInfinity();
    }

    @Test
    public void stateTest() {
        PCollection<String> teamScores =
            p.apply(createStream())
             .apply(ParDo.of(new StateAndTimers.AttributionFn()))
             .apply(ParDo.of(new StateAndTimers.FormatAttributionAsString()));

        PAssert.that(teamScores)
            .containsInAnyOrder("[global window]: imp=123 http://search.com?q=xyz → http://xyz.com/ → http://xyz.com/join-mailing-list 11:01:30 UNKNOWN",
                                "[global window]: imp=456 http://search.com?q=thing → http://xyz.com/thing → http://xyz.com/thing/add-to-cart → "
                                + "http://xyz.com/thing/purchase → http://xyz.com/thing/receipt 11:03:45 UNKNOWN");

        p.run().waitUntilFinish();
    }
}
