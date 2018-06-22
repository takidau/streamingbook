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

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link WindowFn} that windows values into validity windows, where a validity window for
 * an element with event time X is [X, end of time) if there are no elements following it,
 * or [X, event time of next element) otherwise. This type of window captures the idea of
 * a value being valid until overriden by a later value, such as currency conversion rates.
 *
 * Validity windows are particularly interesting because they are an example of windows
 * which split or shrink over time.
 */
public class ValidityWindows extends WindowFn<Object, IntervalWindow> {
    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) {
        // Assign each element into a window from its timestamp until the end of
        // the global window. I'd rather assign to the end of time, but it appears
        // that any window that exceeds the global window is dropped from the output
        // received by PAssert.
        return Arrays.asList(new IntervalWindow(c.timestamp(), GlobalWindow.INSTANCE.maxTimestamp()));
    }

    private List<IntervalWindow> sortedWindows(MergeContext c) {
        List<IntervalWindow> sortedWindows = new ArrayList<>();
        for (IntervalWindow w : c.windows()) {
            sortedWindows.add(w);
        }
        Collections.sort(sortedWindows);
        return sortedWindows;
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
        IntervalWindow last = null;
        for (IntervalWindow w : sortedWindows(c)) {
            if (last != null && w.start().isBefore(last.end())) {
                c.merge(Arrays.asList(last),
                        new IntervalWindow(last.start(), w.start()));
            }
            last = w;
        }
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof ValidityWindows;
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
        if (!this.isCompatible(other)) {
            throw new IncompatibleWindowException(
               other,
               String.format(
                 "%s is only compatible with %s.",
                 ValidityWindows.class.getSimpleName(), ValidityWindows.class.getSimpleName()));
        }
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("ValidityWindows is not allowed in side inputs");
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ValidityWindows;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ValidityWindows.class);
    }
}
