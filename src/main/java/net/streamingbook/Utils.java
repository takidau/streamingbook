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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;

public class Utils {

    public static Instant parseTime(String time) {
        return Instant.parse("T" + time);
    }

    public static DateTimeFormatter TIME_FMT = DateTimeFormat.forPattern("HH:mm:ss");
    
    public static String formatTime(Instant timestamp) {
	if (timestamp == null) {
	    return "null";
	} else if (timestamp.equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            return "TIMESTAMP_MIN_VALUE";
        } else if (timestamp.equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            return "TIMESTAMP_MAX_VALUE";
        } else if (timestamp.equals(GlobalWindow.INSTANCE.maxTimestamp())) {
            return "END_OF_GLOBAL_WINDOW";
        } else {
            return timestamp.toString(TIME_FMT);
        }
    }

    public static String formatWindow(BoundedWindow window) {
        if (window instanceof GlobalWindow) {
            return "[global window]";
        } else if (window instanceof IntervalWindow) {
            IntervalWindow interval = (IntervalWindow) window;
            return "[" + formatTime(interval.start()) + ", " + formatTime(interval.end()) + ")";
        } else {
            return "..., " + formatTime(window.maxTimestamp()) + "]";
        }
    }
    
}
