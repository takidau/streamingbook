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
import static net.streamingbook.Utils.formatTime;
import static org.junit.Assert.assertEquals;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class UtilsTest {
    @Test
    public void parseTimeTest() {
	String time1 = "12:34:56";
	Instant instant = parseTime(time1);
	String time2 = formatTime(instant);
	assertEquals(time1, time2);
    }
}
