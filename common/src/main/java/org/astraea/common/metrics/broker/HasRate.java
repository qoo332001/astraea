/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.metrics.broker;

import java.util.concurrent.TimeUnit;
import org.astraea.common.metrics.HasBeanObject;

public interface HasRate extends HasBeanObject {
  String ONE_MIN_RATE_KEY = "OneMinuteRate";
  String FIVE_MINUTE_RATE = "FiveMinuteRate";
  String FIFTEEN_MINUTE_RATE = "FifteenMinuteRate";

  default double meanRate() {
    return (double) beanObject().attributes().getOrDefault("MeanRate", 0);
  }

  default double oneMinuteRate() {
    return (double) beanObject().attributes().getOrDefault(ONE_MIN_RATE_KEY, 0);
  }

  default double fiveMinuteRate() {
    return (double) beanObject().attributes().getOrDefault(FIVE_MINUTE_RATE, 0.0);
  }

  default double fifteenMinuteRate() {
    return (double) beanObject().attributes().getOrDefault(FIFTEEN_MINUTE_RATE, 0);
  }

  default TimeUnit rateUnit() {
    return (TimeUnit) beanObject().attributes().get("RateUnit");
  }
}
