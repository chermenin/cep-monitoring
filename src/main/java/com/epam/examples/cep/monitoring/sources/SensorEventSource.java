/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.examples.cep.monitoring.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import com.epam.examples.cep.monitoring.events.*;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SensorEventSource extends RichParallelSourceFunction<SensorEvent> {

    private boolean running = true;

    private final int maxSensorId;

    private final long pause;

    private final double pressureStd;

    private final double pressureMean;

    private Random random;

    private int shard;

    private int offset;

    public SensorEventSource(
            int maxSensorId,
            long pause,
            double pressureStd,
            double pressureMean) {
        this.maxSensorId = maxSensorId;
        this.pause = pause;
        this.pressureStd = pressureStd;
        this.pressureMean = pressureMean;
    }

    @Override
    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = getRuntimeContext().getIndexOfThisSubtask();

        offset = (int) ((double) maxSensorId / numberTasks * index);
        shard = (int) ((double) maxSensorId / numberTasks * (index + 1)) - offset;

        random = new Random();
    }

    public void run(SourceContext<SensorEvent> sourceContext) throws Exception {
        while (running) {
            int sensorId = random.nextInt(shard) + offset;
            double pressure = random.nextGaussian() * pressureStd + pressureMean;
            sourceContext.collect(new PressureEvent(sensorId, pressure));
            Thread.sleep(ThreadLocalRandom.current().nextLong(pause) + pause / 2);
        }
    }

    public void cancel() {
        running = false;
    }
}
