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

package com.epam.examples.cep.monitoring;

import com.epam.examples.cep.monitoring.events.*;
import com.epam.examples.cep.monitoring.sources.MonitoringEventSource;
import com.epam.examples.cep.monitoring.sources.SensorEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Another CEP example monitoring program
 * <p>
 * This example program generates a stream of monitoring events which are analyzed using Flink's CEP library.
 * The first input event stream consists of temperature and power events from a set of racks. The second one
 * consists of pressure events from some sensors. The goal is to detect when a rack is about to overheat. And
 * generate alerts when pressure is more than defined value.
 */
public class Example02 {
    private static final long PAUSE = 100;

    private static final double TEMPERATURE_THRESHOLD = 100;
    private static final double PRESSURE_THRESHOLD = 124_544;

    private static final int MAX_RACK_ID = 10;
    private static final int MAX_SENSOR_ID = 10;

    private static final double TEMPERATURE_RATIO = 0.5;
    private static final double POWER_STD = 10;
    private static final double POWER_MEAN = 100;
    private static final double TEMP_STD = 25;
    private static final double TEMP_MEAN = 80;
    private static final double PRESSURE_STD = 29_420;
    private static final double PRESSURE_MEAN = 98_066.5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        // Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Input stream of monitoring events
        DataStream<MonitoringEvent> inputMonitoringEventStream = env
                .addSource(new MonitoringEventSource(
                        MAX_RACK_ID,
                        PAUSE,
                        TEMPERATURE_RATIO,
                        POWER_STD,
                        POWER_MEAN,
                        TEMP_STD,
                        TEMP_MEAN))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        // Input stream of sensor events
        DataStream<SensorEvent> inputSensorEventStream = env
                .addSource(new SensorEventSource(
                        MAX_SENSOR_ID,
                        PAUSE,
                        PRESSURE_STD,
                        PRESSURE_MEAN))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());


        // ***** TEMPERATURE EVENTS *****

        // Warning pattern: Two consecutive temperature events whose temperature is higher than
        // the given threshold appearing within a time interval of 10 seconds
        Pattern<MonitoringEvent, ?> tempWarningPattern = Pattern.<MonitoringEvent>begin("first")
                .subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {

                    @Override
                    public boolean filter(TemperatureEvent event) {
                        return event.getTemperature() >= TEMPERATURE_THRESHOLD;
                    }
                })
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {

                    @Override
                    public boolean filter(TemperatureEvent event) {
                        return event.getTemperature() >= TEMPERATURE_THRESHOLD;
                    }
                })
                .within(Time.seconds(10));

        // Create a pattern stream from our warning pattern
        PatternStream<MonitoringEvent> tempWarningPatternStream = CEP.pattern(
                inputMonitoringEventStream.keyBy("rackID"),
                tempWarningPattern);

        // Generate temperature warnings for each matched warning pattern
        DataStream<TemperatureWarning> tempWarnings = tempWarningPatternStream.select(
                (Map<String, List<MonitoringEvent>> pattern) -> {
                    TemperatureEvent first = (TemperatureEvent) pattern.get("first").get(0);
                    TemperatureEvent second = (TemperatureEvent) pattern.get("second").get(0);

                    return new TemperatureWarning(first.getRackID(),
                            (first.getTemperature() + second.getTemperature()) / 2);
                }
        );

        // Alert pattern: Two consecutive temperature warnings appearing within a time interval of 20 seconds
        Pattern<TemperatureWarning, ?> tempAlertPattern = Pattern.<TemperatureWarning>begin("first")
                .next("second")
                .where(new IterativeCondition<TemperatureWarning>() {

                    @Override
                    public boolean filter(TemperatureWarning warning, Context<TemperatureWarning> context) {
                        TemperatureWarning first = context.getEventsForPattern("first").iterator().next();
                        return warning.getAverageTemperature() > first.getAverageTemperature();
                    }
                })
                .within(Time.seconds(20));

        // Create a pattern stream from our alert pattern
        PatternStream<TemperatureWarning> tempAlertPatternStream = CEP.pattern(
                tempWarnings.keyBy("rackID"),
                tempAlertPattern);

        // Generate a temperature alert only if the second temperature warning's average temperature
        // is higher than first warning's temperature
        DataStream<TemperatureAlert> tempAlerts = tempAlertPatternStream.select(
                (Map<String, List<TemperatureWarning>> pattern) -> {
                    TemperatureWarning first = pattern.get("first").get(0);
                    return new TemperatureAlert(first.getRackID());
                });


        // **** PRESSURE EVENTS ****

        // Warning pattern: Two consecutive pressure events whose pressure value is higher than
        // the given threshold appearing within a time interval of 10 seconds
        Pattern<SensorEvent, ?> presWarningPattern = Pattern.<SensorEvent>begin("first")
                .subtype(PressureEvent.class)
                .where(new SimpleCondition<PressureEvent>() {

                    @Override
                    public boolean filter(PressureEvent event) {
                        return event.getPressure() >= PRESSURE_THRESHOLD;
                    }
                })
                .next("second")
                .subtype(PressureEvent.class)
                .where(new SimpleCondition<PressureEvent>() {

                    @Override
                    public boolean filter(PressureEvent event) {
                        return event.getPressure() >= PRESSURE_THRESHOLD;
                    }
                })
                .within(Time.seconds(10));


        // Create a pattern stream from our warning pattern
        PatternStream<SensorEvent> presWarningPatternStream = CEP.pattern(
                inputSensorEventStream.keyBy("sensorID"),
                presWarningPattern);

        // Generate temperature warnings for each matched warning pattern
        DataStream<PressureWarning> presWarnings = presWarningPatternStream.select(
                (Map<String, List<SensorEvent>> pattern) -> {
                    PressureEvent first = (PressureEvent) pattern.get("first").get(0);
                    PressureEvent second = (PressureEvent) pattern.get("second").get(0);

                    return new PressureWarning(first.getSensorID(),
                            Math.max(first.getPressure(), second.getPressure()));
                }
        );

        // Alert pattern: Two consecutive pressure warnings appearing within a time interval of 20 seconds
        Pattern<PressureWarning, ?> presAlertPattern = Pattern.<PressureWarning>begin("first")
                .next("second")
                .where(new IterativeCondition<PressureWarning>() {

                    @Override
                    public boolean filter(PressureWarning warning, Context<PressureWarning> context) {
                        PressureWarning first = context.getEventsForPattern("first").iterator().next();
                        return warning.getPressure() > first.getPressure();
                    }
                })
                .within(Time.seconds(20));

        // Create a pattern stream from our alert pattern
        PatternStream<PressureWarning> presAlertPatternStream = CEP.pattern(
                presWarnings.keyBy("sensorID"),
                presAlertPattern);

        // Generate a pressure alert only if the second temperature warning's max pressure is higher than
        // first warning's pressure
        DataStream<PressureAlert> presAlerts = presAlertPatternStream.select(
                (Map<String, List<PressureWarning>> pattern) -> {
                    PressureWarning first = pattern.get("first").get(0);
                    return new PressureAlert(first.getSensorID());
                });


        // ***** UNION TWO STREAMS *****

        // Union two types of alerts into one data stream
        DataStream<UniversalEvent> mappedTempAlerts =
                tempAlerts.map((MapFunction<TemperatureAlert, UniversalEvent>) UniversalEvent::new);

        DataStream<UniversalEvent> mappedPresAlerts =
                presAlerts.map((MapFunction<PressureAlert, UniversalEvent>) UniversalEvent::new);

        DataStream<UniversalEvent> allAlerts = mappedTempAlerts.union(mappedPresAlerts);


        // ***** PANIC ALERTS *****

        // Panic alert pattern: Two consecutive temperature and pressure warnings appearing
        // within a time interval of 3 seconds
        Pattern<UniversalEvent, ?> panicAlertPattern = Pattern.<UniversalEvent>begin("first")
                .next("second")
                .where(new IterativeCondition<UniversalEvent>() {

                    @Override
                    public boolean filter(UniversalEvent event, Context<UniversalEvent> context) {
                        UniversalEvent first = context.getEventsForPattern("first").iterator().next();
                        return !first.getEventClass().equals(event.getEventClass());
                    }
                })
                .within(Time.seconds(5));

        // Create a pattern stream from our alert pattern
        PatternStream<UniversalEvent> panicAlertPatternStream = CEP.pattern(
                allAlerts, panicAlertPattern);

        // Generate a panic alert
        DataStream<PanicAlert> panicAlerts = panicAlertPatternStream.select(
                (Map<String, List<UniversalEvent>> pattern) -> {
                    UniversalEvent first = pattern.get("first").get(0);
                    UniversalEvent second = pattern.get("second").get(0);

                    TemperatureAlert tempAlert;
                    PressureAlert presAlert;
                    if (first.getEventClass().equals(TemperatureAlert.class)) {
                        tempAlert = (TemperatureAlert) first.getEvent();
                        presAlert = (PressureAlert) second.getEvent();
                    } else {
                        presAlert = (PressureAlert) first.getEvent();
                        tempAlert = (TemperatureAlert) second.getEvent();
                    }
                    return new PanicAlert(tempAlert.getRackID(), presAlert.getSensorID());
                });

        panicAlerts.print();

        env.execute("CEP monitoring job");
    }
}
