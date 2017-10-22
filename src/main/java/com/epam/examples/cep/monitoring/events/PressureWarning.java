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

package com.epam.examples.cep.monitoring.events;

public class PressureWarning {

    private int sensorID;
    private double pressure;

    public PressureWarning(int sensorID, double pressure) {
        this.sensorID = sensorID;
        this.pressure = pressure;
    }

    public PressureWarning() {
        this(-1, -1);
    }

    public int getSensorID() {
        return sensorID;
    }

    public void setSensorID(int sensorID) {
        this.sensorID = sensorID;
    }

    public double getPressure() {
        return pressure;
    }

    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PressureWarning) {
            PressureWarning other = (PressureWarning) obj;

            return sensorID == other.sensorID && pressure == other.pressure;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * sensorID + Double.hashCode(pressure);
    }

    @Override
    public String toString() {
        return "PressureWarning(" + sensorID + ", " + pressure + ")";
    }
}
