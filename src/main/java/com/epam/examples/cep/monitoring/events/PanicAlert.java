package com.epam.examples.cep.monitoring.events;

public class PanicAlert {

    private int rackID;

    private int sensorID;

    public PanicAlert(int rackID, int sensorID) {
        this.rackID = rackID;
        this.sensorID = sensorID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    public int getSensorID() {
        return sensorID;
    }

    public void setSensorID(int sensorID) {
        this.sensorID = sensorID;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PanicAlert) {
            PanicAlert other = (PanicAlert) obj;

            return sensorID == other.sensorID && rackID == other.rackID;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * sensorID + rackID;
    }

    @Override
    public String toString() {
        return "PanicAlert(" + sensorID + ", " + rackID + ")";
    }
}
