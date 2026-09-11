package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * A van leaving a depot with a list of parcels on it - the demo's second topic, so that a sandbox run has more
 * than one route to dispatch between.
 * <p>
 * A bean with setters, like {@link Order}, and its parcels are carried as tracking numbers rather than as
 * {@link Parcel} objects: a list of strings reads back through any JSON deserialiser, where a list of immutable
 * objects would not. Its time is epoch millis for the reason {@link Order} explains.
 */
@InterfaceStability.Unstable
public class Dispatch {

    private String dispatchId;

    private String depotCity;

    private String driverName;

    private long dispatchedAtEpochMillis;

    private int parcelCount;

    private List<String> trackingNumbers;

    public String getDispatchId() {
        return dispatchId;
    }

    public void setDispatchId(String dispatchId) {
        this.dispatchId = dispatchId;
    }

    public String getDepotCity() {
        return depotCity;
    }

    public void setDepotCity(String depotCity) {
        this.depotCity = depotCity;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public long getDispatchedAtEpochMillis() {
        return dispatchedAtEpochMillis;
    }

    public void setDispatchedAtEpochMillis(long dispatchedAtEpochMillis) {
        this.dispatchedAtEpochMillis = dispatchedAtEpochMillis;
    }

    public int getParcelCount() {
        return parcelCount;
    }

    public void setParcelCount(int parcelCount) {
        this.parcelCount = parcelCount;
    }

    public List<String> getTrackingNumbers() {
        return trackingNumbers;
    }

    public void setTrackingNumbers(List<String> trackingNumbers) {
        this.trackingNumbers = trackingNumbers;
    }

    @Override
    public String toString() {
        return "Dispatch(" + dispatchId + ", " + depotCity + ", " + driverName + ", parcels=" + parcelCount
                + ", at=" + java.time.Instant.ofEpochMilli(dispatchedAtEpochMillis) + ")";
    }
}
