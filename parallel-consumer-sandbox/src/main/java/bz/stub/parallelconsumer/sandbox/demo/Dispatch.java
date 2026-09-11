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
 * <p>
 * <b>The accessors below carry no documentation of their own, deliberately.</b> Each one reads or writes the
 * field above it and does nothing else; what is worth knowing about {@code city} is on the field, and repeating it
 * as "@return the city" on the getter would be noise between a reader and the four classes this package exists to
 * show off.
 */
@InterfaceStability.Unstable
public class Dispatch {

    /**
     * A UUID from the identifier rule, as {@link Order#getOrderId()} is.
     */
    private String dispatchId;

    /**
     * The depot this van left, from the address rules - a real city name.
     */
    private String depotCity;

    /**
     * The driver, from the name rule.
     */
    private String driverName;

    /**
     * When the van left, as epoch millis for the reason {@link Order} gives.
     */
    private long dispatchedAtEpochMillis;

    /**
     * How many parcels are on board, from the quantity rule. Not derived from the list below - the generator fills
     * each field independently, which is worth seeing rather than hiding.
     */
    private int parcelCount;

    /**
     * The parcels, as tracking numbers: the tracking rule gives each the PC-and-ten-digits shape, and a list of
     * strings is what a JSON route can read back. The list is short, because the generator's collection sizes are
     * capped at three so a record still fits on a console line.
     */
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

    /**
     * What a route logs when it dispatches, so the demo's output says which van and which depot.
     */
    @Override
    public String toString() {
        return "Dispatch(" + dispatchId + ", " + depotCity + ", " + driverName + ", parcels=" + parcelCount
                + ", at=" + java.time.Instant.ofEpochMilli(dispatchedAtEpochMillis) + ")";
    }
}
