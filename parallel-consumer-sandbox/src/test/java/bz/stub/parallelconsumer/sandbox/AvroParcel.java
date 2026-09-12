package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * An Avro specific record, hand-written rather than generated.
 * <p>
 * <b>Hand-written on purpose.</b> Generating one would mean adding {@code avro-maven-plugin} and a {@code .avsc}
 * to this module's build for a single fixture, and the plugin's output is exactly the four members below: a static
 * {@code SCHEMA$}, a no-argument constructor, and the indexed {@code get}/{@code put} pair. What the generator
 * under test cares about is that {@code SpecificData} can find the schema by class and build an instance from it,
 * and that is what these four members are.
 * <p>
 * The {@code avro.java.string} property is what makes the string fields come back as {@link String} rather than
 * {@code Utf8} - {@code RandomData} produces {@code Utf8} either way, and the read side is where it is converted.
 * The namespace matches this class's package because {@code SpecificData} resolves a schema to a class by its full
 * name.
 */
public class AvroParcel extends SpecificRecordBase {

    public static final Schema SCHEMA$ = new Schema.Parser().parse("{"
            + "\"type\":\"record\","
            + "\"name\":\"AvroParcel\","
            + "\"namespace\":\"bz.stub.parallelconsumer.sandbox\","
            + "\"fields\":["
            + "{\"name\":\"trackingNumber\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},"
            + "{\"name\":\"recipientName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},"
            + "{\"name\":\"weightGrams\",\"type\":\"int\"}"
            + "]}");

    private String trackingNumber;

    private String recipientName;

    private int weightGrams;

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0:
                return trackingNumber;
            case 1:
                return recipientName;
            case 2:
                return weightGrams;
            default:
                throw new IndexOutOfBoundsException("AvroParcel has three fields, not " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                trackingNumber = value == null ? null : value.toString();
                break;
            case 1:
                recipientName = value == null ? null : value.toString();
                break;
            case 2:
                weightGrams = (Integer) value;
                break;
            default:
                throw new IndexOutOfBoundsException("AvroParcel has three fields, not " + field);
        }
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public String getRecipientName() {
        return recipientName;
    }

    public int getWeightGrams() {
        return weightGrams;
    }

    @Override
    public String toString() {
        return "AvroParcel(" + trackingNumber + ", " + recipientName + ", " + weightGrams + "g)";
    }
}
