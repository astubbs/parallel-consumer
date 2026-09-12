package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.RandomData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * The Avro arm of {@link RandomObjects}, in its own class so that nothing here is loaded unless the caller has
 * already established that Avro is on the classpath - the pattern core uses for its optional Jackson dependency.
 * A user with no Avro route never resolves a single name in this file.
 *
 * <h2>Why not Instancio</h2>
 * A generated Avro class carries a schema that describes its fields more precisely than their Java types do -
 * which union arms are legal, which are nullable, what a logical type means - and a filler that writes fields by
 * reflection produces instances the schema rejects on the way out. Avro ships the filler that reads the schema
 * instead, {@link RandomData}, and it takes a seed, so reproducibility survives the detour.
 *
 * <h2>Why the round trip</h2>
 * {@link RandomData} yields a {@code GenericRecord} - in Avro 1.11 it has no constructor taking a
 * {@code SpecificData}, so it cannot build the generated class directly. Writing that generic record and reading
 * it back with a {@link SpecificDatumReader} is Avro's own conversion, over the same schema on both sides, so it
 * costs a few hundred bytes per record and cannot drift from what the class actually accepts.
 */
final class AvroValues {

    /**
     * No instances: this is a conversion, and it holds nothing between calls.
     */
    private AvroValues() {
    }

    /**
     * @param type      an Avro generated class - the caller has already checked that it implements
     *                  {@code SpecificRecord}
     * @param recordSeed the seed for this one record
     */
    static <T> T create(Class<T> type, long recordSeed) {
        Schema schema = SpecificData.get().getSchema(type);
        Object generic = new RandomData(schema, 1, recordSeed).iterator().next();
        try {
            return toSpecific(type, schema, generic);
        } catch (IOException e) {
            throw new IllegalStateException("Could not convert the generated Avro record for " + type.getName()
                    + " into its specific form - the schema Avro generated for the class and the record built "
                    + "from it disagree, which should not be possible", e);
        }
    }

    /**
     * Avro's own generic-to-specific conversion, written out because Avro has no single call for it: encode the
     * generic record and decode it as the specific one, over the same schema on both sides.
     *
     * @param type   the generated class to end up with
     * @param schema the schema both halves of the round trip read, so neither side can drift from the other
     * @param generic what {@link RandomData} produced
     */
    private static <T> T toSpecific(Class<T> type, Schema schema, Object generic) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(buffer, null);
        new GenericDatumWriter<Object>(schema).write(generic, encoder);
        encoder.flush();

        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema, schema, SpecificData.get());
        return reader.read(null, DecoderFactory.get().binaryDecoder(buffer.toByteArray(), null));
    }
}
