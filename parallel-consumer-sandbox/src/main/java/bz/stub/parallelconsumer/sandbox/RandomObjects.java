package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.instancio.Instancio;
import org.instancio.InstancioApi;
import org.instancio.Model;
import org.instancio.Select;
import org.instancio.settings.Keys;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Fills an instance of a declared type with realistic random data, reproducibly.
 *
 * <h2>Two libraries, two jobs</h2>
 * <b>Instancio</b> walks the object graph: it knows how to reach every field of a class, how to build one that has
 * only a constructor, and how to stop at a cycle. It knows nothing about what a field <em>means</em>.
 * <b>Datafaker</b> knows what an email address looks like and nothing about your class. {@link FieldValues} is the
 * join: a table of field-name rules, each supplying a Datafaker value for the fields it recognises, handed to
 * Instancio as selectors. A field no rule recognises gets Instancio's own random value, which is correct but
 * meaningless - that is the floor, not the goal.
 *
 * <h2>Reproducibility</h2>
 * A run is addressed by index, not by sequence: {@link #create(Class, long)} derives the seed for record
 * <em>n</em> from the base seed and <em>n</em> alone. So the same seed gives the same record for the same index
 * however the generator threads interleave, and a failing record can be reproduced on its own rather than by
 * replaying everything before it.
 *
 * <h2>What it will not do</h2>
 * An <b>Avro</b> specific record is not an ordinary bean - its fields are described by a schema and its builder
 * enforces one - so it is filled from that schema by Avro's own generator ({@link AvroValues}), not by Instancio.
 * A <b>Protobuf</b> message is refused, naming the type: its generated classes have no settable fields at all and
 * only a builder can construct one, which is a filler of its own that this version does not have (KTD9).
 */
@InterfaceStability.Unstable
public final class RandomObjects {

    /**
     * Instancio's default collection sizes run to six elements at every level, which turns a two-list demo type
     * into a page of JSON. Small enough to read in a console sink is the point of the sandbox.
     */
    private static final int MAX_COLLECTION_SIZE = 3;

    private static final String PROTOBUF_PACKAGE = "com.google.protobuf.";

    /**
     * Matched by name rather than by class, for the reason {@link #isAvroSpecificRecord(Class)} gives: naming the
     * interface in code would load Avro, which is optional here.
     */
    private static final String AVRO_SPECIFIC_RECORD = "org.apache.avro.specific.SpecificRecord";

    /**
     * The run's seed. Every record's own seed is derived from it and the record's index - see {@link #seedFor} -
     * so it is kept rather than only consumed at construction.
     */
    private final long seed;

    /**
     * Datafaker's source of randomness, re-seeded before each record. Datafaker holds this instance rather than a
     * copy of it, so re-seeding here re-seeds the faker - which is what makes a record addressable by index
     * without paying to construct a {@code Faker} per record (it loads its value dictionaries on construction).
     */
    private final Random random;

    private final FieldValues fieldValues;

    /**
     * One Instancio model per type this generator has been asked for, because building one is the expensive half
     * and it does not depend on the record.
     * <p>
     * A model is the settings and the whole rule table registered as selectors - twenty-odd of them - and none of
     * that changes between two records of the same type; only the seed does, and a seed is given to
     * {@link Instancio#of(Model)} per record rather than baked into the model. So the table is built once per
     * type instead of once per record.
     * <p>
     * <b>Per generator, never static.</b> The selectors are method references bound to <em>this</em> generator's
     * {@link FieldValues}, which holds the {@link #random} this class re-seeds before every record. A model shared
     * between two generators would draw its values from whichever one built it, and two seeds would stop
     * differing - which is the whole of what a seed is for. A plain map rather than a concurrent one for the same
     * reason the re-seeding is safe: one generator is driven by one thread, its own.
     */
    private final Map<Class<?>, Model<?>> models = new HashMap<>();

    private RandomObjects(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.fieldValues = new FieldValues(random);
    }

    /**
     * @param seed any long; two generators with the same seed produce the same object for the same index
     */
    public static RandomObjects seededWith(long seed) {
        return new RandomObjects(seed);
    }

    /**
     * The seed this generator was built with, so a sandbox can log the number a reader needs to reproduce the run.
     */
    public long seed() {
        return seed;
    }

    /**
     * The record at {@code index} of this seed's sequence.
     *
     * @throws IllegalArgumentException naming the type, when it is one this version cannot fill
     */
    public <T> T create(Class<T> type, long index) {
        long recordSeed = seedFor(index);
        random.setSeed(recordSeed);

        refuseProtobuf(type);
        if (isAvroSpecificRecord(type)) {
            return AvroValues.create(type, recordSeed);
        }
        if (Map.class.isAssignableFrom(type)) {
            @SuppressWarnings("unchecked") T map = (T) fieldValues.map();
            return map;
        }
        return instancio(type, recordSeed);
    }

    /**
     * The seed for one record. A multiply-and-mix rather than {@code seed + index}, so that two generators one
     * apart in seed do not produce overlapping sequences one record apart.
     */
    long seedFor(long index) {
        long mixed = seed * 0x9E3779B97F4A7C15L + index;
        mixed ^= mixed >>> 33;
        mixed *= 0xFF51AFD7ED558CCDL;
        return mixed ^ (mixed >>> 33);
    }

    /**
     * One record of an ordinary Java type, from the cached model plus this record's seed.
     */
    private <T> T instancio(Class<T> type, long recordSeed) {
        Model<?> cached = models.get(type);
        if (cached == null) {
            cached = modelFor(type);
            models.put(type, cached);
        }
        @SuppressWarnings("unchecked") Model<T> model = (Model<T>) cached;
        return Instancio.of(model).withSeed(recordSeed).create();
    }

    /**
     * The settings and the rule table for one type, as a model that can be created from repeatedly. No seed: the
     * seed belongs to the record, and {@link #instancio} supplies it per record.
     */
    private <T> Model<T> modelFor(Class<T> type) {
        InstancioApi<T> api = Instancio.of(type);
        api.withSetting(Keys.COLLECTION_MAX_SIZE, MAX_COLLECTION_SIZE);
        api.withSetting(Keys.COLLECTION_MIN_SIZE, 1);
        api.withSetting(Keys.MAP_MAX_SIZE, MAX_COLLECTION_SIZE);
        api.withSetting(Keys.MAP_MIN_SIZE, 1);
        // Instancio is STRICT by default: a selector matching no field of the target type is an error. The rule
        // table is deliberately generic - most types match a handful of its rules and none of the rest - so
        // strict mode would fail on every type the table was not written for, which is all of them.
        api.lenient();
        // REGISTERED IN REVERSE, because Instancio's LAST matching selector wins, and the table is written most
        // specific first so that it reads well. Measured, not assumed: with the table registered in its own order,
        // a field called emailAddress - which matches both the email rule and the address rule - came out as
        // "60941 Abbott Plaza". HydrationTest#aFieldMatchingTwoRulesGetsTheMoreSpecificOne pins it, because
        // nothing else would go red if Instancio's precedence ever changed.
        List<FieldValues.Rule> rules = fieldValues.rules();
        for (int i = rules.size() - 1; i >= 0; i--) {
            FieldValues.Rule rule = rules.get(i);
            api.supply(Select.fields(rule::matches), rule::value);
        }
        return api.toModel();
    }

    /**
     * Recognised by name rather than by {@code isAssignableFrom}, because that would load the class - and Avro is
     * an optional dependency here, so on a classpath without it the reference itself is the failure.
     */
    static boolean isAvroSpecificRecord(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            for (Class<?> implemented : current.getInterfaces()) {
                if (AVRO_SPECIFIC_RECORD.equals(implemented.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Throws if the type is a Protobuf message, naming it.
     * <p>
     * Walks the hierarchy rather than testing the class itself, because a generated message's own package is the
     * user's: what marks it is {@code com.google.protobuf.GeneratedMessageV3} above it, or a Protobuf interface
     * beside it. Checked before the Avro question so that a type which somehow answered both is refused rather
     * than filled badly.
     */
    private static void refuseProtobuf(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            if (current.getName().startsWith(PROTOBUF_PACKAGE) || implementsProtobuf(current)) {
                throw new IllegalArgumentException("The generator cannot fill the Protobuf message type "
                        + type.getName() + ": a generated Protobuf class has no settable fields and only its "
                        + "builder can construct one, so it needs a filler of its own that this version does not "
                        + "have. Generate this route's records by hand, or declare a plain type for it in the "
                        + "sandbox.");
            }
        }
    }

    /**
     * Whether one level of the hierarchy implements a Protobuf interface - {@code MessageOrBuilder} and its kin,
     * which a generated message carries even where its superclass has been shaded away.
     */
    private static boolean implementsProtobuf(Class<?> type) {
        for (Class<?> implemented : type.getInterfaces()) {
            if (implemented.getName().startsWith(PROTOBUF_PACKAGE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A key for record {@code index}, drawn from a pool of {@code cardinality} distinct values so that keys
     * repeat - which is what makes key ordering, and the shard behaviour underneath it, visible in a sandbox run.
     * A cardinality of one puts every record on one shard; a cardinality equal to the record count puts each on
     * its own.
     */
    public <K> K key(Class<K> type, long index, int cardinality) {
        long pooled = Math.floorMod(index, Math.max(1, cardinality));
        if (String.class.equals(type)) {
            @SuppressWarnings("unchecked") K key = (K) fieldValues.pooledIdentifier(pooled);
            return key;
        }
        if (byte[].class.equals(type)) {
            @SuppressWarnings("unchecked") K key = (K) fieldValues.pooledIdentifier(pooled)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            return key;
        }
        return create(type, pooled);
    }
}
