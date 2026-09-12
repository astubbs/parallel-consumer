package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import net.datafaker.Faker;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * The field-name rules: what a field called {@code deliveryEmail} ought to contain, as against merely what type it
 * is.
 * <p>
 * <b>No library infers this.</b> Instancio knows the field is a {@code String} and gives you
 * {@code "GHQWJZMXKB"}; Datafaker knows how to write an email address but has never heard of your class. The
 * table below is the whole of the join, and it is deliberately a table rather than a clever inference: a rule is
 * a name test, a type test and a supplier, so adding one is a line and reading one takes no context.
 * <p>
 * <b>Order matters, and the table is written most specific first</b> - {@code firstName} before {@code name},
 * {@code email} before {@code address} - because that is how it reads. Instancio's own precedence is the opposite,
 * the LAST matching selector winning, so {@link RandomObjects} registers this table in reverse; the comment there
 * carries the measurement. A field that matches nothing keeps Instancio's own random value.
 */
final class FieldValues {

    /**
     * The clock the sandbox pretends is now. Fixed rather than {@code Instant.now()} so that two runs of the same
     * seed produce the same timestamps, which is the whole of the reproducibility claim - a wall clock would
     * quietly break it for exactly the fields most likely to be asserted on.
     */
    private static final Instant SANDBOX_NOW = Instant.parse("2026-01-01T00:00:00Z");

    /**
     * The demo domain's parcel statuses, which a field called {@code status} is filled from. A closed set rather
     * than a random word, because the README's quickstart filters on one of these - a route that filtered on a
     * value the generator never produces would demonstrate nothing.
     */
    private static final String[] PARCEL_STATUSES = {
            "CREATED", "COLLECTED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "RETURNED"};

    /**
     * Datafaker, which knows what an email address or a city looks like. Built once per generator: it loads its
     * value dictionaries on construction, which is far too much work to repeat per record.
     */
    private final Faker faker;

    /**
     * The randomness both halves share. Held as well as handed to {@link #faker} because {@link #oneOf} draws from
     * it directly, and because {@link RandomObjects} re-seeds this instance before every record - which re-seeds
     * the faker with it, since Datafaker keeps the reference rather than a copy.
     */
    private final Random random;

    /**
     * The rule table, built once. Ordered most specific first, which is how it reads and the reverse of how
     * Instancio applies it - {@link RandomObjects} registers it backwards and explains why there.
     */
    private final List<Rule> rules;

    /**
     * @param random the generator's own randomness, re-seeded per record by its owner rather than by this class
     */
    FieldValues(Random random) {
        this.random = random;
        // Datafaker holds this Random rather than copying it, so RandomObjects re-seeding it re-seeds the faker.
        this.faker = new Faker(Locale.UK, random);
        this.rules = buildRules();
    }

    /**
     * The table, for {@link RandomObjects} to register as Instancio selectors. Returned as it is rather than
     * copied: the caller is one class in this package, and copying a table of twenty-odd rules per model would be
     * work for nobody.
     */
    List<Rule> rules() {
        return rules;
    }

    /**
     * One recognised field: what it is called, what type it has, and what to put in it.
     */
    static final class Rule {

        /**
         * What this rule is called, for {@link #toString()} - which is what a reader sees when a filled object
         * looks wrong and they print the table to find out which rule claimed the field.
         */
        private final String name;

        /**
         * Whether this rule claims a field: its name, already lower-cased, and its type.
         */
        private final BiPredicate<String, Class<?>> test;

        /**
         * The value, drawn fresh on every call - the rule is registered once per model and asked once per field
         * per record.
         */
        private final Supplier<Object> supplier;

        /**
         * Private: rules are only built by {@link #buildRules()} and {@link #text}, so the table stays the one
         * place the vocabulary is defined.
         */
        private Rule(String name, BiPredicate<String, Class<?>> test, Supplier<Object> supplier) {
            this.name = name;
            this.test = test;
            this.supplier = supplier;
        }

        /**
         * @param field a candidate field, from Instancio's walk of the target type
         */
        boolean matches(Field field) {
            return test.test(field.getName().toLowerCase(Locale.ROOT), field.getType());
        }

        /**
         * One value for a field this rule claimed. Not cached: two fields matching the same rule should not get
         * the same value.
         */
        Object value() {
            return supplier.get();
        }

        /**
         * The rule's name alone, so that {@link FieldValues#toString()} renders the table as a readable list.
         */
        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * The table itself: every field name this module recognises, and what it puts there. Written most specific
     * first - {@code firstName} before {@code name}, {@code email} before {@code address} - because that is the
     * order it reads in; the registration reverses it.
     */
    private List<Rule> buildRules() {
        List<Rule> table = new ArrayList<>();

        // Text, most specific first.
        text(table, "email", () -> faker.internet().emailAddress(), "email", "mail");
        text(table, "firstName", () -> faker.name().firstName(), "firstname", "givenname", "forename");
        text(table, "lastName", () -> faker.name().lastName(), "lastname", "surname", "familyname");
        text(table, "company", () -> faker.company().name(), "company", "organisation", "organization", "merchant");
        text(table, "name", () -> faker.name().fullName(), "name", "recipient", "sender", "customer", "contact");
        text(table, "postcode", () -> faker.address().zipCode(), "postcode", "postalcode", "zip");
        text(table, "city", () -> faker.address().city(), "city", "town");
        text(table, "country", () -> faker.address().country(), "country");
        text(table, "address", () -> faker.address().streetAddress(), "address", "street", "line1", "addressline");
        text(table, "phone", () -> faker.phoneNumber().phoneNumber(), "phone", "mobile", "telephone");
        text(table, "currency", () -> faker.currency().code(), "currency");
        text(table, "trackingNumber", () -> faker.numerify("PC##########"), "tracking", "consignment", "barcode");
        text(table, "status", () -> oneOf(PARCEL_STATUSES), "status", "state", "stage");
        text(table, "description", () -> faker.lorem().sentence(), "description", "note", "comment", "reason");
        text(table, "identifier", () -> faker.internet().uuid(), "id", "uuid", "reference", "ref", "key");

        // Money, as BigDecimal or as a double.
        table.add(new Rule("money(BigDecimal)",
                (name, type) -> BigDecimal.class.equals(type) && mentionsMoney(name),
                () -> amountBetween(1, 2500)));
        table.add(new Rule("money(double)",
                (name, type) -> isDouble(type) && mentionsMoney(name),
                () -> amountBetween(1, 2500).doubleValue()));

        // Parcel weights and dimensions, which look nothing like money.
        table.add(new Rule("weight",
                (name, type) -> isDouble(type) && containsAny(name, "weight", "mass", "kg"),
                () -> amountBetween(1, 30).doubleValue()));

        // Counts: a quantity of one to a dozen reads as a real order; Instancio's own int is nine digits.
        table.add(new Rule("quantity",
                (name, type) -> isInt(type) && containsAny(name, "quantity", "qty", "count", "items", "parcels"),
                () -> faker.number().numberBetween(1, 12)));

        // Times, recent rather than random across the epoch.
        table.add(new Rule("instant",
                (name, type) -> Instant.class.equals(type) && mentionsTime(name),
                this::recentInstant));
        table.add(new Rule("epochMillis",
                (name, type) -> isLong(type) && mentionsTime(name),
                () -> recentInstant().toEpochMilli()));
        table.add(new Rule("localDate",
                (name, type) -> LocalDate.class.equals(type) && mentionsTime(name),
                // Not LocalDate.ofInstant: that is Java 9, and this reactor compiles to Java 8 bytecode.
                () -> recentInstant().atZone(ZoneOffset.UTC).toLocalDate()));

        return table;
    }

    /**
     * A field whose name is a plausible identifier and whose value is stable for a given pool position - the
     * sandbox's keys, so that key ordering has something to order.
     */
    String pooledIdentifier(long pooled) {
        return "customer-" + String.format(Locale.ROOT, "%04d", pooled);
    }

    /**
     * The schemaless-JSON payload: field names a reader recognises, so that {@code json(topic)} with no class at
     * all still prints something worth reading (R4).
     */
    Map<String, Object> map() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderId", faker.internet().uuid());
        payload.put("customerName", faker.name().fullName());
        payload.put("email", faker.internet().emailAddress());
        payload.put("city", faker.address().city());
        payload.put("status", oneOf(PARCEL_STATUSES));
        payload.put("totalAmount", amountBetween(1, 2500));
        payload.put("placedAt", recentInstant().toString());
        return payload;
    }

    /**
     * A two-decimal amount in a range, which is what both the money rules and the weight rule want and what none
     * of them should be spelling out separately: one Datafaker draw, then the rounding, so that moving the range
     * cannot accidentally move the draw.
     *
     * @param min the lowest value the draw can take, inclusive
     * @param max the highest value the draw can take
     */
    private BigDecimal amountBetween(int min, int max) {
        return BigDecimal.valueOf(faker.number().randomDouble(2, min, max)).setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * A time in the month before {@link #SANDBOX_NOW}. Recent rather than uniform across the epoch, because a
     * timestamp field in a demo is nearly always meant to be read as "just now, give or take".
     */
    private Instant recentInstant() {
        return SANDBOX_NOW.minus(Duration.ofMinutes(faker.number().numberBetween(0, 60 * 24 * 30)));
    }

    /**
     * Picked with the shared {@link Random} rather than Datafaker's {@code options()} provider, so the choice is
     * seeded by exactly the same stream as everything else here.
     */
    private String oneOf(String[] candidates) {
        return candidates[random.nextInt(candidates.length)];
    }

    /**
     * Adds the commonest shape of rule - a {@code String} field whose name contains any of these fragments -
     * so the table reads as a list of vocabulary rather than as twenty repetitions of the same lambda.
     *
     * @param table     the table being built
     * @param ruleName  the name this rule answers to when the table is printed
     * @param supplier  the value for a field it claims
     * @param fragments lower-case fragments, any one of which claims the field
     */
    private static void text(List<Rule> table, String ruleName, Supplier<Object> supplier, String... fragments) {
        table.add(new Rule(ruleName,
                (name, type) -> String.class.equals(type) && containsAny(name, fragments),
                supplier));
    }

    /**
     * @param fieldName a field name already lower-cased by {@link Rule#matches(Field)}, so no rule has to
     *                  remember to do it
     */
    private static boolean containsAny(String fieldName, String... fragments) {
        for (String fragment : fragments) {
            if (fieldName.contains(fragment)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The money vocabulary, shared by the {@code BigDecimal} rule and the {@code double} one so that the two
     * cannot come to disagree about what a money field is called.
     */
    private static boolean mentionsMoney(String fieldName) {
        return containsAny(fieldName, "amount", "price", "total", "cost", "fee", "charge", "value", "balance");
    }

    /**
     * The time vocabulary, shared by the rules for the several ways a time is spelled in a bean - an
     * {@code Instant}, a {@code long} of epoch millis, a {@code String}.
     */
    private static boolean mentionsTime(String fieldName) {
        return containsAny(fieldName, "time", "date", "at", "when", "since", "instant", "stamp");
    }

    /**
     * Both spellings of a double, because a demo bean uses whichever its author preferred and a rule that matched
     * only the boxed one would silently skip half of them. Same for {@link #isInt} and {@link #isLong}.
     */
    private static boolean isDouble(Class<?> type) {
        return double.class.equals(type) || Double.class.equals(type);
    }

    /**
     * @see #isDouble(Class)
     */
    private static boolean isInt(Class<?> type) {
        return int.class.equals(type) || Integer.class.equals(type);
    }

    /**
     * @see #isDouble(Class)
     */
    private static boolean isLong(Class<?> type) {
        return long.class.equals(type) || Long.class.equals(type);
    }

    /**
     * The rule names in table order, which is what to print when a generated object has a field nobody expected -
     * it says which rules existed and in which order they were considered.
     */
    @Override
    public String toString() {
        return "FieldValues" + Arrays.toString(rules.toArray());
    }
}
