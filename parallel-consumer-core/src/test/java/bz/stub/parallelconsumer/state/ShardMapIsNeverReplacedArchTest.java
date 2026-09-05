package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * The {@code processingShards} map reference in {@link ShardManager} is installed once, at construction, and is
 * never replaced while the consumer is running.
 * <p>
 * The class javadoc says the map is shared between the broker-poll thread (which adds and removes shards) and the
 * controller thread (which reads how much work is queued). Swapping the reference underneath them is strictly worse
 * than any torn read of its contents: a reader that already loaded the old reference keeps operating on a map the
 * writer has abandoned, so work is silently accounted against a map nothing will ever drain. Neither side takes a
 * lock, because the map is a {@link java.util.concurrent.ConcurrentHashMap} and identity was never expected to move.
 * <p>
 * Two doors lead to a replacement, and each rule below shuts one:
 * <ul>
 *     <li>the Lombok {@code @Setter(AccessLevel.PACKAGE)}, which exists so {@code ShardManagerTest} can install a
 *     pre-populated map and has no other caller anywhere;</li>
 *     <li>a direct assignment inside {@link ShardManager} itself, which would bypass the setter and therefore the
 *     first rule.</li>
 * </ul>
 * Until now both were held by nothing but the reviewer noticing. Package-private plus a {@code // visible for
 * testing} comment does not fail a build, and the neighbouring {@link RetryQueue} carries three accessors annotated
 * exactly that way - plus a fourth on {@link ShardManager} itself - that no test anywhere calls, so the annotation
 * is not evidence of a boundary anybody is holding.
 * <p>
 * Tests may replace the map, which is the setter's whole purpose, so this analyses main classes only.
 * <p>
 * If a rule here fails, do not delete it and do not add the new caller to an allowlist: give {@link ShardManager} a
 * method that mutates the existing map, or - if the reference genuinely must move - work out what the poll thread
 * and the controller thread are each holding at that moment first.
 * <p>
 * Scope, stated because a wider rule was considered and rejected: this does NOT police keyed reads of the map.
 * Only one main-code access to the field is {@code getShard}; nearly all the rest are bulk operations over
 * {@code values()}, {@code keySet()}, {@code size()} or the whole map, which {@code getShard(key)} cannot express,
 * and the keyed ones that remain include both writes. A rule requiring the accessor is therefore red on arrival and
 * could only be greened with an allowlist of the methods already there - a frozen baseline, which
 * docs/inflight/static-archunit-main-code-rules.md rules out. The check-then-get seam that motivated the idiom is
 * machine-checked instead by fb-contrib's {@code MUI_CONTAINSKEY_BEFORE_GET}; ArchUnit sees field accesses, not what
 * is invoked on the value they load, so it could never have seen that seam.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer", importOptions = ImportOption.DoNotIncludeTests.class)
class ShardMapIsNeverReplacedArchTest {

    private static final String SHARD_MAP_FIELD = "processingShards";

    private static final String SHARD_MAP_SETTER = "setProcessingShards";

    @ArchTest
    static final ArchRule shard_map_setter_has_no_production_callers =
            noClasses()
                    .should().callMethod(ShardManager.class, SHARD_MAP_SETTER, Map.class)
                    .because("the shard map is shared, unlocked, between the broker-poll thread and the controller "
                            + "thread, so replacing the reference strands whichever of them already loaded the old "
                            + "one - the setter exists only so a test can install a pre-populated map");

    @ArchTest
    static final ArchRule shard_map_field_is_assigned_only_at_construction =
            noClasses()
                    .should().setFieldWhere(assignsTheShardMapAfterConstruction())
                    .because("an assignment inside ShardManager would replace the shared map without going through "
                            + "the setter, so it would bypass the rule above as well as every reader holding the "
                            + "previous reference");

    private static DescribedPredicate<JavaFieldAccess> assignsTheShardMapAfterConstruction() {
        return DescribedPredicate.describe(
                "assigns ShardManager." + SHARD_MAP_FIELD + " outside its constructor and its package-private setter",
                access -> access.getTargetOwner().isEquivalentTo(ShardManager.class)
                        && SHARD_MAP_FIELD.equals(access.getTarget().getName())
                        && !access.getOrigin().isConstructor()
                        && !SHARD_MAP_SETTER.equals(access.getOrigin().getName()));
    }

    /**
     * Both rules name their target as a string, and ArchUnit does not check that the target exists: rename the field
     * or drop the Lombok setter and each rule matches nothing, passes, and has asserted nothing. That is the silent
     * false-green this repo keeps finding, so pin the names the rules depend on.
     * <p>
     * The setter is Lombok-generated, which is exactly why it needs pinning - removing one annotation deletes it,
     * and no compile error anywhere says so.
     */
    @Test
    void theSetterTheRulesNameStillExists() throws NoSuchMethodException {
        assertThat(ShardManager.class.getDeclaredMethod(SHARD_MAP_SETTER, Map.class)).isNotNull();
    }

    @Test
    void theFieldTheRulesNameStillExists() throws NoSuchFieldException {
        assertThat(ShardManager.class.getDeclaredField(SHARD_MAP_FIELD)).isNotNull();
    }
}
