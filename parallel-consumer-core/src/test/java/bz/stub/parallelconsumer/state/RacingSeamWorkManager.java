package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;

/**
 * A {@link WorkManager} that runs another thread's action at one exact instruction inside a production call,
 * so a concurrency window can be driven deterministically rather than raced for.
 * <p>
 * <b>The seam is the subclass's choice; the arming is shared.</b> Each subclass overrides whichever production
 * method sits at the window it is about, computes the real answer first, and calls {@link #fireOnceIfArmed()}
 * before returning it - so the interference lands between that method's answer and whatever the caller does
 * with it. That is the only part that differs between them, which is why everything else lives here.
 * <p>
 * <b>Firing is tracked in an explicit {@link #raceFired} flag, set at firing time, and the armed slot is
 * separate.</b> A cleared armed-slot cannot tell "armed, then fired" from "never armed", so a precondition
 * assertion built on it would pass on a test that forgot to arm - which is a test asserting nothing while
 * looking green. Every arm that drives a window asserts {@link #raceHasFired()} for that reason.
 * <p>
 * <b>One shot, deliberately.</b> The production call under test is often reached more than once in a scenario,
 * and re-firing a whole rebalance on each pass models nothing real.
 *
 * @author Antony Stubbs
 * @see WorkManagerStaleCheckDoubleLookupTest.RacingStaleCheckWorkManager
 * @see RetryQueueRequeueWindowTest.RequeueWindowWorkManager
 */
abstract class RacingSeamWorkManager extends WorkManager<String, String> {

    private transient Runnable interference = () -> {
        // not armed
    };

    private boolean armed;

    private boolean raceFired;

    RacingSeamWorkManager(PCModuleTestEnv module) {
        super(module, new DynamicLoadFactor(2, 4));
    }

    /**
     * Arm the seam with the other thread's action - typically a full production rebalance.
     */
    void arm(Runnable interference) {
        this.interference = interference;
        this.armed = true;
    }

    /**
     * Did the armed action actually run? A window-driving test must assert this, or it cannot tell a
     * reproduction from a scenario that never reached the seam.
     */
    boolean raceHasFired() {
        return raceFired;
    }

    /**
     * Called by a subclass from inside its chosen seam, after the real answer has been computed and before it
     * is returned.
     */
    protected void fireOnceIfArmed() {
        if (armed) {
            armed = false;
            raceFired = true;
            interference.run();
        }
    }
}
