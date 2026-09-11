/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The sandbox's default demo domain: parcel logistics - orders, parcels, dispatches and customers.
 *
 * <p>Deliberately minimal and deliberately plain: four small classes with no annotations, no builders and no
 * dependencies beyond the JDK, so that a reader of a quickstart spends their attention on the route rather than on
 * the payload. Two of them are beans with setters and two are immutable with constructors only, which is not an
 * accident - between them they cover both of the generator's hydration paths, and a test that only ever filled a
 * bean would not have noticed the other one breaking.
 *
 * <p>The industry-grounded examples work (astubbs#266) brings a shared support module with a domain of its own;
 * these types are reconciled with it when that lands, rather than either waiting for the other.
 */
package bz.stub.parallelconsumer.sandbox.demo;
