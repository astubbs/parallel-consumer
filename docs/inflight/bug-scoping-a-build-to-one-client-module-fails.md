# `-pl :<a client module>` fails on its own, and the message names the wrong thing

Found 2026-08-17 while proving the foreign client builds actually compile (astubbs#242). Open, not
fixed.

## What happens

```
./mvnw compile -Dpc.foreignClients -pl :parallel-consumer-proxy-client-go
```

```
Rule 4: org.apache.maven.enforcer.rules.ReactorModuleConvergence failed with message:
Module parents have been found which could not be found in the reactor.
 module: bz.stub.parallelconsumer:parallel-consumer-proxy-client-go:pom:0.6.0.0-SNAPSHOT
```

`ReactorModuleConvergence` is one of the rules in the root pom's `enforce-versions` execution, and it
requires every module's parent to be *in the reactor*. `-pl` on its own builds one module and no
ancestors, so the rule fires on any scoped build in this repo - the client modules are simply where a
person is most likely to try it.

## Why it is worth fixing rather than knowing

**The message describes the enforcer's world, not the user's mistake.** Someone scoping a build to one
language reads "module parents ... could not be found" as a broken pom, not as "add `-am`". The two
workarounds - `-am`, or `-Denforcer.skip=true` - are both discoverable only by already knowing the
answer.

It also collides directly with what the per-language wave workflow encourages: one language at a time,
in its own module. `bin/build-client.sh` sidesteps it by computing a module list and always passing
`-am` (`maven_module_list`, `build_natively`), so **the script is fine and the hand-typed command is
not** - which is the worst division, because the script is what CI runs and the hand-typed form is
what a human reaches for.

## Candidate fixes, none chosen

- **Scope the rule** so `ReactorModuleConvergence` only runs when the whole reactor is present. It has
  no built-in condition for that, so this likely means moving that one rule into a profile that a
  scoped build does not activate - and then the rule silently stops covering scoped builds, which is
  the trade to weigh.
- **Say so in the message.** `maven-enforcer-plugin` supports a `<message>` per rule; a line reading
  *"scoped builds need -am"* costs nothing and removes the whole confusion, without weakening the
  rule. **Cheapest, and probably the right one.**
- **Document it** in the clients README as a known invocation, which is the weakest option and the one
  that rots.

## Not to be confused with

The **foreign clients not building under a bare `mvn compile`** - that is by design and now says so in
the reactor line (fixed 2026-08-17). And the **cold-repository `validate`** question, which is
[`next-nothing-guards-the-cold-build.md`](next-nothing-guards-the-cold-build.md). All three surfaced in
the same session and are three different things.
