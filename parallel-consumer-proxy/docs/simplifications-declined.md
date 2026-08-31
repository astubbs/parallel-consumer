<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Simplifications considered and declined - do not re-propose without new evidence

From the 2026-08-17 simplification pass over the proxy (astubbs#242). **The declined ones are
recorded because the next pass will find them again**, and re-deriving why each is wrong costs more
than reading it. None is deferred work; each is a decision that the current code is better.

**Where the reason is subtle, the code should carry it too** - a note here stops a human, a comment
at the site stops an automated pass. The one marked **(comment)** below is where the diff looks
strictly better and only the reasoning says otherwise.

This file lists only the candidates whose subject is in this module today. The pass covered the whole
sidecar; each remaining entry travels with the code it is about, as that code arrives.

Transport:

- **(comment)** Swapping `AuthorityAllowlistInterceptor`'s hand-written host normaliser for Guava's
  `HostAndPort`. Guava **throws** on bare unbracketed IPv6, which that method deliberately admits - a
  security-posture change wearing a library swap.
- A shared logback-capture test helper; the two call sites do different jobs.
- Any change to `AuthorityAllowlistInterceptor` or `SingleConnectionGuard` - security posture, and
  their apparent simplicity is the point.
