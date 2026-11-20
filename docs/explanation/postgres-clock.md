# Postgres-clock leases

I1–I8 decisions use `now()` in SQL, not the broker wall clock.

- Register: `lease_expires_at = now() + lease`.
- Heartbeat: extends only when epoch matches **and** `lease_expires_at > now()` (A39). A paused broker cannot refresh after unpause and commit under the old epoch.
- Orphan TTL, grace, producer expiry: Postgres timestamps.

A broker with a stale epoch gets `NOT_LEADER_OR_FOLLOWER` / commit reject, then re-registers (A7).
