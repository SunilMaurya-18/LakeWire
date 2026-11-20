# Upload-intent race

A broker can PUT an object, then die before `sl_commit_object`. Another broker must not GC that key until **orphan TTL** after the intent, and must not ack until PUT **and** commit return.

If a zombie resumes after the lease expired and tries to commit, `sl_commit_object` rejects the fenced epoch. The object without a live segment row is an orphan; workers delete it after TTL + GC period (I8). A committed object must never be deleted while a segment references it (I7).

The pause nemesis (docker pause longer than the lease) is the test for this race. See `docs/reports/bugs-found.md`.
