# M6 Native TERM Runtime Receipt

**Goal:** Qualify the retained-stop TERM path on a real cluster, which the [UID receipts, retention and TERM amendment](2026-11-10-m6-final-wiring-receipts-retained-stop-amendment.md) makes mandatory before activation.

**Where it runs:** The single qualifying kind RUN (M7-RUN), inside the M7-FAULTS suspend/resume and deletion-cleanup specs.
No separate cluster or image is needed.

**Checker:** Operator `hack/native-term-receipt.py`, with its tests in `hack/test_native_term_receipt.py`.

## Why a receipt and not a host test

The amendment forbids inferring signal or process closure from a host unit test.
The source-level parts are covered by `TestNativeTermRequest`, `TestNativeStopStartupRace`, `TestNativeRetainedStop*` and `TestNativeDispositionActiveSuspend`.
What only a cluster can show is that the real requester stops the real engine PID 1 and that the Job, Pod and PVC survive with their identities.
The exec call itself leaves no trace in API objects, so the receipt proves the outcome and its order, not the call.

## What the fault spec captures

Capture one receipt for each case while a native follow worker is running, before cutover:

- `suspend`: patch `spec.suspend=true`, wait for `status.phase=Suspended`.
- `delete`: delete the Migration, wait until it is gone.

Write each receipt as JSON into the RUN directory, for example `native-term-16-16-suspend.json`.
Objects are the raw API JSON a live GET returns.

- `schema`: `rpgmigrate.native-term-receipt/v1`.
- `case`: `suspend` or `delete`.
- `runnerImage`: the runner reference with its `@sha256:` digest.
- `requestedAt`: the UTC time the spec sent the patch or the delete.
- `before`: `migration`, `job`, `pod` and `pvc`, read just before the request.
- `terminated`: `migration`, `job` and `pod`, from the first watch event where the worker container is terminated.
- `after`: for `suspend`, `migration`, `job`, `pod` and `pvc` read once the phase is `Suspended`; for `delete`, `migration` is `null`.
- `foreign`: `before` and `after` lists of `{kind, namespace, name, uid, resourceVersion}` for every object in the namespace that the Migration does not own, including at least one sentinel the spec creates.

## What the checker requires

1. The recorded `nativeJobUID` and `nativePVCUID` equal the live Job and PVC, and the ownership chain is Migration, Job, Pod.
1. Before the request, the single worker container is running, has not restarted, and runs the pinned runner digest.
1. The terminated Pod and container are the same ones, still with zero restarts.
1. The container exited with an engine error code and no signal.
   Exit 0 is the natural-completion race, and 137 or 143 mean the process died from a signal, so none of them proves a handled TERM.
1. The exit happened after the request and within 120 seconds of it: the 90-second stop bound plus 30 seconds for the next reconcile to see the intent.
1. When the worker stopped, the Migration already carried the intent and still had its receipts and E; on delete, the cleanup finalizer was still present.
1. On suspend, the Migration is `Suspended` with the same receipts and E, and the Job, Pod and PVC keep their UIDs, are not deleting, and the Job is `Failed` with no TTL and no `spec.suspend`.
1. On delete, the Migration is released.
1. The foreign inventory is nonempty and identical before and after.

Run it on each receipt; exit status 0 is a pass and the verdict JSON carries the receipt SHA-256:

```sh
python3 hack/native-term-receipt.py RUN/native-term-16-16-suspend.json
```

## Acceptance

One passing `suspend` receipt and one passing `delete` receipt, cited by path and SHA-256 in the M6 close packet.
Which native pairs must carry them is a leader decision; the TERM path does not depend on the PostgreSQL major.

## Not covered

- A lost exec reply and the handler-not-ready startup race need fault injection inside the requester; they stay source-level only.
- Retry quiescence after resume belongs to the M7-FAULTS suspend/resume data checks.
- The receipt does not bind the engine's stderr diagnostic text, which is not a stable contract.
