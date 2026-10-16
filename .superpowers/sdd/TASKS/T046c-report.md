# T046c report — badge sync via subscription.state/done events

## Status
DONE. Frontend-only change to `frontend/src/views/ConsumeView.vue` (+ rebuilt `frontend/dist`). No Rust source changes, no events.ts changes, no new deps.

## Grep-verified wire facts (anti-phantom)

Event name constants (`crates/kafka-core/src/protocol.rs:51-52`):
- `SUBSCRIPTION_STATE: &str = "subscription.state"`
- `SUBSCRIPTION_DONE: &str = "subscription.done"`

Payload DTO shapes (`crates/kafka-app/src/bus.rs:68-81`, `rename_all = "camelCase"`):
- `subscription.state` → `SubscriptionState { subscription_id: SubscriptionId, state: SubscriptionStatus, reason: Option<String> }`
  wire JSON: `{subscriptionId, state, reason?}`
- `subscription.done` → `SubscriptionDone { subscription_id: SubscriptionId, delivered: u64, reason: Option<String> }`
  wire JSON: `{subscriptionId, delivered, reason?}`

`SubscriptionStatus` (`bus.rs:28-35`, `rename_all = "snake_case"`): `"running" | "paused" | "done"`.

## Implementation (ConsumeView.vue)

- `subDone` ref + `markDone()`; handlers `onSubState` / `onSubDone` filter by `subscriptionId === subId` (same guard pattern as existing `onMessage`).
- `subscription.state`: flips badge `running`/`paused`; `state === "done"` also routes to `markDone()`.
- `subscription.done`: `markDone()` → `onMessage` guard stops table appends, control bar (pause/resume/seek inputs+buttons) greyed out via `:disabled="subDone"`, badge shows 「已结束」 (slate), hint 「订阅已结束」 shown.
- Listeners registered in `onMounted` via `on(...)` and cleaned in `onBeforeUnmount` (`offSubState?.()`, `offSubDone?.()`) — matches existing `events.ts on()` cleanup pattern.
- `subDone` reset to `false` on new `subscribe()`.

## Lines added
~30 new source lines in ConsumeView.vue (script +26, template +4 net incl. in-place disabled/class edits). Under 60 budget.

## Verification (R23)
1. `npm run build` — OK, new bundle `dist/assets/index-CFsVhe-o.js` (139.23 kB), index.html hash swapped.
2. `cargo build -p main-bin` — `Finished dev profile ... in 4.70s` (PowerShell exit-code artifact from stderr redirect only).
3. Smoke: launched `target\debug\main-bin.exe`, 8s, `CloseMainWindow()` → `Closed=True Exited=True`; no app ERROR logs (only WebView2 internal diagnostic logs).
4. `git status` — zero Rust changes (only foreign uncommitted `agents.md` + untracked `.opencode/`, untouched).

## Foreign changes noted
`agents.md` modified and `.opencode/` untracked pre-existed this task; not touched, not committed.
