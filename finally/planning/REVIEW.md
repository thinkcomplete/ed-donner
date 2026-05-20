# Review of `PLAN.md`

## Findings

1. **Current-price coverage is underspecified for held tickers outside the watchlist.**  
   Severity: high  
   Sections: §6 Shared Price Cache, §8 Portfolio, §2 Manage the watchlist  
   The plan says the stream pushes prices for "all tickers known to the system" and then equates that to the single user's watchlist. That breaks once a user buys a ticker and later removes it from the watchlist: portfolio valuation, unrealized P&L, and trade validation still require live prices for held positions. The spec should explicitly define the tracked ticker set as at least `watchlist ∪ open positions`, otherwise backend and frontend agents can make incompatible assumptions.

2. **The API contract is too vague for endpoints the frontend depends on immediately.**  
   Severity: high  
   Sections: §8 API Endpoints, §10 Frontend Design  
   `GET /api/portfolio`, `GET /api/watchlist`, and `POST /api/chat` are described only in prose. There are no response examples, field names, nullability rules, or error payload shapes. Given that multiple agents are expected to build against this plan independently, this is a coordination risk: small schema mismatches here will cascade into broken UI wiring and brittle E2E tests.

3. **Trade-failure handling in the chat flow is internally inconsistent.**  
   Severity: high  
   Sections: §9 How It Works, §9 Auto-Execution  
   The plan says the LLM returns structured output, then the backend executes trades, then "if a trade fails validation, the error is included in the chat response so the LLM can inform the user." At that point the LLM has already spoken. The spec needs one concrete behavior: either the backend augments the assistant message after execution, or the API returns a separate `errors` / `execution_results` field that the frontend renders independently.

4. **Mock LLM behavior is not defined tightly enough for deterministic tests.**  
   Severity: medium  
   Sections: §5 Environment Variables, §9 LLM Mock Mode, §12 E2E Tests  
   `LLM_MOCK=true` is intended to support reproducible E2E runs, but the plan never defines what the mock returns for a given input. Without a fixed contract, test authors and backend implementers can each invent different mock semantics, which defeats determinism. The plan should specify either a static canned response or a small explicit rule set.

5. **"Daily change %" is specified in the UI without a source of truth in simulator mode.**  
   Severity: medium  
   Sections: §2 What the User Can Do, §6 Simulator, §10 Layout  
   The simulator is continuous GBM seeded from initial prices, but the UI asks for daily change percentages. There is no defined reference price for "daily." That ambiguity will show up immediately in both watchlist rows and positions tables. The plan should define a baseline such as session-open seed price, midnight snapshot, or remove the metric in simulator mode.

6. **Dynamic ticker onboarding is undefined for the simulator path.**  
   Severity: medium  
   Sections: §6 Simulator, §6 Massive API, §8 Watchlist  
   The Massive path explicitly polls the union of watched tickers; the simulator path says nothing about what happens when a new ticker is added mid-session. The missing pieces are whether generation begins immediately, how the initial seed is chosen, and what happens for unknown symbols. This needs to be specified or agents will implement incompatible behavior.

7. **The plan mixes product spec with agent-specific implementation instructions.**  
   Severity: low  
   Sections: §9 LLM Integration opening paragraph  
   "Use cerebras-inference skill" is a workflow instruction for coding agents, not product behavior. Keeping agent instructions inside the functional spec makes the document less stable as a system contract and harder to reuse outside this course setup.

8. **Charting guidance is technically inconsistent.**  
   Severity: low  
   Sections: §10 Technical Notes  
   The plan says a "canvas-based charting library" is preferred and then lists "Lightweight Charts or Recharts." Recharts is SVG-based, not canvas-based. That should be corrected so the frontend agent is not following contradictory guidance.

## Open Questions

1. Should database initialization happen at app startup or lazily on first request? The plan names both.
2. Is trade history intentionally stored-only, or should there be a `GET /api/trades` endpoint?
3. What should `GET /api/watchlist` return before a newly added ticker has a live price?
4. How many prior chat turns should be included in each LLM request?
5. What should empty states look like for the heatmap and P&L chart when there are no positions?

## Suggested Tightening

- Add concrete JSON examples for `GET /api/portfolio`, `GET /api/watchlist`, `POST /api/portfolio/trade`, and `POST /api/chat`, including error cases.
- Define the canonical tracked-ticker set once: `watchlist ∪ open positions`, for both pricing and streaming.
- Specify exact mock-chat behavior under `LLM_MOCK=true`.
- Resolve simulator-only UX ambiguities now: "daily change %", new ticker seeding, and missing-price handling.
- Move agent/course instructions into a separate notes file so `PLAN.md` remains a clean product and interface contract.
