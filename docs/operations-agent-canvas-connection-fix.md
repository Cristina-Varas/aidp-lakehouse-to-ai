# Canvas connection compatibility — 2026-11-04

The owner reported `AFV_NODE_DISCONNECTED` for both **Chat trigger** and
**Operations adviser** in Workbench. Read-only Ask-AIDP inspection verified the
canonical agent and workspace before changing its draft.

The saved graph contained one `start` → `planner` edge, but used `bottom` and
`top` as `srcNodeOutput` and `destNodeInput`. Those identify visual port sides;
the inspected Workbench public client requires structural port names. Its
`hydrateDiagramEdgesForEditor` drops edges whose port names do not equal
`getCanonicalNodeOutputName(source.type)` and
`getCanonicalNodeInputName(target.type)`. For these node types they are:

```json
{
  "srcNodeId": "start",
  "destNodeId": "planner",
  "srcNodeOutput": "START_NODE_OUTPUT",
  "destNodeInput": "AGENT_INPUT"
}
```

The server's native validator already accepted the old persisted graph. Its
success alone did not reproduce or disprove the editor's reported error. The
compatibility issue was identified by inspecting the editor's actual hydration
and canonical-port code, plus the installed AIDP SDK's `AgentEdge` schema.

## Executed repair

- Preserved the original complete draft and a restoration request outside Git,
  under `/private/tmp/aidp-existing-mcp/operations-agent-0.6.0/1104-connection-*`.
- Immediately reread the draft and required equality with the inspected snapshot
  before the single update. The service did not expose an ETag on those reads.
- Changed only `srcNodeOutput` and `destNodeInput` on the existing edge; preserved
  its key, node identities, edge type and all other graph fields.
- Read back the updated graph and required exact equality with the proposed graph.
  Nodes, instructions, selected models, all seven tools, RAG configuration and
  other metadata remained unchanged.
- Ran native `agent validate-agent`: `isValidAgent=true`, `validationError=[]`.
- Did not redeploy, invoke chat, modify computes, rerun acceptance, train, score,
  or write business data. Existing deployment and app configuration are unchanged.

Reload the agent in Workbench to load the corrected draft before saving from an
old open editor tab. A full browser-editor validation after reload is not claimed
by this SDK repair receipt. Agent policy acceptance remains pending.

## Repository prevention

`native_canvas_diagram` now emits the structural port names. Regression coverage
checks conversion from both the old `output`/`input` fixture and the observed
`bottom`/`top` representation, unchanged inputs, preserved model/instructions/tool
configurations, and idempotent generation.

Executed locally:

```sh
.venv/bin/python -m unittest discover -s tests -p test_agent_native_sql.py -v
```

All **8 tests passed**. SQL parameter tests in this file use a local SQLite
adaptation; they do not establish native parameter guarantees. The native draft
readback/validation above are separate real AIDP operations. This repair needs no
wheel installation or notebook execution; the generator correction applies when
running the repository source. Existing published wheels were not rebuilt.
