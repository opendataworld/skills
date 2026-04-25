# Repository Agent Instructions

Scope: entire repository.

This repository follows the shared OpenAutonomyX instruction layer in `openautonomyx/common-instructions` and is scoped to OpenDataWorld skill documentation and reusable skill assets.

## Shared references

Use these shared references as the default baseline:

- `standards/engineering-execution-standard.md`
- `policies/context-and-guardrails-policy.md`
- `policies/test-and-process-improvement-policy.md`
- `policies/airgapped-operation-policy.md`

Do not duplicate shared policies or reusable prompt packs here unless this repository explicitly owns the skill-specific content.

## In scope

- Skill documentation
- Skill metadata and usage notes
- Skill examples and evaluation notes
- Review logs for substantial skill changes
- OpenDataWorld-specific skill guidance

## Out of scope

- Organization-level vision or strategy source documents
- Platform implementation code that belongs in product repositories
- Large datasets or binary model artifacts
- Generic shared prompts that belong in `common-instructions`

## Documentation rules

1. Keep skill docs concise and execution-oriented.
2. Use clear in-scope and out-of-scope boundaries.
3. Include usage examples and expected outputs when helpful.
4. Document dependencies, risks, and review requirements.
5. Record substantial changes in `reviews/` when applicable.
6. Require reviewer approval and HITL sign-off for production-facing skill behavior.
