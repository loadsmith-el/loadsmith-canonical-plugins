# `.agents/` — source of truth for AI agents

This folder defines how AI agents operate in this repository. It is the shared,
**agent-agnostic** knowledge base read directly by the multi-agent ecosystem
(Codex, Gemini, …). Claude reaches the same content through the root `CLAUDE.md`,
which is a thin pointer to [AGENTS.md](AGENTS.md).

## Layout

```
.agents/
└── AGENTS.md     ← operating instructions for this repo (start here)
```

## Skills convention

This repo has no command/skill entry points yet. When one is added, put its real,
agent-agnostic logic in `.agents/skills/<name>.md` and keep the Claude-specific
slash command in `.claude/commands/<name>.md` as a thin **stub** (frontmatter +
a redirect here), so the knowledge base stays centralized in `.agents/`.
