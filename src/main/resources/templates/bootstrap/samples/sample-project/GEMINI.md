# GEMINI.md

Guidance for Gemini working in this **Starlake / Starflow** project.

The full contract is in [AGENTS.md](AGENTS.md) — what this project contains, where
to look things up, and how to hand back a change. Read it before your first edit.
The rules below are the part you must never skip.

@AGENTS.md

## Ground rules

**1 — Never assume. Read the project first.**
Do not answer a question about this project from a template, from a sample you
have seen elsewhere, or from an earlier turn in the conversation. List the
directory, open the file, run the command. A name that looks familiar because
another Starlake project uses it is a reason to check, not a reason to trust.

**2 — Never invent syntax.** YAML keys, CLI flags, expectation macro names and
write strategies either exist in this project's authorities or they do not
exist. YAML keys → `metadata/starlake.json`, the JSON Schema for every `.sl.yml`.
Expectation macros → `ls metadata/expectations/`, and that listing is the whole
list. CLI flags → `starlake <command> --help`. A key you half-remember from
documentation is not evidence: if you cannot point to where a name comes from,
do not write it.

**3 — Cite where each answer came from.** For every non-trivial claim, name the
file you read or the command you ran. "According to `metadata/load/x/y.sl.yml`"
is an answer; "typically, Starlake uses…" is a guess wearing an answer's clothes.

**4 — Run the gate. Do not predict it.** After any change to `metadata/`, run
`starlake validate` — and `starlake test` / `starlake dag-generate` when they
apply — and report the real output. Never write "this should validate".

**5 — Change only what was asked.** One table, one task, one file. Do not
reformat, reorder attributes, "fix" neighbouring files, or delete comments you
did not write. Attribute order is part of a table's contract.

**6 — "I checked, and it is not there" is a correct answer.** So is "I don't
know". A plausible answer that turns out to be invented costs more than a
question.
