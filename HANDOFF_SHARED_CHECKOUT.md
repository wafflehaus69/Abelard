# OPERATIONS HANDOFF — the shared checkout: collisions, sweeps, and overwrites

Issued by: the SC-1 (scout_daemon) build session, 2026-08-16
Target: every engineer and every Claude Code session working in `C:\Users\mdiba\Code\Abelard`
Status: **operational guidance + one unresolved gap for Mando.** Not doctrine. The
doctrine is `doctrine/ENGINEERING.md` — [E18], [E20], and its amendment R-B6-5 —
and where this document and the ledger disagree, **the ledger wins.**

Read §4 before your next commit. It is four commands.

---

## 0. The situation, in one paragraph

We have five or more concurrent workstreams committing into **one working tree**.
The doctrine already ruled the fix (a `git worktree` per workstream, E18 as
amended 2026-08-13), and **nobody is using it** — `git worktree list` returns a
single entry. The consequence is not hypothetical: work has been captured by
commits that did not describe it, and a commit deliberately held back for review
was published by somebody else's `git push`. Nothing has been lost. The cost so
far is false alarms, misattributed history, and one bypassed review gate.

---

## 1. What is already ruled — do not re-litigate

Read these before proposing anything. They are ratified.

| Entry | Rule |
|---|---|
| **E18** (+ amendment R-B6-5) | A working tree has one writer. **Serialization is withdrawn — a worktree per workstream is the standing mechanism.** |
| **E18 ledger protocol** | `doctrine/ENGINEERING.md` is the *named exception* that stays in the shared checkout. Entries commit **single-file, atomic, one entry per commit**. On a numbering collision the **later** writer renumbers their own entry. IDs are **stable once pushed**. |
| **E20** | Commit a tracked file in the **same turn you edit it**, or work in your own worktree. Stage by **explicit pathspec** — never `git add -A` or `git add .`. The repair window for a bad commit message **closes at push**, not at the next commit. |

Two things the amendment establishes that are easy to miss:

- **Pathspec staging is necessary and insufficient.** The one confirmed sweep
  (`97fa231`) happened *through* an explicit pathspec, because the foreign edit
  sat inside a file that session was legitimately editing. No staging discipline
  separates two writers inside one file. Only separate trees do.
- **The deciding incident was one failure, not two.** Don't inflate the count.

---

## 2. The measurement — what "too many cooks" looks like right now

Taken 2026-08-16. Reproduce with the commands in §4.

```
worktrees in use                     1        (E18 says one per workstream)
workstreams committing in last 24h   5        consensus, capex_daemon,
                                              scout_daemon, smart_money_daemon,
                                              doctrine
uncommitted paths in the shared tree 61       across 8 areas
```

The 61 uncommitted paths are the **sweep surface** — anything a careless
`git add -A` in any session would capture:

| area | paths |
|---|---:|
| `daemons/chatter_daemon` | 21 |
| `daemons/biz_daemon` | 18 |
| `daemons/smart_money_daemon` | 9 |
| `daemons/common` | 7 |
| `scripts` | 2 |
| `daemons/news_watch_daemon` | 2 |
| `docs`, `consensus` | 1 each |

**One good-news finding: the ledger protocol is working.** Audited every commit
that ever touched `ENGINEERING.md` — exactly one was multi-file (`97fa231`, the
known sweep), and the only multi-entry commit is `a325338`, which *created* the
ledger with its first 17 entries. Every commit since the protocol landed has been
single-file and single-entry. The remaining exposure is in code, not doctrine.

---

## 3. Incident record

Three collisions in one session, all on the shared tree. Listed so the failure
modes are recognisable, not to assign blame — every one of these was a normal
person doing normal work in a tree someone else was also writing.

1. **Content sweep — `97fa231`.** A doctrine entry (E19) sat edited-but-uncommitted
   while another session committed. Its commit message names only an APLD citation
   correction; its diff also carries all 26 lines of E19. The entry is findable in
   the file and unfindable in the log. *Cause: a tracked file left modified across
   turns.* Mitigation now ruled: E20.

2. **Numbering collision — E21 → E22.** Two sessions drafted a ledger entry for
   the same number concurrently. Resolved exactly as the protocol says: the later
   writer (this session) renumbered to E22 and recorded the collision in the entry.
   *This one worked.* It is the example to copy.

3. **Held commit published by a third party — `db81871`.** A commit was made
   deliberately unpushed, pending Mando's disk review. Another session ran
   `git push` on the shared branch minutes later and carried it to `origin/main`.
   The review gate was bypassed without anybody deciding to bypass it. **No
   ratified rule covers this** — see §5.

---

## 4. Detection — the commands, all verified

### 4.1 Before you start work (10 seconds)

```bash
git worktree list; git log --since='60 minutes ago' --oneline | wc -l; git status --porcelain | wc -l
```

Read it as: *how many trees exist*, *is anyone else committing right now*, *how
big is the sweep surface I am about to work beside*. If the tree count is 1 and
the recent-commit count is not 0, **you are sharing and someone else is active.**

### 4.2 Before you commit — never skip this

```bash
git diff --cached --name-status
```

Every path in that output must be one you touched on purpose. If a path you do
not recognise appears, **stop** — it belongs to another session. Unstage it with
`git restore --staged <path>` rather than committing it under your message.

Stage by explicit pathspec, always:

```bash
git add path/one.py path/two.py
```

### 4.3 After you commit, before you assume it is held

```bash
git rev-list --count origin/main..HEAD
```

Then, for a specific commit you intend to hold:

```bash
git merge-base --is-ancestor <sha> origin/main && echo PUBLISHED || echo local-only
```

**Do not infer publication state from memory or from a truncated `git log`.**
E18's corollary exists because non-linear history truncates misleadingly.

### 4.4 Periodic audit — has the ledger protocol held?

```bash
for sha in $(git log --format='%h' -- doctrine/ENGINEERING.md); do printf "%s files=%s entries=%s %s\n" "$sha" "$(git show --stat --format='' $sha | grep -c '|')" "$(git show $sha -- doctrine/ENGINEERING.md | grep -c '^+## E[0-9]')" "$(git log -1 --format='%s' $sha | cut -c1-45)"; done
```

Any row with `files>1` is a sweep or a protocol violation. Any row with
`entries>1` after `a325338` is a batching violation.

---

## 5. The gap — a held commit has no protection

E18 protects the tree. E20 protects the file and the commit. **Nothing protects
an unpushed commit from another session's push.** `git push` on a shared branch
publishes every ancestor, including commits a different session was holding for
review, and git records no pusher identity in a plain clone, so it is not even
attributable afterwards.

This matters because "commit it, hold for review" is the standing review gate for
work that touches production — it is how `news_watch`'s BASILIC_MANUAL describes
the boundary (*"You never auto-commit or auto-push… Stop at the commit
boundary"*). That gate currently depends on every other session choosing not to
push, which is not a mechanism.

**Options, for Mando to rule — I am not picking one:**

- **(a) Worktrees actually adopted.** Already ruled in E18; simply not practised.
  A worktree on its own branch cannot be published by another session's push of
  `main`. This costs nothing new and closes the gap as a side effect.
- **(b) Hold on a branch, never on `main`.** Work that must not publish gets
  committed to `wip/<workstream>` instead of sitting unpushed on `main`.
- **(c) Accept it and drop the "held" language.** If anything on `main` may be
  published at any moment, then say so, and stop describing commits as held —
  a gate that only sometimes holds is worse than a documented absence of one.

My recommendation is **(a)**, because it is already ruled and needs adoption
rather than a new rule. But the ruling is Mando's.

---

## 6. The short version

1. **Take a worktree.** `git worktree add ../abelard-<workstream> -b <workstream>`.
   It is the standing mechanism and almost nobody is using it.
2. **Stage by explicit path. Read `--cached --name-status` before every commit.**
3. **Commit a tracked file in the turn you edit it.** Do not leave it modified
   across turns on the shared tree.
4. **Ledger entries go in the shared checkout only** — single file, one entry,
   atomic. Later writer renumbers on a collision.
5. **Verify publication state with `merge-base --is-ancestor`. Never assume.**
6. **Do not treat an unpushed commit on `main` as held** until §5 is ruled.

---

*Written from one session's vantage. The incident record in §3 is what this
session directly observed and verified; it is not a claim that these are the only
collisions that have occurred. A broader forensic pass over history was running
when this was written — if it surfaces more, append them to §3 rather than
rewriting it.*
