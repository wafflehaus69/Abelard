"""CD-DASH2 P2 — the digest subcommand cannot reach an outward send.

Proved over the call graph, not asserted in prose. The governing principle is
that files may be automated and outward sends may not, so `digest` is the only
subcommand that gets a schedule — and the property that makes that safe has to
be mechanically checkable, or it is just a promise about today's code.
"""
import ast
import inspect
import pathlib

from abelard_queue import consumer

# Every function in this module that can put bytes on the wire, plus the
# requests entry points they use.
SENDING_NAMES = {"send_telegram", "run_dispatch", "post", "requests"}


def _module_tree():
    return ast.parse(pathlib.Path(inspect.getfile(consumer)).read_text(encoding="utf-8"))


def _functions(tree):
    return {n.name: n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _calls(node):
    out = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Name):
                out.add(f.id)
            elif isinstance(f, ast.Attribute):
                out.add(f.attr)
                if isinstance(f.value, ast.Name):
                    out.add(f.value.id)
    return out


def _reachable(start, funcs, seen=None):
    """Every function name reachable from `start`, transitively."""
    seen = seen if seen is not None else set()
    for name in _calls(funcs[start]):
        if name in seen:
            continue
        seen.add(name)
        if name in funcs:
            _reachable(name, funcs, seen)
    return seen


def test_run_digest_cannot_reach_a_send():
    """The load-bearing property: scheduling digest schedules no outward act."""
    funcs = _functions(_module_tree())
    reach = _reachable("run_digest", funcs)
    offending = reach & SENDING_NAMES
    assert not offending, "digest can reach {}".format(sorted(offending))


def test_run_dispatch_CAN_reach_a_send():
    """Control: if this fails the graph walk is broken and the test above is
    vacuous — a check that cannot detect the bad case proves nothing."""
    funcs = _functions(_module_tree())
    reach = _reachable("run_dispatch", funcs)
    assert reach & SENDING_NAMES, "graph walk found no send from run_dispatch"


def test_the_digest_branch_in_main_calls_only_run_digest():
    """Reachability is necessary but not sufficient — main() dispatches on a
    string, so the branch itself is checked."""
    funcs = _functions(_module_tree())
    main = funcs["main"]
    for node in ast.walk(main):
        if not isinstance(node, ast.Compare):
            continue
        if not any(isinstance(c, ast.Constant) and c.value == "digest"
                   for c in node.comparators):
            continue
        parent = next((p for p in ast.walk(main)
                       if isinstance(p, ast.If) and p.test is node), None)
        assert parent is not None
        called = _calls(parent) - {"len"}
        assert "run_digest" in called
        assert not (called & SENDING_NAMES), \
            "digest branch reaches {}".format(sorted(called & SENDING_NAMES))
        return
    raise AssertionError("no digest branch found in main()")
