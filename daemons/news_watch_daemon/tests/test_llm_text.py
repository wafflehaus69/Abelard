"""Tests for the shared LLM-output fence stripper."""

from __future__ import annotations

from news_watch_daemon.llm_text import strip_code_fences


def test_no_fence_returns_stripped():
    assert strip_code_fences('  {"a": 1}  ') == '{"a": 1}'


def test_json_fence_stripped():
    assert strip_code_fences('```json\n{"a": 1}\n```') == '{"a": 1}'


def test_bare_fence_stripped():
    assert strip_code_fences('```\n{"a": 1}\n```') == '{"a": 1}'


def test_fence_with_surrounding_whitespace():
    assert strip_code_fences('  \n```json\n{"a": 1}\n```\n  ') == '{"a": 1}'


def test_idempotent():
    once = strip_code_fences('```json\n{"a": 1}\n```')
    assert strip_code_fences(once) == once


def test_plain_json_untouched():
    assert strip_code_fences('{"events": [], "narrative": "x"}') == \
        '{"events": [], "narrative": "x"}'


def test_empty_and_whitespace():
    assert strip_code_fences("") == ""
    assert strip_code_fences("   \n  ") == ""
