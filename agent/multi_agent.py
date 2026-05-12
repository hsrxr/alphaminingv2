"""
@deprecated: 已迁移至 agent.multi.cli
请使用: python -m agent.multi.cli
"""

from agent.multi.cli import build_parser, main  # noqa: F401

if __name__ == "__main__":
    main()
