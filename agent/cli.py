"""
@deprecated: 已迁移至 agent.direct.cli
请使用: python -m agent.direct.cli
"""

from agent.direct.cli import build_parser, main  # noqa: F401

if __name__ == "__main__":
    main()
