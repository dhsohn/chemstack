from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EngineConfigOptions:
    config: str | None = None


@dataclass(frozen=True)
class EngineRuntimeOptions:
    config: str | None = None
    repo_root: str | None = None


@dataclass(frozen=True)
class WorkflowEngineOptions:
    crest: EngineConfigOptions
    xtb: EngineConfigOptions
    orca: EngineRuntimeOptions

    @classmethod
    def from_values(
        cls,
        *,
        crest_config: str | None = None,
        xtb_config: str | None = None,
        orca_config: str | None = None,
        orca_repo_root: str | None = None,
    ) -> WorkflowEngineOptions:
        return cls(
            crest=EngineConfigOptions(
                config=crest_config,
            ),
            xtb=EngineConfigOptions(
                config=xtb_config,
            ),
            orca=EngineRuntimeOptions(
                config=orca_config,
                repo_root=orca_repo_root,
            ),
        )

    @property
    def crest_config(self) -> str | None:
        return self.crest.config

    @property
    def xtb_config(self) -> str | None:
        return self.xtb.config

    @property
    def orca_config(self) -> str | None:
        return self.orca.config

    @property
    def orca_repo_root(self) -> str | None:
        return self.orca.repo_root
