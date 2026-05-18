from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EngineRuntimeOptions:
    config: str | None = None
    executable: str = ""
    repo_root: str | None = None


@dataclass(frozen=True)
class WorkflowEngineOptions:
    crest: EngineRuntimeOptions
    xtb: EngineRuntimeOptions
    orca: EngineRuntimeOptions

    @classmethod
    def from_values(
        cls,
        *,
        crest_auto_config: str | None,
        crest_auto_executable: str,
        crest_auto_repo_root: str | None,
        xtb_auto_config: str | None,
        xtb_auto_executable: str,
        xtb_auto_repo_root: str | None,
        orca_auto_config: str | None,
        orca_auto_executable: str,
        orca_auto_repo_root: str | None,
    ) -> WorkflowEngineOptions:
        return cls(
            crest=EngineRuntimeOptions(
                config=crest_auto_config,
                executable=crest_auto_executable,
                repo_root=crest_auto_repo_root,
            ),
            xtb=EngineRuntimeOptions(
                config=xtb_auto_config,
                executable=xtb_auto_executable,
                repo_root=xtb_auto_repo_root,
            ),
            orca=EngineRuntimeOptions(
                config=orca_auto_config,
                executable=orca_auto_executable,
                repo_root=orca_auto_repo_root,
            ),
        )

    def for_engine(self, engine: str) -> EngineRuntimeOptions | None:
        normalized = engine.strip().lower()
        if normalized == "crest":
            return self.crest
        if normalized == "xtb":
            return self.xtb
        if normalized == "orca":
            return self.orca
        return None

    @property
    def crest_auto_config(self) -> str | None:
        return self.crest.config

    @property
    def crest_auto_executable(self) -> str:
        return self.crest.executable

    @property
    def crest_auto_repo_root(self) -> str | None:
        return self.crest.repo_root

    @property
    def xtb_auto_config(self) -> str | None:
        return self.xtb.config

    @property
    def xtb_auto_executable(self) -> str:
        return self.xtb.executable

    @property
    def xtb_auto_repo_root(self) -> str | None:
        return self.xtb.repo_root

    @property
    def orca_auto_config(self) -> str | None:
        return self.orca.config

    @property
    def orca_auto_executable(self) -> str:
        return self.orca.executable

    @property
    def orca_auto_repo_root(self) -> str | None:
        return self.orca.repo_root
