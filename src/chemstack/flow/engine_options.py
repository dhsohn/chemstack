from __future__ import annotations

from dataclasses import dataclass

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE


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
        crest_config: str | None = None,
        crest_executable: str = "chemstack_crest",
        crest_repo_root: str | None = None,
        xtb_config: str | None = None,
        xtb_executable: str = "chemstack_xtb",
        xtb_repo_root: str | None = None,
        orca_config: str | None = None,
        orca_executable: str = CHEMSTACK_EXECUTABLE,
        orca_repo_root: str | None = None,
    ) -> WorkflowEngineOptions:
        return cls(
            crest=EngineRuntimeOptions(
                config=crest_config,
                executable=crest_executable,
                repo_root=crest_repo_root,
            ),
            xtb=EngineRuntimeOptions(
                config=xtb_config,
                executable=xtb_executable,
                repo_root=xtb_repo_root,
            ),
            orca=EngineRuntimeOptions(
                config=orca_config,
                executable=orca_executable,
                repo_root=orca_repo_root,
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
    def crest_config(self) -> str | None:
        return self.crest.config

    @property
    def crest_executable(self) -> str:
        return self.crest.executable

    @property
    def crest_repo_root(self) -> str | None:
        return self.crest.repo_root

    @property
    def xtb_config(self) -> str | None:
        return self.xtb.config

    @property
    def xtb_executable(self) -> str:
        return self.xtb.executable

    @property
    def xtb_repo_root(self) -> str | None:
        return self.xtb.repo_root

    @property
    def orca_config(self) -> str | None:
        return self.orca.config

    @property
    def orca_executable(self) -> str:
        return self.orca.executable

    @property
    def orca_repo_root(self) -> str | None:
        return self.orca.repo_root
