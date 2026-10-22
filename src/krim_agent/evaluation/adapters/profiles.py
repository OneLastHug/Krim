"""可信host配置、固定campaign Store身份；公开ID不作为控制权限。"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from krim_agent.contracts import ServiceError, fingerprint

from ..domain import DatasetSpec, TaskSpec, clone, execution_conditions, neutral_fingerprint


def file_identity(path: Path, *, directory: bool = False) -> dict[str, Any]:
    for parent in [*reversed(path.parents), path]:
        if parent.is_symlink():
            raise ServiceError("target_binding_changed")
    st = path.lstat()
    if (
        not stat.S_ISDIR(st.st_mode)
        if directory
        else not stat.S_ISREG(st.st_mode) or st.st_nlink != 1
    ):
        raise ServiceError("target_binding_changed")
    body: dict[str, Any] = {"st_dev": st.st_dev, "st_ino": st.st_ino, "st_uid": st.st_uid}
    if not directory:
        body.update(type="regular", nlink=1)
    return body


def file_digest(path: Path) -> str:
    file_identity(path)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def program_materials(
    family: str,
    worker: Path,
    *,
    native_prefix: list[str] | None = None,
    native_files: dict[str, str] | None = None,
) -> dict[str, str]:
    """从实际路径/字节编译主体摘要；调用者不能用自报digest代替这些材料。"""
    runtime = {
        "executable": sys.executable,
        "realpath": str(Path(sys.executable).resolve()),
        "sha256": file_digest(Path(sys.executable).resolve()),
        "prefix": sys.prefix,
        "base_prefix": sys.base_prefix,
    }
    program = {
        "path": str(worker),
        "realpath": str(worker.resolve()),
        "sha256": file_digest(worker),
    }
    if native_prefix is None:
        chain: dict[str, Any] = {"runtime": runtime, "program": program}
        runtime_hash = neutral_fingerprint(runtime)
    else:
        material = {}
        for name, expected in (native_files or {}).items():
            actual = file_digest(Path(name))
            if actual != expected:
                raise ServiceError("target_binding_changed")
            material[name] = {"realpath": str(Path(name).resolve()), "sha256": actual}
        prefix_exe = Path(native_prefix[0])
        material[
            str(
                prefix_exe,
            )
        ] = {"realpath": str(prefix_exe.resolve()), "sha256": file_digest(prefix_exe.resolve())}
        chain = {"argv_prefix": native_prefix, "materials": material, "program": program}
        runtime_hash = neutral_fingerprint(material)
    here = Path(__file__).parent
    adapter_files = {"codex_cli.py": file_digest(here / "codex_cli.py")}
    if family == "claude":
        adapter_files["claude_code.py"] = file_digest(here / "claude_code.py")
    codec_files = {"codex_events.py": file_digest(here / "codex_events.py")}
    if family == "claude":
        codec_files["claude_events.py"] = file_digest(here / "claude_events.py")
    return {
        "executable_chain_digest": neutral_fingerprint(chain),
        "runtime_digest": runtime_hash,
        "adapter_digest": neutral_fingerprint(adapter_files),
        "codec_digest": neutral_fingerprint(codec_files),
        "process_owner_digest": file_digest(here / "process.py"),
    }


@dataclass(frozen=True)
class CampaignBootstrap:
    root: Path
    slots: dict[str, dict[str, Any]]
    fixture_only: bool = True
    campaign_id: str = "fixture-campaign"
    store_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    m0: dict[str, Any] | None = None

    def check(self) -> None:
        if not self.root.is_absolute() or ".." in self.root.parts or not self.slots:
            raise ServiceError("invalid_input")
        for parent in [*reversed(self.root.parents), self.root]:
            if parent.is_symlink():
                raise ServiceError("target_binding_changed")
        if not self.fixture_only:
            require_m0(self.m0)
        if any(s["scope"] != "local" or s["repetition"] != 0 for s in self.slots.values()):
            raise ServiceError("invalid_input")


@dataclass(frozen=True)
class CampaignStoreBinding:
    admission: dict[str, Any]

    @property
    def database(self) -> Path:
        return Path(self.admission["store_binding"]["database"])

    @property
    def marker(self) -> dict[str, Any]:
        return {
            k: clone(self.admission[k])
            for k in [
                "campaign_id",
                "campaign_store_id",
                "store_binding_digest",
                "slot_policy_digest",
            ]
        }

    def assert_store(self, path: Path, *, writer_fd: int | None = None) -> None:
        bound = self.admission["store_binding"]
        if str(path) != bound["database"] or ".." in path.parts:
            raise ServiceError("target_binding_changed")
        try:
            for key, identity, directory in [
                ("canonical_root", "root_identity", True),
                ("database", "database_identity", False),
                ("writer_lock", "writer_lock_identity", False),
            ]:
                actual = file_identity(Path(bound[key]), directory=directory)
                if actual != bound[identity] or actual["st_uid"] != os.getuid():
                    raise ServiceError("target_binding_changed")
            if writer_fd is not None:
                st = os.fstat(writer_fd)
                if (st.st_dev, st.st_ino, st.st_uid) != tuple(
                    bound["writer_lock_identity"][k] for k in ["st_dev", "st_ino", "st_uid"]
                ):
                    raise ServiceError("target_binding_changed")
            unsigned = {
                k: self.admission[k]
                for k in [
                    "campaign_id",
                    "campaign_store_id",
                    "store_binding",
                    "scope",
                    "slot_policy_digest",
                ]
            }
            if neutral_fingerprint(unsigned) != self.admission["store_binding_digest"]:
                raise ServiceError("target_binding_changed")
        except (OSError, ValueError, KeyError, TypeError):
            raise ServiceError("target_binding_changed") from None

    def slot(self, request: dict[str, Any], dataset: dict[str, Any]) -> dict[str, Any]:
        slots = self.admission["slots_by_target"]
        record = slots.get(request["target_key"], {}).get(request["request_key"])
        if record is None or record["request_hash"] != fingerprint(request):
            raise ServiceError("invalid_input", 422)
        if (
            len(
                dataset["cases"],
            )
            != 1
            or request["execution"]["repetitions"] != 1
            or request["scope"] != "local"
        ):
            raise ServiceError("invalid_input", 422)
        return cast(dict[str, Any], clone(record))


def bind_bootstrap(bootstrap: CampaignBootstrap) -> CampaignStoreBinding:
    root = bootstrap.root
    database = root / "eval.sqlite"
    bound = {
        "canonical_root": str(root),
        "database": str(database),
        "writer_lock": str(database) + ".lock",
        "uid": os.getuid(),
        "root_identity": file_identity(root, directory=True),
        "database_identity": file_identity(database),
        "writer_lock_identity": file_identity(Path(str(database) + ".lock")),
    }
    grouped: dict[str, dict[str, Any]] = {}
    for key, slot in bootstrap.slots.items():
        grouped.setdefault(slot["target"], {})[key] = clone(slot)
    data = {
        "campaign_id": bootstrap.campaign_id,
        "campaign_store_id": bootstrap.store_id,
        "store_binding": bound,
        "scope": "local",
        "slot_policy_digest": neutral_fingerprint(grouped),
    }
    return CampaignStoreBinding(
        data | {"store_binding_digest": neutral_fingerprint(data), "slots_by_target": grouped}
    )


_AUTHORITY_SEAL = object()


class NativeAuthority(dict[str, Any]):
    """来自固定host原件的内存品牌；不是新的权限服务或防篡改承诺。"""

    def __init__(self, value: dict[str, Any], seal: object) -> None:
        if seal is not _AUTHORITY_SEAL:
            raise ValueError("native_not_ready")
        super().__init__(clone(value))
        self._seal = seal


def validate_quota(limits: Any) -> None:
    maxima = {
        "model_requests": 12,
        "input_tokens": 12000,
        "output_tokens": 8000,
        "total_tokens": 20000,
        "usd": 1,
        "cli_launches": 1,
    }
    if not isinstance(limits, dict) or set(limits) != set(maxima):
        raise ValueError("native_not_ready")
    for key, maximum in maxima.items():
        actual = limits[key]
        if type(actual) not in {int, float} or not 0 < actual <= maximum:
            raise ValueError("native_not_ready")


def require_m0(value: dict[str, Any] | None) -> None:
    if (
        not isinstance(value, NativeAuthority)
        or value._seal is not _AUTHORITY_SEAL
        or value.get("inference_authorized") is not True
    ):
        raise ValueError("native_not_ready")
    for name in ["version_schema", "auth_lease", "hard_quota", "isolation_entry"]:
        receipt = value.get(name)
        if (
            not isinstance(receipt, dict)
            or receipt.get("verified") is not True
            or not receipt.get("sha256")
        ):
            raise ValueError("native_not_ready")
    validate_quota(value["hard_quota"].get("limits"))
    if value["hard_quota"].get("actuation") not in {"native", "gateway", "account"}:
        raise ValueError("native_not_ready")


def read_m0_authority(key: str) -> NativeAuthority:
    """安装后bootstrap使用的独立M0原件；没有profile也不需要伪造Store身份。"""
    root_value = os.environ.get("KRIM_PN2_ROOT")
    if not root_value:
        raise ValueError("native_not_ready")
    path = Path(root_value) / "release/M0-READY.json"
    try:
        file_identity(path)
        data = json.loads(path.read_text())
        if data.get("stage") != "PN2_M0_READY" or data.get("approved") is not True:
            raise ValueError("native_not_ready")
        authority = NativeAuthority(data["targets"][key], _AUTHORITY_SEAL)
        require_m0(authority)
        return authority
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("native_not_ready") from None


def load_profile(path: Path, key: str) -> NativeProfile:
    """CLI只选择由固定N2/release原件授权的profile，不能自报READY。"""
    root_value = os.environ.get("KRIM_PN2_ROOT")
    if not root_value:
        raise ValueError("native_not_ready")
    root = Path(root_value)
    authority_file = root / "release/PROFILE-READY.json"
    try:
        file_identity(authority_file)
        authority = json.loads(authority_file.read_text())
        approved = authority["profiles"][key]
        if (
            authority.get("inference_authorized") is not True
            or authority.get("stage") != "PN2_M0_READY"
            or approved["path"] != str(path)
            or approved["sha256"] != file_digest(path)
        ):
            raise ValueError("native_not_ready")
        data = json.loads(path.read_text())
        grant = NativeAuthority(authority["targets"][key], _AUTHORITY_SEAL)
        require_m0(grant)
        binding = CampaignStoreBinding(data["admission"])
        if str(binding.database.parent) != authority["campaign_root"]:
            raise ValueError("native_not_ready")
        return NativeProfile(
            key,
            data["family"],
            Path(data["worker"]),
            data["environment"],
            data["common"],
            data["subject"],
            binding,
            fixture_only=False,
            m0=grant,
            auth_sources=grant["auth_lease"].get("environment_files", {}),
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("native_not_ready") from None


@dataclass
class NativeProfile:
    key: str
    family: str
    worker: Path
    environment: dict[str, str] = field(repr=False)
    common: dict[str, Any]
    subject: dict[str, Any]
    binding: CampaignStoreBinding
    fixture_only: bool = True
    m0: dict[str, Any] | None = None
    secrets: tuple[str, ...] = field(default=(), repr=False)
    auth_sources: dict[str, str] = field(default_factory=dict, repr=False)
    _frozen: dict[str, Any] = field(init=False, repr=False)
    _executable_hash: str = field(init=False, repr=False)
    _worker_hash: str = field(init=False, repr=False)
    _environment_frozen: dict[str, str] = field(init=False, repr=False)
    _m0_frozen: dict[str, Any] | None = field(init=False, repr=False)
    _secrets_frozen: tuple[str, ...] = field(init=False, repr=False)
    _auth_frozen: dict[str, str] = field(init=False, repr=False)
    _material_frozen: dict[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.family not in {"codex", "claude"}:
            raise ServiceError("invalid_input")
        if self.fixture_only and self.key != "fixture-" + self.family:
            raise ValueError("native_not_ready")
        if not self.fixture_only:
            require_m0(self.m0)
            assert self.m0 is not None
            if (
                self.subject.get("model") != self.m0["version_schema"].get("model")
                or self.subject.get("provider_protocol")
                != self.m0["version_schema"].get("provider_protocol")
                or self.subject.get("service_speed") != "standard"
            ):
                raise ValueError("native_not_ready")
            if self.key != {"codex": "codex-cli", "claude": "claude-code"}[self.family]:
                raise ValueError("native_not_ready")
        if not self.fixture_only:
            assert self.m0 is not None
        prefix = (
            None
            if self.fixture_only
            else cast(dict[str, Any], self.m0)["version_schema"].get(
                "argv_prefix",
                [str(self.worker)],
            )
        )
        native_files = (
            None
            if self.fixture_only
            else cast(
                dict[str, Any],
                self.m0,
            )["version_schema"].get(
                "files",
                {},
            )
        )
        self._material_frozen = program_materials(
            self.family,
            self.worker,
            native_prefix=prefix,
            native_files=native_files,
        )
        if any(self.subject.get(key) != value for key, value in self._material_frozen.items()):
            raise ServiceError("target_binding_changed")
        if self.common.get("verifier_digest") != file_digest(
            Path(__file__).parents[1] / "verifiers.py"
        ):
            raise ServiceError("target_binding_changed")
        self.environment = dict(self.environment)
        public_names = {
            "PATH",
            "LANG",
            "LD_LIBRARY_PATH",
            "PYTHONDONTWRITEBYTECODE",
            "OPENAI_BASE_URL",
            "ANTHROPIC_BASE_URL",
            "SSL_CERT_FILE",
            "NODE_EXTRA_CA_CERTS",
        }
        private_names = {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"}
        if not set(
            self.environment,
        ) <= public_names | private_names or not all(
            type(v) is str for v in self.environment.values()
        ):
            raise ServiceError("invalid_input")
        for key in private_names & self.environment.keys():
            if not any(secret and secret in self.environment[key] for secret in self.secrets):
                raise ValueError("credential_lease_required")
        public = {k: v for k, v in self.environment.items() if k in public_names}
        if self.common.get("public_environment") != public:
            raise ServiceError("target_binding_changed")
        if neutral_fingerprint(
            self.subject,
        ) not in self.common.get("allowed_subject_fingerprints", []):
            raise ServiceError("target_binding_changed")
        self._secrets_frozen = tuple(self.secrets)
        self.auth_sources = dict(self.auth_sources)
        self._auth_frozen = dict(self.auth_sources)
        self._environment_frozen = dict(self.environment)
        self._m0_frozen = copy.deepcopy(dict(self.m0)) if self.m0 is not None else None
        self.common, self.subject = clone(self.common), clone(self.subject)
        self._executable_hash = file_digest(Path(sys.executable).resolve())
        self._worker_hash = file_digest(self.worker)
        self._frozen = self.descriptor()

    def descriptor(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "version": "fixture-native/1" if self.fixture_only else self.subject["cli_version"],
            "kind": "fixture-native" if self.fixture_only else "native-cli",
            "fixture_only": self.fixture_only,
            "implementation_digest": file_digest(self.worker),
            "execution_conditions": execution_conditions(
                {
                    "schema_version": "native-cli/1",
                    "common": clone(self.common),
                    "subject": clone(self.subject),
                    "admission": clone(self.binding.admission),
                },
            ),
        }

    def assert_current(self) -> None:
        self.binding.assert_store(self.binding.database)
        if not self.fixture_only:
            assert self.m0 is not None
        prefix = (
            None
            if self.fixture_only
            else cast(dict[str, Any], self.m0)["version_schema"].get(
                "argv_prefix", [str(self.worker)]
            )
        )
        native_files = (
            None
            if self.fixture_only
            else cast(dict[str, Any], self.m0)["version_schema"].get("files", {})
        )
        if (
            program_materials(
                self.family, self.worker, native_prefix=prefix, native_files=native_files
            )
            != self._material_frozen
        ):
            raise ServiceError("target_binding_changed")
        if (
            self.environment != self._environment_frozen
            or self.m0 != self._m0_frozen
            or self.secrets != self._secrets_frozen
            or self.auth_sources != self._auth_frozen
            or self.descriptor() != self._frozen
            or file_digest(Path(sys.executable).resolve()) != self._executable_hash
            or file_digest(self.worker) != self._worker_hash
        ):
            raise ServiceError("target_binding_changed")

    def agent_environment(self) -> tuple[dict[str, str], tuple[str, ...]]:
        """仅在reservation已核实的agent effect前读取最小新根lease，不哈希凭据。"""
        self.assert_current()
        env, secrets = dict(self.environment), list(self.secrets)
        if self.auth_sources:
            require_m0(self.m0)
            assert self.m0 is not None
            root = Path(os.environ["KRIM_PN2_ROOT"]) / "private-auth"
            for key, name in self.auth_sources.items():
                if key not in {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"}:
                    raise ValueError("native_not_ready")
                path = Path(name)
                if not path.is_relative_to(root) or ".." in path.parts:
                    raise ValueError("native_not_ready")
                expected = self.m0["auth_lease"]["file_identities"][key]
                actual = file_identity(path)
                st = path.lstat()
                if (
                    actual != expected
                    or st.st_uid != os.getuid()
                    or st.st_mode & 0o077
                    or not 1 <= st.st_size <= 4096
                ):
                    raise ServiceError("target_binding_changed")
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    live = os.fstat(fd)
                    if (live.st_dev, live.st_ino, live.st_size) != (
                        st.st_dev,
                        st.st_ino,
                        st.st_size,
                    ):
                        raise ServiceError("target_binding_changed")
                    raw = os.read(fd, 4097)
                    after = os.fstat(fd)
                    if (after.st_mtime_ns, after.st_ctime_ns) != (st.st_mtime_ns, st.st_ctime_ns):
                        raise ServiceError("target_binding_changed")
                finally:
                    os.close(fd)
                value = raw.decode("utf-8").strip()
                if not value or len(raw) > 4096:
                    raise ValueError("native_not_ready")
                env[key] = value
                secrets.append(value)
        return env, tuple(secrets)

    def argv(self, directory: Path) -> list[str]:
        if self.fixture_only:
            return [
                sys.executable,
                "-I",
                "-B",
                str(self.worker),
                "--family",
                self.family,
                "--mode",
                self.subject.get("fixture_mode", "normal"),
            ]
        require_m0(self.m0)
        model = self.subject["model"]
        assert self.m0 is not None
        prefix = self.m0["version_schema"].get("argv_prefix", [str(self.worker)])
        if (
            not isinstance(prefix, list)
            or not prefix
            or not all(isinstance(v, str) for v in prefix)
        ):
            raise ValueError("native_not_ready")
        if self.family == "codex":
            return prefix + [
                "--ask-for-approval",
                "never",
                "--sandbox",
                "workspace-write",
                "exec",
                "--json",
                "--ephemeral",
                "--model",
                model,
                "--cd",
                str(directory),
                "--config",
                "model_reasoning_effort=" + json.dumps(self.subject["effort"]),
                "-",
            ]
        return prefix + [
            "--print",
            "--input-format",
            "text",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            model,
            "--max-turns",
            "12",
            "--max-budget-usd",
            "1.00",
            "--settings",
            self.subject["settings_file"],
        ]


def slot_for(request: TaskSpec, dataset: DatasetSpec) -> dict[str, Any]:
    return {
        "scope": request.scope,
        "target": request.target_key,
        "kind": "fixture",
        "slot_id": request.request_key,
        "request_hash": fingerprint(request.model_dump(mode="json")),
        "dataset_fingerprint": dataset.ref().fingerprint,
        "case_id": dataset.cases[0].case_id,
        "repetition": 0,
        "execution": request.execution.model_dump(mode="json"),
    }
