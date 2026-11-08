"""固定运行时镜像与启动规格；未核验镜像一律拒绝解析，失败即关闭。

C1交付：本模块声明规格与校验；RunscRunner按注入的ResolvedRuntime生成docker
参数、容器入口与cgroup核对值（python监管门已接线）。node监管门尚未存在于
runner，注入node runtime仍fail-closed拒绝，不编造镜像或监管行为。
"""

from __future__ import annotations

import re
from pathlib import Path
from posixpath import basename
from typing import Literal, Self

from pydantic import Field, ValidationError, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel, fingerprint, parse_json

ProfileProgram = Literal["python", "node"]
ProfilePurpose = Literal["normal_job", "dependency_install", "connector"]

# 镜像只接受裸 sha256 id（本机已核对镜像，仅限dev_only）或固定官方仓库的 repo@sha256；
# repo段不允许':'，因此tag/latest写法在本模块无法通过校验。
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
DIGEST_IMAGE = re.compile(r"[A-Za-z0-9._/-]+@sha256:[0-9a-f]{64}\Z")
PROGRAM_IMAGE_REPOS: dict[str, frozenset[str]] = {
    "python": frozenset({"docker.io/library/python"}),
    "node": frozenset({"docker.io/library/node"}),
}

# 与RunscRunner当前固定值一致的上限；放宽需要整合者同步改runner并重跑验收。
ARGV_ITEM_LIMIT = 32
ARGV_BYTES_LIMIT = 8192
MAX_OUTPUT_BYTES = 32768
MAX_WALL_SECONDS = 60
MANIFEST_MAX_BYTES = 65536

# tmpfs固定附加旗标不可配置；接入方必须原样传递。
TMPFS_FLAGS = ("nosuid", "nodev", "noexec")

PATH_SEGMENT = re.compile(r"[A-Za-z0-9._\-]+\Z")


def _container_path(value: str, *, max_depth: int) -> str:
    if (
        not value.startswith("/")
        or value == "/"
        or value.endswith("/")
        or "//" in value
        or len(value) > 128
    ):
        raise ValueError("容器路径必须为绝对文件路径")
    segments = value[1:].split("/")
    if not 1 <= len(segments) <= max_depth or any(
        segment in ("", ".", "..") or not PATH_SEGMENT.fullmatch(segment) for segment in segments
    ):
        raise ValueError("容器路径包含无效段")
    return value


class TmpfsMount(StrictModel):
    """容器内tmpfs声明；nosuid/nodev/noexec为固定旗标，不进入可配置字段。"""

    destination: str
    size_bytes: int = Field(ge=65536, le=536870912)
    mode: int = Field(default=0o1777, ge=0, le=0o7777)

    @model_validator(mode="after")
    def _valid_destination(self) -> Self:
        _container_path(self.destination, max_depth=4)
        return self


class RuntimeProfile(StrictModel):
    """单个(program, purpose)的完整运行规格；安全属性为字面量，不可放宽。"""

    program: ProfileProgram
    purpose: ProfilePurpose = "normal_job"
    interpreter: str
    cpu_cores: float = Field(default=1.0, gt=0, le=4)
    memory_bytes: int = Field(default=1073741824, ge=33554432, le=8589934592)
    memory_swap_bytes: int = Field(default=1073741824, ge=33554432, le=8589934592)
    pids_limit: int = Field(default=512, ge=16, le=512)
    nproc_limit: int = Field(default=128, ge=1, le=128)
    tmpfs: tuple[TmpfsMount, ...] = Field(
        default=(TmpfsMount(destination="/tmp", size_bytes=134217728),),
        min_length=1,
        max_length=4,
    )
    output_limit_bytes: int = Field(default=MAX_OUTPUT_BYTES, ge=1024, le=MAX_OUTPUT_BYTES)
    wall_seconds: int = Field(default=20, ge=1, le=MAX_WALL_SECONDS)
    max_wall_seconds: int = Field(default=MAX_WALL_SECONDS, ge=1, le=MAX_WALL_SECONDS)
    supervisor_grace_seconds: int = Field(default=15, ge=1, le=30)
    tool_timeout_seconds: int = Field(default=90, ge=30, le=180)
    argv_bytes_limit: int = Field(default=ARGV_BYTES_LIMIT, ge=256, le=ARGV_BYTES_LIMIT)
    dependency_dirs: tuple[str, ...] = Field(default=(), max_length=4)
    workdir: str = "/workspace"
    read_only_root: Literal[True] = True
    run_as_uid: int = Field(default=65532, ge=1, le=65534)
    run_as_gid: int = Field(default=65532, ge=1, le=65534)
    network: Literal["none"] = "none"
    dropped_capabilities: Literal["ALL"] = "ALL"
    no_new_privileges: Literal[True] = True

    @field_validator("tmpfs", "dependency_dirs", mode="before")
    @classmethod
    def _freeze_sequence(cls, value: object) -> object:
        # JSON里的数组在strict模式下不会自动转tuple，这里显式转换保持字段不可变。
        if type(value) is list:
            return tuple(value)
        return value

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        _container_path(self.interpreter, max_depth=6)
        expected = "python" if self.program == "python" else "node"
        if expected not in basename(self.interpreter):
            raise ValueError("解释器路径与运行时不匹配")
        _container_path(self.workdir, max_depth=4)
        if self.workdir == "/":
            raise ValueError("工作目录不能为根")
        if self.memory_swap_bytes != self.memory_bytes:
            raise ValueError("禁用交换：swap上限必须等于内存上限")
        if self.wall_seconds > self.max_wall_seconds:
            raise ValueError("wall超出本profile上限")
        if self.wall_seconds + self.supervisor_grace_seconds > self.tool_timeout_seconds:
            raise ValueError("wall加监管宽限必须留出工具超时余量")
        destinations = {mount.destination for mount in self.tmpfs}
        if "/tmp" not in destinations:
            raise ValueError("必须声明noexec的/tmp tmpfs作为可写临时目录")
        for directory in self.dependency_dirs:
            _container_path(directory, max_depth=6)
            if directory == self.workdir or not directory.startswith(self.workdir + "/"):
                raise ValueError("依赖目录必须声明在工作目录之下")
            if any(
                directory == target or directory.startswith(target + "/") for target in destinations
            ):
                raise ValueError("依赖目录不能与tmpfs重叠")
        return self

    @property
    def safe_flags(self) -> tuple[str, ...]:
        """隔离旗标由profile生成，argv只做数组拼接，不经过shell。"""
        if self.program == "python":
            return ("-I",)
        heap_mb = self.memory_bytes // (2 * 1024 * 1024)
        return (
            "--disallow-code-generation-from-strings",
            f"--max-old-space-size={heap_mb}",
        )

    def validate_argv(self, argv: list[str]) -> list[str]:
        """与RunscRunner的argv校验同语义；总字节按UTF-8编码计。"""
        if (
            type(argv) is not list
            or not 1 <= len(argv) <= ARGV_ITEM_LIMIT
            or any(type(item) is not str or "\0" in item for item in argv)
            or sum(len(item.encode()) for item in argv) > self.argv_bytes_limit
        ):
            raise ServiceError("invalid_input", 422)
        return list(argv)

    def payload_argv(self, argv: list[str]) -> list[str]:
        """用户程序在监管门之后的完整调用数组。"""
        return [self.interpreter, *self.safe_flags, *self.validate_argv(argv)]

    def gate_entry(self, gate_source: str) -> list[str]:
        """容器入口数组。python与RunscRunner历史入口逐项一致；node监管门
        尚未实现，runner在构造时同样拒绝node runtime，未接线前一律拒绝。"""
        if self.program == "python":
            return [self.interpreter, "-I", "-u", "-c", gate_source]
        raise ServiceError("sandbox_backend_unavailable", 503)


# RunscRunner两参构造的兼容默认：逐字段等于profile化之前runner的固定参数
# （1 CPU、1GiB内存且swap禁用、512 pids、nproc 128、/tmp 128MiB noexec tmpfs、
# 输出32768、wall≤60、监管宽限15、uid/gid 65532、只读根、无网络）。
# 放宽任何字段都必须同步修改runner并重跑验收。
LEGACY_PYTHON_PROFILE = RuntimeProfile(program="python", interpreter="/usr/bin/python3.12")


class RuntimeProfileEntry(StrictModel):
    """manifest条目：dev核验镜像与未构建设计区分登记，不带生产兼容结论。"""

    name: str
    profile: RuntimeProfile
    availability: Literal["dev_verified", "not_built"]
    scope: Literal["dev_only", "release"] | None = None
    image: str | None = None

    @field_validator("name")
    @classmethod
    def _valid_name(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,63}", value):
            raise ValueError("profile名只允许小写字母、数字和连字符")
        return value

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.availability == "not_built":
            if self.image is not None:
                raise ValueError("未构建条目不得携带镜像引用")
            return self
        if self.image is None or self.scope is None:
            raise ValueError("已核验条目必须固定digest镜像并登记scope")
        if IMAGE_ID.fullmatch(self.image):
            if self.scope != "dev_only":
                raise ValueError("本机镜像id只能登记为dev_only已知镜像")
            return self
        if not DIGEST_IMAGE.fullmatch(self.image):
            raise ValueError("镜像必须为sha256 id或官方仓库@sha256，禁止tag/latest")
        repo = self.image.split("@", 1)[0]
        if repo not in PROGRAM_IMAGE_REPOS[self.profile.program]:
            raise ValueError("仅接受已登记的官方仓库digest")
        return self


class ResolvedRuntime(StrictModel):
    name: str
    program: ProfileProgram
    purpose: ProfilePurpose
    scope: Literal["dev_only", "release"]
    image: str
    profile: RuntimeProfile

    @property
    def fingerprint(self) -> str:
        """绑定评测/依赖结果的稳定指纹：镜像+完整规格的规范JSON哈希。"""
        return fingerprint({"image": self.image, "profile": self.profile.model_dump(mode="json")})


class RuntimeCatalog(StrictModel):
    schema_version: Literal[1] = 1
    profiles: tuple[RuntimeProfileEntry, ...] = Field(min_length=1, max_length=16)

    @field_validator("profiles", mode="before")
    @classmethod
    def _freeze_sequence(cls, value: object) -> object:
        if type(value) is list:
            return tuple(value)
        return value

    @model_validator(mode="after")
    def _unique(self) -> Self:
        names = [entry.name for entry in self.profiles]
        keys = [(entry.profile.program, entry.profile.purpose) for entry in self.profiles]
        if len(set(names)) != len(names):
            raise ValueError("profile名重复")
        if len(set(keys)) != len(keys):
            raise ValueError("同一program+purpose只能登记一个profile")
        return self

    @classmethod
    def load(cls, path: Path) -> RuntimeCatalog:
        try:
            raw = path.read_bytes()
            if len(raw) > MANIFEST_MAX_BYTES:
                raise ValueError("manifest超出大小上限")
            value = parse_json(raw.decode())
            return cls.model_validate(value)
        except (OSError, ValueError, ValidationError):
            raise ServiceError("sandbox_config_invalid", 422) from None

    def entry(self, program: ProfileProgram, purpose: ProfilePurpose) -> RuntimeProfileEntry:
        for entry in self.profiles:
            if entry.profile.program == program and entry.profile.purpose == purpose:
                return entry
        raise ServiceError("invalid_input", 422)

    def profile(self, program: ProfileProgram, purpose: ProfilePurpose) -> RuntimeProfile:
        """只要规格存在即返回，构建状态由resolve把关；供负载定义与适配预检使用。"""
        return self.entry(program, purpose).profile

    def resolve(
        self,
        program: ProfileProgram,
        purpose: ProfilePurpose,
        *,
        allow_dev: bool = False,
        wall_seconds: int | None = None,
    ) -> ResolvedRuntime:
        """失败即关闭：未构建、dev_only未显式允许、镜像缺失一律拒绝。"""
        entry = self.entry(program, purpose)
        if entry.availability != "dev_verified" or entry.image is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        if entry.scope == "dev_only" and not allow_dev:
            raise ServiceError("sandbox_backend_unavailable", 503)
        if wall_seconds is not None and wall_seconds > entry.profile.max_wall_seconds:
            raise ServiceError("invalid_input", 422)
        return ResolvedRuntime(
            name=entry.name,
            program=program,
            purpose=purpose,
            scope=entry.scope if entry.scope is not None else "dev_only",
            image=entry.image,
            profile=entry.profile,
        )


class BenignPayload(StrictModel):
    """良性兼容负载定义；workspace_fixture非空时验收方须先在/workspace写入该文件。"""

    name: str
    program: ProfileProgram
    argv: list[str] = Field(min_length=1, max_length=ARGV_ITEM_LIMIT)
    expect_stdout: str = Field(min_length=1, max_length=256)
    workspace_fixture: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("name")
    @classmethod
    def _valid_name(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,63}", value):
            raise ValueError("负载名只允许小写字母、数字和连字符")
        return value

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        if (
            any(type(item) is not str or "\0" in item for item in self.argv)
            or sum(len(item.encode()) for item in self.argv) > ARGV_BYTES_LIMIT
        ):
            raise ValueError("负载argv无效")
        if self.workspace_fixture is not None:
            _container_path(self.workspace_fixture, max_depth=6)
        return self


PYTHON_BENIGN_PAYLOADS: tuple[BenignPayload, ...] = (
    BenignPayload(
        name="python-runtime-report",
        program="python",
        argv=[
            "-c",
            "import json,platform,sys\n"
            "print(json.dumps({'implementation':platform.python_implementation(),"
            "'major':sys.version_info[0],'minor':sys.version_info[1],"
            "'isolated':int(sys.flags.isolated),'no_user_site':int(sys.flags.no_user_site)},"
            "separators=(',',':')))",
        ],
        expect_stdout='"isolated":1',
    ),
    BenignPayload(
        name="python-tmpfs-write",
        program="python",
        argv=[
            "-c",
            "import json,os,tempfile\n"
            "fd,path=tempfile.mkstemp(dir='/tmp')\n"
            "os.write(fd,b'krim')\nos.close(fd)\n"
            "print(json.dumps({'tmp_write':open(path,'rb').read()==b'krim'},separators=(',',':')))",
        ],
        expect_stdout='"tmp_write":true',
    ),
    BenignPayload(
        name="python-pure-compute",
        program="python",
        argv=[
            "-c",
            "primes=[n for n in range(2,200) if all(n%d for d in range(2,int(n**0.5)+1))]\n"
            "print('primes:%d'%len(primes))",
        ],
        expect_stdout="primes:46",
    ),
    BenignPayload(
        name="python-workspace-read",
        program="python",
        argv=["-c", "import pathlib\nprint(pathlib.Path('/workspace/krim-benign.txt')"
                      ".read_text(encoding='utf-8'))"],
        expect_stdout="krim-benign-ok",
        workspace_fixture="/workspace/krim-benign.txt",
    ),
)

NODE_BENIGN_PAYLOADS: tuple[BenignPayload, ...] = (
    BenignPayload(
        name="node-runtime-report",
        program="node",
        argv=[
            "--eval",
            "console.log(JSON.stringify({major:process.versions.node.split('.')[0],"
            "evalBlocked:(()=>{try{new Function('return 1');return false}"
            "catch(e){return true}})()}))",
        ],
        expect_stdout='"evalBlocked":true',
    ),
    BenignPayload(
        name="node-tmpfs-write",
        program="node",
        argv=[
            "--eval",
            "const fs=require('fs');const p='/tmp/krim-node-benign-'+process.pid;"
            "fs.writeFileSync(p,'krim');"
            "console.log(JSON.stringify({tmp_write:fs.readFileSync(p,'utf8')==='krim'}))",
        ],
        expect_stdout='"tmp_write":true',
    ),
    BenignPayload(
        name="node-pure-compute",
        program="node",
        argv=["--eval", "let s=0;for(let i=1;i<=100;i++)s+=i;console.log('sum:%d',s)"],
        expect_stdout="sum:5050",
    ),
)

BENIGN_PAYLOADS: tuple[BenignPayload, ...] = PYTHON_BENIGN_PAYLOADS + NODE_BENIGN_PAYLOADS
