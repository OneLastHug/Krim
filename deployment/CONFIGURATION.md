# 服务配置与启动

服务只接受显式指定的主配置文件。样例位于 [`krim.yaml.example`](krim.yaml.example)，复制后按部署路径保存，并确保主机用户独占读取：

```bash
install -m 600 deployment/krim.yaml.example /etc/krim/krim.yaml
uv run --frozen python -m krim_agent serve --config /etc/krim/krim.yaml
```

样例把 `model` 设为 `null`：API、管理和历史读取可以启动，但运行就绪为 false，提交不会生成假回答。要启用模型，在 `model` 中填写真实兼容端点和模型 ID；密钥只写环境变量引用，例如：

```yaml
model:
  base_url: https://provider.example/v1
  model: your-model-id
  api_key:
    env: KRIM_MODEL_API_KEY
```

启动服务前，由 systemd、容器运行时或受控启动器提供 `KRIM_MODEL_API_KEY`。配置加载器不会自动读取 `.env`；环境值只会在配置显式写出 `env:` 引用时被取用。访问令牌、MCP 凭据也使用同一引用格式，不能把秘密值写入 YAML。

主配置中的 `environment.sandbox`、`environment.development`、`tools` 和 `access` 为内联、强类型配置。所有主机文件路径（数据库、TLS证书、运行环境、密钥文件和产物目录等）以主配置文件所在目录为基准；MCP 命令参数、URL 和应用工作目录不作路径重写。修改配置后重启服务生效。

本机 CLI 写入也必须明确指定同一主文件，例如：

```bash
uv run --frozen python -m krim_agent run --config /etc/krim/krim.yaml \
  --text "任务内容" --key request-001
uv run --frozen python -m krim_agent serve-development --config /etc/krim/krim.yaml
```

`serve-development` 还要求 `environment.development` 已配置。只读本地命令也通过 `--config` 获取数据库位置；远程 `--url` 命令不读取本地服务配置。旧环境变量如 `KRIM_AGENT_PROVIDER` 不会覆盖 YAML，CLI 只报告被忽略变量的名称。

## 一次性迁移

迁移器把旧 `.env` 和显式给出的旧 JSON 配置合并成单个 YAML。它只解析简单 `KEY=VALUE` 行，不执行 shell，也不复制或输出密钥值；旧访问策略内的令牌会替换为待配置的环境变量引用。发现开发执行预算冲突时不会自动选边，先处理报告的冲突再重新运行：

```bash
uv run --frozen python -m krim_agent.config_migration \
  --env-file .env \
  --sandbox .data/sandbox.json \
  --development .data/development.json \
  --tools .data/tools.json \
  --access .data/access.json \
  --workspace .data/workspace.json \
  --output krim.yaml
```

不需要的旧输入参数可省略。迁移输出文件也要求服务用户独占访问；把报告中的环境变量名配置到受控秘密来源后，再以 `--config krim.yaml` 启动。
