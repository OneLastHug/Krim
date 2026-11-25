# 完整工作台网关样板

当前有效样板：`docs/prototypes/krim-gateway-full.html`。脚本、样式和字体内嵌，可独立打开。默认显示「接入与模型」，点击「返回对话」查看完整聊天工作台。

## 当前设计

按用户指定的 Magpie 方向重做：

- 供应商合并为紧凑行式列表，显示地址、模型数和密钥设置状态。
- 添加供应商使用分组模板：DeepSeek、OpenAI 兼容、Ollama、LM Studio、自定义。
- 点击供应商打开宽弹窗；标签与字段横向对齐，正文可滚动，保存/取消固定在底部。
- 模型直接在供应商弹窗内添加和展开；推理档位在模型内折叠编辑，无嵌套弹窗。
- 编辑使用草稿，保存才写入浏览器；关闭前可放弃草稿，删除需确认。
- 聊天底栏保留原控件，模型菜单增加搜索、供应商分组和选中状态。

参考本机 Magpie 的 `site/public/img/providers-light.png`、`add-zh-light.png` 以及 `internal/gui/assets/app.js` / `app.css`。设计记录在 `docs/prototypes/gateway-redesign-notes.md`。此前两表版和左右分栏版均已被替换。

## 查看路径

1. 打开 HTML，点击供应商行，查看连接、模型与推理档位编辑。
2. 「添加供应商」选择模板，填写 API 地址和模型后保存。
3. 「返回对话」，在底栏选择模型和 Extra 推理档位。
4. 账号菜单或模型菜单可以再次进入「接入与模型」。

复用现有 WorkbenchLayout、AppSidebar、ChatWorkbench、ChatHistory、ChatComposer、DevelopmentPanel 和 Portal 组件。正式 `frontend/src` 未修改。

设置与预览消息仅写当前浏览器。API Key 只保存是否设置的标记，不保存密钥内容，不请求真实模型或正式后端。

## 开发与导出

- 预览配置：`frontend/vite.gateway-preview.config.ts`，端口 5188。
- 启动项：`krim-gateway-full-preview`。
- 类型检查：`node frontend/node_modules/typescript/bin/tsc -p frontend/gateway-preview/tsconfig.json --noEmit`。
- 重新导出：`node frontend/gateway-preview/export-html.mjs`。
- 输出及设计资料只留本地，不发布。尚未接入正式前端。

## 本轮验证

实际打开供应商编辑、添加模板和推理折叠区，检查桌面/375px、长名称、空字段错误、深色主题。走通新供应商与模型保存、自定义 effort、浏览器持久化、聊天模型搜索和推理菜单联动。确认取消删除保留草稿，清理验收数据。

最终 TypeScript 检查和生产构建通过，导出 HTML 为 7.4 MiB。已在静态服务打开导出文件，检查默认设置页和供应商弹窗；0 外部脚本、0 外部样式、字体加载完成、无失败请求及控制台错误。`git diff --name-only -- frontend/src` 为空。
