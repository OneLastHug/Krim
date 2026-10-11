/** Agent 卡片只投影服务端事实；控件保持身份，后台刷新保留用户输入。 */
import { requestJson } from "./api.js";
import { t } from "./i18n.js";
import { TasksUI } from "./tasks-ui.js";
import { WorkspaceUI } from "./workspace-ui.js";
import { WorkspaceChangesUI } from "./workspace-changes-ui.js";
import { DirectoryChangesUI } from "./directory-changes-ui.js";
import { CommandsUI } from "./commands-ui.js";
export class AgentsUI {
    root;
    actions;
    cards = new Map();
    constructor(root, actions) {
        this.root = root;
        this.actions = actions;
    }
    async focusWorkspaceProposal(agentId, proposalId, operationId) {
        const card = this.cards.get(agentId);
        if (!card)
            throw new Error(t("Agent 实例不可用"));
        await card.changes.focusProposal(proposalId, operationId);
    }
    async focusDirectoryProposal(agentId, proposalId) {
        const card = this.cards.get(agentId);
        if (!card)
            throw new Error(t("Agent不存在或尚未读取"));
        await card.directories.focusProposal(proposalId);
        card.root.scrollIntoView({ block: "nearest" });
    }
    render(agents, sessionFor) {
        const live = new Set(agents.map(agent => agent.agent_id));
        for (const [id, card] of this.cards)
            if (!live.has(id)) {
                card.commands.dispose();
                card.directories.dispose();
                card.root.remove();
                this.cards.delete(id);
            }
        for (const agent of agents) {
            let card = this.cards.get(agent.agent_id);
            if (!card) {
                card = this.create(agent);
                this.cards.set(agent.agent_id, card);
                this.root.append(card.root);
            }
            card.view = agent;
            card.checkpoints.hidden = !agent.session.permissions.workspace;
            card.checkpoints.textContent = t("查看文件检查点");
            card.tasks.update();
            card.workspace.update(Boolean(agent.session.permissions.workspace), agent.session.status === "open");
            card.changes.update(Boolean(agent.session.permissions.workspace), agent.session.status === "open", agent.session.permissions.mode);
            card.directories.update(Boolean(agent.session.permissions.workspace), agent.session.status === "open", agent.session.permissions.mode);
            card.commands.update(agent);
            card.title.textContent = `${agent.name} · ${this.actions.status(agent.status)}`;
            const sessionLabels = { open: "会话开启", closed: "会话关闭", expired: "会话过期", destroyed: "会话销毁" };
            card.meta.textContent = `${t(sessionLabels[agent.session.status])} · ${t("轮次")} ${agent.turn}/${Number(agent.session.permissions.max_turns || 0)} · ${t("配置版本")} ${agent.session.profile_revision ?? "—"}`;
            const busy = ["queued", "running"].includes(agent.status);
            card.execute.textContent = t(agent.status === "created" ? "执行任务" : "继续任务");
            card.execute.disabled = busy || agent.session.status !== "open";
            card.stop.disabled = !busy;
            card.control.textContent = t(agent.session.status === "open" ? "关闭会话" : "重开会话");
            card.control.disabled = ["expired", "destroyed"].includes(agent.session.status);
            const stream = sessionFor(agent.topic_id);
            const ownStream = stream?.run.id === agent.current_run_id ? stream : undefined;
            card.draft.textContent = ownStream?.draft || "";
            card.draft.hidden = !card.draft.textContent;
            card.result.textContent = agent.result?.text || agent.result?.error || "";
            card.result.hidden = !card.result.textContent;
            card.activity.replaceChildren();
            for (const value of ownStream?.activity || []) {
                const item = document.createElement("li");
                item.textContent = value;
                card.activity.append(item);
            }
            if (ownStream && !ownStream.terminal && !ownStream.connected) {
                const item = document.createElement("li");
                item.textContent = t("后台流断开，可重连");
                card.activity.append(item);
            }
            if (card.history.open && card.historyTurn !== agent.turn)
                void this.loadHistory(card).catch(error => this.actions.notice(String(error), true));
        }
    }
    create(agent) {
        const root = document.createElement("article");
        root.className = "agent-card";
        root.dataset.agentId = agent.agent_id;
        const title = document.createElement("strong"), meta = document.createElement("small");
        const identifier = document.createElement("small");
        identifier.textContent = agent.agent_id;
        const draft = document.createElement("pre"), result = document.createElement("pre");
        draft.className = "agent-draft";
        result.className = "agent-result";
        const activityPanel = document.createElement("details"), activityTitle = document.createElement("summary"), activity = document.createElement("ol");
        activityTitle.textContent = t("执行步骤与工具");
        activityPanel.append(activityTitle, activity);
        const input = document.createElement("textarea");
        input.rows = 2;
        input.maxLength = 8192;
        input.setAttribute("aria-label", `${agent.name} ${t("后续任务")}`);
        input.placeholder = t("输入此 Agent 的下一轮任务");
        const execute = document.createElement("button"), stop = document.createElement("button"), control = document.createElement("button"), destroy = document.createElement("button"), open = document.createElement("button");
        for (const button of [execute, stop, control, destroy, open]) {
            button.type = "button";
            button.className = "secondary";
        }
        stop.textContent = t("停止");
        destroy.textContent = t("销毁");
        open.textContent = t("查看会话历史");
        const checkpoints = document.createElement("button");
        checkpoints.type = "button";
        checkpoints.className = "secondary";
        checkpoints.onclick = () => void this.actions.openCheckpoints(agent.agent_id).catch(error => this.actions.notice(String(error), true));
        const controls = document.createElement("div");
        controls.className = "actions";
        controls.append(execute, stop, control, open, checkpoints, destroy);
        const history = document.createElement("details"), historyTitle = document.createElement("summary"), turns = document.createElement("select"), transcript = document.createElement("pre");
        historyTitle.textContent = t("轮次历史");
        turns.setAttribute("aria-label", `${agent.name} ${t("历史轮次")}`);
        transcript.className = "agent-history";
        history.append(historyTitle, turns, transcript);
        const tasks = new TasksUI(agent.agent_id, () => card.view.session.status, this.actions.notice);
        const workspace = new WorkspaceUI(agent.agent_id, this.actions.notice);
        const changes = new WorkspaceChangesUI(agent.agent_id, this.actions.notice);
        const directories = new DirectoryChangesUI(agent.agent_id, this.actions.notice);
        const commands = new CommandsUI(agent.agent_id, this.actions.notice, proposalId => changes.focusProposal(proposalId), proposalId => directories.focusProposal(proposalId));
        root.append(title, meta, identifier, draft, activityPanel, result, input, controls, history, tasks.root, workspace.root, changes.root, directories.root, commands.root);
        const card = { root, view: agent, title, meta, draft, result, activity, input, execute, stop, control, history, turns, transcript, historyTurn: -1, historyVersion: 0, tasks, workspace, changes, directories, commands, checkpoints };
        execute.onclick = () => {
            const prompt = input.value.trim() || this.actions.prompt();
            if (!prompt) {
                this.actions.notice(t("请先输入要继续的 Agent 任务"));
                return;
            }
            execute.disabled = true;
            void this.actions.start(card.view, prompt).then(() => { if (input.value.trim() === prompt)
                input.value = ""; }).catch(error => this.actions.notice(String(error), true)).finally(() => { execute.disabled = ["queued", "running"].includes(card.view.status) || card.view.session.status !== "open"; });
        };
        stop.onclick = () => void this.actions.stop(card.view).catch(error => this.actions.notice(String(error), true));
        control.onclick = () => void this.actions.control(card.view, card.view.session.status === "open" ? "close" : "reopen").catch(error => this.actions.notice(String(error), true));
        destroy.onclick = () => void this.actions.destroy(card.view).catch(error => this.actions.notice(String(error), true));
        open.onclick = () => void this.actions.openTopic(card.view.topic_id).catch(error => this.actions.notice(String(error), true));
        history.ontoggle = () => { if (history.open && card.historyTurn !== card.view.turn)
            void this.loadHistory(card).catch(error => this.actions.notice(String(error), true)); };
        turns.onchange = () => void this.readHistory(card).catch(error => this.actions.notice(String(error), true));
        return card;
    }
    async loadHistory(card) {
        const turn = card.view.turn;
        card.historyTurn = turn;
        try {
            const items = await requestJson(`/v1/agents/${encodeURIComponent(card.view.agent_id)}/turns`);
            const previous = card.turns.value;
            card.turns.replaceChildren();
            for (const item of items) {
                const option = document.createElement("option");
                option.value = item.run_id;
                option.textContent = `${t("轮次")} ${item.turn} · ${this.actions.status(item.status)}`;
                card.turns.append(option);
            }
            card.turns.value = items.some(item => item.run_id === previous) ? previous : items.at(-1)?.run_id || "";
            await this.readHistory(card);
        }
        catch (error) {
            card.historyTurn = -1;
            throw error;
        }
    }
    async readHistory(card) {
        const runId = card.turns.value, version = ++card.historyVersion;
        if (!runId) {
            card.transcript.textContent = t("暂无执行轮次");
            return;
        }
        const { messages } = await requestJson(`/v1/runs/${encodeURIComponent(runId)}/messages`);
        if (version !== card.historyVersion)
            return;
        card.transcript.dataset.runId = runId;
        card.transcript.textContent = messages.map(message => {
            const step = Number(message.details.step || 0) + 1;
            if (message.role === "tool")
                return `${t("工具结果")} ${String(message.details.name || "")}\n${message.content}`;
            if (message.role === "assistant")
                return `${t("模型步骤")} ${step}\n${message.content}${message.details.tool_calls ? "\n" + JSON.stringify(message.details.tool_calls, null, 2) : ""}`;
            return message.content;
        }).join("\n\n");
    }
}
