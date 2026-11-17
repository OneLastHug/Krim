import { request } from "../api";
import {
  parseInteractionRequest,
  parseInteractionState,
  type InteractionAnswerBody,
  type InteractionCancelBody,
  type InteractionRequest,
  type InteractionState,
} from "./interaction-contracts";

const basePath = (workspaceId: string, sessionId: string) =>
  "/v2/workspaces/" + encodeURIComponent(workspaceId) + "/sessions/" +
  encodeURIComponent(sessionId) + "/interactions";

export async function readInteractionState(
  workspaceId: string,
  sessionId: string,
  apiKey: string,
  signal?: AbortSignal,
): Promise<InteractionState> {
  return parseInteractionState(await request(basePath(workspaceId, sessionId), apiKey, undefined, signal));
}

export async function readInteractionRequest(
  workspaceId: string,
  sessionId: string,
  requestId: string,
  apiKey: string,
): Promise<InteractionRequest> {
  const path = basePath(workspaceId, sessionId) + "/" + encodeURIComponent(requestId);
  return parseInteractionRequest(await request(path, apiKey));
}

export async function answerInteraction(
  workspaceId: string,
  sessionId: string,
  requestId: string,
  body: InteractionAnswerBody,
  apiKey: string,
): Promise<InteractionRequest> {
  const path = basePath(workspaceId, sessionId) + "/" + encodeURIComponent(requestId) + "/answer";
  return parseInteractionRequest(await request(path, apiKey, body));
}

export async function cancelInteraction(
  workspaceId: string,
  sessionId: string,
  requestId: string,
  body: InteractionCancelBody,
  apiKey: string,
): Promise<InteractionRequest> {
  const path = basePath(workspaceId, sessionId) + "/" + encodeURIComponent(requestId) + "/cancel";
  return parseInteractionRequest(await request(path, apiKey, body));
}
