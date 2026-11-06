import { useEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { accountError, accountRequest, parseAccountProfile, parseConnectionProfile, type AccountProfile, type ConnectionProfile } from "../lib/accounts";
import type { Actor } from "../lib/session-contracts";

interface Options {
  apiKey: string;
  identity: Actor | null;
  targetKey: string | null;
  targetFingerprint?: string;
  onUnauthorized: () => void;
  onIdentityStale?: () => void;
  paused?: boolean;
}

export function useAccountIdentity({ apiKey, identity, targetKey, targetFingerprint, onUnauthorized, onIdentityStale, paused = false }: Options) {
  const owner = JSON.stringify([apiKey, identity?.principal_id, identity?.generation]);
  const liveOwner = useRef(owner); liveOwner.current = owner;
  const unauthorized = useRef(onUnauthorized); unauthorized.current = onUnauthorized;
  const identityStale = useRef(onIdentityStale); identityStale.current = onIdentityStale;
  const [revision, refresh] = useState(0);
  useEffect(() => {
    if (!identity || paused) return;
    const update = () => refresh(value => value + 1);
    window.addEventListener("focus", update);
    return () => window.removeEventListener("focus", update);
  }, [owner, paused]);
  const [savedProfile, setProfile] = useState<{ owner: string; value: AccountProfile | null; legacy: boolean; error: string } | null>(null);
  const connectionOwner = JSON.stringify([owner, targetKey, targetFingerprint]);
  const liveConnection = useRef(connectionOwner); liveConnection.current = connectionOwner;
  const [savedConnection, setConnection] = useState<{ owner: string; value: ConnectionProfile | null; error: string } | null>(null);

  useEffect(() => {
    if (paused) return;
    if (!identity) { setProfile(null); return; }
    const controller = new AbortController();
    const owns = () => !controller.signal.aborted && liveOwner.current === owner;
    void accountRequest("/v2/me/profile", apiKey, { signal: controller.signal }).then(raw => {
      const value = parseAccountProfile(raw);
      if (value.principal_id !== identity.principal_id) throw new Error("账号资料与当前身份不一致");
      if (value.generation !== identity.generation) {
        if (owns()) identityStale.current?.();
        throw new Error("账号权限已更新，请重新读取身份");
      }
      if (owns()) setProfile({ owner, value, legacy: false, error: "" });
    }).catch(error => {
      if (!owns()) return;
      if (error instanceof RequestError && error.status === 401) { unauthorized.current(); return; }
      const legacy = error instanceof RequestError && error.code === "account_management_unavailable";
      setProfile({ owner, value: null, legacy, error: legacy ? "" : accountError(error) });
    });
    return () => controller.abort();
  }, [owner, revision, paused]);

  useEffect(() => {
    if (paused) return;
    if (!identity || !targetKey) { setConnection(null); return; }
    const controller = new AbortController();
    const owns = () => !controller.signal.aborted && liveConnection.current === connectionOwner;
    void accountRequest(`/v2/targets/${encodeURIComponent(targetKey)}/connection-profile`, apiKey, { signal: controller.signal }).then(raw => {
      const value = parseConnectionProfile(raw);
      if (value.target_key !== targetKey || targetFingerprint && value.target_fingerprint !== targetFingerprint) throw new Error("会话接入已变化，请刷新后重新读取");
      if (owns()) setConnection({ owner: connectionOwner, value, error: "" });
    }).catch(error => {
      if (!owns()) return;
      if (error instanceof RequestError && error.status === 401) { unauthorized.current(); return; }
      setConnection({ owner: connectionOwner, value: null, error: accountError(error) });
    });
    return () => controller.abort();
  }, [connectionOwner, revision, paused]);

  const profileState = savedProfile?.owner === owner ? savedProfile : null;
  const connectionState = savedConnection?.owner === connectionOwner ? savedConnection : null;
  return {
    profile: profileState?.value ?? null,
    profileLoading: !!identity && !profileState,
    profileError: profileState?.error ?? "",
    legacy: profileState?.legacy ?? false,
    connection: connectionState?.value ?? null,
    connectionLoading: !!identity && !!targetKey && !connectionState,
    connectionError: connectionState?.error ?? "",
    refresh: () => refresh(value => value + 1),
  };
}
