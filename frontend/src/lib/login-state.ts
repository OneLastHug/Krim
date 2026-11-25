export const LOGIN_STATE = "krim.login/1";

interface LoginState { key: string; loggedOut: boolean }

export function readLoginState(): LoginState {
  try {
    const raw = localStorage.getItem(LOGIN_STATE);
    if (raw !== null) {
      const value: unknown = JSON.parse(raw);
      if (typeof value === "object" && value !== null && "key" in value && "loggedOut" in value
        && typeof value.key === "string" && value.key.length <= 256 && typeof value.loggedOut === "boolean") {
        return { key: value.loggedOut ? "" : value.key, loggedOut: value.loggedOut };
      }
    }
  } catch { /* 浏览器禁用存储或缓存损坏时仍允许手动登录。 */ }
  return { key: "", loggedOut: false };
}

export function clearLoginState(rejectedKey: string): void {
  // 旧请求的 401 不能清除刚切换或轮换的新密钥。
  if (rejectedKey && readLoginState().key === rejectedKey) saveLoginState({ key: "", loggedOut: true });
}

export function saveLoginState(value: LoginState): void {
  try {
    localStorage.setItem(LOGIN_STATE, JSON.stringify({ key: value.loggedOut ? "" : value.key, loggedOut: value.loggedOut }));
  } catch { /* 存储不可用时保留当前页面内的登录能力。 */ }
}
