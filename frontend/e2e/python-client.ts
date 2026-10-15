/** 浏览器测试的 Python 客户端入口；与调用者工作目录无关。 */
import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";

const defaultPython = fileURLToPath(new URL(
  process.platform === "win32" ? "../../.venv/Scripts/python.exe" : "../../.venv/bin/python",
  import.meta.url,
));

// 安装包验证显式选择隔离环境，普通源码验证使用项目原虚拟环境。
export const pythonExecutable = process.env.KRIM_E2E_PYTHON ?? defaultPython;
if (!isAbsolute(pythonExecutable)) throw new Error("KRIM_E2E_PYTHON 必须是解释器绝对路径");

export function pythonFixtureArgs(name: string): string[] {
  if (!/^[a-z][a-z0-9_]*\.py$/.test(name)) throw new Error("Python fixture 必须是脚本文件名");
  return ["-I", fileURLToPath(new URL(`../../tests/fixtures/${name}`, import.meta.url))];
}
