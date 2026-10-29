#!/usr/bin/env python3
"""Portable demo entry point. Python 3.9+, Docker Engine and Compose v2."""
import argparse
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRET_KEYS = {"MYSQL_ROOT_PASSWORD", "MYSQL_PASSWORD", "PLATFORM1_DB_PASSWORD",
               "PLATFORM2_DB_PASSWORD", "GRAFANA_PASSWORD", "DEMO_TOKEN", "HUB_OPS_TOKEN"}


def init_env(root):
    path = root / ".env"
    if path.exists():
        return False
    lines = []
    for line in (root / ".env.example").read_text(encoding="utf-8").splitlines():
        key, sep, _ = line.partition("=")
        lines.append(key + "=" + secrets.token_hex(24) if sep and key in SECRET_KEYS else line)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write("\n".join(lines) + "\n")
        path.chmod(0o600)
    except FileExistsError:
        return False
    return True


def read_env(path, literal=False):
    values = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line if literal else line.strip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or key in values:
            raise ValueError(".env 中存在无效或重复配置项")
        if any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("环境配置不允许控制字符")
        if not literal and not re.fullmatch(r"[A-Za-z0-9_./:@?&=,%+()\-]*", value):
            raise ValueError(".env 请使用不带引号的单行字母、数字及URL字符，不支持变量或命令展开")
        if key.endswith("_PORT") and (not value.isdigit() or not 1 <= int(value) <= 65535):
            raise ValueError(".env 端口必须为1至65535的整数")
        values[key] = value
    return values


def init_native(root):
    path = root / ".env.native"
    if path.exists():
        return False
    text = (root / "config/native.env.example").read_text(encoding="utf-8")
    values = {}
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.startswith("#"):
            values[key] = secrets.token_hex(24) if key in SECRET_KEYS - {"MYSQL_ROOT_PASSWORD"} else value
    lines = []
    for line in text.splitlines():
        key, sep, _ = line.partition("=")
        lines.append(key + "=" + values[key] if sep and key in values else line)
    for key in ("MYSQL_PASSWORD", "PLATFORM1_DB_PASSWORD", "PLATFORM2_DB_PASSWORD"):
        if key not in values:
            lines.append(key + "=" + secrets.token_hex(24))
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write("\n".join(lines) + "\n")
        path.chmod(0o600)
    except FileExistsError:
        return False
    return True


def native_values(values):
    resolved = {"NATIVE_GATEWAY_LISTEN": "127.0.0.1:28080", "HUB_LISTEN": "127.0.0.1:28180",
                "NATIVE_PLATFORM1_LISTEN": "127.0.0.1:28081", "NATIVE_PLATFORM2_LISTEN": "127.0.0.1:28082",
                "NATIVE_UPSTREAM_LISTEN": "127.0.0.1:28083", "HUB_KAFKA_BROKERS": "127.0.0.1:9092",
                "HUB_INSTANCE_ID": "hub-local", "HUB_MAX_INSTANCES": "1"}
    resolved.update(values)
    for key, user, schema, password in (("HUB_DSN", "hub", "partner_hub", "MYSQL_PASSWORD"),
            ("PLATFORM1_DSN", "mock_p1", "mock_platform1", "PLATFORM1_DB_PASSWORD"),
            ("PLATFORM2_DSN", "mock_p2", "mock_platform2", "PLATFORM2_DB_PASSWORD")):
        resolved.setdefault(key, user + ":" + resolved[password] + "@tcp(" + resolved["MYSQL_ADDR"] + ")/" + schema + "?parseTime=true&timeout=1s&readTimeout=2s&writeTimeout=2s")
    for key, listen in (("NATIVE_HUB_URL", "HUB_LISTEN"), ("NATIVE_PLATFORM1_URL", "NATIVE_PLATFORM1_LISTEN"),
                        ("NATIVE_PLATFORM2_URL", "NATIVE_PLATFORM2_LISTEN"), ("NATIVE_UPSTREAM_URL", "NATIVE_UPSTREAM_LISTEN"),
                        ("HUB_TEST_URL", "NATIVE_GATEWAY_LISTEN")):
        resolved.setdefault(key, "http://" + resolved[listen])
    resolved.setdefault("HUB_PLATFORM1_URL", resolved["NATIVE_PLATFORM1_URL"] + "/notifications/trades")
    resolved.setdefault("MOCK_KAFKA_BROKERS", resolved["HUB_KAFKA_BROKERS"])
    return resolved


def native_environment(values):
    env = dict(os.environ, **values)
    # Always build runnable host binaries, even after a cross-compilation.
    env.pop("GOOS", None)
    env.pop("GOARCH", None)
    return env


def native_urls(values):
    base = "http://" + values["NATIVE_GATEWAY_LISTEN"]
    for name, suffix in (("中台运维", "/hub/"), ("上游交易台", "/mock/upstream/"),
                         ("平台 A", "/mock/p1/"), ("平台 B", "/mock/p2/")):
        print(name + "  " + base + suffix)
    print("页面令牌使用 .env.native 的 DEMO_TOKEN。")


def native_up(values):
    env = native_environment(values)
    output = ROOT / ".local/native-bin"
    logs = ROOT / ".local/native-logs"
    output.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    binaries = {}
    for name in ("hub", "mock", "gateway"):
        binary = output / (name + (".exe" if os.name == "nt" else ""))
        run(["go", "build", "-trimpath", "-o", str(binary), "./cmd/" + name], env=env)
        binaries[name] = str(binary)
    commands = [("platform1", [binaries["mock"], "platform1", "-listen", values["NATIVE_PLATFORM1_LISTEN"]]),
                ("platform2", [binaries["mock"], "platform2", "-listen", values["NATIVE_PLATFORM2_LISTEN"]]),
                ("hub", [binaries["hub"]]), ("upstream", [binaries["mock"], "upstream", "-listen", values["NATIVE_UPSTREAM_LISTEN"]]),
                ("gateway", [binaries["gateway"]])]
    children, streams = [], []
    try:
        for name, command in commands:
            stream = (logs / (name + ".log")).open("a", encoding="utf-8")
            streams.append(stream)
            children.append(subprocess.Popen(command, cwd=ROOT, env=env, stdout=stream, stderr=stream))
        base = "http://" + values["NATIVE_GATEWAY_LISTEN"]
        deadline = time.monotonic() + 90
        while True:
            if any(child.poll() is not None for child in children):
                raise ValueError("本次启动的应用退出，请查看 .local/native-logs；不会停止其他进程或重置依赖")
            try:
                for endpoint in ("/readyz", "/hub/", "/mock/p1/", "/mock/p2/", "/mock/upstream/"):
                    with urllib.request.urlopen(base + endpoint, timeout=2) as response:
                        if response.status != 200:
                            raise ValueError("页面或依赖尚未就绪")
                break
            except (urllib.error.URLError, TimeoutError, ValueError):
                if time.monotonic() >= deadline:
                    raise ValueError("90秒内未就绪，请核对本地依赖、端口及 .local/native-logs")
                time.sleep(1)
        native_urls(values)
        print("本地源码模式已就绪；Ctrl-C停止本次应用，外部依赖和数据保留。")
        while not any(child.poll() is not None for child in children):
            time.sleep(1)
        raise ValueError("应用退出，请核对 .local/native-logs")
    finally:
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=35)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for stream in streams:
            stream.close()


def run(args, **kwargs):
    return subprocess.run(args, check=True, cwd=ROOT, **kwargs)


def urls(values):
    base = "http://127.0.0.1:" + values.get("HUB_HTTP_PORT", "8080")
    for name, suffix in (("中台运维", "/hub/"), ("上游交易台", "/mock/upstream/"),
                         ("平台 A", "/mock/p1/"), ("平台 B", "/mock/p2/")):
        print(name + "  " + base + suffix)
    print("Grafana     http://127.0.0.1:" + values.get("GRAFANA_HOST_PORT", "3000") + "/d/hub-overview")
    print("Prometheus  http://127.0.0.1:" + values.get("PROMETHEUS_HOST_PORT", "9090"))
    print("页面令牌使用 .env 的 DEMO_TOKEN；Grafana 使用 GRAFANA_USER / GRAFANA_PASSWORD。")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "up", "down", "restart", "status", "logs", "urls", "check",
                                           "native-init", "native-migrate", "native-up"))
    args = parser.parse_args(argv)
    if args.command == "native-init":
        print("已生成 .env.native；请先配置已有数据库root密码及依赖地址" if init_native(ROOT) else "保留现有 .env.native")
        return 0
    if args.command.startswith("native-"):
        values = native_values(read_env(ROOT / ".env.native", literal=True))
        run(["go", "run", "./cmd/migrate", "-dir", "deploy/mysql", "-kafka"], env=native_environment(values))
        if args.command == "native-up":
            native_up(values)
        return 0
    if args.command in ("init", "up", "check"):
        print("已生成 .env（独立随机凭据）" if init_env(ROOT) else "保留现有 .env")
    if args.command == "init":
        return 0
    if not (ROOT / ".env").is_file():
        raise ValueError("缺少 .env；请先运行 python3 scripts/demo.py init")
    values = read_env(ROOT / ".env")
    if args.command == "urls":
        urls(values)
        return 0
    env = dict(os.environ, **values)
    compose = ["docker", "compose", "--env-file", str(ROOT / ".env"), "--project-directory", str(ROOT)]
    if args.command in ("up", "check"):
        run(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"], env=env)
        run(["docker", "compose", "version"], env=env)
        run(compose + ["config", "--quiet"], env=env)
    if args.command == "up":
        run(compose + ["up", "-d", "--build", "--wait", "--wait-timeout", "300"], env=env)
        urls(values)
    elif args.command != "check":
        suffix = {"down": ["down"], "restart": ["restart"], "status": ["ps", "-a"], "logs": ["logs", "-f", "--tail=100"]}[args.command]
        run(compose + suffix, env=env)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError) as error:
        print("启动条件不满足：" + str(error), file=sys.stderr)
        raise SystemExit(1)
    except subprocess.CalledProcessError as error:
        print("命令执行失败，请检查上方Docker日志；不会自动清空数据卷。", file=sys.stderr)
        raise SystemExit(error.returncode)
    except KeyboardInterrupt:
        raise SystemExit(130)
