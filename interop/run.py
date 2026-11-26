"""Run cross-language checks in a fresh namespace and reap every child."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import uuid


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return "127.0.0.1:%s" % sock.getsockname()[1]


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def launch(stack, args, env):
    proc = subprocess.Popen(args, env=env)
    stack.callback(stop, proc)
    return proc


def wait_for(proc, probe):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{proc.args[0]} exited with {proc.returncode}")
        try:
            if probe():
                return
        except OSError:
            pass
        time.sleep(0.04)
    raise TimeoutError(f"readiness timeout for {proc.args[0]}")


def listening(addr):
    host, port = addr.split(":")
    with socket.create_connection((host, int(port)), timeout=0.2):
        return True


def start_etcd(stack, tmp, env):
    binary = env.get("ETCD_BIN") or shutil.which("etcd")
    if not binary:
        raise RuntimeError("Set ETCD_BIN to an etcd v3 server binary")
    peer = "http://" + address()
    endpoint = "http://" + address()
    proc = launch(stack, [binary, "--name", "interop", "--data-dir", str(tmp / "data"),
        "--listen-client-urls", endpoint, "--advertise-client-urls", endpoint,
        "--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
        "--initial-cluster", "interop=" + peer, "--log-level", "error"], env)
    wait_for(proc, lambda: json.load(urllib.request.urlopen(endpoint + "/health", timeout=1))["health"] == "true")
    return endpoint


def run(mode, go, rust, tmp):
    env = os.environ.copy()
    env.pop("ETCD_ENDPOINT", None)
    env["RPC_NAMESPACE"] = "interop-" + uuid.uuid4().hex
    with ExitStack() as stack:
        if mode == "all":
            env["ETCD_ENDPOINT"] = start_etcd(stack, tmp, env)
        elif mode != "direct":
            raise ValueError("expected direct or all")
        for server, client, identity in [(go, rust, "go"), (rust, go, "rust")]:
            with ExitStack() as peers:
                addr = address()
                proc = launch(peers, [server, "serve", addr, identity], env)
                wait_for(proc, lambda: listening(addr))
                target = "discovery" if mode == "all" else addr
                reply = subprocess.check_output([client, "check", target], env=env, text=True, timeout=20)
                assert reply.strip() == identity, reply
                print(f"PASS {mode} client -> {identity} server (four RPC shapes, metadata, status, deadline)", flush=True)


if __name__ == "__main__":
    run(*sys.argv[1:4], Path(sys.argv[4]))
