"""Run cross-language checks in a fresh namespace and reap every child."""
from contextlib import ExitStack
import base64
from collections import Counter
import json
import select
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


def post(env, method, payload):
    req = urllib.request.Request(env["ETCD_ENDPOINT"] + "/v3/kv/" + method,
        data=json.dumps(payload).encode(), headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=3) as response:
        return json.load(response)


def records(env):
    prefix = ("/" + env["RPC_NAMESPACE"] + "/interop.Echo/").encode()
    encode = lambda data: base64.b64encode(data).decode()
    return post(env, "range", {"key": encode(prefix), "range_end": encode(prefix[:-1] + b"0")}).get("kvs", [])


def govern(env, changes):
    """Compare revisions and put all changes in one transaction under the original leases."""
    success = []
    compare = []
    for rec in records(env):
        value = json.loads(base64.b64decode(rec["value"]))
        value.update(changes[value["address"]])
        compare.append({"target": "MOD", "key": rec["key"], "mod_revision": rec["mod_revision"]})
        success.append({"request_put": {"key": rec["key"], "lease": rec["lease"],
            "value": base64.b64encode(json.dumps(value).encode()).decode()}})
    assert len(success) == len(changes)
    assert post(env, "txn", {"compare": compare, "success": success}).get("succeeded") is True


def sample(proc, count):
    proc.stdin.write(f"{count}\n")
    proc.stdin.flush()
    if not select.select([proc.stdout], [], [], 5)[0]:
        raise TimeoutError("timed out waiting for samples")
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"sampling peer exited: {proc.wait()}")
    ids = line.strip().split(",")
    assert len(ids) == count, ids
    return Counter(ids)


def weighted(stack, server, client, env):
    addrs = [address(), address()]
    servers = [launch(stack, [server, "serve", addr, name], env) for addr, name in zip(addrs, ["A","B"])]
    for addr, proc in zip(addrs, servers):
        wait_for(proc, lambda: listening(addr))
    wait_for(servers[0], lambda: len(records(env)) == 2)
    sampler = subprocess.Popen([client, "sample", "discovery"], text=True, env=env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stack.callback(stop, sampler)
    for weights in [(5, 1), (1, 5)]:
        govern(env, {addr: {"weight": w} for addr, w in zip(addrs, weights)})
        short = Counter(dict(zip(["A", "B"], weights)))
        wait_for(sampler, lambda: sample(sampler, 6) == short)
        observed = sample(sampler, 60)
        assert observed == Counter({k: v*10 for k, v in short.items()}), observed
        print(f"PASS {Path(client).name} weights {weights} -> {dict(observed)}", flush=True)
    govern(env, {addrs[0]: {"status": "down"}, addrs[1]: {}})
    wait_for(sampler, lambda: sample(sampler, 6) == {"B": 6})
    govern(env, {addrs[0]: {"status": "up"}, addrs[1]: {}})
    wait_for(sampler, lambda: sample(sampler, 6) == {"A": 1, "B": 5})
    stop(servers[1])
    wait_for(sampler, lambda: len(records(env)) == 1 and sample(sampler, 6) == {"A": 6})
    sampler.stdin.close()
    assert sampler.wait(timeout=5) == 0


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

            if mode == "all":
                # Isolate every scenario; a leaked registration is a test failure.
                assert not records(env), "peer left registration after graceful shutdown"
                with ExitStack() as peers:
                    weighted(peers, server, client, env)
                assert not records(env), "weighted peers did not unregister"
                with ExitStack() as peers:
                    addr = address()
                    proc = launch(peers, [server, "serve", addr, identity], dict(env, RPC_LIMIT_ALL="1"))
                    wait_for(proc, lambda: listening(addr) and len(records(env)) == 1)
                    subprocess.run([client, "limited", "discovery"], env=env, timeout=12, check=True)
                assert not records(env)
                print(f"PASS {identity} server governance and unary limit status", flush=True)


if __name__ == "__main__":
    run(*sys.argv[1:4], Path(sys.argv[4]))
