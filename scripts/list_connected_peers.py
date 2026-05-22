#!/usr/bin/env python3
"""
List connected Subtensor libp2p peers (peer id, role, IPs, multiaddrs).

Requires JSON-RPC with unsafe methods enabled:
  --rpc-methods=unsafe --unsafe-rpc-external  (if using --rpc-external)

Data sources (merged per peer):
  - system_peers                    -> peerId, roles, bestNumber
  - system_unstable_networkState    -> knownAddresses + endpoint (libp2p backend)
  - --bootnodes / --reserved-nodes  -> static multiaddrs (litep2p fallback)
  - ss on p2p port                  -> optional TCP fallback (--use-ss)

With --network-backend libp2p, networkState.connectedPeers includes addresses.
With litep2p, connectedPeers is empty; use --use-ss or DHT discovery instead.

Examples:
  python3 scripts/list_connected_peers.py
  python3 scripts/list_connected_peers.py --format json
  python3 scripts/list_connected_peers.py --use-ss
  python3 scripts/list_connected_peers.py --public-only
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


MULTIADDR_WITH_P2P_RE = re.compile(
    r"/(ip4|ip6|dns4?|dnsaddr)/([^/]+)(?:/tcp/(\d+))?(?:/[^/]*)*/p2p/([A-Za-z0-9]+)",
    re.IGNORECASE,
)
IP_IN_MULTIADDR_RE = re.compile(r"/(ip4|ip6)/([^/]+)", re.IGNORECASE)
P2P_ONLY_RE = re.compile(r"/p2p/([A-Za-z0-9]+)", re.IGNORECASE)
PROC_NAME = "node-subtensor"
PRIVATE_IP_RE = re.compile(
    r"^(127\.|10\.|192\.168\.|169\.254\.|"
    r"172\.(1[6-9]|2[0-9]|3[0-1])\.|"
    r"11\.|100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.)"
)


@dataclass
class PeerRow:
    peer_id: str
    role: str
    ips: list[str] = field(default_factory=list)
    addresses: list[str] = field(default_factory=list)
    best_number: int | None = None
    ip_sources: list[str] = field(default_factory=list)

    def sorted_ips(self) -> list[str]:
        return sorted(set(self.ips), key=lambda x: (":" in x, x))

    def sorted_addresses(self) -> list[str]:
        return sorted(set(self.addresses))


def rpc(url: str, method: str, params: list[Any] | None = None) -> Any:
    body = json.dumps({"id": 1, "jsonrpc": "2.0", "method": method, "params": params or []}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} calling {method}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Cannot reach RPC at {url}: {e}") from e

    if "error" in data:
        err = data["error"]
        msg = err.get("message", err)
        hint = ""
        if "unsafe" in str(msg).lower():
            hint = " (start node with --rpc-methods=unsafe and --unsafe-rpc-external)"
        raise RuntimeError(f"RPC {method} failed: {msg}{hint}")
    return data.get("result")


def resolve_host(host: str) -> list[str]:
    host = host.strip().lower()
    if host in ("localhost",):
        return ["127.0.0.1"]
    if re.match(r"^[\da-f.:]+$", host, re.IGNORECASE):
        return [host]
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    out: list[str] = []
    for info in infos:
        ip = info[4][0]
        if ip not in out:
            out.append(ip)
    return out


def is_public_ip(ip: str) -> bool:
    return not PRIVATE_IP_RE.match(ip)


def ips_from_multiaddr(addr: str) -> list[str]:
    """Extract IP literals from any /ip4/ or /ip6/ segment in a multiaddr."""
    ips: list[str] = []
    for kind, host in IP_IN_MULTIADDR_RE.findall(addr):
        if kind.lower() == "ip4" or kind.lower() == "ip6":
            if host not in ips:
                ips.append(host)
    return ips


def parse_multiaddr(addr: str) -> tuple[str | None, list[str]]:
    """Return (peer_id, [ip, ...]) from a substrate multiaddr string."""
    m = MULTIADDR_WITH_P2P_RE.search(addr)
    if m:
        host_kind, host, _port, peer_id = m.groups()
        if host_kind.lower().startswith("ip"):
            return peer_id, [host]
        return peer_id, resolve_host(host)

    ips = ips_from_multiaddr(addr)
    m2 = P2P_ONLY_RE.search(addr)
    if m2:
        return m2.group(1), ips
    return None, ips


def add_peer_address(
    store: dict[str, dict[str, set[str]]],
    peer_id: str,
    addr: str,
    source: str,
) -> None:
    if not peer_id or not addr:
        return
    entry = store.setdefault(peer_id, {"ips": set(), "addrs": set(), "sources": set()})
    entry["addrs"].add(addr)
    entry["sources"].add(source)
    for ip in ips_from_multiaddr(addr):
        entry["ips"].add(ip)


def endpoint_multiaddrs(endpoint: Any) -> list[str]:
    """Pull multiaddr strings from networkState endpoint (dialing / listening)."""
    if not isinstance(endpoint, dict):
        return []
    out: list[str] = []

    dialing = endpoint.get("dialing")
    if isinstance(dialing, list) and dialing and isinstance(dialing[0], str):
        out.append(dialing[0])

    listening = endpoint.get("listening")
    if isinstance(listening, dict):
        # send_back_addr is the remote peer; local_addr is our socket (skip it).
        for key in ("send_back_addr", "sendBackAddr"):
            val = listening.get(key)
            if isinstance(val, str) and val.startswith("/"):
                out.append(val)

    return out


def addresses_from_network_state(state: dict[str, Any] | None) -> dict[str, dict[str, set[str]]]:
    """
    peer_id -> {ips, addrs, sources} from system_unstable_networkState.
    """
    out: dict[str, dict[str, set[str]]] = {}
    if not state:
        return out

    def ingest(peers: dict[str, Any] | None, connected: bool) -> None:
        if not peers:
            return
        src = "networkState.connected" if connected else "networkState.known"
        for peer_id, info in peers.items():
            if not isinstance(info, dict):
                continue
            for addr in info.get("knownAddresses") or info.get("known_addresses") or []:
                if isinstance(addr, str):
                    add_peer_address(out, peer_id, addr, src)
            for addr in endpoint_multiaddrs(info.get("endpoint")):
                add_peer_address(out, peer_id, addr, "networkState.endpoint")

    ingest(state.get("connectedPeers") or state.get("connected_peers"), connected=True)
    ingest(state.get("notConnectedPeers") or state.get("not_connected_peers"), connected=False)
    return out


def collect_multiaddrs_from_argv(argv: list[str]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    i = 0
    while i < len(argv):
        if argv[i] in ("--bootnodes", "--reserved-nodes", "--reserved-node") and i + 1 < len(argv):
            for chunk in argv[i + 1].split(","):
                chunk = chunk.strip()
                if not chunk:
                    continue
                peer_id, ips = parse_multiaddr(chunk)
                if peer_id:
                    mapping.setdefault(peer_id, set()).update(ips)
        i += 1
    return mapping


def find_node_subtensor_cmdline() -> list[str] | None:
    try:
        __import__("pathlib").Path("/proc").iterdir()
    except OSError:
        return None

    for entry in __import__("pathlib").Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if PROC_NAME.encode() not in raw:
            continue
        return raw.replace(b"\x00", b" ").decode(errors="replace").split()
    return None


def network_backend_from_argv(argv: list[str] | None) -> str | None:
    if not argv:
        return None
    for i, arg in enumerate(argv):
        if arg == "--network-backend" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--network-backend="):
            return arg.split("=", 1)[1]
    return None


def tcp_remote_ips(p2p_port: int) -> list[str]:
    cmd = ["ss", "-tn", "state", "established", f"( sport = :{p2p_port} )"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=10)
    except FileNotFoundError:
        return []
    if proc.returncode != 0:
        return []

    ips: list[str] = []
    for line in proc.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        remote = parts[3]
        host = remote.rsplit(":", 1)[0]
        if host.startswith("["):
            host = host[1:-1]
        if host and host not in ips:
            ips.append(host)
    return ips


def assign_tcp_ips(
    rows: dict[str, PeerRow],
    tcp_ips: list[str],
    static_map: dict[str, set[str]],
    infer: bool,
) -> list[str]:
    ip_to_peers: dict[str, list[str]] = {}
    for peer_id, ips in static_map.items():
        for ip in ips:
            ip_to_peers.setdefault(ip, []).append(peer_id)

    unmapped: list[str] = []
    for ip in tcp_ips:
        candidates = [p for p in ip_to_peers.get(ip, []) if p in rows]
        if len(candidates) == 1:
            pid = candidates[0]
            if ip not in rows[pid].ips:
                rows[pid].ips.append(ip)
                rows[pid].ip_sources.append("tcp+multiaddr")
        else:
            unmapped.append(ip)

    if not infer or not unmapped:
        return unmapped

    without = [p for p, r in rows.items() if not r.ips]
    if len(without) == len(unmapped) and without:
        for pid, ip in sorted(zip(without, sorted(unmapped))):
            rows[pid].ips.append(ip)
            rows[pid].ip_sources.append("tcp-inferred")
        return []

    return unmapped


def fetch_peers(
    rpc_url: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, set[str]]], str | None, int]:
    peer_list = rpc(rpc_url, "system_peers") or []
    local_peer_id = rpc(rpc_url, "system_localPeerId")
    net_state = rpc(rpc_url, "system_unstable_networkState")
    net_data = addresses_from_network_state(net_state if isinstance(net_state, dict) else None)
    connected_count = len((net_state or {}).get("connectedPeers") or {})
    return peer_list, net_data, local_peer_id, connected_count


def apply_network_data(rows: dict[str, PeerRow], net_data: dict[str, dict[str, set[str]]], public_only: bool) -> None:
    for pid, data in net_data.items():
        if pid not in rows:
            continue
        row = rows[pid]
        for addr in data.get("addrs", set()):
            if addr not in row.addresses:
                row.addresses.append(addr)
        for ip in data.get("ips", set()):
            if public_only and not is_public_ip(ip):
                continue
            if ip not in row.ips:
                row.ips.append(ip)
        for src in data.get("sources", set()):
            if src not in row.ip_sources:
                row.ip_sources.append(src)


def build_rows(
    peer_list: list[dict[str, Any]],
    net_data: dict[str, dict[str, set[str]]],
    static_map: dict[str, set[str]],
    tcp_ips: list[str],
    infer_tcp: bool,
    public_only: bool,
) -> tuple[list[PeerRow], list[str]]:
    rows: dict[str, PeerRow] = {}

    for p in peer_list:
        pid = p.get("peerId") or p.get("peer_id") or ""
        if not pid:
            continue
        role = p.get("roles") or p.get("role") or "UNKNOWN"
        best = p.get("bestNumber") or p.get("best_number")
        rows[pid] = PeerRow(peer_id=pid, role=str(role), best_number=best)

    apply_network_data(rows, net_data, public_only)

    for pid, ips in static_map.items():
        if pid not in rows:
            continue
        for ip in ips:
            if public_only and not is_public_ip(ip):
                continue
            if ip not in rows[pid].ips:
                rows[pid].ips.append(ip)
                rows[pid].ip_sources.append("multiaddr")

    unmapped_tcp = assign_tcp_ips(rows, tcp_ips, static_map, infer_tcp) if tcp_ips else []
    ordered = sorted(rows.values(), key=lambda r: (r.role != "AUTHORITY", r.role, r.peer_id))
    return ordered, unmapped_tcp


def print_table(
    rows: list[PeerRow],
    local_peer_id: str | None,
    network_backend: str | None,
    connected_in_state: int,
    unmapped_tcp: list[str],
    tcp_rows: bool,
) -> None:
    if local_peer_id:
        print(f"local_peer_id: {local_peer_id}")
    if network_backend:
        print(f"network_backend: {network_backend}")
    print(f"connected_peers: {len(rows)} (networkState.connectedPeers: {connected_in_state})")
    with_ips = sum(1 for r in rows if r.ips)
    print(f"peers_with_ips: {with_ips}")
    print()
    print(f"{'ROLE':<12} {'PEER_ID':<52} {'IPS'}")
    print("-" * 100)
    for r in rows:
        ips = ", ".join(r.sorted_ips()) if r.ips else "-"
        print(f"{r.role:<12} {r.peer_id:<52} {ips}")
    if tcp_rows and unmapped_tcp:
        for ip in sorted(unmapped_tcp):
            print(f"{'TCP':<12} {'-':<52} {ip}")
    elif unmapped_tcp:
        print()
        print(f"tcp without peer id mapping ({len(unmapped_tcp)}):")
        for ip in sorted(unmapped_tcp):
            print(f"  {ip}")
        print("  (use --tcp-rows to include them in the table)")


def print_json(
    rows: list[PeerRow],
    local_peer_id: str | None,
    network_backend: str | None,
    connected_in_state: int,
    unmapped_tcp: list[str],
) -> None:
    litep2p_empty = connected_in_state == 0
    payload = {
        "local_peer_id": local_peer_id,
        "network_backend": network_backend,
        "network_state_connected_peers": connected_in_state,
        "connected_peers": [
            {
                "peer_id": r.peer_id,
                "role": r.role,
                "ips": r.sorted_ips(),
                "addresses": r.sorted_addresses(),
                "best_number": r.best_number,
                "ip_sources": r.ip_sources,
            }
            for r in rows
        ],
        "unmapped_tcp_ips": sorted(unmapped_tcp),
    }
    if litep2p_empty:
        payload["note"] = (
            "networkState.connectedPeers is empty. Node may be on litep2p, or libp2p RPC "
            "is not populated yet. Use --network-backend libp2p or --use-ss."
        )
    elif connected_in_state == 0:
        payload["note"] = (
            "networkState.connectedPeers is empty; IPs may be incomplete. "
            "Prefer --network-backend libp2p for per-peer addresses via RPC."
        )
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")


def print_csv(rows: list[PeerRow]) -> None:
    print("role,peer_id,ips,addresses,best_number")
    for r in rows:
        ips = ";".join(r.sorted_ips())
        addrs = ";".join(r.sorted_addresses())
        best = "" if r.best_number is None else str(r.best_number)
        print(f"{r.role},{r.peer_id},{ips},{addrs},{best}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rpc-url", default="http://127.0.0.1:9944", help="Subtensor HTTP RPC URL")
    parser.add_argument("--p2p-port", type=int, default=30333, help="P2P TCP port for ss lookup")
    parser.add_argument("--format", choices=("table", "json", "csv"), default="table")
    parser.add_argument(
        "--use-ss",
        action="store_true",
        help="Also match remote IPs from ss (fallback; skipped by default when networkState has peers)",
    )
    parser.add_argument(
        "--public-only",
        action="store_true",
        help="Omit private/link-local IPs (10/8, 172.16/12, etc.)",
    )
    parser.add_argument(
        "--infer-tcp",
        action="store_true",
        help="Assign unmapped TCP IPs to peers without IPs when counts match (best-effort)",
    )
    parser.add_argument(
        "--tcp-rows",
        action="store_true",
        help="Append unmapped TCP endpoints as rows (peer_id '-', role TCP)",
    )
    parser.add_argument(
        "--no-proc",
        action="store_true",
        help="Do not read flags from running node-subtensor process",
    )
    parser.add_argument(
        "--multiaddr",
        action="append",
        default=[],
        metavar="ADDR",
        help="Extra multiaddr (repeatable)",
    )
    args = parser.parse_args()

    argv = None if args.no_proc else find_node_subtensor_cmdline()
    network_backend = network_backend_from_argv(argv)

    static_map: dict[str, set[str]] = {}
    if argv:
        for pid, ips in collect_multiaddrs_from_argv(argv).items():
            static_map.setdefault(pid, set()).update(ips)

    for addr in args.multiaddr:
        peer_id, ips = parse_multiaddr(addr)
        if peer_id:
            static_map.setdefault(peer_id, set()).update(ips)

    try:
        peer_list, net_data, local_peer_id, connected_in_state = fetch_peers(args.rpc_url)
    except RuntimeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    # Default: use networkState only (libp2p). Fall back to ss when connectedPeers is empty (litep2p).
    use_ss = args.use_ss or connected_in_state == 0
    tcp_ips: list[str] = tcp_remote_ips(args.p2p_port) if use_ss else []
    rows, unmapped_tcp = build_rows(
        peer_list, net_data, static_map, tcp_ips, args.infer_tcp, args.public_only
    )

    if args.format == "json":
        print_json(rows, local_peer_id, network_backend, connected_in_state, unmapped_tcp)
    elif args.format == "csv":
        print_csv(rows)
    else:
        print_table(rows, local_peer_id, network_backend, connected_in_state, unmapped_tcp, args.tcp_rows)

    return 0


if __name__ == "__main__":
    sys.exit(main())
