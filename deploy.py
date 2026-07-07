#!/usr/bin/env python3
"""deploy.py - Deploy the weather edge aggregator to an EC2 Ubuntu instance over SSH.

Uploads the app files, provisions a Python venv, installs dependencies, and
installs/restarts a systemd service running uvicorn. Requires the local
OpenSSH client (ssh/scp) and SSH access to the target instance.
"""
import argparse
import subprocess
import sys

APP_FILES = [
    "aggregator.py",
    "weather_source.py",
    "polymarket_source.py",
    "server.py",
    "dashboard.py",
    "requirements.txt",
]

SERVICE_UNIT = """[Unit]
Description=Weather Edge Engine Node
After=network.target

[Service]
User={user}
WorkingDirectory={remote_dir}
EnvironmentFile=-{remote_dir}/.env
ExecStart={remote_dir}/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


def run_ssh(host, key, user, command):
    subprocess.run(
        ["ssh", "-i", key, "-o", "StrictHostKeyChecking=accept-new", f"{user}@{host}", command],
        check=True,
    )


def run_scp(host, key, user, local_paths, remote_dir):
    subprocess.run(
        ["scp", "-i", key, "-o", "StrictHostKeyChecking=accept-new", *local_paths, f"{user}@{host}:{remote_dir}/"],
        check=True,
    )


def deploy(host, key, user, remote_dir, env_file=None):
    print(f"[1/6] Ensuring {remote_dir} exists on {host}...")
    run_ssh(host, key, user, f"mkdir -p {remote_dir}")

    print("[2/6] Uploading application files...")
    run_scp(host, key, user, APP_FILES, remote_dir)

    if env_file:
        print(f"[3/6] Uploading env file {env_file} -> {remote_dir}/.env ...")
        run_scp(host, key, user, [env_file], remote_dir)
        run_ssh(host, key, user, f"mv {remote_dir}/{env_file.split('/')[-1]} {remote_dir}/.env && chmod 600 {remote_dir}/.env")
    else:
        print("[3/6] No --env-file given, skipping (VALID_PREMIUM_KEYS etc. will be unset).")

    print("[4/6] Installing system deps and Python venv...")
    run_ssh(
        host, key, user,
        "sudo apt-get update -y && sudo apt-get install -y python3-venv python3-pip && "
        f"python3 -m venv {remote_dir}/venv && "
        f"{remote_dir}/venv/bin/pip install --upgrade pip && "
        f"{remote_dir}/venv/bin/pip install -r {remote_dir}/requirements.txt"
    )

    print("[5/6] Writing systemd service unit...")
    unit_content = SERVICE_UNIT.format(user=user, remote_dir=remote_dir)
    run_ssh(host, key, user, f"sudo tee /etc/systemd/system/weather-edge.service > /dev/null << 'EOF'\n{unit_content}EOF")

    print("[6/6] Enabling and starting the service...")
    run_ssh(
        host, key, user,
        "sudo systemctl daemon-reload && "
        "sudo systemctl enable weather-edge.service && "
        "sudo systemctl restart weather-edge.service"
    )

    print(f"\nDeployed. Check status: ssh -i {key} {user}@{host} 'sudo systemctl status weather-edge.service'")
    print(f"App should be reachable at http://{host}:8000/api/v1/weather/edges "
          f"(make sure port 8000 is open in the EC2 instance's security group).")


def main():
    parser = argparse.ArgumentParser(description="Deploy weather-edge-aggregator to an EC2 Ubuntu instance over SSH.")
    parser.add_argument("--host", required=True, help="EC2 instance public IP or DNS name")
    parser.add_argument("--key", required=True, help="Path to the SSH private key (.pem)")
    parser.add_argument("--user", default="ubuntu", help="SSH user (default: ubuntu)")
    parser.add_argument("--remote-dir", default="/home/ubuntu/weather-edge-aggregator", help="Remote deploy directory")
    parser.add_argument("--env-file", default=None, help="Local .env file to upload (e.g. containing VALID_PREMIUM_KEYS); see .env.example")
    args = parser.parse_args()

    try:
        deploy(args.host, args.key, args.user, args.remote_dir, args.env_file)
    except subprocess.CalledProcessError as e:
        print(f"[DEPLOY FAILED] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
