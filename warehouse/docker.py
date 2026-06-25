import subprocess

build_command = [
    "docker", "buildx", "build",
    "--ssh", "default",
    "--platform", "linux/amd64",
    "-t", "australia-southeast1-docker.pkg.dev/warehouse-main/meltano/meltano-warehouse-main:prod",
    ".",
]

push_command = [
    "docker", "push",
    "australia-southeast1-docker.pkg.dev/warehouse-main/meltano/meltano-warehouse-main:prod",
]

env = {**subprocess.os.environ, "DOCKER_BUILDKIT": "1"}


def run_cmd(cmd):
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


if __name__ == "__main__":
    print("Building Docker image...")
    run_cmd(build_command)

    print("Pushing Docker image...")
    run_cmd(push_command)

    print("Done.")
