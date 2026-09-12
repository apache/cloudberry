# Apache Cloudberry (Incubating) Docker image (Rocky Linux 9)

This directory contains Docker build definitions for a single-node Apache Cloudberry container image.

## Build

Build from the current source tree (multi-stage build using a pre-built builder image):

```bash
docker build \
  -f devops/build/packaging/docker/Dockerfile.rocky9 \
  -t apache/cloudberry:dev .
```

Override the builder image (for example, pin to a digest/tag or use a locally-built builder):

```bash
docker build \
  -f devops/build/packaging/docker/Dockerfile.rocky9 \
  --build-arg BUILDER_IMAGE=apache/incubator-cloudberry:cbdb-build-rocky9-latest \
  -t apache/cloudberry:dev .
```

## Run

On first startup the container initializes a single-node cluster under `/data0` and starts it. By default, host connections use `trust` authentication to facilitate seamless development and testing workflows.

```bash
docker volume create cloudberry_data

docker run --rm -it \
  --name cloudberry-db \
  -p 5432:5432 \
  -v cloudberry_data:/data0 \
  apache/cloudberry:dev
```

When run interactively (with `-it` and without `-d`), the container initializes the cluster and immediately drops you into a `psql` prompt.

If you prefer to run it in the background (detached), use the `-d` flag. **Important**: Do not combine `-d` with `-t` or `-it`, otherwise the container will attempt to start the interactive SQL prompt and exit immediately.

```bash
docker run --rm -d \
  --name cloudberry-db \
  -p 5432:5432 \
  -v cloudberry_data:/data0 \
  apache/cloudberry:dev
```

If you require production-level security (e.g., password enforcement), simply run the container with `-e POSTGRES_HOST_AUTH_METHOD=md5` and provide a `POSTGRES_PASSWORD`:

```bash
docker run --rm -it \
  -d \
  --name cloudberry-db \
  -e POSTGRES_HOST_AUTH_METHOD=md5 \
  -e POSTGRES_PASSWORD=your_secure_password \
  -p 5432:5432 \
  -v cloudberry_data:/data0 \
  apache/cloudberry:dev
```

## Connect / Inspect

From the host (assuming `trust` default or matching passwords):

```bash
psql -h localhost -p 5432 -U gpadmin -d gpadmin
```

From inside the container (environment variables are already globally injected):

```bash
docker exec -it cloudberry-db psql -d postgres
```

Cluster status and logs:

```bash
docker exec cloudberry-db gpstate -s
docker logs cloudberry-db
```

## Notes

- **Timezone:** The container defaults to `UTC` (`TZ=UTC`). To use a different timezone, pass the `TZ` environment variable during run (e.g., `-e TZ=Asia/Shanghai`).
- **Graceful Shutdown:** The entrypoint natively traps `SIGTERM`, `SIGINT`, and `SIGQUIT` to perform a safe `gpstop -a -M fast`. Standard `docker stop <container>` is perfectly safe and ensures data consistency.
- **Internal SSH:** `sshd` is started exclusively for internal cluster communication. Port 22 is not exposed by default.
- **System Limits:** For maximum performance, consider explicitly raising container limits at runtime:
  `--ulimit nofile=524288:524288 --ulimit nproc=131072:131072`.
- **Tuning Configs:** Environment system configurations are powerfully reused from `devops/sandbox/configs/`:
  `/etc/security/limits.d/90-cbdb-limits.conf` and `/etc/sysctl.d/90-cbdb-sysctl.conf`.
