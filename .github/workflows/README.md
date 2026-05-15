# CI/CD Pipeline

Tự động hoá build, test, scan, và publish image cho hệ thống MLOps Customer Churn.

## Workflows

| File | Trigger | Mục đích |
|------|---------|----------|
| `ci.yml` | push/PR `main`,`develop` | Lint (ruff + black) + unit tests cho `model_pipeline` |
| `model-tests.yml` | push/PR `model_pipeline/**` + cron T2 02:00 UTC | Integration tests với MinIO + MLflow server thực |
| `docker-build.yml` | push `main`, tag `v*.*.*`, PR | Build & push 3 image (`serving-api`, `serving-ui`, `data-simulator`) lên `ghcr.io` |
| `security-scan.yml` | push/PR `main` + cron 04:00 UTC daily | Trivy quét filesystem + Docker images (SARIF upload vào tab Security) |

## Sơ đồ luồng

```
                ┌──────────────────────────┐
   PR open ───► │ ci.yml (lint + unit)     │ ◄─── chặn merge nếu fail
                │ model-tests.yml          │
                │ security-scan.yml (PR)   │
                └──────────────────────────┘
                            │
                       PR merged ──► main
                            │
                ┌───────────┴──────────────┐
                ▼                          ▼
   docker-build.yml             security-scan.yml (cron)
   build & push ghcr.io          quét hằng ngày
   tag = sha-xxxxxxx, latest

                Tag v1.2.3 ──► docker-build.yml ──► tag v1.2.3 + v1.2 + latest
```

## Image được publish

```
ghcr.io/<owner>/churn-serving-api:<tag>
ghcr.io/<owner>/churn-serving-ui:<tag>
ghcr.io/<owner>/churn-data-simulator:<tag>
```

Tag rule (theo `docker/metadata-action`):
- Branch push → `main`, `develop`
- PR → `pr-<n>`
- Commit SHA → `sha-<7 chars>`
- SemVer tag `v1.2.3` → `1.2.3`, `1.2`, `latest`

## Secrets & permissions

Workflow chỉ dùng **`GITHUB_TOKEN`** mặc định — không cần thêm secret thủ công. Cần bật:

1. **Settings → Actions → General → Workflow permissions** = *Read and write*
2. **Settings → Packages**: image lần đầu push sẽ là private; vào `https://github.com/users/<owner>/packages/container/churn-serving-api/settings` để chuyển public nếu cần.
3. **Settings → Code security → Code scanning**: cần enable để Trivy SARIF hiển thị.

## Cách dùng

### Trigger thủ công
```bash
gh workflow run ci.yml
gh workflow run docker-build.yml
gh workflow run security-scan.yml
```

### Pull image về deploy
```bash
docker pull ghcr.io/<owner>/churn-serving-api:latest
docker pull ghcr.io/<owner>/churn-serving-ui:latest
```

### Tạo release
```bash
git tag v1.0.0
git push origin v1.0.0
# → docker-build.yml tự động publish image với tag 1.0.0, 1.0, latest
```

## Local lint (giống CI)

```bash
pip install ruff==0.6.9 black==24.10.0
ruff check model_pipeline serving_pipeline data-pipeline data-simulator
black --check model_pipeline serving_pipeline data-pipeline data-simulator
```

## Dependabot

`.github/dependabot.yml` tự động mở PR weekly cho:
- GitHub Actions versions
- `pip` requirements của 3 module
- Base image trong Dockerfile

## Troubleshooting

- **`docker login` fail trên ghcr.io**: kiểm tra `Workflow permissions = Read and write`.
- **Trivy SARIF không upload**: repo cần là public, hoặc bật GitHub Advanced Security cho private repo.
- **Unit tests fail vì import**: `PYTHONPATH=model_pipeline` đã set sẵn trong workflow; kiểm tra test có dùng `from src.xxx` đúng pattern.
- **Integration tests timeout**: MLflow server cần ~30s khởi động; tăng `timeout` ở step `Run integration tests`.
