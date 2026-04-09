# Cloudflare Tunnel Setup

This project uses Cloudflare Tunnel in Docker with a token from Cloudflare Zero Trust.

## 1. Prepare token

Create `.env` from the sample file:

```powershell
Copy-Item .\cloudflare_tunnel\.env.example .\cloudflare_tunnel\.env
```

Open `cloudflare_tunnel/.env` and paste your token:

```env
CLOUDFLARE_TOKEN=your_tunnel_token_here
```

You can get the token from:

`Cloudflare Zero Trust -> Networks -> Tunnels -> <your tunnel> -> Configure -> Docker`

## 2. Start tunnel container

Make sure the shared Docker network exists:

```powershell
docker network inspect aio-network *> $null
if ($LASTEXITCODE -ne 0) { docker network create aio-network }
```

Start tunnel:

```powershell
docker compose -f .\cloudflare_tunnel\docker-compose.yml --env-file .\cloudflare_tunnel\.env up -d
```

Check logs:

```powershell
docker logs aio_cloudflared -f
```

Healthy tunnel logs should contain:

- `Registered tunnel connection`
- `Updated to new configuration`

## 3. Configure routes in Cloudflare Dashboard

Go to:

`Cloudflare Zero Trust -> Networks -> Tunnels -> <your tunnel> -> Configure -> Published application routes`

Use these internal service URLs:

| Public hostname | Service URL |
|---|---|
| `api.mlopsvn.space.ngohongthong.studio` | `http://churn-prediction-api:8000` |
| `ui.mlopsvn.space.ngohongthong.studio` | `http://churn-prediction-ui:7823` |
| `mlflow.mlopsvn.space.ngohongthong.studio` | `http://mlflow_server:5000` |
| `airflow.mlopsvn.space.ngohongthong.studio` | `http://airflow-airflow-apiserver-1:8080` |
| `superset.mlopsvn.space.ngohongthong.studio` | `http://aio_superset:8088` |
| `grafana.mlopsvn.space.ngohongthong.studio` | `http://aivn-grafana:3000` |

Important:

- Use container names, not `localhost`
- Use UI internal port `7823`, not host port `7860`
- Use Airflow container `airflow-airflow-apiserver-1`, not `airflow-apiserver`

## 4. Verify services before exposing

Check containers:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Quick local checks:

```powershell
Invoke-RestMethod http://localhost:8000/health/
Invoke-RestMethod http://localhost:5000
```

Expected API health response:

```json
{"status":"healthy","model_loaded":true}
```

## 5. Common issues

### Tunnel works but public URL fails

Usually one of these:

- target container is not running
- wrong container name in route
- service is unhealthy
- cloudflared is not attached to `aio-network`

### API route returns error

Check:

```powershell
docker logs churn-prediction-api --tail 50
```

### Tunnel route updated but still not working

Check:

```powershell
docker logs aio_cloudflared --tail 100
```

You should see the latest `Updated to new configuration` line.

## 6. Stop and restart

```powershell
docker compose -f .\cloudflare_tunnel\docker-compose.yml down
docker compose -f .\cloudflare_tunnel\docker-compose.yml --env-file .\cloudflare_tunnel\.env up -d
docker compose -f .\cloudflare_tunnel\docker-compose.yml restart
```
