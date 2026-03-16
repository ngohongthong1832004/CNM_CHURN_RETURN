# Building a Cloudflare Tunnel for MLOpsVN

## Login to Cloudflare

```bash
cloudflared tunnel login 
# -> /home/mlops/.cloudflared/cert.pem
```

## Create a Tunnel

```bash
cloudflared tunnel create mlops-tunnel 
#-> /home/mlops/.cloudflared/83ce2279-8fa1-44d0-9977-89678f0354e3.json
```

## Copy Configuration File

```bash
sudo cp ./cloudflare_tunnel/config.yml /etc/cloudflared/config.yml
```

## Route DNS Traffic

```bash
cloudflared tunnel route dns mlops-tunnel api.mlopsvn.space
cloudflared tunnel route dns mlops-tunnel ui.mlopsvn.space
```

## Run the Tunnel

```bash
cloudflared tunnel run mlops-tunnel
```

## Set Up as a Service

```bash
sudo cloudflared service install
```
