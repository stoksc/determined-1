docker run -d \
  --volume=/:/rootfs:ro \
  --volume=/var/run:/var/run:ro \
  --volume=/sys:/sys:ro \
  --volume=/var/lib/docker/:/var/lib/docker:ro \
  --volume=/dev/disk/:/dev/disk:ro \
  --publish=8080:8080 \
  --detach=true \
  --name=cadvisor \
  --privileged \
  --device=/dev/kmsg \
  gcr.io/cadvisor/cadvisor:v0.37.0

docker run -d \
  --gpus=all \
  --publish=9400:9400 \
  --name=dgcm-exporter \
  nvidia/dcgm-exporter:2.0.13-2.1.2-ubuntu18.04

