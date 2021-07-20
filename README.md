[![Build](https://img.shields.io/github/workflow/status/vshn/statefulset-resize-controller/Pull%20Request)][build]
![Go version](https://img.shields.io/github/go-mod/go-version/vshn/statefulset-resize-controller)
[![Version](https://img.shields.io/github/v/release/vshn/statefulset-resize-controller)][releases]
[![License](https://img.shields.io/github/license/vshn/statefulset-resize-controller)][license]

# Statefulset Resize Controller

The Statefulset Resize Controller is a Kubernetes operator to enable resizing PVCs of Statefulsets.

## Concept

Statefulsets have the option to provide one or more `volumeClaimTemplates`.
For each of these `volumeClaimTemplates` k8s will create a persistent volume claim per pod.

What Statefulsets do no support however, is updating these `volumeClaimTemplates`.
Depending on the storage backends, PVCs are not mutable either and different features are or are not available.
This means increasing the storage size of a Statefulset involves quite a lot of manual steps.

The Statefulset Resize Controller is general solution to this problem and does not require any additional support from the storage backend.
When recreating a Statefulset using `--cascade=orphan`, the controller will notice the change, scale down the Statefulset, recreate the PVCs, and migrate the data.

### Example

Get access to a Kubernetes cluster that has support for automatic PV provisioning.
Start the controller with:

```
make run
```

Create a Statefulset using a `1G` volume, by applying:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nginx
  labels:
    app: nginx
spec:
  ports:
  - port: 80
    name: web
  clusterIP: None
  selector:
    app: nginx
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: nginx 
  serviceName: "nginx"
  replicas: 3 
  template:
    metadata:
      labels:
        app: nginx 
    spec:
      terminationGracePeriodSeconds: 10
      containers:
      - name: nginx
        image: k8s.gcr.io/nginx-slim:0.8
        ports:
        - containerPort: 80
          name: web
        volumeMounts:
        - name: www
          mountPath: /usr/share/nginx/html
  volumeClaimTemplates:
  - metadata:
      name: www
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi

```

Make sure the StatefulSet starts up successfully. 
At this point you can write something to one of the volumes, the data will be persistent through the update.

To initiate the resizing, remove the StatefulSet without touching the pods by running:

```
kubectl delete sts web --cascade=orphan
```

Now increase the requested storage to `2G` and reapply the StatefulSet.
You should see the StatefulSet being scaled down to 0.
Then a backup of the volumes will be created, and the PVCs will be recreated and restored.
After a few seconds the StatefulSet should scale back up and its PVCs should be resized.

## Contributing

The Statefulset Resize Controller is written using the [Operator SDK](https://sdk.operatorframework.io/docs).

You'll need:

- A running Kubernetes cluster (minishift, minikube, k3s, ... you name it)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) and [kustomize](https://kubernetes-sigs.github.io/kustomize/installation/)
- Go development environment
- Your favorite IDE (with a Go plugin)
- Docker
- make
  
These are the most common make targets: `build`, `test`, `run`.
Run `make help` to get an overview over the relevant targets and their intentions.
