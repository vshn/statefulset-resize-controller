# Architecture

This document describes the high-level architecture of the Statefulset Resize Controller.
It should give you a good impression on *how* it works and *where* to change things.

## Overview

On a very high-level, the Statefulset Resize Controller watches changes on Statefulsets and reacts to them.
This is the general way Kubernetes Operators work and we call this reacting to resource changes the *reconcile loop*.
If you are not familiar with Kubernetes Operators it might be helpful to read up on them before proceeding.

The controller will notice Statefulsets that should be resized, make a copy of the current volumes by creating new volumes with the requested size, delete the previous volume and then restore the original data from the previous volumes to the new volumes.
It does this by only interacting with the Kubernetes API and has no access to other systems.

### Code Map

```
statefulset-resize-controller
├── controllers           # Core controller package - only place that interacts with k8s
│   ├── controller.go       # Entry point of reconcile loop
│   ├── statefulset.go      # Fetch and update Sts, initiate resize
│   ├── pvc.go              # Fetch relevant PVCs
│   ├── backup.go           # Create backup PVC and job
│   ├── restore.go          # Recreate PVC and restore data
│   └── copy.go             # Handle job creation
├── statefulset           # Wrapper handling modification of k8s Statefulset resources
├── pvc                   # Wrapper handling modification of k8s PVC resources
├── cmd                   # Entry point - main.go
└── naming                # General helper package for naming
```

vshn.net

## Resize Steps

There are five distinct steps that are part of a successful resize. 
Each of these steps is abstracted as an idempotent function that will return whether it completed or has completed successfully before.


All information on the state of the resize is stored as annotations of the statefulset.

### Find Resizable PVCs

TODO DIAGRAM 

The controller is notified of any statefulset change.
This means as the very first step we detect whether the statefulset needs to be resized.


This first includes fetching the StatefulSet, and checking whether we are already in the process of resizing.
It then finds all PVCs that are smaller then the PVC template of the StatefulSet.
If there are any, they are stored on the StatefulSet and we proceed with the resizing.

Fetching the statefulset is handled in `controllers/statefulset.go`. 
Finding PVCs is handled in `controller/pvc.go`.

### Scale Down

TODO DIAGRAM 

Before any volume resizing can happen we scale down the statefulset to avoid data corruption. 
The original number replicas is stored as an annotation.


This is handled in `controllers/statefulset.go` and `statefulset/`.
### Backup

TODO DIAGRAM 

As soon as the statefulset has scaled down, the controller initiates a backup of the to be resized PVCs.
This means for each PVC it will create a new PVC with the same size as the original and it will start a job that mounts both PVCs and will `rsync` the data to the backup.

This is handled in `controllers/backup.go` and `controllers/copy.go`

### Restore

TODO DIAGRAM 

When the backup job completed successfully, the original PVC will be deleted and recreated with the new size.
A new job will be started to restore the backed up data to the new, larger, PVC.

This is handled in `controllers/restore.go` and `controllers/copy.go`

### Scale Up 

TODO DIAGRAM 

After a successful restore of all PVCs, the statefulset is scaled back up to its original size and all remaining annotations are cleared.

This is handled in `controllers/statefulset.go` and `statefulset/`.

## Failure Handling

TODO
