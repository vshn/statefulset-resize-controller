package controllers

import (
	"context"

	"github.com/vshn/statefulset-resize-controller/naming"
	"github.com/vshn/statefulset-resize-controller/statefulset"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RbacObjects struct {
	Name           string
	ServiceAccount corev1.ServiceAccount
	RoleBinding    rbacv1.RoleBinding
	Created        bool
}

type rbacObjCtxKey string

const (
	RbacObjCtxKey     = rbacObjCtxKey("RbacObjects")
	RbacObjNamePrefix = "sts-resize-sync-job-"
)

func (r StatefulSetReconciler) createRbacObjs(ctx context.Context, si *statefulset.Entity) (RbacObjects, error) {
	objs := RbacObjects{
		Created: false,
	}

	sts, err := si.StatefulSet()
	if err != nil {
		return objs, err
	}
	objs.Name = rbacObjName(sts.Name)

	if r.SyncClusterRole != "" {
		sa := newSA(sts.Namespace, objs.Name)
		sa, err := r.getOrCreateSA(ctx, sa)
		if err != nil {
			return objs, err
		}
		objs.ServiceAccount = sa
		rb := newRB(sts.Namespace, objs.Name, sa.Name, r.SyncClusterRole)
		objs.RoleBinding, err = r.getOrCreateRB(ctx, rb)
		if err != nil {
			return objs, err
		}

		objs.RoleBinding = rb
		objs.Created = true
	}

	return objs, nil
}

func rbacObjName(stsName string) string {
	// Shorten statefulset name to ensure prefix+stsName remains <= 63
	// characters. Ignored error cannot occur since 63-prefixLen is > 8.
	prefixLen := len(RbacObjNamePrefix)
	stsName, _ = naming.ShortenName(stsName, 63-prefixLen)
	return RbacObjNamePrefix + stsName
}

func (r StatefulSetReconciler) deleteRbacObjs(ctx context.Context, objs RbacObjects) error {
	if r.SyncClusterRole == "" || !objs.Created {
		// nothing to delete, return
		return nil
	}

	pol := metav1.DeletePropagationForeground
	// Clean up RoleBinding
	err := r.Client.Delete(ctx, &objs.RoleBinding, &client.DeleteOptions{
		PropagationPolicy: &pol,
	})
	if err != nil {
		return err
	}
	// Clean up ServiceAccount
	return r.Client.Delete(ctx, &objs.ServiceAccount, &client.DeleteOptions{
		PropagationPolicy: &pol,
	})
}

func (r *StatefulSetReconciler) getOrCreateSA(ctx context.Context, sa v1.ServiceAccount) (v1.ServiceAccount, error) {
	found := v1.ServiceAccount{}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(&sa), &found)
	if apierrors.IsNotFound(err) {
		return sa, r.Client.Create(ctx, &sa)
	}
	if err != nil {
		return sa, err
	}
	return found, nil
}

func (r *StatefulSetReconciler) getOrCreateRB(ctx context.Context, rb rbacv1.RoleBinding) (rbacv1.RoleBinding, error) {
	found := rbacv1.RoleBinding{}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(&rb), &found)
	if apierrors.IsNotFound(err) {
		return rb, r.Client.Create(ctx, &rb)
	}
	if err != nil {
		return rb, err
	}
	return found, nil
}

func newSA(namespace, objname string) v1.ServiceAccount {
	return v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objname,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
	}
}

func newRB(namespace, objname, saname, crname string) rbacv1.RoleBinding {
	return rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objname,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     crname,
		},
		Subjects: []rbacv1.Subject{
			rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saname,
				Namespace: namespace,
			},
		},
	}
}
