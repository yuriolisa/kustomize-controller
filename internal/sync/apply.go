package sync

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type Sync struct {
	// Log debug level
	Log logr.Logger
}

type ApplyOptions struct {
	Kustomization types.NamespacedName
	Timeout       time.Duration

	// Impersonation defaults to user
	// gotk:<Kustomization.Namespace>:reconciler
	Impersonation *ApplyImpersonation

	// DryRun client or server
	DryRun string
}

type ApplyImpersonation struct {
	ServiceAccount *types.NamespacedName
	Username       *types.NamespacedName
}

func (i ApplyImpersonation) String() string {
	name := ""
	switch {
	case i.Username != nil:
		name = i.Username.Name
	case i.ServiceAccount != nil:
		name = i.ServiceAccount.String()
	}
	return name
}

type ApplyResult struct {
	Objects []ApplyResultObject
}

type ApplyResultObject struct {
	GroupVersionKind schema.GroupVersionKind
	NamespacedName   types.NamespacedName

	// Status can be
	// created, modified, deleted, unchanged
	Status string
}

func (s *Sync) Apply(manifestsPath string, kubeConfigPath string, opts ApplyOptions) (*ApplyResult, error) {
	applyCmd := fmt.Sprintf("kubectl apply -f %s --cache-dir=/tmp", manifestsPath)

	// TODO: set options
	applyCmd = fmt.Sprintf("%s --timeout=%s", applyCmd, opts.Timeout.String())
	if opts.DryRun != "" {
		applyCmd = fmt.Sprintf("%s --dry-run=%s", applyCmd, opts.DryRun)
	}

	// TODO: override the default kubeconfig
	if kubeConfigPath != "" {
		applyCmd = fmt.Sprintf("%s --kubeconfig=%s", applyCmd, kubeConfigPath)
	} else {
		// TODO: setup kubectl impersonation
		impersonation := opts.Impersonation
		if opts.Impersonation == nil {
			impersonation = &ApplyImpersonation{
				Username: &types.NamespacedName{
					Name: fmt.Sprintf("gotk:%s:reconciler", opts.Kustomization.Namespace),
				},
			}
		}

		s.Log.V(2).Info(
			fmt.Sprintf("impersonating %s using %s", impersonation.String(), kubeConfigPath),
			"kustomization", opts.Kustomization.String())
	}

	// TODO: run the apply command
	timeoutCmd := opts.Timeout + (time.Second * 1)
	ctxCmd, cancel := context.WithTimeout(context.Background(), timeoutCmd)
	defer cancel()

	cmd := exec.CommandContext(ctxCmd, "/bin/sh", "-c", applyCmd)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("apply timeout: %w", err)
		}
		return nil, fmt.Errorf("apply failed: %s", string(output))
	}

	// TODO: transform output into ApplyResult
	result := s.parseOutput(output)

	return result, nil
}

func (s *Sync) parseOutput(in []byte) *ApplyResult {
	result := ApplyResult{
		Objects: []ApplyResultObject{},
	}
	input := strings.Split(string(in), "\n")
	if len(input) == 0 {
		return nil
	}
	var parts []string
	for _, str := range input {
		if str != "" {
			parts = append(parts, str)
		}
	}
	for _, str := range parts {
		kv := strings.Split(str, " ")
		if len(kv) > 1 {
			obj := ApplyResultObject{
				GroupVersionKind: schema.GroupVersionKind{
					Group:   "",
					Version: "",
					Kind:    strings.Split(kv[1], "/")[0],
				},
				NamespacedName: types.NamespacedName{
					Namespace: "",
					Name:      strings.Split(kv[1], "/")[1],
				},
				Status: kv[1],
			}
			result.Objects = append(result.Objects, obj)
		}
	}

	return &result
}
