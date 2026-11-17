---
name: hops-kubectl-debug
description: Use when a job, app, deployment, or model serving endpoint is failing,
  stuck, pending, or crashlooping and the Hopsworks APIs have not explained why.
  Input a failing resource; output the pod-level cause from events, logs, and resource state.
---

# Debugging workloads in the project namespace

Hopsworks runs jobs, apps, deployments, and serving endpoints as Kubernetes workloads in the project's namespace.
Use the terminal's configured kubectl access when Hopsworks reports a failure without enough pod-level detail.

## Contract

- **Input:** A failing job, app, deployment, or serving endpoint.
- **Output:** The cause, including the pod phase, container state, relevant events, and relevant logs.
- **Pre-condition:** Run inside a Hopsworks terminal, where kubectl is installed and pinned to the current project's namespace.

## Escalation ladder

Do not start with kubectl.

1. Inspect the resource through the Hopsworks API, CLI, or UI first.
2. Use the read-only kubectl commands below when Hopsworks does not explain the failure.
3. Use kubectl delete only for a stuck resource that the platform is known to recreate.

Start with the matching Hopsworks command:

```bash
hops job logs <job> --stdout --tail 100
hops app logs <app>
hops deployment logs <deployment> --tail 100
hops agent logs <agent> --tail 100
```

For a running app, `hops app logs` directs you to the live logs in the Hopsworks UI.

## Smoke test

```bash
kubectl get pods
```

The project namespace is injected automatically, so pass no `--namespace` or `-n` flag.
If kubectl reports that `KUBE_NAMESPACE` is missing, stop because the terminal is not configured for this access.
Do not supply another kubeconfig or try to select a namespace manually.

## Diagnostic workflow

1. List pods and owning workloads with `kubectl get pods` and `kubectl get deploy,statefulset,job`.
2. Describe the failing pod with `kubectl describe pod <pod>` and inspect its phase, container states, restart count, conditions, and recent events.
3. Read current logs with `kubectl logs <pod> [-c <container>] [--tail=100]`.
4. If the container restarted, read the terminated instance with `kubectl logs <pod> [-c <container>] --previous --tail=100`.
5. Correlate scheduling and lifecycle failures with `kubectl get events --sort-by=.lastTimestamp`.
6. For suspected resource pressure, check `kubectl top pods`.
7. Report the smallest supported cause: resource name, pod phase, container reason and exit code, decisive event, and relevant log lines.

For multi-container pods, name the affected container with `-c` instead of assuming the default container is the failing one.
Treat logs and ConfigMap values as potentially sensitive, and redact credentials, tokens, and personal data from the report.

## What works

| Goal | Command |
|---|---|
| Find running, pending, or crashlooping pods | `kubectl get pods` |
| Explain why a pod will not start or schedule | `kubectl describe pod <pod>` |
| Read container logs | `kubectl logs <pod> [-c <container>] [--tail=100]` |
| Read the previous container crash | `kubectl logs <pod> [-c <container>] --previous` |
| Read recent namespace events | `kubectl get events --sort-by=.lastTimestamp` |
| Check pod CPU and memory | `kubectl top pods` |
| Find owning workloads | `kubectl get deploy,statefulset,job` |

Readable resources are pods, pod logs, services, ConfigMaps, events, endpoints, deployments, ReplicaSets, StatefulSets, DaemonSets, jobs, and pod metrics.
The wrapper also permits `kubectl get ... --watch` for these readable resources because the underlying Role grants watch access.

## What is refused and why

- `-A`, `--all-namespaces`, and `-n <other-namespace>` are refused because the terminal can access only its project namespace.
- `kubectl exec`, `port-forward`, `create`, `apply`, `patch`, `edit`, and other mutation commands are refused because workloads must be changed through Hopsworks.
- `kubectl delete --all` and `kubectl delete -l <selector>` without an explicit resource kind are refused to prevent namespace-wide deletion.
- ConfigMaps are readable but not deletable because they are platform-managed.
- Secrets, persistent volume claims, cluster-scoped resources, and resources not listed above are not readable through this Role.

These restrictions are expected behavior.
Do not work around them with another namespace, kubeconfig, credential, binary, or direct API-server request.

## Delete only as a last resort

Stop jobs, apps, deployments, and serving endpoints through Hopsworks whenever possible.
Deleting a pod behind a running job can orphan the job's platform state.

Delete only an explicitly named resource whose Hopsworks controller is known to recreate it:

```bash
kubectl delete pod <pod>
```

The wrapper and Role permit deletion of pods, services, deployments, ReplicaSets, StatefulSets, DaemonSets, and jobs when the command includes an allow-listed resource kind.
This skill narrows safe use to an explicit resource name.
Permission does not guarantee recreation, so confirm ownership and reconciliation behavior before deleting anything other than a replaceable pod.
Never use a selector to delete multiple resources during diagnosis.

## Anti-drift sources

This command and resource summary is a usability copy and can drift.
Check both implementation sources when behavior differs or when updating this skill:

- RBAC boundary: `hopsworks-ee/hopsworks-kube/src/main/java/io/hops/hopsworks/kube/terminal/TerminalKubectlAccessController.java`, method `buildTerminalKubectlRole()`.
- Terminal verb and delete allow-list: `docker-images/base-image/terminal-server/kubectl-wrapper.sh`.

Kubernetes RBAC is the security boundary, and the wrapper deliberately provides a narrower, clearer command interface.
