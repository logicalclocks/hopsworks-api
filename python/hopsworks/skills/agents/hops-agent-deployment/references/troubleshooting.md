# When an agent fails to answer or start

**When every chat fails.** A running agent whose handler raises answers `/v1/chat` with a 500 whose body says why (`detail.code` `agent_error`, the exception in `detail.message`), and the traceback is in `hops agent logs`; an agent built on an older image returns a bare `500 Internal Server Error`, and then the log is the only place the reason is. A frequent cause after changing the model is a setting the new model refuses: gpt-6 and the o-series fix their own sampling and reject `temperature`, so leave it out for them.

**When a start takes too long, or says nothing.** A first start on a node that has not pulled the agent image can take ten minutes or more, and `start(await_running=...)` giving up does **not** stop the deployment — it usually comes up shortly after. When `hops agent info` shows an empty status and `hops agent logs` prints only `[failed to read pod logs]`, the container never started, so there are no logs to read: look at the pod instead.

```bash
kubectl get pods -n <project> | grep <name>-predictor    # ImagePullBackOff, Pending, OOMKilled
kubectl describe pod -n <project> <pod>                  # Events says which
```

See **hops-kubectl-debug**. `Pending` with no events is usually resources; `ImagePullBackOff` on a first deploy is the image still being pulled or missing from the cluster's registry.
