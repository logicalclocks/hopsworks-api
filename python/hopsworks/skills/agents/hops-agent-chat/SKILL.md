---
name: hops-agent-chat
description: Use when putting a Hopsworks agent deployment in front of people through a chat page — the agent's ready-made chat app, customizing it (branding, layout, routes), or embedding the chat in an app of your own (FastAPI, Flask, Node.js) with the `@hopsworks/agent-chat` package. Auto-invoke for "chat app", "chat UI for my agent", "customize the chat page", "put the agent chat in my app", `create_chat_app`, `customize_chat_app`, `hopsworks-agent-chat`, or `create_app(agent=...)`. Input an agent deployment; output a chat people can open, as the platform's page or inside your own.
---

# A chat for an agent

An agent deployment (see **hops-agent-deployment**) answers the [chat contract](https://docs.hopsworks.ai/) over `/v1/run` (AG-UI). Three ways to put a page in front of it, from least to most work. Pick the first that fits; the later ones are opt-in, never the default.

| Need | Do |
| --- | --- |
| A chat page, now | the agent's **chat app**: `agent.create_chat_app()` |
| The same page, your look or layout, routes of your own | **customize** it: `agent.customize_chat_app()`, edit the project folder |
| The chat inside an app that already has a page of its own | **embed** it: `create_app(..., agent=agent)` + `hopsworks-agent-chat` beside your server |

Never write a chat page against the agent's API from scratch for these: the package already handles who is chatting, keeping each person's conversations apart, no serving key in the browser, the agent's questions (human in the loop), feedback, attachments, and waking an agent that scales to zero.

## 1. The chat app

```python
agent = project.get_agent_serving().get_agent("support")
app = agent.create_chat_app()  # app "support_chat", started; or deploy_agent(..., chat_app=True)
print(app.app_url)
agent.chat_app()  # the app, or None
```

Members open it with their Hopsworks login; `app.make_public()` shares it with anyone holding the link (needs `enable_public_apps`). Branding without code: the app's `CHAT_TITLE`, `CHAT_WELCOME`, `CHAT_ACCENT_COLOR`, `CHAT_LOGO_URL`, `CHAT_THEME_DIR` (a project folder with `theme.css` and a logo), `CHAT_SHOW_TOOL_CALLS`. Deleting the agent deletes its chat app; a data owner deletes it alone with `agent.delete_chat_app()`.

## 2. Customizing the chat app

```python
app = agent.customize_chat_app()  # restarts from Resources/chat-apps/support_chat
```

The app copies its template into that folder (`server.mjs` on `@hopsworks/agent-chat`, `web/main.jsx`, `web/theme.css`, `AGENTS.md`) and runs from it. **Read the folder's `AGENTS.md` before changing anything**: `<HopsworksChat />` slots (`headerStart`, `headerEnd`, `footer`, `children`, `components`, `chatProps`), the exported parts (`Header`, `Sidebar`, `Questions`, `AssistantMessage`, `NegativeFeedback`, `useChat()`), `startChatApp()` settings and its `routes` hook (given `who`: never trust the request for who is asking), and what must stay the package's. The package is in the app's image, not the terminal's: try a change with `app.redeploy()` and `app.get_logs()`; a folder that fails to build is logged and the default page served. Removing `CHAT_APP_DIR` from the app's variables goes back to the default app.

## 3. Embedding the chat in an app of your own

For an app that already has a UI (a FastAPI or Flask app with its own pages, say) and wants the chat as a part of it. Opt-in per app: name the agent on it.

```python
apps = project.get_app_api()
app = apps.create_app(
    "support_ui",
    app_path="Resources/support_ui/app.py",
    app_kind="CUSTOM",
    entrypoint_command=(
        "cd $APP_DIR && hopsworks-agent-chat --port 8081 --base-path chat/ & "
        "cd $APP_DIR && uvicorn app:app --host 0.0.0.0 --port $APP_PORT"
    ),
    app_port=8000,
    agent=agent,  # the Agent, or its id; an agent of this project
    db_access=False,
)
```

With `agent=` Hopsworks starts the app as it starts a chat app: `CHAT_AGENT_URL` (and `CHAT_AGENT_HOST`) say where the agent is inside the cluster, `HOPSWORKS_APP_IDENTITY_KEY` is the key the proxy signs members' names with. Without it the app gets neither (the default). In the UI it is the **Agent** setting of the app.

The app then runs three parts:

- **`hopsworks-agent-chat --port 8081 --base-path chat/`**: the chat server alone, no page, from the `@hopsworks/agent-chat` package in `python-app-pipeline` (`--help` for the options; `CHAT_PORT` and `CHAT_BASE_PATH` are the same as variables). Not on `$APP_PORT`: that is the app's own.
- **A forward of `chat/*` in the app's own server** to `http://127.0.0.1:8081/*`: same method and body, the `Cookie` header and the `Set-Cookie` headers back, and the proxy's `X-Hopsworks-User`, `X-Hopsworks-User-Time`, `X-Hopsworks-User-Signature` and `X-Forwarded-For` headers, which are how the chat server knows who is chatting; streamed, since replies are SSE. In FastAPI that is an `httpx.AsyncClient` with `client.build_request(...)` + `client.send(..., stream=True)` returned as a `StreamingResponse` (the docs page "Share an agent through a chat app" has the twenty lines).
- **The page**, React, bundled from the package: `mount(<HopsworksChat apiBase="chat/" />)` from `@hopsworks/agent-chat/react`, the same slots and parts as a customized chat app. Build it at start with the package (`node -e "import('@hopsworks/agent-chat').then((m) => m.buildPage('.', './static/chat'))"` for a folder holding `web/main.jsx`), or with a bundler of your own, and serve the output as static files.

`--base-path` and `apiBase` are the same path: the server makes the links in the replies it streams with the one, the page its requests with the other.

Keep the chat server's own rules: do not add routes that read other people's conversations, never render what the agent or a visitor said as HTML, and keep the forward to the signed headers above (do not let the page set `X-Hopsworks-User`: the server refuses what the signature does not cover). A public link on such an app makes the agent reachable by anyone holding the link, as it does for a chat app.

## Pitfalls

- A FastAPI app embeds the chat on `python-app-pipeline` (FastAPI, uvicorn, httpx and the package are there); `python-agent-pipeline` has no `@hopsworks/agent-chat`.
- `create_app(agent=...)` only takes an agent deployment of the same project (`PYTHON_APP_AGENT_INVALID` otherwise); a model deployment is not an agent.
- The chat server answers 401 to anything that did not come through the Hopsworks proxy once it has a key: a forward that drops the signed headers makes every member a visitor or a refusal.
- Forwarding must stream: a proxy that buffers the reply holds the SSE until the turn ends, and long turns hit the gateway's timeout.
