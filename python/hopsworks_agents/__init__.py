#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Agents on Hopsworks.

- ``hopsworks_agents.protocol`` -- the serving side: ``AgentApp`` and the
  Hopsworks Agent Protocol, memory, tracing, the wire conventions. Runs inside
  an agent deployment, on every request.
- ``hopsworks_agents.eval`` -- the operating side: the evaluation runner, the
  judges, the failure analysis, and the client behind
  ``project.get_agent_serving()``. Runs in jobs and notebooks.

The two never import each other's heavy parts; ``protocol`` must stay light
enough for a request path, and this package imports neither so that importing
one never pays for the other.
"""
