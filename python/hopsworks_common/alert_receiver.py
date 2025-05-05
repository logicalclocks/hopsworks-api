#
#   Copyright 2025 Hopsworks AB
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

import json
from typing import Optional

import humps
from hopsworks_common import util


class EmailConfig:
    def __init__(
        self,
        to=None,
        send_resolved: bool = False,
    ):
        if not isinstance(send_resolved, bool):
            raise TypeError("send_resolved must be set to a boolean value")
        if not isinstance(to, str):
            raise TypeError("to must be set to a string value")
        self._send_resolved = send_resolved
        self._to = to

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the email config as a dictionary"""
        return {
            "to": self._to,
            "sendResolved": self._send_resolved,
        }


class _EmailConfig:
    def __init__(
        self,
        to=None,
        send_resolved: bool = False,
        **kwargs,
    ):
        self._send_resolved = send_resolved
        self._to = to

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def to(self) -> Optional[str]:
        """return the email address of the email config"""
        return self._to

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the email config as a dictionary"""
        return {
            "to": self._to,
            "sendResolved": self._send_resolved,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"EmailConfig({self._to!r})"


class SlackConfig:
    def __init__(
        self,
        channel=None,
        send_resolved: bool = False,
    ):
        if not isinstance(send_resolved, bool):
            raise TypeError("send_resolved must be set to a boolean value")
        if not isinstance(channel, str):
            raise TypeError("to must be set to a string value")
        self._send_resolved = send_resolved
        self._channel = channel

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the slack config as a dictionary"""
        return {
            "channel": self._channel,
            "sendResolved": self._send_resolved,
        }


class _SlackConfig:
    def __init__(
        self,
        channel=None,
        send_resolved: bool = False,
        **kwargs,
    ):
        self._send_resolved = send_resolved
        self._channel = channel

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def channel(self) -> Optional[str]:
        """return the channel of the slack config"""
        return self._channel

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the slack config as a dictionary"""
        return {
            "channel": self._channel,
            "sendResolved": self._send_resolved,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"SlackConfig({self._channel!r})"


class PagerDutyConfig:
    def __init__(
        self,
        service_key=None,
        routing_key=None,
        send_resolved: bool = False,
    ):
        if not isinstance(send_resolved, bool):
            raise TypeError("send_resolved must be set to a boolean value")
        if not isinstance(service_key, str):
            raise TypeError("service_key must be set to a string value")
        if not isinstance(routing_key, str):
            raise TypeError("routing_key must be set to a string value")
        self._send_resolved = send_resolved
        self._service_key = service_key
        self._routing_key = routing_key

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the pager duty config as a dictionary"""
        return {
            "serviceKey": self._service_key,
            "routingKey": self._routing_key,
            "sendResolved": self._send_resolved,
        }


class _PagerDutyConfig:
    def __init__(
        self,
        service_key=None,
        routing_key=None,
        send_resolved: bool = False,
        **kwargs,
    ):
        self._send_resolved = send_resolved
        self._service_key = service_key
        self._routing_key = routing_key

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def service_key(self) -> Optional[str]:
        """return the service key of the pager duty config"""
        return self._service_key

    @property
    def routing_key(self) -> Optional[str]:
        """return the routing key of the pager duty config"""
        return self._routing_key

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the pager duty config as a dictionary"""
        return {
            "serviceKey": self._service_key,
            "routingKey": self._routing_key,
            "sendResolved": self._send_resolved,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"PagerDutyConfig({self._service_key!r}, {self._routing_key!r})"


class WebhookConfig:
    def __init__(
        self,
        url=None,
        send_resolved: bool = False,
    ):
        if not isinstance(send_resolved, bool):
            raise TypeError("send_resolved must be set to a boolean value")
        if not isinstance(url, str):
            raise TypeError("url must be set to a string value")
        self._send_resolved = send_resolved
        self._url = url

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the webhook config as a dictionary"""
        return {
            "url": self._url,
            "sendResolved": self._send_resolved,
        }


class _WebhookConfig:
    def __init__(
        self,
        url=None,
        send_resolved: bool = False,
        **kwargs,
    ):
        self._send_resolved = send_resolved
        self._url = url

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def url(self) -> Optional[str]:
        """return the url of the webhook config"""
        return self._url

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the webhook config as a dictionary"""
        return {
            "url": self._url,
            "sendResolved": self._send_resolved,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"WebhookConfig({self._url!r})"


class AlertReceiver:
    NOT_FOUND_ERROR_CODE = 390003
    def __init__(
        self,
        name=None,
        email_configs=None,
        slack_configs=None,
        pager_duty_configs=None,
        webhook_configs=None,
        **kwargs,
    ):
        self._name = name
        self._email_configs = (
            [_EmailConfig.from_response_json(config) for config in email_configs]
            if email_configs
            else None
        )
        self._slack_configs = (
            [_SlackConfig.from_response_json(config) for config in slack_configs]
            if slack_configs
            else None
        )
        self._pager_duty_configs = (
            [
                _PagerDutyConfig.from_response_json(config)
                for config in pager_duty_configs
            ]
            if pager_duty_configs
            else None
        )
        self._webhook_configs = (
            [_WebhookConfig.from_response_json(config) for config in webhook_configs]
            if webhook_configs
            else None
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if "items" in json_decamelized:
                return [cls(**receiver) for receiver in json_decamelized["items"]]
            else:
                return []
        else:
            return cls(**json_decamelized)

    @property
    def name(self) -> Optional[str]:
        """return the name of the alert receiver"""
        return self._name

    @property
    def email_configs(self) -> Optional[list]:
        """return the email configs of the alert receiver"""
        return self._email_configs

    @property
    def slack_configs(self) -> Optional[list]:
        """return the slack configs of the alert receiver"""
        return self._slack_configs

    @property
    def pager_duty_configs(self) -> Optional[list]:
        """return the pager duty configs of the alert receiver"""
        return self._pager_duty_configs

    @property
    def webhook_configs(self) -> Optional[list]:
        """return the webhook configs of the alert receiver"""
        return self._webhook_configs

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the alert receiver as a dictionary"""
        dict = {
            "name": self._name,
        }
        if self._email_configs:
            dict["email_configs"] = [config.to_dict() for config in self._email_configs]
        if self._slack_configs:
            dict["slack_configs"] = [config.to_dict() for config in self._slack_configs]
        if self._pager_duty_configs:
            dict["pager_duty_configs"] = [
                config.to_dict() for config in self._pager_duty_configs
            ]
        if self._webhook_configs:
            dict["webhook_configs"] = [
                config.to_dict() for config in self._webhook_configs
            ]
        return dict

    def __str__(self):
        return self.json()

    def _get_config(self):
        if self._email_configs:
            return self._email_configs
        if self._slack_configs:
            return self._slack_configs
        if self._pager_duty_configs:
            return self._pager_duty_configs
        if self._webhook_configs:
            return self._webhook_configs

    def __repr__(self):
        return f"AlertReceiver({self._name!r}, {self._get_config()!r})"
