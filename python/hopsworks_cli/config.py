"""Configuration management for CLI"""

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, Dict

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


# Default configuration
DEFAULT_HOST = "stagingmain.devnet.hops.works"
DEFAULT_PORT = 443

# Configuration file location
CONFIG_DIR = Path.home() / ".hopsworks"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


@dataclass
class Profile:
    """Configuration profile"""
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    project: Optional[str] = None
    api_key_file: Optional[str] = None
    hostname_verification: bool = False
    trust_store_path: Optional[str] = None
    engine: str = "python"

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)


class Config:
    """Configuration manager"""

    def __init__(self):
        self.config_file = CONFIG_FILE
        self.profiles: Dict[str, Profile] = {}
        self.default_profile = "default"
        self.load()

    def load(self):
        """Load configuration from file"""
        if not YAML_AVAILABLE:
            # If YAML not available, use default profile only
            self.profiles["default"] = Profile()
            return

        if self.config_file.exists():
            try:
                with open(self.config_file) as f:
                    data = yaml.safe_load(f) or {}
                    self.default_profile = data.get("default_profile", "default")

                    profiles_data = data.get("profiles", {})
                    for name, profile_data in profiles_data.items():
                        self.profiles[name] = Profile(**profile_data)

                    # Ensure default profile exists
                    if self.default_profile not in self.profiles:
                        self.profiles[self.default_profile] = Profile()
            except Exception as e:
                # If loading fails, use default
                self.profiles["default"] = Profile()
        else:
            # Create default profile
            self.profiles["default"] = Profile()

    def save(self):
        """Save configuration to file"""
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML is required for saving configuration. Install with: pip install pyyaml")

        # Create config directory if it doesn't exist
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        data = {
            "default_profile": self.default_profile,
            "profiles": {
                name: profile.to_dict()
                for name, profile in self.profiles.items()
            }
        }

        with open(self.config_file, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

    def get_profile(self, name: Optional[str] = None) -> Profile:
        """
        Get profile by name or default

        # Arguments
            name: Profile name, defaults to default_profile

        # Returns
            Profile object
        """
        profile_name = name or self.default_profile

        if profile_name not in self.profiles:
            # Create new profile with defaults
            self.profiles[profile_name] = Profile()

        return self.profiles[profile_name]

    def add_profile(self, name: str, profile: Profile):
        """
        Add or update a profile

        # Arguments
            name: Profile name
            profile: Profile object
        """
        self.profiles[name] = profile

    def delete_profile(self, name: str):
        """
        Delete a profile

        # Arguments
            name: Profile name
        """
        if name in self.profiles:
            del self.profiles[name]
            # If deleted profile was default, switch to 'default'
            if self.default_profile == name:
                self.default_profile = "default"
                if "default" not in self.profiles:
                    self.profiles["default"] = Profile()

    def set_default_profile(self, name: str):
        """
        Set default profile

        # Arguments
            name: Profile name
        """
        if name not in self.profiles:
            self.profiles[name] = Profile()
        self.default_profile = name
