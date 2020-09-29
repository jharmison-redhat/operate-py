#!/usr/bin/env python3
"""
operate.py

A wrapper around the Operator SDK to assist with building and bundling Ansible
operators for simple use-cases.

WORKFLOW:
IF --remove:
    - remove cr
    - uninstall operator
ELSE:
    IF initialize:
        - install operator-sdk
        - initialize operator
        - create api for each Kind
    IF build:
        - create operator images
    IF deploy:
        - build tasks
        IF --namespace=:
            - kustomize namespace
        - validate cluster access
        - install operator
        IF --custom-resource=:
            - deploy CR
    IF bundle:
        - install osdk
        - build tasks
        - build bundles
    IF push:
        - validate credentials
        - bundle tasks
        - push images
"""

from typing import List, Iterable, TypeVar
import click
import sys
import yaml
import subprocess
import shlex
import os
import os.path
import platform
import logging
import logging.handlers
from operator_sdk_manager.update import operator_sdk_update


if platform.system() != "Linux":
    raise RuntimeError("operate.py is designed only for Linux.")

T = TypeVar("Operator")


class Operator(object):
    """
    A class to keep track of our operator settings file. Includes configuration
    information about our operator and bundle as well as version information.
    Can use this information to execute various operator-sdk and opm commands
    to manage your operator project.
    """
    osdk_config = {
        "osdk_path": os.path.join(
            os.environ["HOME"], ".local", "bin", "operator-sdk"
        ),
        "osdk_url": ("https://github.com/operator-framework/operator-sdk/"
                     "releases/download/v"),
    }

    def __init__(self, image: str = None, version: str = None,
                 channels: List[str] = [], kinds: List[str] = [],
                 default_sample: str = None, domain: str = None,
                 group: str = None, api_version: str = None,
                 initialized: bool = False, verbosity: int = None) -> None:
        self.image = image
        self.version = version
        self.channels = channels
        self.kinds = kinds
        self.default_sample = default_sample
        self.domain = domain
        self.group = group
        self.api_version = api_version
        self.runtime = self._determine_runtime()
        self.logger = self.get_logger(verbosity=verbosity)

    def __repr__(self) -> str:
        return ("OperatorSettings(image={}, version={}, channels={}, kinds={},"
                " default_sample={}, domain={}, group={}, api_version={}"
                " initialized={})").format(
            self.image,
            self.version,
            self.channels,
            self.kinds,
            self.sample,
            self.domain,
            self.group,
            self.api_version,
            self.initialized
        )

    def get_logger(self, verbosity: int = None) -> logging.Logger:
        """
        Creates a logger in a dynamic way, allowing us to call it multiple
        times if needed and only creating it once.
        """
        logger = logging.getLogger('Operator')
        logger.setLevel(logging.DEBUG)

        if len(logger.handlers) == 0:
            # A well-parsable format
            _format = '{asctime} {name} [{levelname:^9s}]: {message}'
            formatter = logging.Formatter(_format, style='{')

            stderr = logging.StreamHandler()
            stderr.setFormatter(formatter)
            if verbosity is not None:
                # Sets log level based on verbosity, verbosity=3 is the most
                #   verbose log level
                stderr.setLevel(40 - (min(3, verbosity) * 10))
            else:
                # Use WARNING level verbosity
                stderr.setLevel(40)
            logger.addHandler(stderr)

            if os.path.exists('/dev/log'):
                # Use the default syslog socket at INFO level
                syslog = logging.handlers.SysLogHandler(address='/dev/log')
                syslog.setFormatter(formatter)
                syslog.setLevel(logging.INFO)
                logger.addHandler(syslog)

        elif verbosity is not None:
            # We may have already created the logger, but without specifying
            #   verbosity. So here, we grab the stderr handler and set its
            #   verbosity level.
            stderr = logger.handlers[0]
            stderr.setLevel(40 - (min(3, verbosity) * 10))

        return logger

    @classmethod
    def load(cls, file: str = "operate.yml") -> T:
        """
        Alternate constructor that lods the necessary operator settings from a
        properly structured yaml file.
        """
        logger = cls.get_logger()
        with open(file) as f:
            settings = yaml.safe_load(f)
        logger.debug("Recovered settings:")
        logger.debug(settings)

        return cls(image=settings.get("image"),
                   version=settings.get("version"),
                   channels=settings.get("channels"),
                   kinds=settings.get("kinds"),
                   default_sample=settings.get("default-sample"),
                   domain=settings.get("domain"),
                   group=settings.get("group"),
                   api_version=settings.get("api-version"))

    @staticmethod
    def _utf8ify(line_bytes: List[bytes] = None) -> str:
        """
        Decodes type line_bytes input as utf-8 and strips excess whitespace
        from the end.
        """
        return line_bytes.decode("utf-8").rstrip()

    @classmethod
    def shell(cls, cmd: str = None, fail: bool = True) -> Iterable[str]:
        """
        Runs a command in a subprocess, yielding lines of output from it and
        optionally failing with its non-zero return code.
        """
        logger = cls.get_logger()
        logger.debug("Running: {}".format(cmd))
        proc = subprocess.Popen(shlex.split(cmd),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        for line in map(cls._utf8ify, iter(proc.stdout.readline, b'')):
            logger.debug("Line:    {}".format(line))
            yield line

        ret = proc.wait()
        if fail and ret != 0:
            logger.error("Command errored: {}".format(cmd))
            exit(ret)
        elif ret != 0:
            logger.warning("Command returned {}: {}".format(ret, cmd))

    def _install_operator_sdk(self, version: str = "latest") -> None:
        """
        Downloads the requested OSDK binary from their releases page, saves it
        in the configured osdk_path with a version identifier postpended, and
        symlinks into place.
        """
        installed_version = operator_sdk_update(
            directory=os.path.dirname(self.osdk_config["osdk_path"]),
            path=os.path.dirname(self.osdk_config["osdk_path"]),
            version=version
        )
        self.logger.info("Operator SDK version {} installed".format(
            installed_version
        ))

    @classmethod
    def _determine_runtime(cls) -> str:
        """
        Determine the container runtime that should be used to build operator
        images.
        """
        for line in cls.shell("which docker", fail=False):
            if line.endswith("/docker") and not os.path.islink(line):
                return "docker"
        for line in cls.shell("which podman", fail=False):
            if line.endswith("/podman") and not os.path.islink(line):
                return "podman"
        raise RuntimeError("Unable to identify a container runtime!")

    def initialize_operator(self) -> None:
        """
        Initialize an Ansible Operator SDK operator and create the APIs
        represented by the Kinds specified in the settings.
        """
        if self.initialized:
            return

        _ = self.shell(
            "operator-sdk init --plugins=ansible --domain={}".format(
                self.domain
            )
        )
        _ = [self.shell(
            "operator-sdk create api --group={} --version={} --kind={}".format(
                self.group, self.version, kind
            )
        ) for kind in self.kinds]

        self.initialized = True

    def _build_operator(self, tag: str = None) -> None:
        if tag is None:
            build_tag = self.version
        else:
            build_tag = tag
        self.logger.info("Building {}:{}".format(self.image, build_tag))


# We'll be using these repeatedly
def verbose_opt(func):
    return click.option(
        "-v", "--verbose", count=True,
        help="Increase verbosity (specify multiple times for more)."
    )(func)


def tag_extension_opt(func):
    return click.option(
        "-t", "--tag-extension",
        help="Extend the tag of the index image with an identifier."
    )(func)


@click.group(invoke_without_command=True)
@verbose_opt
@click.version_option()
def main(verbose):
    """
    Build and push Operator Framework-based Ansible operator and OLM Bundle
    """
    logger = Operator.get_logger(verbose)
    logger.debug(sys.argv)
    logger.debug(f"verbose: {verbose}")
