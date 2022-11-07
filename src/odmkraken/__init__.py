from .busspeeds.repo import busspeeds as repo_busspeeds
try:
  from ._version import __version__
except ImportError:
  __version__ = '(unknown)'  # simplifies handling of tests
