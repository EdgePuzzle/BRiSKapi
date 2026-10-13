"""Deprecated: the BRiSK site client is briskapi.brisk (it serves every supported broker's BRiSK)."""
import sys
import warnings

from briskapi import brisk

warnings.warn('briskapi.sbi is now briskapi.brisk', DeprecationWarning, stacklevel=2)
sys.modules[__name__] = brisk
