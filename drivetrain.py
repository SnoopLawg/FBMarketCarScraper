"""Drivetrain detection from car name text and model-specific defaults."""

import re

# ── Known default drivetrains by model ────────────────────────────
# These are the BASE drivetrain — many models offer AWD as an option.
# If a listing explicitly says AWD/4WD, that overrides the default.
_MODEL_DEFAULTS = {
    # Trucks — many base trims are RWD (SR, PreRunner, etc.)
    # Only give 4WD credit when explicitly stated in listing text.
    "tacoma": "unknown",
    "tundra": "unknown",
    "f150": "unknown",
    "f-150": "unknown",
    "ranger": "unknown",
    "colorado": "unknown",
    "silverado": "unknown",
    "sierra": "unknown",
    "frontier": "unknown",
    "gladiator": "4wd",    # Gladiator is always 4WD

    # SUVs — varies widely
    "4runner": "4wd",
    "wrangler": "4wd",
    "rav4": "fwd",         # Base is FWD, AWD is option
    "cr-v": "fwd",         # Base is FWD, AWD is option
    "crv": "fwd",
    "forester": "awd",     # Subaru = standard AWD
    "outback": "awd",
    "crosstrek": "awd",
    "impreza": "awd",
    "wrx": "awd",
    "cx-5": "fwd",
    "cx5": "fwd",
    "tucson": "fwd",
    "rogue": "fwd",
    "highlander": "fwd",
    "pilot": "fwd",
    "passport": "fwd",
    "santa fe": "fwd",
    "sorento": "fwd",
    "sportage": "fwd",
    "equinox": "fwd",
    "escape": "fwd",

    # Cars — mostly FWD
    "camry": "fwd",
    "corolla": "fwd",
    "civic": "fwd",
    "accord": "fwd",
    "elantra": "fwd",
    "sonata": "fwd",
    "altima": "fwd",
    "sentra": "fwd",
    "mazda3": "fwd",
    "mazda6": "fwd",
}

# Patterns to match in car_name (order matters — check specific first)
_DRIVETRAIN_PATTERNS = [
    (r'\b4x4\b', "4wd"),
    (r'\b4wd\b', "4wd"),
    (r'\bfour[- ]?wheel[- ]?drive\b', "4wd"),
    (r'\bawd\b', "awd"),
    (r'\ball[- ]?wheel[- ]?drive\b', "awd"),
    (r'\bfwd\b', "fwd"),
    (r'\bfront[- ]?wheel[- ]?drive\b', "fwd"),
    (r'\b2wd\b', "2wd"),
    (r'\b2wd\b', "2wd"),
    (r'\brwd\b', "rwd"),
    (r'\brear[- ]?wheel[- ]?drive\b', "rwd"),
    (r'\bprerunner\b', "rwd"),     # Toyota PreRunner = always RWD
]


def detect_drivetrain(car_name, car_query=""):
    """Detect drivetrain from listing text, falling back to model default.

    Returns: ("awd" | "4wd" | "fwd" | "2wd" | "rwd" | "unknown", source)
        source: "explicit" if found in text, "default" if from model table, "unknown"
    """
    text = (car_name or "").lower()

    # 1) Check explicit patterns in car_name
    for pattern, dt in _DRIVETRAIN_PATTERNS:
        if re.search(pattern, text):
            return dt, "explicit"

    # 2) Fall back to model-specific default
    query = (car_query or "").lower().strip()
    for model_key, default_dt in _MODEL_DEFAULTS.items():
        if model_key in query or model_key in text:
            return default_dt, "default"

    return "unknown", "unknown"


def drivetrain_label(dt):
    """Human-friendly label for drivetrain type."""
    return {
        "awd": "AWD",
        "4wd": "4WD",
        "fwd": "FWD",
        "2wd": "2WD",
        "rwd": "RWD",
        "unknown": "",
    }.get(dt, "")


def is_awd_or_4wd(dt):
    """True if drivetrain is AWD or 4WD (the premium types in snowy states)."""
    return dt in ("awd", "4wd")
