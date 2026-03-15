#!/usr/bin/env python3
"""Convert an image to a 64x64 PNG favicon.

Usage: python make_favicon.py <input_image>
Outputs: static/favicon.png
"""
import sys
from pathlib import Path
from PIL import Image

if len(sys.argv) < 2:
    print("Usage: python make_favicon.py <input_image>")
    sys.exit(1)

src = Path(sys.argv[1])
if not src.exists():
    print(f"File not found: {src}")
    sys.exit(1)

out = Path(__file__).parent / "static" / "favicon.png"
out.parent.mkdir(exist_ok=True)

img = Image.open(src)
# Resize to 64x64, preserving aspect ratio with padding
img.thumbnail((64, 64), Image.LANCZOS)
# If not square, center on transparent canvas
if img.size != (64, 64):
    canvas = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    x = (64 - img.size[0]) // 2
    y = (64 - img.size[1]) // 2
    canvas.paste(img, (x, y))
    img = canvas

img.save(out, "PNG")
print(f"Saved {out} ({img.size[0]}x{img.size[1]})")
