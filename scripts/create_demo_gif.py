"""Compose demo PNGs into an animated GIF for README/LinkedIn previews.

Requires: imageio and pillow

Usage:
    python scripts/create_demo_gif.py --out docs/images/demo.gif
"""
from __future__ import annotations
from pathlib import Path
import imageio
from PIL import Image

DEFAULT_OUT = Path("docs/images/demo.gif")
FRAMES = [
    "docs/images/screenshot-producer-terminal.png",
    "docs/images/demo_events_json.png",
    "docs/images/kafka-logs.png",
]

def main(out_path: Path = DEFAULT_OUT):
    frames = []
    for p in FRAMES:
        pp = Path(p)
        if not pp.exists():
            continue
        img = Image.open(pp).convert("RGBA")
        frames.append(img)
    if not frames:
        print("No frames found in docs/images/")
        return
    # save gif via imageio
    images = [imageio.v2.imread(str(pp)) for pp in FRAMES if Path(pp).exists()]
    imageio.mimsave(str(out_path), images, duration=1.0)
    print("Wrote GIF to", out_path)

if __name__ == "__main__":
    main()
