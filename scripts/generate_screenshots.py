from __future__ import annotations
import json
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path("docs/images")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Try to load a monospace font; fall back to default
def get_font(size=14):
    try:
        return ImageFont.truetype("Consolas.ttf", size)
    except Exception:
        try:
            return ImageFont.truetype("DejaVuSansMono.ttf", size)
        except Exception:
            return ImageFont.load_default()

# Terminal-style image
terminal_lines = []
for i in range(1, 6):
    terminal_lines.append(f"[demo] produced event evt_{i} -> topic=github_events")
terminal_lines.append("")
terminal_lines.append("[demo] Wrote 5 events to docs\\images\\demo_events.json")
terminal_lines.append("[demo] You can now take screenshots of this terminal output and the file at the path above.")

font = get_font(16)
padding = 16
# compute line height using ImageDraw.textbbox for compatibility
tmp_img = Image.new("RGB", (10, 10))
tmp_draw = ImageDraw.Draw(tmp_img)
bbox = tmp_draw.textbbox((0, 0), "A", font=font)
line_height = (bbox[3] - bbox[1]) + 6
width = 1000
height = padding * 2 + line_height * len(terminal_lines)
img = Image.new("RGB", (width, height), (2, 34, 45))
d = ImageDraw.Draw(img)
text_color = (158, 230, 212)
for idx, line in enumerate(terminal_lines):
    d.text((padding, padding + idx * line_height), line, font=font, fill=text_color)
img_path = OUT_DIR / "screenshot-producer-terminal.png"
img.save(img_path)
print(f"Wrote {img_path}")

# JSON preview image
json_path = Path("docs/images/demo_events.json")
if json_path.exists():
    with open(json_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    pretty = json.dumps(data, indent=2)
else:
    pretty = "[]"

lines = pretty.splitlines()
font = get_font(14)
# recompute line height for JSON font size
tmp_img = Image.new("RGB", (10, 10))
tmp_draw = ImageDraw.Draw(tmp_img)
bbox = tmp_draw.textbbox((0, 0), "A", font=font)
line_height = (bbox[3] - bbox[1]) + 4
width = 900
# compute height with max 60 lines clamp
max_lines = 60
height = padding * 2 + line_height * min(len(lines), max_lines)
img = Image.new("RGB", (width, height), (12, 18, 28))
d = ImageDraw.Draw(img)
text_color = (207, 225, 255)
# draw a simple box
for idx, line in enumerate(lines[:max_lines]):
    d.text((padding, padding + idx * line_height), line, font=font, fill=text_color)
json_img_path = OUT_DIR / "demo_events_json.png"
img.save(json_img_path)
print(f"Wrote {json_img_path}")
