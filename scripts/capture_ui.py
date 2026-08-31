from __future__ import annotations

import argparse
import math
import shutil
import subprocess
import tempfile
from pathlib import Path

from playwright.sync_api import sync_playwright


def ease_in_out(value: float) -> float:
    return 0.5 - math.cos(math.pi * value) / 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture the running MobilityFlow Control Room as PNG, MP4 and GIF"
    )
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--output-dir", type=Path, default=Path("docs"))
    parser.add_argument("--chromium", default="/snap/bin/chromium")
    parser.add_argument("--fps", type=int, default=12)
    return parser.parse_args()


def capture_frames(args: argparse.Namespace, frames_dir: Path) -> Path:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = args.output_dir / "control-room.png"
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=args.chromium,
            headless=True,
            args=["--no-sandbox", "--disable-gpu"],
        )
        page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=1)
        page.goto(args.base_url, wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#source-records')?.textContent?.includes('455')",
            timeout=15_000,
        )
        page.screenshot(path=str(screenshot_path), full_page=True)

        page_height = page.evaluate("document.documentElement.scrollHeight")
        max_scroll = max(0, page_height - 900)
        scenes = [0, 0, 720, 720, 1370, 1370, max_scroll, max_scroll]
        frame_number = 0
        frames_per_scene = args.fps
        for start, end in zip(scenes, scenes[1:], strict=False):
            for step in range(frames_per_scene):
                progress = step / max(frames_per_scene - 1, 1)
                scroll_y = round(start + (end - start) * ease_in_out(progress))
                page.evaluate("scrollY => window.scrollTo(0, scrollY)", scroll_y)
                page.screenshot(path=str(frames_dir / f"frame-{frame_number:04d}.png"))
                frame_number += 1
        browser.close()
    return screenshot_path


def encode_video(args: argparse.Namespace, frames_dir: Path) -> tuple[Path, Path]:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise SystemExit("ffmpeg is required to encode the capture")
    mp4_path = args.output_dir / "mobilityflow-live.mp4"
    gif_path = args.output_dir / "mobilityflow-live.gif"
    input_pattern = str(frames_dir / "frame-%04d.png")
    subprocess.run(
        [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-framerate",
            str(args.fps),
            "-i",
            input_pattern,
            "-c:v",
            "libx264",
            "-crf",
            "20",
            "-preset",
            "medium",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(mp4_path),
        ],
        check=True,
    )
    subprocess.run(
        [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-framerate",
            str(args.fps),
            "-i",
            input_pattern,
            "-vf",
            (
                "fps=8,scale=960:-1:flags=lanczos,split[s0][s1];"
                "[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer"
            ),
            str(gif_path),
        ],
        check=True,
    )
    return mp4_path, gif_path


def main() -> None:
    args = parse_args()
    frames_dir = Path(tempfile.mkdtemp(prefix="mobilityflow-capture-"))
    screenshot_path = capture_frames(args, frames_dir)
    mp4_path, gif_path = encode_video(args, frames_dir)
    print(
        "Control Room capture complete: "
        f"{screenshot_path.name}, {mp4_path.name}, {gif_path.name}"
    )


if __name__ == "__main__":
    main()
