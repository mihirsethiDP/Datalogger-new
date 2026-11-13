"""Transcribe a Hindi/Hinglish recording to English with faster-whisper.

Usage: python transcribe.py <audio_path> [out_path]
"""
import sys
import time

from faster_whisper import WhisperModel


def ts(seconds):
    s = int(seconds)
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def main():
    audio = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else "transcript.txt"

    # CPU only on this machine; int8 keeps "medium" fast enough for an hour of audio.
    model = WhisperModel("medium", device="cpu", compute_type="int8")
    # Force Hindi as the source language: auto-detect tends to call Hinglish "en",
    # and then the Hindi stretches get transliterated instead of translated.
    segments, info = model.transcribe(
        audio,
        task="translate",
        language="hi",
        beam_size=5,
        vad_filter=True,
    )

    start = time.time()
    with open(out_path, "w", encoding="utf-8") as f:
        for seg in segments:
            f.write(f"[{ts(seg.start)} - {ts(seg.end)}] {seg.text.strip()}\n")
            f.flush()
            pct = 100 * seg.end / info.duration
            print(f"\r{ts(seg.end)} / {ts(info.duration)} ({pct:.0f}%)", end="", file=sys.stderr)

    print(f"\nDone in {(time.time() - start) / 60:.1f} min -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
