#!/usr/bin/env python3
"""Local privacy redaction for ECHOREPO photographs.

Uses RapidOCR text DETECTION ONLY (no text extraction or logging), plus
OpenCV's frontal-face Haar cascade and QR-code detector. No image data is sent to a cloud API.

This is best-effort detection: small/oblique plates, handwriting, profiles,
and non-frontal faces may be missed. Human review is necessary before treating
images as guaranteed anonymized.
"""
from __future__ import annotations

import argparse
from functools import lru_cache
from pathlib import Path


def _libraries():
    import cv2
    import numpy as np
    return cv2, np


@lru_cache(maxsize=1)
def _ocr_engine():
    from rapidocr import RapidOCR
    return RapidOCR()


@lru_cache(maxsize=1)
def _face_cascade():
    cv2, _ = _libraries()
    classifier_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    cascade = cv2.CascadeClassifier(classifier_path)
    if cascade.empty():
        raise RuntimeError(f"Could not load OpenCV face cascade: {classifier_path}")
    return cascade


def _bounding_rect(polygon, scale: float, width: int, height: int, margin: float):
    """Return an expanded, clamped rectangle in full-size image coordinates."""
    _, np = _libraries()
    points = np.asarray(polygon, dtype=float).reshape(-1, 2) * scale
    x1 = max(0, float(points[:, 0].min()))
    y1 = max(0, float(points[:, 1].min()))
    x2 = min(width, float(points[:, 0].max()))
    y2 = min(height, float(points[:, 1].max()))
    if x2 <= x1 or y2 <= y1:
        return None
    pad_x = max(6, (x2 - x1) * margin)
    pad_y = max(6, (y2 - y1) * margin)
    x1 = max(0, int(x1 - pad_x))
    y1 = max(0, int(y1 - pad_y))
    x2 = min(width, int(x2 + pad_x + 1))
    y2 = min(height, int(y2 + pad_y + 1))
    return (x1, y1, x2, y2) if x2 > x1 and y2 > y1 else None


def _obscure_region(rgb, rect, mode: str):
    """Overwrite pixels (strong mosaicing + Gaussian, or a solid rectangle)."""
    cv2, np = _libraries()
    x1, y1, x2, y2 = rect
    roi = rgb[y1:y2, x1:x2]
    h, w = roi.shape[:2]
    if h < 1 or w < 1:
        return
    if mode == "solid":
        # A single opaque, locally matched color provides the strongest obscuring.
        roi[:] = np.median(roi.reshape(-1, 3), axis=0).astype(np.uint8)
    elif mode == "blur":
        # Deliberately much stronger than a normal photographic Gaussian blur:
        # discard ~95% of the original spatial resolution, then smooth.
        tiny = cv2.resize(roi, (max(1, w // 20), max(1, h // 20)),
                          interpolation=cv2.INTER_AREA)
        pixelated = cv2.resize(tiny, (w, h), interpolation=cv2.INTER_LINEAR)
        roi[:] = cv2.GaussianBlur(pixelated, (0, 0),
                                  sigmaX=max(3, w / 15), sigmaY=max(3, h / 15))
    else:
        raise ValueError("mode must be 'blur' or 'solid'")


def redact_pil_image(image, *, blur_text=True, blur_faces=True, blur_qr=True,
                     mode="blur", detect_max_side=2600, margin=0.20):
    """Return (redacted_PIL_RGB_image, {'text_regions': N, 'faces': N}).

    Input must already have EXIF orientation applied. Raises if a detector fails;
    the caller MUST NOT publish the original as a fallback.
    """
    from PIL import Image
    cv2, np = _libraries()

    if mode not in {"blur", "solid"}:
        raise ValueError("Invalid redaction mode")
    if detect_max_side < 512:
        raise ValueError("detect_max_side must be >= 512")
    if not (0 <= margin <= 1):
        raise ValueError("margin must be in [0,1]")
    if not (blur_text or blur_faces or blur_qr):
        raise ValueError("At least one privacy detector must be enabled")

    full = image.convert("RGB")
    width, height = full.size
    if min(width, height) < 1:
        raise ValueError("Empty image")
    scale = max(1.0, max(width, height) / detect_max_side)
    if scale > 1:
        work = full.resize((max(1, round(width / scale)),
                            max(1, round(height / scale))), Image.Resampling.LANCZOS)
    else:
        work = full
    # Use exact size ratio to map detections to original pixels; rounding may differ.
    sx = width / work.width
    sy = height / work.height
    regions = []
    counts = {"text_regions": 0, "faces": 0, "qr_codes": 0}

    if blur_text:
        # Detection only is deliberately chosen. It does NOT require the OCR model
        # to recognize words, number plates or a particular written language.
        result = _ocr_engine()(work, use_det=True, use_cls=False, use_rec=False)
        boxes = getattr(result, "boxes", None)
        if boxes is not None:
            for polygon in boxes:
                points = np.asarray(polygon, dtype=float).reshape(-1, 2)
                points[:, 0] *= sx
                points[:, 1] *= sy
                rect = _bounding_rect(points, 1.0, width, height, margin)
                if rect:
                    regions.append(rect)
                    counts["text_regions"] += 1

    if blur_qr:
        detector = cv2.QRCodeDetector()
        work_bgr = cv2.cvtColor(np.asarray(work), cv2.COLOR_RGB2BGR)
        found, qr_points = detector.detectMulti(work_bgr)
        if found and qr_points is not None:
            for polygon in qr_points:
                points = np.asarray(polygon, dtype=float).reshape(-1, 2)
                points[:, 0] *= sx
                points[:, 1] *= sy
                rect = _bounding_rect(points, 1.0, width, height, max(margin, 0.30))
                if rect:
                    regions.append(rect)
                    counts["qr_codes"] += 1

    if blur_faces:
        work_rgb = np.asarray(work)
        gray = cv2.cvtColor(work_rgb, cv2.COLOR_RGB2GRAY)
        faces = _face_cascade().detectMultiScale(gray, scaleFactor=1.1,
                                                 minNeighbors=4, minSize=(24, 24))
        for x, y, w, h in faces:
            rect = _bounding_rect([(x * sx, y * sy),
                                   ((x + w) * sx, (y + h) * sy)],
                                  1.0, width, height, max(margin, 0.30))
            if rect:
                regions.append(rect)
                counts["faces"] += 1

    rgb = np.asarray(full).copy()
    for rect in regions:
        _obscure_region(rgb, rect, mode)
    return Image.fromarray(rgb, "RGB"), counts


def main():
    parser = argparse.ArgumentParser(description="Preview ECHOREPO privacy redaction offline")
    parser.add_argument("image", type=Path)
    parser.add_argument("--output", "-o", type=Path, required=True)
    parser.add_argument("--mode", choices=("blur", "solid"), default="blur")
    parser.add_argument("--text-only", action="store_true")
    parser.add_argument("--faces-only", action="store_true")
    parser.add_argument("--no-qr", action="store_true", help="Do not redact QR codes")
    parser.add_argument("--detect-max-side", type=int, default=2600)
    args = parser.parse_args()
    if args.text_only and args.faces_only:
        parser.error("Cannot combine --text-only and --faces-only")
    from PIL import Image, ImageOps
    with Image.open(args.image) as original:
        oriented = ImageOps.exif_transpose(original)
        redacted, counts = redact_pil_image(
            oriented,
            blur_text=not args.faces_only,
            blur_faces=not args.text_only,
            blur_qr=not args.no_qr,
            mode=args.mode,
            detect_max_side=args.detect_max_side,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    redacted.save(args.output, format="JPEG", quality=90, optimize=True)
    print(f"Redacted: {counts['text_regions']} text regions, {counts['faces']} faces, "
          f"{counts['qr_codes']} QR codes. "
          f"Saved {args.output}")


if __name__ == "__main__":
    main()
