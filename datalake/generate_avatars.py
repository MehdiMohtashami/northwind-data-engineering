"""
One-shot generator: creates a simple placeholder avatar (colored circle +
initials + EmployeeID) per current DimEmployees row and uploads it to the
MinIO data lake, keyed by EmployeeCode (the OLTP EmployeeID).

Run once via a throwaway container on nw_de_net:
    docker run --rm --network nw_de_net -v $(pwd)/datalake:/app -w /app \
      -e CH_HOST=clickhouse -e CH_PASSWORD=... \
      -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ROOT_USER=... -e MINIO_ROOT_PASSWORD=... \
      python:3.11-slim bash -c "pip install --quiet pillow boto3 clickhouse-connect && python generate_avatars.py"

Prints one JSON line per uploaded object (EmployeeCode, object key, url,
content_type, size) -- this is the data lake's metadata, later loaded into
employee_photo_catalog (see link_photos_to_dw.py).
"""
import json
import os

import boto3
import clickhouse_connect
from PIL import Image, ImageDraw, ImageFont

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "NorthwindDW")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_PUBLIC_BASE_URL = os.environ.get("MINIO_PUBLIC_BASE_URL", "http://localhost:9002")
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
BUCKET = "employee-photos"

SIZE = 256
PALETTE = [
    (231, 76, 60), (52, 152, 219), (46, 204, 113), (155, 89, 182), (241, 196, 15),
    (230, 126, 34), (26, 188, 156), (52, 73, 94), (149, 165, 166),
]


def make_avatar(employee_id, first_name, last_name):
    color = PALETTE[(employee_id - 1) % len(PALETTE)]
    img = Image.new("RGB", (SIZE, SIZE), color="white")
    draw = ImageDraw.Draw(img)
    draw.ellipse([8, 8, SIZE - 8, SIZE - 8], fill=color)

    initials = f"{first_name[0]}{last_name[0]}".upper()
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 96)
        small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
    except OSError:
        font = ImageFont.load_default()
        small_font = font

    bbox = draw.textbbox((0, 0), initials, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(((SIZE - tw) / 2 - bbox[0], (SIZE - th) / 2 - bbox[1] - 10), initials, fill="white", font=font)

    code = f"EMP{employee_id}"
    bbox2 = draw.textbbox((0, 0), code, font=small_font)
    tw2 = bbox2[2] - bbox2[0]
    draw.text(((SIZE - tw2) / 2, SIZE - 44), code, fill="white", font=small_font)

    return img


def main():
    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE,
    )
    res = ch.query(
        "SELECT EmployeeID, FirstName, LastName FROM DimEmployees FINAL WHERE is_current = 1 ORDER BY EmployeeID"
    )
    employees = res.result_rows

    s3 = boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER, aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )

    catalog = []
    for employee_id, first_name, last_name in employees:
        code = f"EMP{employee_id}"
        key = f"{code}.png"
        img = make_avatar(employee_id, first_name, last_name)

        import io
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        data = buf.getvalue()

        s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType="image/png")

        record = {
            "employee_code": code,
            "employee_id": employee_id,
            "object_key": key,
            "url": f"{MINIO_PUBLIC_BASE_URL}/{BUCKET}/{key}",
            "content_type": "image/png",
            "size_bytes": len(data),
        }
        catalog.append(record)
        print(json.dumps(record))

    with open("/app/photo_catalog.json", "w") as f:
        json.dump(catalog, f, indent=2)


if __name__ == "__main__":
    main()
