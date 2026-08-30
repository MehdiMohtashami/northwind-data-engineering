"""
One-shot: extracts the real Northwind.dbo.Employees.Photo blobs (OLE-wrapped
bitmaps) from SQL Server, strips the OLE Object header to recover the raw BMP,
converts to PNG via Pillow, and uploads to the MinIO `employee-photos` bucket
keyed by EmployeeCode (EMP<EmployeeID>.png) -- same keying as the placeholder
avatars this replaces, so DimEmployees.PhotoUrl keeps resolving unchanged.

Falls back to the existing initials avatar (regenerated in-process) for any
employee whose blob fails to decode as a BMP.

Run once via a throwaway container on nw_de_net:
    docker run --rm --network nw_de_net -v $(pwd)/datalake:/app -w /app \
      -e MSSQL_HOST=sqlserver -e MSSQL_SA_PASSWORD=... \
      -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ROOT_USER=... -e MINIO_ROOT_PASSWORD=... \
      python:3.11-slim bash -c "pip install --quiet pymssql pillow boto3 && python extract_real_photos.py"
"""
import io
import json
import os

import boto3
import pymssql
from PIL import Image, ImageDraw, ImageFont

MSSQL_HOST = os.environ.get("MSSQL_HOST", "sqlserver")
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433"))
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
MSSQL_DB = os.environ.get("MSSQL_DB", "Northwind")

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


def make_fallback_avatar(employee_id, first_name, last_name):
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


def extract_bmp(blob):
    """OLE Object columns wrap the payload in an OLE header; the real BMP
    starts at the 'BM' magic bytes, typically ~78 bytes in for Access-style
    OLE bitmaps. Search for it rather than assuming a fixed offset."""
    idx = blob.find(b"BM")
    if idx == -1:
        return None
    return blob[idx:]


def main():
    conn = pymssql.connect(
        server=MSSQL_HOST, port=MSSQL_PORT, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DB,
    )
    cur = conn.cursor(as_dict=True)
    cur.execute("SELECT EmployeeID, FirstName, LastName, Photo FROM dbo.Employees ORDER BY EmployeeID")
    rows = cur.fetchall()
    conn.close()

    s3 = boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER, aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )

    catalog = []
    decoded, fell_back = [], []
    for row in rows:
        employee_id = row["EmployeeID"]
        code = f"EMP{employee_id}"
        key = f"{code}.png"
        blob = row["Photo"]

        img = None
        source = "real_photo"
        bmp_bytes = extract_bmp(bytes(blob)) if blob else None
        if bmp_bytes:
            try:
                img = Image.open(io.BytesIO(bmp_bytes))
                img.load()
                img = img.convert("RGB")
            except Exception:
                img = None

        if img is None:
            img = make_fallback_avatar(employee_id, row["FirstName"], row["LastName"])
            source = "fallback_avatar"
            fell_back.append(code)
        else:
            decoded.append(code)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        data = buf.getvalue()

        s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType="image/png")

        catalog.append({
            "employee_code": code,
            "employee_id": employee_id,
            "object_key": key,
            "url": f"{MINIO_PUBLIC_BASE_URL}/{BUCKET}/{key}",
            "content_type": "image/png",
            "size_bytes": len(data),
            "source": source,
        })
        print(json.dumps(catalog[-1]))

    with open("/app/photo_catalog.json", "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"\nDecoded from real photo: {len(decoded)} -> {decoded}")
    print(f"Fell back to initials avatar: {len(fell_back)} -> {fell_back}")


if __name__ == "__main__":
    main()
