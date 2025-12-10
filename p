import json
import boto3
import xml.etree.ElementTree as ET

s3 = boto3.client("s3")

BUCKET = "your-bucket-name"
XML_KEY = "pole_annotation/hive_sample.xml"
OUTPUT_KEY = "converted/annotations.json"

def convert_xml_to_labelstudio_json(xml_content):
    root = ET.fromstring(xml_content)
    
    results = []

    for image in root.findall('.//image'):
        img_name = image.attrib["name"]
        img_w = float(image.attrib["width"])
        img_h = float(image.attrib["height"])

        predictions = []

        # Convert Boxes → Label Studio Rectangles
        for box in image.findall('box'):
            label = box.attrib["label"]
            xtl, ytl = float(box.attrib["xtl"]), float(box.attrib["ytl"])
            xbr, ybr = float(box.attrib["xbr"]), float(box.attrib["ybr"])

            width = xbr - xtl
            height = ybr - ytl

            predictions.append({
                "value": {
                    "x": (xtl / img_w) * 100,
                    "y": (ytl / img_h) * 100,
                    "width": (width / img_w) * 100,
                    "height": (height / img_h) * 100,
                    "rotation": 0,
                    "rectanglelabels": [label]
                },
                "from_name": "label",
                "to_name": "image",
                "type": "rectanglelabels",
                "score": 0.99
            })

        # Convert Polygon → Label Studio Polygons (if required)
        for poly in image.findall('polygon'):
            label = poly.attrib["label"]

            points = []
            for pt in poly.findall('point'):
                x = float(pt.attrib['x']) / img_w * 100
                y = float(pt.attrib['y']) / img_h * 100
                points.append([x, y])

            predictions.append({
                "value": {
                    "points": points,
                    "polygonlabels": [label]
                },
                "from_name": "label",
                "to_name": "image",
                "type": "polygonlabels",
                "score": 0.99
            })

        results.append({
            "data": {"image": img_name},
            "predictions": [{"result": predictions}]
        })

    return results


def handler(event=None, context=None):
    # Read XML from S3
    xml_data = s3.get_object(Bucket=BUCKET, Key=XML_KEY)['Body'].read().decode()

    # Convert
    output_json = convert_xml_to_labelstudio_json(xml_data)

    # Save JSON to S3 (optional)
    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output_json, indent=4),
        ContentType="application/json"
    )

    print(f"Converted and uploaded to s3://{BUCKET}/{OUTPUT_KEY}")


if __name__ == "__main__":
    handler()
