import os
import xml.etree.ElementTree as ET
from config import XML_FILE

def init_xml():
    xml_folder = os.path.dirname(XML_FILE)
    if xml_folder and not os.path.exists(xml_folder):
        os.makedirs(xml_folder)

    if not os.path.exists(XML_FILE):
        root = ET.Element("Jobs")
        ET.ElementTree(root).write(XML_FILE)

def load_jobs():
    try:
        tree = ET.parse(XML_FILE)
        return tree, tree.getroot()
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse {XML_FILE}: {e}")

def save_jobs(tree):
    tree.write(XML_FILE)

def get_tag_value(job, tag, default=None):
    elem = job.find(tag)
    return elem.text.strip() if elem is not None and elem.text else default

def ensure_tags(job, defaults):
    for tag, val in defaults.items():
        if job.find(tag) is None:
            ET.SubElement(job, tag).text = val

