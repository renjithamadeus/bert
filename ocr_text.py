import re
import io
import xml.etree.ElementTree as ET

_NAMESPACES = {
    'abbyy': 'http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml'
}


def _cleanup_block(text):
    """Cleanup whitespace in text."""
    text = re.sub(r'[\n\s]*\n[\n\s]*', '\n', text)
    text = re.sub(r' +', ' ', text)
    return text


def _cleanup_doc(text):
    """Cleanup whitespace in document, preserve text blocks."""
    return re.sub(r'[\n\s]*\n\s*\n[\n\s]*', '\n\n', text)


def _chars(tree):
    for block in tree.findall('.//abbyy:block', namespaces=_NAMESPACES):
        sio = io.StringIO()
        for par in block.findall('.//abbyy:par', namespaces=_NAMESPACES):
            for line in par.findall('.//abbyy:line', namespaces=_NAMESPACES):
                for formatting in line.findall('.//abbyy:formatting', namespaces=_NAMESPACES):
                    for char in formatting.findall('./abbyy:charParams', namespaces=_NAMESPACES):
                        sio.write(char.text[0])
                sio.write('\n')
            sio.write('\n')
        text = sio.getvalue()
        text = _cleanup_block(text)
        if text:
            yield text


def convert(text):
    """Parse XML file and convert to plain text."""
    tree = ET.fromstring(text)
    txt = '\n'.join(ch for ch in _chars(tree))
    return _cleanup_doc(txt)