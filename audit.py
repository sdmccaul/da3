import xml.etree.cElementTree as ET
from collections import Counter, defaultdict
import re
import datetime
import pprint
import json


# street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

def map_zone():
    pass

def validate_lat_lon(lat, lon):
    maxlat = 41.9574000
    maxlon = -71.2326000
    minlat = 41.6406000
    minlon = -71.5237000
    flat = float(lat)
    flon = float(lon)
    if ( minlat <= flat <= maxlat ) and ( minlon <= flon <= maxlon ):
        return [ flat, flon ]
    else:
        return None

def validate_street(raw):
    if ',' in raw:
        raw = raw[:raw.index(',')]
    cameled = [ w.capitalize() for w in raw.strip('.').split(' ') ]
    if cameled[0].isdigit():
        cameled = cameled[1:]
    street_map = {
        'Ave' : 'Avenue',
        'St'  : 'Street',
        'Sq'  : 'Square',
        'Dr'  : 'Drive',
        'Blvd' : 'Boulevard',
        'Rd' : 'Road',
        'Ct' : 'Court',
        'Wy' : 'Way',
        'Hwy' : 'Highway',
        'Bowenstreet' : 'Bowen Street'
    }
    if cameled[-1] in street_map:
        cameled[-1] = street_map[cameled[-1]]
    cleaned = ' '.join(cameled)
    return cleaned

def model_addr_val(mongo_obj, k):
    v = mongo_obj['addr'][k]
    if k == 'postcode':
        valid = validate_postcode(v)
    # elif k == 'housenumber':
    #     valid = validate_housenumber(v)
    elif k == 'street':
        valid = validate_street(v)
    else:
        return mongo_obj
    mongo_obj['addr'][k] = valid
    return mongo_obj

def validate_postcode(raw):
    zipcode = re.split('[- ]', raw)[0]
    if len(zipcode) > 5:
        if zipcode == '029212':
            zipcode = '02912'
        else:
            zipcode = None
    return zipcode

def validate_housenumber(raw):
    pass

def validate_timestamp(raw):
    try:
        parsed = datetime.datetime.strptime(raw, '%Y-%m-%dT%H:%M:%SZ')
        return raw
    except:
        return None

def model_created_attrs(elem, mongo_obj):
    mongo_obj['created'] = { 'version' : None,
                    'changeset' : None,
                    'timestamp' : None,
                    'user' : None,
                    'uid' : None }
    for crt_attr in mongo_obj['created']:
        mongo_obj['created'][crt_attr] = elem.attrib[crt_attr]
    mongo_obj['created']['timestamp'] = validate_timestamp(
        mongo_obj['created']['timestamp'])
    return mongo_obj

def model_tag(tagElem, mongo_obj):
    reserved = ['pos','created', 'datatype']
    k = tagElem.attrib['k']
    v = tagElem.attrib['v']
    if ':' in k:
        parent, child = k.split(':',1)
        if parent in reserved:
            return mongo_obj
        elif parent not in mongo_obj or isinstance(mongo_obj[parent], str):
            mongo_obj[parent] = { child: v }
        else:
            mongo_obj[parent][child] = v
        if parent == 'addr':
            mongo_obj = model_addr_val(mongo_obj, child)
    else:
        mongo_obj[k] = v
    return mongo_obj

def model_elem(elem, childRef=None):
    mongo_obj = { 'datatype' : elem.tag }
    if childRef:
        mongo_obj[childRef] = []
    else:
        mongo_obj['pos'] = validate_lat_lon(
            elem.attrib['lat'], elem.attrib['lon'] )

    mongo_obj = model_created_attrs(elem, mongo_obj)
    for child in list(elem):
        if child.tag == childRef:
            mongo_obj[childRef].append(child.attrib['ref'])
        elif child.tag == 'tag':
            mongo_obj = model_tag(child, mongo_obj)
        else:
            print 'unexpected child: ', child.tag
    return mongo_obj


# def is_street_name(elem):
#     return (elem.attrib['k'] == "addr:street")

# def audit_street_type(street_types, street_name):
#     m = street_type_re.search(street_name)
#     if m:
#         street_type = m.group()
#         street_types[street_type] += 1

# def validate_timestamp(elem):
#     ts = elem.attrib['timestamp']
#     dt = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')
#     timestamps[dt.strftime('%Y-%m')] += 1


def get_element(osm_file, skip=('osm')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag not in skip:
            yield elem
            root.clear()

def traverse_elements(osmFile):
    # Audit street names
    # Lat, Long within bounds
    # handle problem attribute names, ie, with colons
    tag_count = Counter()
    with_text = Counter()
    attrib_count = defaultdict(Counter)
    child_count = defaultdict(Counter)
    attribute_sample = defaultdict(dict)
    tag_k_v = dict()
    with open('data/providence_et_al.json','w') as f:
        f.write('[\n')
        for elem in get_element(osmFile):
            if elem.text is True:
                with_text[elem.tag] += 1
            tag_count[elem.tag] += 1
            for k in elem.attrib:
                attrib_count[elem.tag][k] += 1
                attribute_sample[elem.tag][k] = elem.attrib[k]
            for child in list(elem):
                child_count[elem.tag][child.tag] += 1
            mongo_obj = None
            if elem.tag == 'tag':
                tag_k_v[elem.attrib['k']] = elem.attrib['v']
            elif elem.tag == 'node':
                mongo_obj = model_elem(elem)
            elif elem.tag == 'way':
                mongo_obj = model_elem(elem, 'nd')
            elif elem.tag == 'relation':
                mongo_obj = model_elem(elem, 'member')
            if mongo_obj:
                f.write(json.dumps(mongo_obj, sort_keys=True, indent=4))
                f.write(',\n')
        f.write('{}')
        f.write(']')

    # Asserts every element has every attribute present
    for tag in attrib_count:
        for k,v in attrib_count[tag].items():
            assert v == tag_count[tag]

    # Asserts no elements contain text
    assert dict(with_text) == {}

    # Asserts that all child elements are children of a top-level element
    # e.g., there are no orphaned elements
    child_nodes = ('nd', 'tag', 'member')
    for c in child_nodes:
        total = 0
        for parent, children in child_count.items():
            for child in children:
                if c == child:
                    total += children[child]
        assert total == tag_count[c]

    data_key = { '1_tag_count' : dict(tag_count) }
    data_key['2_attribute_sample'] = dict(attribute_sample)
    data_key['3_child_count'] = { parent: dict(child_count[parent])
                                    for parent in child_count }
    data_key['4_tag_attributes'] = tag_k_v

    with open('data_key.json','w') as f:
        json.dump(data_key, f, sort_keys=True, indent=4)

if __name__ == "__main__":
    traverse_elements('data/providence_et_al.xml')
