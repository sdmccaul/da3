import xml.etree.cElementTree as ET
from collections import Counter, defaultdict
import re
import datetime
import pprint
import json


# street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

def map_zone():
    mapped_zones = {
        "residential" : "residential",
        "commercial" : "commercial",
        "industrial" : "industrial",
        "civic" : "civic",
        "cinema" : "commercial",
        "parking" : "commercial",
        "school" : "civic",
        "university" : "civic",
        "place_of_worship" : "civic",
        "townhall" : "civic",
        "fire_station" : "civic",
        "social_facility" : "civic",
        "restaurant" : "commercial",
        "cafe" : "commercial",
        "pharmacy" : "commercial",
        "public_building" : "civic",
        "fast_food" : "commercial",
        "bank" : "commercial",
        "fuel" : "commercial",
        "kindergarten" : "civic",
        "pub" : "commercial",
        "theatre" : "commercial",
        "courthouse" : "civic",
        "library" : "civic",
        "police" : "civic",
        "car_wash" : "commercial",
        "post_office" : "civic",
        "swimming_pool" : "civic",
        "veterinary" : "commercial",
        "doctors" : "commercial",
        "car_rental" : "commercial",
        "dining_hall" : "civic",
        "prison" : "civic",
        "hospital" : "civic",
        "grave_yard" : "civic",
        "community_centre" : "civic",
        "nightclub" : "commercial",
        "college" : "civic",
        "ferry_terminal" : "civic",
        "clinic" : "commercial",
        "bicycle_parking" : "civic",
        "bar" : "commercial",
        "fountain" : "civic",
        "car_sharing" : "commercial",
        "parking_space" : "commercial",
        "childcare" : "civic",
        "toilets" : "civic",
        "dentist" : "commercial",
        "waste_transfer_station" : "civic",
        "retail" : "commercial",
        "warehouse" : "industrial",
        "office" : "commercial",
        "house" : "residential",
        "apartments" : "residential",
        "hospital" : "commercial",
        "dormitory" : "civic",
        "hotel" : "commercial",
        "construction" : "industrial",
        "garage" : "industrial",
        "chapel" : "civic",
        "shed" : "industrial",
        "kindergarten" : "civic",
        "refectory" : "civic",
        "stable" : "industrial",
        "recreation_ground" : "civic",
        "golf_course" : "civic",
        "nature_reserve" : "civic",
        "park" : "civic",
        "playground" : "civic",
        "pitch" : "civic",
        "ice_rink" : "civic",
        "stadium" : "civic",
        "track" : "civic",
        "sports_centre" : "civic",
        "miniature_golf" : "commercial",
        "bowling" : "civic",
        "marina" : "civic",
        "fitness_centre" : "civic",
        "firepit" : "civic",
        "swimming_pool" : "civic",
        "garden" : "civic",
        "dog_park" : "civic",
        "court" : "civic",
        "conservation" : "civic",
        "farm" : "industrial",
        "reservoir_watershed" : "civic",
        "cemetery" : "civic",
        "recreation_ground" : "civic",
        "forest" : "industrial",
        "village_green" : "civic",
        "grass" : "industrial",
        "farmland" : "industrial",
        "quarry" : "industrial",
        "meadow" : "industrial",
        "reservoir" : "civic",
        "military" : "industrial",
        "farmyard" : "industrial",
        "landfill" : "industrial",
        "brownfield" : "industrial",
        "churchyard" : "civic",
        "allotments" : "industrial",
    }

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
    data_obj_count = defaultdict(Counter)
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
                data_obj_count[mongo_obj['datatype']].update(mongo_obj.keys())
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
    data_key['4_json_data_stats'] = { parent: dict(data_obj_count[parent])
                                    for parent in data_obj_count }
    data_key['5_tag_attributes'] = tag_k_v 

    with open('data_key.json','w') as f:
        json.dump(data_key, f, sort_keys=True, indent=4)

if __name__ == "__main__":
    traverse_elements('data/providence_et_al.xml')
