import xml.etree.cElementTree as ET
from collections import Counter, defaultdict
import re
import datetime
import pprint
import json
import sys


def model_zones(mongo_obj, k, v):
    mapped_zones = {
        'building' : {
            "college" : "civic",
            "public" : "civic",
            "house" : "residential",
            "office" : "commercial",
            "school" : "civic",
            "commercial" : "commercial",
            "retail" : "commercial",
            "civic" : "civic",
            "warehouse" : "industrial",
            "industrial" : "industrial",
            "residential" : "residential",
            "apartments" : "residential",
            "church" : "civic",
            "dormitory" : "civic",
            "university" : "civic",
            "hospital" : "civic",
            "roof" : "industrial",
            "construction" : "industrial",
            "hotel" : "commercial",
            "garage" : "industrial",
            "chapel" : "civic",
            "shed" : "industrial",
            "kindergarten" : "civic",
            "refectory" : "civic",
            "stable" : "industrial"
        },
        'amenity' : {
            "police" : "civic",
            "fire_station" : "civic",
            "library" : "civic",
            "school" : "civic",
            "townhall" : "civic",
            "grave_yard" : "civic",
            "hospital" : "civic",
            "place_of_worship" : "civic",
            "nursing_home" : "civic",
            "post_office" : "civic",
            "community_centre" : "civic",
            "university" : "civic",
            "clinic" : "civic",
            "kindergarten" : "civic",
            "courthouse" : "civic",
            "prison" : "civic",
            "fast_food" : "commercial",
            "cafe" : "commercial",
            "bar" : "commercial",
            "bus_station" : "civic",
            "restaurant" : "commercial",
            "theatre" : "commercial",
            "pharmacy" : "commercial",
            "bank" : "commercial",
            "fuel" : "commercial",
            "cinema" : "commercial",
            "swimming_pool" : "civic",
            "car_rental" : "commercial",
            "car_wash" : "commercial",
            "fountain" : "civic",
            "pub" : "commercial",
            "swings" : "civic",
            "piercing" : "commercial",
            "retail" : "commercial",
            "childcare" : "civic",
            "marketplace" : "commercial",
            "social_facility" : "civic",
            "arts_centre" : "civic",
            "ferry_terminal" : "civic",
            "bbq" : "commercial",
            "college" : "civic",
            "social_centre" : "civic",
            "dojo" : "commercial",
            "dentist" : "commercial",
            "ice_cream" : "commercial",
            "public_building" : "civic",
            "veterinary" : "commercial",
            "dining_hall" : "civic",
            "nightclub" : "commercial",
            "waste_transfer_station" : "industrial"
        },
        'leisure': {
            "park" : "civic",
            "sports_centre" : "civic",
            "marina" : "commercial",
            "playground" : "civic",
            "dog_park" : "civic",
            "fitness_centre" : "commercial",
            "pitch" : "civic",
            "dance" : "commercial",
            "recreation_ground" : "civic",
            "nature_reserve" : "civic",
            "golf_course" : "commercial",
            "ice_rink" : "civic",
            "stadium" : "commercial",
            "miniature_golf" : "commercial",
            "bowling" : "commercial",
            "swimming_pool" : "civic",
            "garden" : "civic",
            "court" : "civic"
        },
        'landuse': {
            "reservoir" : "industrial",
            "cemetery" : "civic",
            "quarry" : "industrial",
            "farm" : "industrial",
            "reservoir_watershed" : "industrial",
            "recreation_ground" : "civic",
            "retail" : "commercial",
            "village_green" : "civic",
            "residential" : "residential",
            "industrial" : "industrial",
            "farmland" : "industrial",
            "construction" : "industrial",
            "commercial" : "commercial",
            "military" : "industrial",
            "farmyard" : "industrial",
            "landfill" : "industrial",
            "brownfield" : "industrial",
            "churchyard" : "civic",
            "allotments" : "industrial"
        }
    }
    if k in mapped_zones and v in mapped_zones[k]:
        mongo_obj['zone'] = mapped_zones[k][v]
    return mongo_obj

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
        return { 'bad_pos' : True, 'pos_val': [ flat, flon ] }

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
        return datetime.datetime.strptime(raw, '%Y-%m-%dT%H:%M:%SZ')
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
    valid_date = validate_timestamp( mongo_obj['created']['timestamp'] )
    if valid_date :
        mongo_obj['created']['year'] = valid_date.year
        mongo_obj['created']['month'] = valid_date.month
        mongo_obj['created']['day'] = valid_date.day
    else:
        del mongo_obj['created']['timestamp']
    return mongo_obj

def model_tag(tagElem, mongo_obj):
    # Existing fields in the mongo_obj;
    # we don't want them to be overwritten
    reserved = ['pos','created', 'datatype']
    k = tagElem.attrib['k']
    v = tagElem.attrib['v']
    # On the off-chance there's a name collison,
    # just skip and return
    if k in reserved:
        return mongo_obj
    if ':' in k:
        # Building a nested dictionary 
        ## Only split on the first colon
        parent, child = k.split(':',1)
        if parent in reserved:
            return mongo_obj
        ## If the field doesn't exist in the mongo_obj,
        ## create a new nested dictionary.
        ## A field may already exist with the same name,
        ## but storing a string value instead of a dictionary.
        ## If so, overwrite.
        elif parent not in mongo_obj or isinstance(mongo_obj[parent], str):
            mongo_obj[parent] = { child: v }
        ## Otherwise, add the sub-field to existing dictionary. 
        else:
            mongo_obj[parent][child] = v
        if parent == 'addr':
            mongo_obj = model_addr_val(mongo_obj, child)
    else:
        mongo_obj[k] = v
    # Some 'k' values of interest
    zones = [ 'building', 'amenity', 'leisure', 'landuse' ]
    if k in zones:
        mongo_obj = model_zones(mongo_obj, k, v)
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
    # Borrowed from course materials
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

def parse_osm_xml(osmFile, destination):

    # A set of counters for data auditing and cross-validation
    ## Counts number of elements (tags)
    element_count = Counter()
    ## Counts elements with text content (should be 0)
    with_text = Counter()
    ## For each type of element, count their attributes
    attr_count = defaultdict(Counter)
    ## For each type of element, count their children
    child_count = defaultdict(Counter)
    ## For each type of element,
    ## store a sample of each attribute value
    attr_sample = defaultdict(dict)
    ## For each Tag element, store the values
    ## for "key" and "value" attributes
    tag_sample = dict()
    ## Count the document contents for the JSON data
    ## being written to MongoDB
    data_obj_count = defaultdict(Counter)

    # The main process
    with open(destination,'w') as f:
        # Write some helper text to ensure the output
        # file is valid JSON
        f.write('[\n')
        for elem in get_element(osmFile):
            if elem.text is True:
                with_text[elem.tag] += 1
            element_count[elem.tag] += 1
            for k in elem.attrib:
                # Count the occurence of an attribute/element
                attr_count[elem.tag][k] += 1
                # Sample the attribute's value
                attr_sample[elem.tag][k] = elem.attrib[k]
            #Tally all the children of the element
            for child in list(elem):
                child_count[elem.tag][child.tag] += 1
            # Begin building the dict for eventual JSON data
            # 3 top-level elements -- meta, bounds, note --
            # are ignored. Child-level elements --
            # nd, tag, and member -- are handled when
            # their parent element is processed. Focus is on
            # the central elements: node, way, relation.
            skip_elements = [ 'meta', 'bounds', 'note', 'nd' ]
            ## we're not processing these,
            ## so just move to the next element
            if elem.tag in skip_elements:
                continue
            mongo_obj = None
            ## We'll still take a sample of the value for
            ## each key in a tag, but otherwise skip them
            if elem.tag == 'tag':
                tag_sample[elem.attrib['k']] = elem.attrib['v']
                continue
            ## Conditional handling for each of the
            ## 3 essential element types
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
        # More helper text for valid JSON
        f.write('{}')
        f.write(']')

    # Asserts every element has every relevant attribute present
    for element in attr_count:
        for attr,count in attr_count[element].items():
            assert count == element_count[element]

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
        assert total == element_count[c]

    data_key = { '1_element_count' : dict(element_count) }
    data_key['2_attribute_sample'] = dict(attr_sample)
    data_key['3_child_count'] = { parent: dict(child_count[parent])
                                    for parent in child_count }
    data_key['4_json_data_stats'] = { parent: dict(data_obj_count[parent])
                                    for parent in data_obj_count }
    data_key['5_tag_attributes'] = tag_sample

    with open('data_key.json','w') as f:
        json.dump(data_key, f, sort_keys=True, indent=4)

if __name__ == "__main__":
    xml_in = sys.argv[1]
    json_out = sys.argv[2]
    parse_osm_xml(xml_in, json_out)