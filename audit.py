import xml.etree.cElementTree as ET
from collections import Counter, defaultdict
import re
import datetime
import pprint

pp = pprint.PrettyPrinter(indent=4)
###
#Notes
# count children of elements, percentages

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
users = Counter()
sources = Counter()
attributions = Counter()
timestamps = Counter()
tag_parents = set()

class Entity:
    def __init__(self):
        self.tag = None
        self.count = 0
        self.attributes = defaultdict(Counter)
        self.children = []

attrs = defaultdict(Entity)

def entity_audit(elem, auditor):
    if elem.tag not in auditor:
        auditor[elem.tag] = {   'text_sample': elem.text,
                                'count': 0,
                                'children': Counter(),
                                'attribute_count': Counter(),
                                'attribute_sample': dict() }
    auditor[elem.tag]['count'] += 1
    for child in list(elem):
        auditor[elem.tag]['children'][child.tag] += 1
    for attr in elem.attrib:
        auditor[elem.tag]['attribute_count'][attr] += 1
        auditor[elem.tag]['attribute_sample'][attr] = elem.attrib[attr]
    return auditor

def model_way(elem):
    mongo_obj = { 'nodes': [] }
    mongo_obj = model_created_attrs(elem, mongo_obj)
    for child in list(elem):
        if child.tag == 'nd':
            mongo_obj = model_way_node(child, mongo_obj)
            #['nodes'].append(child.attr['ref'])
        elif child.tag == 'tag':
            mongo_obj = model_tag(child, mongo_obj)
        else:
            print 'unexpected child for way: ', child.tag
    return mongo_obj


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        street_types[street_type] += 1

def audit_user(elem):
    user = elem.attrib['user']
    users[user] += 1

def audit_source(elem):
    key = elem.attrib['k']
    if key == 'source':
        sources[elem.attrib['v']] += 1
    elif key == 'attribution':
        attributions[elem.attrib['v']] += 1

def audit_timestamp(elem):
    ts = elem.attrib['timestamp']
    dt = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')
    timestamps[dt.strftime('%Y-%m')] += 1

def audit(osmfile):
    osm_file = open(osmfile, "r")
    element_count = Counter()
    tag_vals = {}
    auditor = {}
    tag_type = Counter()
    for event, elem in ET.iterparse(osm_file, events=('end',)):        
        element_count[elem.tag] += 1
        if elem.tag not in tag_vals:
            # tag_vals[elem.tag] = defaultdict(set)
            tag_vals[elem.tag] = Counter()
        auditor = entity_audit(elem, auditor)
        for tag in elem.iter("tag"):
            tag_vals[elem.tag][tag.attrib['k']] += 1
            if tag.attrib['k'] == 'type':
                tag_type[tag.attrib['v']] += 1
        # if elem.findall('tag'):
        #     tag_parents.add(elem.tag)

        # tags[elem.tag] += 1
        # for attr in elem.attrib:
        #     attrs[elem.tag][attr] += 1
        # if elem.tag == "node" or elem.tag == "way":
        #     # audit_user(elem)
        #     audit_timestamp(elem)
            # for tag in elem.iter("tag"):
            #     audit_source(tag)
            #     if is_street_name(tag):
            #         audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    for a in auditor:
        auditor[a]['children'] = dict(auditor[a]['children'])
        auditor[a]['attribute_count'] = dict(auditor[a]['attribute_count'])
    for t in tag_vals:
        tag_vals[t] = dict(tag_vals[t])
    pp.pprint(dict(element_count))
    pp.pprint(auditor)
    pp.pprint(dict(tag_type))
    pp.pprint(tag_vals.keys())
    for t in tag_vals:
        for k in tag_vals[t].keys():
            if ":" in k:
                print k
    return


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
    #check "text" values
    #confirm attribute counts match node counts
    #check child sums match node counts
    #proportion with/without children (nodes with/without tags, etc)
    #k attributes with ":"
    #sample values for every kind of attribute
    tag_count = Counter()
    with_text = Counter()
    attrib_count = defaultdict(Counter)
    child_count = defaultdict(Counter)
    attribute_sample = defaultdict(dict)
    tag_k_v = dict()
    for elem in get_element(osmFile):
        if elem.text is True:
            with_text[elem.tag] += 1
        tag_count[elem.tag] += 1
        for k in elem.attrib:
            attrib_count[elem.tag][k] += 1
            attribute_sample[elem.tag][k] = elem.attrib[k]
        for child in list(elem):
            child_count[elem.tag][child.tag] += 1
        if elem.tag == 'tag':
            tag_k_v[elem.attrib['k']] = elem.attrib['v'] 

    # for a in attrib_count:
    #     attrib_count[a] = dict(attrib_count[a])
    # for c in child_count:
    #     child_count[c] = dict(child_count[c])
    # tag_count = dict(tag_count)
    # attrib_count = dict(attrib_count)
    # child_count = dict(child_count)

    # Asserts every element has every attribute present
    for tag in attrib_count:
        for k,v in attrib_count[tag].items():
            assert v == tag_count[tag]

    # Asserts no elements contain text
    assert dict(with_text) == {}

    # Asserts that all child elements are children of a top-level element
    # e.g., there are no orphaned elements
    child_nodes = ('nd', 'tag', 'member')
    # child_shares = defaultdict(dict)
    for c in child_nodes:
        total = 0
        for parent, children in child_count.items():
            for child in children:
                if c == child:
                    total += children[child]
                    # child_shares[c][parent] = float(children[child]) / tag_count[c]
        assert total == tag_count[c]

    # pp.pprint(tag_count)
    # pp.pprint(attrib_count)
    pp.pprint(dict(child_count))
    # pp.pprint(dict(with_text))
    pp.pprint(dict(attribute_sample))
    colons = [ c.split(':') for c in tag_k_v if ':' in c ]
    k_vs = defaultdict(dict)
    for c in colons:
        k_vs[c[0]][c[1]] = tag_k_v[':'.join(c)]
    k_vs = dict(k_vs)
    del k_vs['name']
    pp.pprint(k_vs)

if __name__ == "__main__":
    traverse_elements('data/providence_et_al.xml')
    # out = audit('data/providence_et_al.xml')
    # print tags.most_common(100)
    # for tag in out:
    #     print tag
    #     print out[tag]
    # pp.pprint(out)
    # x = users.most_common(100)
    # print x
	# for k,v in users.items():
	# 	print k, v