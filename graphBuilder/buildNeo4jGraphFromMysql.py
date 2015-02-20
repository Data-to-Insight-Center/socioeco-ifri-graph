#! /usr/bin/env python
#
# Copyright 2015 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import csv, string, pprint, sys
import MySQLdb as mdb
from sets import Set
from py2neo import neo4j, node, rel

'''
    You need to modify the configurations to run the program
'''
# IFRI database configuration
mysql_config = {
    'user': 'root',
    'passwd': '',
    'host': '127.0.0.1',
    'db': 'ifri',
    'charset': 'utf8',
}

# object and element csv file cofiguration
csv_config = {
    'delimiter': ',',
    'quotechar': '"',
    'escapechar': '"',
    'quoting': csv.QUOTE_MINIMAL,
}

object_file_name = 'Objects.csv'
objects_config = {
    'id_field': 'ObjectId',
    'query_field': 'Query',
    'parent_id_field': 'ParentId',
    'parameter_1': 'Parameter1',
    'label_field': 'URI',
    'index_field': 'Index',
}

element_file_name = 'Elements.csv'
elements_config = {
    'object_id_field': 'Object ID',
    'column_name_field': 'Field Name',
    'ses_class': 'SES Class',
}

# sql query that returns the root objects
root_query = 'SELECT * FROM OVERSITE'

pp = pprint.PrettyPrinter(indent=4)

'''
    Main program starts here
'''
# string.replace() wrapper that handles unicode and non-strings
def replaceStr(x, s1, s2):
    if isinstance(x, basestring):
        return string.replace(x.encode('utf-8'), s1, s2)
    else:
        return str(x)   # to solve json serialization problem for datetime

def getQueryResult(query):
    """ For given sql query, returns a tuple: (header, [records])
    """
    try:
        con = None
        cursor = None

        con = mdb.connect(**mysql_config)
        cursor = con.cursor()
        cursor.execute(query)

        num_fields = len(cursor.description)
        header = [i[0] for i in cursor.description]

        rows = []
        row = cursor.fetchone()
        while row:
          row_formatted = [ replaceStr(x, '\r\n', '\n') for x in row ]
          rows.append(row_formatted)
          row = cursor.fetchone()    

        return (header, rows)

    except mdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
        
    finally:    
        if cursor is not None:
            cursor.close()
        if con is not None:
            con.close()

def getNodes(object_attributes, parent_node_attributes):
    """ Returns the list of nodes (dict of attributes) for the given object attributes
        from the csv file, and the parent node's attributes
    """
    query = object_attributes[objects_config['query_field']]
    parameter_info = object_attributes[objects_config['parameter_1']]
    # TODO: need a better way to encode parameter in the csv
    parameter_name = unicode(string.split(parameter_info, ':')[0])

    if not parameter_name in parent_node_attributes:
        print 'Error: parent_node_attributes doesn\'t have key ' + parameter_name
        sys.exit(0)

    query = replaceStr(query, '?', str(parent_node_attributes[parameter_name]))
    print query
    header, records = getQueryResult(query)
    
    nodes = []
    for record in records:
        nodes.append(dict(zip(header, record)))
    
    return nodes

def linkToSES(node, attributes, graph_db):
    """ Find the ses class for the given element node.
        Create a new ses node if not exist, link the element node to it. 
    """
    ses_class = attributes[elements_config['ses_class']]
    if ses_class is None or ses_class == '':
        print 'Error: node ' + attributes + ' doesn\'t have ses class'
        sys.exit(0)

    ses_node = graph_db.get_or_create_indexed_node(
        'SES', # index name
        'ses_class', # index key
        ses_class, # index value
        {elements_config['ses_class']: ses_class}
    )
    ses_node.set_labels('ses_class')  
    graph_db.create(rel(node, "belongs to", ses_node))

def createNode(node_attributes, object_id, objects, elements, graph_db):
    """ Create in Neo4j the object node and its standalone element nodes
        Returns the object node reference in Neo4j
    """
    index_field = objects[object_id][objects_config['index_field']]
    if index_field in node_attributes:
        object_node = graph_db.get_or_create_indexed_node(
            'ID',
            'index_field',
            node_attributes[index_field],
            node_attributes
            #{index_field: node_attributes[index_field]}
        )
        #object_node.set_properties(node_attributes)
    else:
        object_node, = graph_db.create(node(node_attributes))
    
    object_node.add_labels(objects[object_id][objects_config['label_field']])

    # if this object has standalone elements
    for field_name, value in node_attributes.items():
        if (object_id, field_name) in elements:
                element_attributes = elements[(object_id, field_name)]
                element_attributes[field_name] = value
                element_node, = graph_db.create(node(element_attributes))
                # label the nodes as elements
                element_node.add_labels("Element")
                graph_db.create(rel(object_node, "has element", element_node))
                # link the element node to a ses concept
                linkToSES(element_node, element_attributes, graph_db)   

    return object_node

def createTree(root_node_attributes, root_object_id, objects, elements, graph_db):
    """ Create in Neo4j the root node and its all descendants (recursively)
        Returns the root node reference in Neo4j
    """
    root_node = createNode(
        root_node_attributes, 
        root_object_id, 
        objects, 
        elements, 
        graph_db
    )  
    
    if 'child_id_field' in objects[root_object_id]:
        child_ids = objects[root_object_id]['child_id_field']    
        print root_object_id, child_ids

        for child_id in child_ids:
            child_nodes_attributes = getNodes(objects[child_id], root_node_attributes)
            for child_node_attributes in child_nodes_attributes:
                child_node = createTree(
                    child_node_attributes, 
                    child_id, 
                    objects,
                    elements, 
                    graph_db
                )
                # child_node.add_labels(objects[child_id][objects_config['label_field']])
                graph_db.create(rel(root_node, "has child", child_node))            
    
    return root_node

def verify_object_attributes(header_list, field_config):
    """ Return true if the header list has all the required fields
    """
    header_set = Set(header_list)

    for key, value in field_config.items():
        if value not in header_set:
            print value + ' doesn\'t exist'
            return False

    return True

'''
Main program starts from here
'''    

objects = {}
# get all objects from Object csv file
with open(object_file_name, 'rU') as csvfile:
    reader = csv.reader(csvfile, dialect='excel')
    header = reader.next()
    
    # verify the header has all the required field names
    if not verify_object_attributes(header, objects_config):
        print 'Error: Object csv file doesn\'t have all the configured fields'
        sys.exit(0)

    id_index = -1
    for i in range (len(header)):
        if header[i] == objects_config['id_field']:
            id_index = i
    
    for row in reader:
        object_id = row[id_index]
        objects[object_id] = dict(zip(header, row))
        #pp.pprint(objects[object_id])

root_objects = []
for object_id, object_attributes in objects.items():
    parent_id = object_attributes[objects_config['parent_id_field']]
    if parent_id is None or parent_id == '':
        root_objects.append(object_id)
        continue

    if not 'child_id_field' in objects[parent_id]:
        objects[parent_id]['child_id_field'] = []
    
    objects[parent_id]['child_id_field'].append(object_id)

for object_id, object_attributes in objects.items():
    if 'child_id_field' in object_attributes:
        child_ids = object_attributes['child_id_field']    
        print object_id, child_ids

elements = {}
# get all object elements from Element csv file
with open(element_file_name, 'rU') as csvfile:
    reader = csv.reader(csvfile, dialect='excel')
    header = reader.next()
      
    # verify the header has all the required field names
    if not verify_object_attributes(header, elements_config):
        print 'Error: Element csv file doesn\'t have all the configured fields'
        sys.exit(0)
    
    for row in reader:
        attribute_dict = dict(zip(header, row))
        object_id = attribute_dict[elements_config['object_id_field']]
        column_name = attribute_dict[elements_config['column_name_field']]
        
        if object_id != None and object_id != '':
            elements[(object_id, column_name)] = attribute_dict

print root_objects
if len(root_objects) != 1:
    print 'Error: There could only be one top level object type'
    sys.exit(0)
root_object_id = root_objects[0]

graph_db = neo4j.GraphDatabaseService()
# clear the graph db
graph_db.clear()

header, records = getQueryResult(root_query)
for record in records:
    fake_parent_node_attributes = dict(zip(header, record))
    nodes_dict = getNodes(objects[root_object_id], fake_parent_node_attributes)
    for node_dict in nodes_dict:
        createTree(node_dict, root_object_id, objects, elements, graph_db)





