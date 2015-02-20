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
from sets import Set
from py2neo import neo4j, node, rel

'''
    You need to modify the configurations to run the program
'''
# ses framework csv file cofiguration
ses_file_name = 'ses_class.csv'
ses_config = {
    'name_field': 'Name',
    'parent_field': 'Parent',
    'description_field': 'Description',
}

'''
    Main program starts here
'''
def verify_attributes(header_list, field_config):
    """ Return true if the header list has all the required fields
    """
    header_set = Set(header_list)

    for key, value in field_config.items():
        if value not in header_set:
            print value + ' doesn\'t exist'
            return False

    return True

ses_class = {}
# get all objects from Object csv file
with open(ses_file_name, 'rU') as csvfile:
    reader = csv.reader(csvfile, dialect='excel')
    header = reader.next()
    
    # verify the header has all the required field names
    if not verify_attributes(header, ses_config):
        print 'Error: SES csv file doesn\'t have all the configured fields'
        sys.exit(0)
    
    for row in reader:
        attributes = dict(zip(header, row))
        ses_class[attributes[ses_config['name_field']]] = attributes

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(ses_class)

graph_db = neo4j.GraphDatabaseService()
# graph_db.clear()

for ses_name, attributes in ses_class.items():
    ses_desc = attributes[ses_config['description_field']]
    ses_properties = {
        'ses_class': ses_name, 
        'description': ses_desc,
    }
    ses_node = graph_db.get_or_create_indexed_node(
        'SES', # index name
        'ses_class', # index key
        ses_name # index value
    )
    ses_node.set_labels('ses_class')
    ses_node.set_properties(ses_properties)

    parent_name = attributes[ses_config['parent_field']]
    if parent_name != None and parent_name != '':          
        parent_node = graph_db.get_or_create_indexed_node(
            'SES', # index name
            'ses_class', # index key
            parent_name # index value
        )
        graph_db.create(rel(ses_node, "subcategory of", parent_node))




