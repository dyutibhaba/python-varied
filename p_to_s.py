import os
import sys
import xml.etree.ElementTree as ET
from tempfile import mkstemp
from shutil import move
from os import fdopen, remove
from xml.etree.ElementTree import tostring



def read_and_rename_from_p_to_s(self, filepath):
    
    tree = ET.parse(filepath)
    
    root = tree.getroot()
    new_tree = ET.tostring(root)
    for occs in root.findall('occurrences'):
        for occ in occs.findall('occurrence'):
            part = occ.find('cadreference').attrib.get('path').split('.')[0]
            replaced = part.replace("P", "S")
            new_tree = new_tree.replace(part, replaced)
    root = ET.fromstring(new_tree)
    tree = ET.ElementTree(root)
            #print occ.find('cadreference').text.replace("__1411714P01_3D_F_001-00.CATPart", "__1411714S01_3D_F_001-00.CATPart")
    tree.write(filepath, encoding='latin-1')

    

read_and_rename_from_p_to_s()