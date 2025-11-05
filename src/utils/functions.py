import re
import pandas as pd

def hash_tag(text):    
    try:
        hash_tag_list = re.findall(r'#([^#\s]+)', text)
    except:
        hash_tag_list = []
        
    return hash_tag_list

def hash_tag2(text):    
    try:
        hash_tag_list = re.findall(r'#(\S+)', text)
    except:
        hash_tag_list = []
        
    return hash_tag_list