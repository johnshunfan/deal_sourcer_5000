#! /usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
from datetime import datetime
import tldextract

import us

def format_domain(data):
    if data == '':
        return None
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../static/tld_set_snapshot.json')
    ae_extract = tldextract.TLDExtract(cache_file=file_path)
    ext = ae_extract(data)
    if ext.domain == 'angel' and ext.suffix == 'co':
        return ext.domain + '.' + ext.suffix + data[data.find('angel.co') + 8:]
    if ext.subdomain == 'www' or ext.subdomain == '':
        return ext.domain + '.' + ext.suffix
    else:
        return ext.subdomain + '.' + ext.domain + '.' + ext.suffix

def format_string(data, length=255):
    """Format a string
    """
    data = data.decode('utf-8', 'ignore')[:length]

    re_pattern = re.compile(u'[^\u0000-\uD7FF\uE000-\uFFFF]', re.UNICODE)
    return re_pattern.sub(u'\uFFFD', data)[:length]
    #return data.decode('utf-8', 'ignore')[:length]

def format_cb_usd_amt(data):
    i = format_number(data)
    if not i is None:
        return format(i / float(1000000), '.1f')
    return None

def format_state(data):
    """ Attempt to match a string as a US state."""
    if us.states.lookup(data) is None:
        return None
    else:
        return us.states.lookup(data).name

def format_number(data):
    """ Format a number as an integer for database storage.

    formats a number by removing:
    leading and trailing whitespace
    dollar signs
    commas
    if the string contains non-numberical chars,
    return null
    otherwise, return the number
    """
    # strip commas and leading/trailing spaces
    data = data.strip().replace(',', '').replace('$', '')
    try:
        return int(data)
    except:
        return None

def format_float(data):
    """Format data into a one decimal place float."""
    try:
        return format(float(data), '.1f')
    except:
        return None

def format_date(data, format_string='%Y-%m-%d'):
    """Formats a crunchbase number for database storage."""
    if (data == '') or 'BC' in data:
        return None
    return datetime.strptime(data, format_string)

def split_investors_pb(investor_string):
    """Format pitchbook investor strings.

    takes an input such as: Adara Ventures (Alberto Gomez), Telefnica Ventures,
    Trident Capital Cybersecurity (Alberto Ypez)
    turns it into an array
    returns a dict with an array
    """
    raw_investor_array = investor_string.split(', ')
    investor_array = []
    for i in range(len(raw_investor_array)):
        index = raw_investor_array[i].find('(')
        if ( index == -1):
            investor_array.append(raw_investor_array[i])
        else:
            investor_array.append(raw_investor_array[i][:index - 1])
    return investor_array

def split_investors_cb(investor_string):
    """Format cb investor strings.

    takes an input such as Lead - Chrysalis Ventures, Lead - Arboretum Ventures
    turns it into an array
    returns a dict with 2 arrays: lead and other investors
    """
    investor_array = investor_string.split(', ')
    output_array = []
    for i in range(len(investor_array)):
        if investor_array[i].startswith('Lead - '):
            investor_array[i] = investor_array[i].replace('Lead - ', '')
        output_array.append(investor_array[i])
    return output_array

def split_investors_cb_old(investor_string):
    """Format cb investor strings.
    takes an input such as Lead - Chrysalis Ventures, Lead - Arboretum Ventures
    turns it into an array
    returns a dict with 2 arrays: lead and other investors
    """
    investor_array = investor_string.split(', ')
    lead_array = []
    other_array = []
    for i in range(len(investor_array)):
        if investor_array[i].startswith('Lead - '):
            investor_array[i] = investor_array[i].replace('Lead - ', '')
            lead_array.append(investor_array[i])
        else:
            other_array.append(investor_array[i])
    return { 'lead':lead_array, 'others':other_array }

def format_website(website_string):
    return

def load_into_database():
    return None

if __name__ == "__main__":
    #print(split_investors_pb('Adara Ventures (Alberto Gomez), Telefnica Ventures, Trident Capital Cybersecurity (Alberto Ypez)'))
    print format_string('Invoicing app that helps freelancers get paid faster! ðŸš€')

