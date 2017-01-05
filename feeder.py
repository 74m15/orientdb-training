# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 12:14:58 2017

@author: p.capuano
"""

from argparse import ArgumentParser
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from sys import stdout

import pyorient
import json

def main(*args):
    parser = ArgumentParser("BDC graphDB feeder")
    
    parser.add_argument("-s",   "--server", help="Host (name or IP address) where OrientDB is running", type=str, dest="arg.host", default="localhost")
    parser.add_argument("-p",   "--port", help="Port where OrientDB is listening (binary protocol)", type=int, dest="arg.port", default=2424)
    parser.add_argument("-db",  "--database", help="Database to open (mode is always 'remote')", type=str, dest="arg.database", required=True)
    parser.add_argument("-u",   "--user", help="User to login", type=str, dest="arg.user", required=True)
    parser.add_argument("-pw",  "--password", help="User's pssword", type=str, dest="arg.password", required=True)
    parser.add_argument("-doc", "--document", help="Document (BKPF) filename", type=str, dest="arg.docfile")
    parser.add_argument("-pos", "--position", help="Position (BSEG) filename", type=str, dest="arg.posfile")
    parser.add_argument("-m",   "--mod", help="Remainder of enumeration", type=int, default=0, dest="arg.ratio.mod")
    parser.add_argument("-q",   "--quot", help="Quotient of enumeration", type=int, default=1, dest="arg.ratio.quot")
        
    opts = vars(parser.parse_args())

    print("Opening '{0}' database...".format(opts["arg.database"]))
    client = pyorient.OrientDB(opts["arg.host"], opts["arg.port"])
    client.db_open(opts["arg.database"], opts["arg.user"], opts["arg.password"])

    print("Testing database...")
    client.command("select count(*) from VDocument")
    
    fix_identity = lambda x: x
    fix_date = lambda x: x[0:4] + "-" + x[4:6] + "-" + x[6:]
    fix_number = lambda x: int(x)
    fix_decimal = lambda x: Decimal(x)
    
    keep_doc = { 
        "GJAHR" : fix_number, 
        "BELNR" : fix_identity, 
        "BLART" : fix_identity, 
        "BUDAT" : fix_date, 
        "BLDAT" : fix_date 
    }
    
    keep_pos = {
        "HKONT" : fix_identity, 
        "BUZEI" : fix_identity, 
        "SHKZG" : fix_identity, 
        "DMBTR" : fix_decimal
    }

    ratio_mod = opts["arg.ratio.mod"]
    ratio_quot = opts["arg.ratio.quot"]
    
    if (opts["arg.docfile"] is not None):
        print("Processing document (BKPF) file... ({0})".format(datetime.now()))
        
        with open(opts["arg.docfile"],"rt") as f:
            count = 0
            done = 0
            
            while True:
                line = f.readline()
                
                if (line is None or len(line) == 0):
                    break
                
                if (count % ratio_quot == ratio_mod):
                    raw = json.loads(line)
                    record = { k : keep_doc[k](raw[k]) for k in raw.keys() if k in keep_doc.keys() }
                    record["KEY"] = "{GJAHR}-{BELNR}".format(**record)
                    
                    data = { "@VDocument" : record }
                    
                    client.record_create(-1, data)
                    
                    done += 1
                
                count += 1
                
                if (count % 1000 == 0):
                    print(".", end="")
                    stdout.flush()
                    
                if (count % 80000 == 0):
                    print(" {0}".format(count))
                    stdout.flush()
                    
            print("\n\nRercords read: {0}".format(count))
            print("Rercords processed: {0}".format(done))
            
        print("Finished processing document (BKPF) file ({0})".format(datetime.now()))
    
    if (opts["arg.posfile"] is not None):
        print("Processing position (BSEG) file... ({0})".format(datetime.now()))
        
        with open(opts["arg.posfile"],"rt") as f:
            count = 0
            done = 0
            
            @lru_cache(maxsize=1000000)
            def getRid(key):
                return client.query("select from VDocument where KEY = '{0}'".format(key))[0]._rid
                
            while True:
                line = f.readline()
                
                if (line is None or len(line) == 0):
                    break
                
                if (count % ratio_quot == ratio_mod):
                    raw = json.loads(line)
                    record = { k : keep_pos[k](raw[k]) for k in raw.keys() if k in keep_pos.keys() }
                    record["KEY"] = "{GJAHR}-{BELNR}-{BUZEI}".format(**raw)
                    
                    data = { "@VPosition" : record }
                    
                    pos_rid = client.record_create(-1, data)._rid
                    doc_rid = getRid("{GJAHR}-{BELNR}".format(**raw))
                    
                    client.command("create EDGE EParent from {0} to {1}".format(pos_rid, doc_rid))
                    client.command("create EDGE EChildren from {0} to {1}".format(doc_rid, pos_rid))
    
                    if (raw["AUGGJ"] != "0000"):
                        clear_rid = getRid("{AUGGJ}-{AUGBL}".format(**raw))
                        
                        client.command("create EDGE EClearing from {0} to {1}".format(pos_rid, clear_rid))
                    
                    done += 1
                
                count += 1
                
                if (count % 500 == 0):
                    print(".", end="")
                    stdout.flush()
                    
                if (count % 40000 == 0):
                    print(" {0}".format(count))
                    stdout.flush()
                    
            print("\n", getRid.cache_info())
            
            print("\n\nRercords read: {0}".format(count))
            print("Rercords processed: {0}".format(done))
            
        print("Finished processing position (BSEG) file ({0})".format(datetime.now()))
        
    print("Closing database...")
    client.db_close()
    
    print("Done!")


if (__name__ == "__main__"):
    main()
  