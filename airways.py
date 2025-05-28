from typing import List

import geojson
import numpy
import psycopg
from numexpr.necompiler import double
from shapely.geometry.linestring import LineString
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry, WKTElement
import sqlite3

import geopandas as gpd
from shapely.geometry import Point
import numpy as np
conn = sqlite3.connect('./opt/NAV2504001.sqlite')
cursor = conn.cursor()



if __name__ == '__main__':
    cursor.execute('''
        SELECT
            A.SEQ_NR,
            A.FILE_RECD_NR,
            A.CUST_AREA,
            A.ROUTE_IDENT,
            A.FIX_IDENT,
            COALESCE(W.WAYPOINT_LAT , V.VOR_LAT , N.NDB_LAT, D.DME_LAT ) AS LATITUDE,
            COALESCE(W.WAYPOINT_LON , V.VOR_LON , N.NDB_LON, D.DME_LON ) AS LONGITUDE,
            A.OUTBOUND_MAG_COURSE,
            A.ROUTE_DISTANCE_FROM,
            A.INBOUND_MAG_COURSE,
            A.FIX_ICAO_CODE
        FROM ENROUTE_AIRWAYS A
        LEFT JOIN WAYPOINT W
            ON A.FIX_IDENT = W.WAYPOINT_IDENT AND A.FIX_ICAO_CODE = W.WAYPOINT_ICAO_CODE
        LEFT JOIN VHF_NAVAID V
            ON A.FIX_IDENT = V.VOR_IDENT AND A.FIX_ICAO_CODE = V.VHF_ICAO_CODE
        LEFT JOIN VHF_NAVAID D
            ON A.FIX_IDENT = D.DME_IDENT AND A.FIX_ICAO_CODE = D.VHF_ICAO_CODE
        LEFT JOIN NDB_NAVAID N
            ON A.FIX_IDENT = N.NDB_IDENT AND A.FIX_ICAO_CODE = N.NDB_ICAO_CODE
        WHERE A.ROUTE_IDENT IS NOT NULL
            AND A.FIX_IDENT IS NOT NULL
        AND (A.FIX_ICAO_CODE IN
            ('ZB','ZG','ZH','ZJ','ZL','ZP','ZS','ZU','ZW','ZY') or true)
        ORDER BY A.ROUTE_IDENT, A.SEQ_NR;    
    ''')

    airways_rows = cursor.fetchone()

    last_row = None
    feats = []
    for current_row in airways_rows:
        print(current_row)
        if last_row is None:
            last_row = current_row
            continue
        if last_row[3] != current_row[3]:
            last_row = current_row
            continue
        if last_row[0] == current_row[0]:
            continue
        from_lat = last_row[5]
        from_lon = last_row[6]
        lat = current_row[5]
        lon = current_row[6]

        # from_point = Point(float(from_lat), float(from_lon))
        # to_point = Point(float(lon), float(lat))

        leg = geojson.LineString([(double(from_lon), double(from_lat)), (double(lon), double(lat))])
        feat = geojson.Feature(geometry=leg, properties={
            "leg_ident": current_row[3],
            "leg_distance": current_row[8],
            "file_recd_nr": current_row[1],
        })

        feats.append(feat)

        last_row = current_row

        if current_row[8] == "0000":
            last_row = None
            continue

    featureCollection = geojson.FeatureCollection(feats)
    with open("example.geojson", "w", encoding="utf-8") as f:
        geojson.dump(featureCollection, f)



