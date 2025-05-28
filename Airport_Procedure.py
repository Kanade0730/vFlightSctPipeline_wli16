import geojson
import sqlite3

conn = sqlite3.connect('./NAV2504001.sqlite')
cursor = conn.cursor()

cursor.execute('''
        SELECT
            A.ARPT_IDENT,
            A.PROC_IDENT,
            A.TRANSITION_IDENT,
            A.FIX_IDENT,
            COALESCE(W.WAYPOINT_LAT , V.VOR_LAT , N.NDB_LAT, D.DME_LAT ) AS LATITUDE,
            COALESCE(W.WAYPOINT_LON , V.VOR_LON , N.NDB_LON, D.DME_LON ) AS LONGITUDE
        FROM AIRPORT_PROCEDURE A
        LEFT JOIN WAYPOINT W
            ON A.FIX_IDENT = W.WAYPOINT_IDENT AND A.FIX_ICAO_CODE = W.WAYPOINT_ICAO_CODE
        LEFT JOIN VHF_NAVAID V
            ON A.FIX_IDENT = V.VOR_IDENT AND A.FIX_ICAO_CODE = V.VHF_ICAO_CODE
        LEFT JOIN VHF_NAVAID D
            ON A.FIX_IDENT = D.DME_IDENT AND A.FIX_ICAO_CODE = D.VHF_ICAO_CODE
        LEFT JOIN NDB_NAVAID N
            ON A.FIX_IDENT = N.NDB_IDENT AND A.FIX_ICAO_CODE = N.NDB_ICAO_CODE
        WHERE (A.ARPT_IDENT IN('ZBAA') AND A.PROC_IDENT IN ('AVBO6J') OR A.PROC_IDENT IN ('AVBO7X')) 
        ORDER BY A.ARPT_IDENT, A.PROC_IDENT;
    ''')

airport_procedure = cursor.fetchall()

all_line_features = []
grouped_procedures = {}

for row in airport_procedure:
    arpt_ident = row[0]
    proc_ident = row[1]
    group_key = (arpt_ident, proc_ident)

    if group_key not in grouped_procedures:
        grouped_procedures[group_key] = []
    grouped_procedures[group_key].append(row)

final_transition_lines = {}

for (arpt_ident, proc_ident), rows_in_group in grouped_procedures.items():
    current_segment_coords = []
    # 为了获取精确的 FIX_IDENT，需要存储原始行数据
    current_segment_rows = []

    for row in rows_in_group:
        print(row)
        transition_ident = row[2] if row[2] is not None else ''
        fix_ident = row[3]
        lat = row[4]
        lon = row[5]

        if lat is None or lon is None:
            print(f"Warning: Skipping point for {arpt_ident}-{proc_ident}-{fix_ident} due to missing coordinates.")
            current_segment_coords = []
            current_segment_rows = []
            continue

        point_coord = (float(lon), float(lat))
        current_segment_coords.append(point_coord)
        current_segment_rows.append(row)

        if transition_ident:
            line_key = (arpt_ident, proc_ident, transition_ident)
            # 存储坐标列表、起始行和结束行
            final_transition_lines[line_key] = {
                'coords': list(current_segment_coords),
                'start_row': current_segment_rows[0],
                'end_row': row
            }

for line_key, line_data in final_transition_lines.items():
    arpt_ident, proc_ident, transition_ident = line_key
    coords = line_data['coords']
    start_row = line_data['start_row']
    end_row = line_data['end_row']

    if len(coords) >= 2:
        start_fix_ident = start_row[3]
        end_fix_ident = end_row[3]

        line_properties = {
            "ARPT_IDENT": arpt_ident,
            "PROC_IDENT": proc_ident,
            "TRANSITION_IDENT": transition_ident,
            "fix_ident_start": start_fix_ident,
            "fix_ident_end": end_fix_ident
        }

        line_geometry = geojson.LineString(coords)
        feature = geojson.Feature(geometry=line_geometry, properties=line_properties)
        all_line_features.append(feature)

featureCollection = geojson.FeatureCollection(all_line_features)

with open("sid.geojson", "w", encoding="utf-8") as f:
    geojson.dump(featureCollection, f,indent=4)