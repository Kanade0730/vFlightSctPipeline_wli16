import geojson
import sqlite3
from collections import defaultdict

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
        WHERE (A.ARPT_IDENT IN('ZBAA')AND A.SUBS_CODE IN ('D')AND A.FIX_IDENT NOT IN ('')) 
        ORDER BY A.ARPT_IDENT, A.PROC_IDENT;
    ''')

airport_procedure_temp = cursor.fetchall()
airport_procedure = airport_procedure_temp[::-1]

all_line_features = []
grouped_procedures = {}

grouped_procedures = defaultdict(list)
# First pass: Group all rows by (ARPT_IDENT, PROC_IDENT)
for row in airport_procedure:
    print(row)
    arpt_ident = row[0]
    proc_ident = row[1]
    group_key = (arpt_ident, proc_ident)
    grouped_procedures[group_key].append(row)

all_line_features = []

for (arpt_ident, proc_ident), rows_in_group in grouped_procedures.items():
    # Separate common points and transition-specific points
    common_points = []
    transition_specific_points = defaultdict(list)  # Key: transition_ident, Value: list of (coord, row) tuples

    for row in rows_in_group:
        transition_ident = row[2] if row[2] is not None else ''
        lat = row[4]
        lon = row[5]

        if lat is None or lon is None:
            print(f"Warning: Skipping point for {arpt_ident}-{proc_ident}-{row[3]} due to missing coordinates.")
            continue

        point_coord = (float(lon), float(lat))  # GeoJSON is [longitude, latitude]

        if transition_ident == '':
            common_points.append((point_coord, row))  # Store coordinate and original row
        else:
            transition_specific_points[transition_ident].append((point_coord, row))

    # Now, generate LineString features for each transition
    for transition_ident, points_with_rows in transition_specific_points.items():
        # A transition's line starts with common points, then its specific points
        # Use a set to track seen coordinates to avoid adding duplicates if common points
        # are also explicitly part of a transition's points and order is preserved.
        # However, for LineString, usually direct concatenation is desired if order is key.
        # For simplicity, we just concatenate. If exact point duplication must be removed,
        # you'd need a more robust de-duplication that preserves order.

        # Collect coordinates for the current line
        current_line_coords = []
        current_line_rows = []

        # Add common points first
        for coord, row_data in common_points:
            current_line_coords.append(coord)
            current_line_rows.append(row_data)

        # Then add specific transition points
        for coord, row_data in points_with_rows:
            current_line_coords.append(coord)
            current_line_rows.append(row_data)

        # Ensure we have at least two points to form a line
        if len(current_line_coords) >= 2:
            start_row = current_line_rows[0]
            end_row = current_line_rows[-1]

            # Extract FIX_IDENTs for properties
            start_fix_ident = start_row[3]
            end_fix_ident = end_row[3]

            line_properties = {
                "ARPT_IDENT": arpt_ident,
                "PROC_IDENT": proc_ident,
                "TRANSITION_IDENT": transition_ident,
                # It seems you wanted start_fix_ident to be the end_fix_ident,
                # and end_fix_ident to be the start_fix_ident based on your original code's mapping.
                # Let's align with that, or reverse if it was a typo:
                "start_fix_ident": start_fix_ident,  # The FIX_IDENT of the first point in this segment
                "end_fix_ident": end_fix_ident  # The FIX_IDENT of the last point in this segment
            }

            line_geometry = geojson.LineString(current_line_coords)
            feature = geojson.Feature(geometry=line_geometry, properties=line_properties)
            all_line_features.append(feature)

featureCollection = geojson.FeatureCollection(all_line_features)

# Print to console (optional)
# print(geojson.dumps(featureCollection, indent=4))

# Save to file
output_filename = "departure.geojson"
with open(output_filename, "w", encoding="utf-8") as f:
    geojson.dump(featureCollection, f, indent=4)

print(f"GeoJSON output saved to {output_filename}")