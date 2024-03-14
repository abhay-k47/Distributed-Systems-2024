from flask import Flask, request, jsonify, make_response
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

def create_connection():
    """Create a database connection to the MySQL server."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',  # Or your host, if different
            user='root',  # Your MySQL username
            password='Chadwick@12',  # Your MySQL password
            database='test_db'  # Your database name
        )
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query, values=None, select=False):
    """Execute the given SQL query on the provided connection."""
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query, values)
        if select:
            result = cursor.fetchall()
            return result, cursor.rowcount
        else:
            connection.commit()
            return None, cursor.rowcount  # Now returns the number of affected rows for non-SELECT queries
    except Error as e:
        print(f"The error '{e}' occurred")
        return None, 0  # Returns None and 0 if an error occurred
    finally:
        cursor.close()




@app.route('/config', methods=['POST'])
def configure_shards():
    data = request.json
    schema = data['schema']
    shards = data['shards']
    connection = create_connection()

    for shard in shards:
        # Construct the CREATE TABLE SQL query based on the schema and shard name
        table_name = f"shard_{shard}"
        columns_with_types = ', '.join([f"{col} {dtype}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});"

        execute_query(connection, create_table_query)

    if connection:
        connection.close()

    return jsonify({"message": f"Server0:{', '.join(shards)} configured", "status": "success"}), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # Create an empty response
    response = make_response('', 200)
    return response

@app.route('/copy', methods=['GET'])
def copy_shard_data():
    data = request.json
    if not data or 'shards' not in data:
        return jsonify({"error": "Invalid request"}), 400  # Or another appropriate response

    shards = data['shards']
    # Continue as before

    connection = create_connection()
    if not connection:
        return jsonify({"error": "Database connection failed"}), 500
    
    response_data = {}
    for shard in shards:
        table_name = f"shard_{shard}"
        select_query = f"SELECT * FROM {table_name};"
        results = execute_query(connection, select_query, select=True)
        response_data[shard] = results if results else []

    connection.close()
    return jsonify({"data": response_data, "status": "success"}), 200

@app.route('/read', methods=['POST'])
def read_data():
    data = request.json
    shard = data['shard']
    stud_id_range = data['Stud_id']
    low_id, high_id = stud_id_range['low'], stud_id_range['high']
    
    connection = create_connection()
    if not connection:
        return jsonify({"error": "Database connection failed"}), 500

    table_name = f"shard_{shard}"
    select_query = f"""
    SELECT * FROM {table_name} 
    WHERE Stud_id BETWEEN {low_id} AND {high_id};
    """
    
    results = execute_query(connection, select_query, select=True)
    connection.close()
    
    if results is None:
        return jsonify({"error": "Query execution failed"}), 500

    return jsonify({"data": results, "status": "success"}), 200

@app.route('/write', methods=['POST'])
def write_data():
    data = request.json
    shard = data['shard']
    curr_idx = data['curr_idx']
    entries = data['data']
    
    connection = create_connection()
    if not connection:
        return jsonify({"error": "Database connection failed"}), 500

    total_inserted = 0
    for entry in entries:
        insert_query = f"""
        INSERT INTO shard_{shard} (Stud_id, Stud_name, Stud_marks) 
        VALUES (%s, %s, %s);
        """
        _, affected_rows = execute_query(connection, insert_query, (entry['Stud_id'], entry['Stud_name'], entry['Stud_marks']))
        total_inserted += affected_rows

    connection.close()
    new_idx = curr_idx + total_inserted
    return jsonify({"message": "Data entries added", "current_idx": new_idx, "status": "success"}), 200

# Example usage in the update_data function
@app.route('/update', methods=['PUT'])
def update_data():
    # Similar setup as before
    data = request.json
    shard = data['shard']
    stud_id = data['Stud_id']
    entry_data = data['data']
    connection = create_connection()
    if not connection:
        return jsonify({"error": "Database connection failed"}), 500

    table_name = f"shard_{shard}"
    update_query = f"""
    UPDATE {table_name} 
    SET Stud_name = %s, Stud_marks = %s
    WHERE Stud_id = %s;
    """
    _, affected_rows = execute_query(connection, update_query, (entry_data['Stud_name'], entry_data['Stud_marks'], stud_id))

    connection.close()
    if affected_rows == 0:
        return jsonify({"message": f"No data entry found for Stud_id:{stud_id}", "status": "failed"}), 404
    return jsonify({"message": f"Data entry for Stud_id:{stud_id} updated", "status": "success"}), 200

@app.route('/del', methods=['DELETE'])
def delete_data():
    data = request.json
    shard = data['shard']
    stud_id = data['Stud_id']
    
    connection = create_connection()
    if not connection:
        return jsonify({"error": "Database connection failed"}), 500

    delete_query = f"DELETE FROM shard_{shard} WHERE Stud_id = %s;"
    _, affected_rows = execute_query(connection, delete_query, (stud_id,))
    connection.close()

    if affected_rows == 0:
        return jsonify({"message": f"No data entry found for Stud_id:{stud_id}", "status": "failed"}), 404
    return jsonify({"message": f"Data entry with Stud_id:{stud_id} removed", "status": "success"}), 200



if __name__ == '__main__':
    app.run(debug=True)


