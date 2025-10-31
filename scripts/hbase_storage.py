import happybase
import yaml

def load_config():
    with open('/root/config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def create_table():
    config = load_config()
    connection = happybase.Connection(config['hbase']['host'])
    
    table_name = 'pollution_metrics'
    
    # Delete table if it exists
    if table_name.encode() in connection.tables():
        connection.delete_table(table_name, disable=True)
    
    # Create new table
    connection.create_table(
        table_name,
        {
            'daily_metrics': dict(),
            'sensor_data': dict()
        }
    )
    
    return connection.table(table_name)

def store_metrics(table, date, metrics):
    row_key = date.encode('utf-8')
    table.put(row_key, {
        b'daily_metrics:co': str(metrics['co']).encode('utf-8'),
        b'daily_metrics:nox': str(metrics['nox']).encode('utf-8'),
        b'daily_metrics:no2': str(metrics['no2']).encode('utf-8')
    })
