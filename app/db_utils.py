import os
import json
from my_tools import TOOL_CLASSES
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSON as PostgresJSON
import logging

# Use environment variable DB_URL for Postgres
# Default to PostgreSQL connection
DEFAULT_DB_URL = 'postgresql://postgres:postgres@localhost:5432/crewai'
DB_URL = os.getenv('DB_URL', DEFAULT_DB_URL)
logging.debug(f"DB_URL:{DB_URL}")

# Create a SQLAlchemy Engine.
# For example, DB_URL could be:
#   "postgresql://username:password@hostname:5432/dbname"
engine = create_engine(DB_URL, echo=False)

def get_db_connection():
    """
    Return a context-managed connection from the SQLAlchemy engine.
    """
    return engine.connect()

def create_tables():
    """
    Create the necessary tables if they don't exist.
    Uses JSON type for the data field in PostgreSQL, or TEXT in other databases.
    """
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    if is_postgres:
        # PostgreSQL version with native JSON type
        create_sql = text('''
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                entity_type TEXT,
                data JSONB
            )
        ''')
    else:
        # Generic version for other databases
        create_sql = text('''
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                entity_type TEXT,
                data TEXT
            )
        ''')
    
    with get_db_connection() as conn:
        conn.execute(create_sql)
        conn.commit()

def initialize_db():
    """
    Initialize the database by creating tables if they do not exist.
    """
    create_tables()


def save_entity(entity_type, entity_id, data):
    """
    Save an entity to the database.
    Handles JSON serialization based on database type.
    """
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    upsert_sql = text('''
        INSERT INTO entities (id, entity_type, data)
        VALUES (:id, :etype, :data)
        ON CONFLICT(id) DO UPDATE
            SET entity_type = EXCLUDED.entity_type,
                data = EXCLUDED.data
    ''')
    
    with get_db_connection() as conn:
        # Always serialize to JSON string for consistency
        # This ensures compatibility with all database adapters
        json_data = json.dumps(data)
        
        conn.execute(
            upsert_sql,
            {
                "id": entity_id,
                "etype": entity_type,
                "data": json_data,
            }
        )
        conn.commit()

def load_entities(entity_type):
    """
    Load entities of a specific type from the database.
    Handles JSON deserialization based on database type.
    """
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    query = text('SELECT id, data FROM entities WHERE entity_type = :etype')
    with get_db_connection() as conn:
        result = conn.execute(query, {"etype": entity_type})
        # result.mappings() gives us rows as dicts (if using SQLAlchemy 1.4+)
        rows = result.mappings().all()
    
    # Process the data based on database type
    processed_rows = []
    for row in rows:
        # Since we're always storing serialized JSON strings now,
        # we always need to deserialize the data
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        processed_rows.append((row["id"], data))
    
    return processed_rows

def delete_entity(entity_type, entity_id):
    delete_sql = text('''
        DELETE FROM entities
        WHERE id = :id AND entity_type = :etype
    ''')
    with get_db_connection() as conn:
        conn.execute(delete_sql, {"id": entity_id, "etype": entity_type})
        conn.commit()

def save_tools_state(enabled_tools):
    data = {
        'enabled_tools': enabled_tools
    }
    save_entity('tools_state', 'enabled_tools', data)

def load_tools_state():
    rows = load_entities('tools_state')
    if rows:
        return rows[0][1].get('enabled_tools', {})
    return {}

def save_agent(agent):
    data = {
        'created_at': agent.created_at,
        'role': agent.role,
        'backstory': agent.backstory,
        'goal': agent.goal,
        'allow_delegation': agent.allow_delegation,
        'verbose': agent.verbose,
        'cache': agent.cache,
        'llm_provider_model': agent.llm_provider_model,
        'temperature': agent.temperature,
        'max_iter': agent.max_iter,
        'tool_ids': [tool.tool_id for tool in agent.tools]  # Save tool IDs
    }
    save_entity('agent', agent.id, data)

def load_agents():
    from my_agent import MyAgent
    rows = load_entities('agent')
    tools_dict = {tool.tool_id: tool for tool in load_tools()}
    agents = []
    for row in rows:
        data = row[1]
        tool_ids = data.pop('tool_ids', [])
        agent = MyAgent(id=row[0], **data)
        agent.tools = [tools_dict[tool_id] for tool_id in tool_ids if tool_id in tools_dict]
        agents.append(agent)
    return sorted(agents, key=lambda x: x.created_at)

def delete_agent(agent_id):
    delete_entity('agent', agent_id)

def save_task(task):
    data = {
        'description': task.description,
        'expected_output': task.expected_output,
        'async_execution': task.async_execution,
        'agent_id': task.agent.id if task.agent else None,
        'context_from_async_tasks_ids': task.context_from_async_tasks_ids,
        'context_from_sync_tasks_ids': task.context_from_sync_tasks_ids,
        'created_at': task.created_at
    }
    save_entity('task', task.id, data)

def load_tasks():
    from my_task import MyTask
    rows = load_entities('task')
    agents_dict = {agent.id: agent for agent in load_agents()}
    tasks = []
    for row in rows:
        data = row[1]
        agent_id = data.pop('agent_id', None)
        task = MyTask(id=row[0], agent=agents_dict.get(agent_id), **data)
        tasks.append(task)
    return sorted(tasks, key=lambda x: x.created_at)

def delete_task(task_id):
    delete_entity('task', task_id)

def save_crew(crew):
    data = {
        'name': crew.name,
        'process': crew.process,
        'verbose': crew.verbose,
        'agent_ids': [agent.id for agent in crew.agents],
        'task_ids': [task.id for task in crew.tasks],
        'memory': crew.memory,
        'cache': crew.cache,
        'planning': crew.planning,
        'max_rpm' : crew.max_rpm,
        'manager_llm': crew.manager_llm,
        'manager_agent_id': crew.manager_agent.id if crew.manager_agent else None,
        'created_at': crew.created_at
    }
    save_entity('crew', crew.id, data)

def load_crews():
    from my_crew import MyCrew
    rows = load_entities('crew')
    agents_dict = {agent.id: agent for agent in load_agents()}
    tasks_dict = {task.id: task for task in load_tasks()}
    crews = []
    for row in rows:
        data = row[1]
        crew = MyCrew(
            id=row[0], 
            name=data['name'], 
            process=data['process'], 
            verbose=data['verbose'], 
            created_at=data['created_at'], 
            memory=data.get('memory'),
            cache=data.get('cache'),
            planning=data.get('planning'),
            max_rpm=data.get('max_rpm'), 
            manager_llm=data.get('manager_llm'),
            manager_agent=agents_dict.get(data.get('manager_agent_id'))
        )
        crew.agents = [agents_dict[agent_id] for agent_id in data['agent_ids'] if agent_id in agents_dict]
        crew.tasks = [tasks_dict[task_id] for task_id in data['task_ids'] if task_id in tasks_dict]
        crews.append(crew)
    return sorted(crews, key=lambda x: x.created_at)

def delete_crew(crew_id):
    delete_entity('crew', crew_id)

def save_tool(tool):
    data = {
        'name': tool.name,
        'description': tool.description,
        'parameters': tool.get_parameters()
    }
    save_entity('tool', tool.tool_id, data)

def load_tools():
    rows = load_entities('tool')
    tools = []
    for row in rows:
        data = row[1]
        tool_class = TOOL_CLASSES[data['name']]
        tool = tool_class(tool_id=row[0])
        tool.set_parameters(**data['parameters'])
        tools.append(tool)
    return tools

def delete_tool(tool_id):
    delete_entity('tool', tool_id)

def export_to_json(file_path):
    """
    Export all entities to a JSON file.
    Handles JSON deserialization based on database type.
    """
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    with get_db_connection() as conn:
        # Use SQLAlchemy's text() for raw SQL
        query = text('SELECT * FROM entities')
        result = conn.execute(query)
        
        # Convert to list of dictionaries
        rows = []
        for row in result:
            # For PostgreSQL with JSONB, the data might already be deserialized
            # For other databases, we need to deserialize from JSON string
            data = row.data
            if not is_postgres or isinstance(data, str):
                data = json.loads(data)
                
            rows.append({
                'id': row.id,
                'entity_type': row.entity_type,
                'data': data
            })

        # Write to file
        with open(file_path, 'w') as f:
            json.dump(rows, f, indent=4)

def import_from_json(file_path):
    """
    Import entities from a JSON file.
    Handles JSON serialization based on database type.
    """
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    with open(file_path, 'r') as f:
        data = json.load(f)

    with get_db_connection() as conn:
        for entity in data:
            # Use SQLAlchemy's text() for raw SQL with parameters
            upsert_sql = text('''
                INSERT INTO entities (id, entity_type, data)
                VALUES (:id, :etype, :data)
                ON CONFLICT(id) DO UPDATE
                    SET entity_type = EXCLUDED.entity_type,
                        data = EXCLUDED.data
            ''')
            
            # For PostgreSQL with JSONB, we can pass the data directly
            # For other databases, we need to serialize to JSON string
            json_data = entity['data'] if is_postgres else json.dumps(entity['data'])
            
            conn.execute(
                upsert_sql,
                {
                    "id": entity['id'],
                    "etype": entity['entity_type'],
                    "data": json_data
                }
            )
            
        conn.commit()
        
def save_result(result):
    """Save a result to the database."""
    data = {
        'crew_id': result.crew_id,
        'crew_name': result.crew_name,
        'inputs': result.inputs,
        'result': result.result,
        'created_at': result.created_at
    }
    save_entity('result', result.id, data)

def load_results():
    """Load all results from the database."""
    from result import Result
    rows = load_entities('result')
    results = []
    for row in rows:
        data = row[1]
        result = Result(
            id=row[0],
            crew_id=data['crew_id'],
            crew_name=data['crew_name'],
            inputs=data['inputs'],
            result=data['result'],
            created_at=data['created_at']
        )
        results.append(result)
    return sorted(results, key=lambda x: x.created_at, reverse=True)

def delete_result(result_id):
    """Delete a result from the database."""
    delete_entity('result', result_id)