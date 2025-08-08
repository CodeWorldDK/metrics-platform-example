# metrics-platform-example

from graphviz import Digraph

# Create sequence diagram using Graphviz (will resemble sequence flow)
diagram = Digraph("Liquidity_Metric_DAG_Sequence", format="png")

# Define nodes
diagram.node("User", "User / Trigger")
diagram.node("Airflow", "Airflow DAG")
diagram.node("DCatalog", "Datacatalog Service")
diagram.node("ECS", "ECS Fargate Task")
diagram.node("S3", "AWS S3 Storage")

# Create edges for sequence
diagram.edge("User", "Airflow", "Trigger DAG with Execution Context")
diagram.edge("Airflow", "DCatalog", "Fetch input/output S3 paths")
diagram.edge("Airflow", "ECS", "Submit Fargate task")
diagram.edge("ECS", "S3", "Read input dataset")
diagram.edge("ECS", "ECS", "Transform data")
diagram.edge("ECS", "S3", "Write output dataset")
diagram.edge("ECS", "DCatalog", "Update output path metadata")
diagram.edge("Airflow", "ECS", "Trigger next dependent node(s)")

# Save diagram
output_path = '/mnt/data/liquidity_metric_dag_sequence'
diagram.render(output_path, cleanup=True)

output_path + ".png"
