HAve data in column named full_lineage with lineage value delimitted by pipe symbol and within lineage table and column value delimitted by tilde symbol as below and the first level lineage is also present in column top_root

full_lineage
asasfaa~afafa|asssss~reqrrq|aadfaf~aagaga|asdasfa~ggwg
fagdggeg~h4h44h|ju6k66k~7ii7g4trh|rtrhh~r44h44h4h|oliree~2r3grgbrb
fagdggeg~h4h44h|ju6k66k~7ii7g4trh|asfasfa m2432432~wfwfw m3324234|rtrhh~r44h44h4h|wwfwrfw m131413~wfwfwef m31423|oliree~2r3grgbrb
fagdggeg~h4h44h|fafafd #m32432424~fasfsa #m33433|ju6k66k~7ii7g4trh|rtrhh~r44h44h4h|oliree~2r3grgbrb

top_root
asasfaa~afafa
fagdggeg~h4h44h
fagdggeg~h4h44h
fagdggeg~h4h44h

python code to parse each row split the value using lineage delimitter pipe symbol and store it in a list
and search for models with patter m character followed by more than 4 digits and store it in a model list for that particular record
if models are not present then write the lineage data as is and write the indices of the lineage in the list 1,2,3,..n
if only one model is present then
	write a lineage with first level and the subsequent lineage till the first model lineage delimitted by pipe and write the indices (based on 1) of the lineage used 
	write last model lineage and subsequent lineage till the last level lineage delimitted by pipe  and write the indices (based on 1) of the lineage used 
if more than one model is present then
	write a lineage with first level and the subsequent lineage till the first model lineage delimitted by pipe  and write the indices (based on 1) of the lineage used 
	write first model lineage and the last model lineage delimitted by pipe  and write the indices (based on 1) of the lineage used 
	write last model lineage and subsequent lineage till the last level lineage delimitted by pipe  and write the indices (based on 1) of the lineage used 
if more than two model is present continue the previous step accordinlgy



=======================================

Need to track the ou


given a digraph and a start node. use breadth first search and track visited nodes and don't visit a node more than once and if present store it in a different list named loop_list. 
Track the 



Have data in oracle table as source target pairs. When generating lineage paths using the data in python digraph the lineage paths is beyond 20Million 

what are the reasons for such explosion in lineage path 

Explain with simple illustration  with supporting figure, math and sample data in a powerpoint slide


>>>>>>>>>>>>.

Remove the last row from the table in the pdf and modify the amount 960.52 to 1307.67 and generatee new pdf similar to the original format with modified data


>>>>>>>>>>>>>>>>
Have a oracle table with graph data as source and target column. Write a oracle query to generate the lineage for a given source value or node to all its distinct target or leaf nodes


SELECT DISTINCT target AS leaf_node
FROM graph_data
START WITH source = :input_source -- Replace with your source value, e.g., 'A'
CONNECT BY NOCYCLE PRIOR target = source
WHERE target NOT IN (SELECT source FROM graph_data);

Get the shortest path for a given source column or node to all the leaf nodes or ultimate target columns reachable instead of all possible paths

WITH recursive_paths AS (
    SELECT source, target, source || ' -> ' || target AS path, 1 AS path_length
    FROM graph_data
    WHERE source = 'A'
    UNION ALL
    SELECT g.source, g.target, p.path || ' -> ' || g.target, p.path_length + 1
    FROM recursive_paths p
    JOIN graph_data g ON p.target = g.source
    WHERE p.path NOT LIKE '%' || g.target || '%'
),
leaf_nodes AS (
    SELECT target
    FROM graph_data
    WHERE target NOT IN (SELECT source FROM graph_data)
),
shortest_paths AS (
    SELECT r.target AS leaf_node, r.path, r.path_length,
           ROW_NUMBER() OVER (PARTITION BY r.target ORDER BY r.path_length) AS rn
    FROM recursive_paths r
    WHERE r.target IN (SELECT target FROM leaf_nodes)
)
SELECT leaf_node, path, path_length
FROM shortest_paths
WHERE rn = 1
ORDER BY leaf_node;

>>>>>>>>>>>>>>>>>>>>>>>>limit recursions to maximum depth of 39 in the ORacle CTE provided above

WITH recursive_paths AS (
    -- Anchor: Start from the given source node
    SELECT source, target, source || ' -> ' || target AS path, 1 AS path_length
    FROM graph_data
    WHERE source = :input_source -- Replace with your source value, e.g., 'A'
    UNION ALL
    -- Recursive: Extend paths by joining with graph_data, limit depth to 39
    SELECT g.source, g.target, p.path || ' -> ' || g.target, p.path_length + 1
    FROM recursive_paths p
    JOIN graph_data g ON p.target = g.source
    WHERE p.path NOT LIKE '%' || g.target || '%' -- Avoid cycles
    AND p.path_length < 39 -- Limit recursion depth
),
leaf_nodes AS (
    -- Identify leaf nodes (targets with no outgoing edges)
    SELECT target
    FROM graph_data
    WHERE target NOT IN (SELECT source FROM graph_data)
),
shortest_paths AS (
    -- Find shortest paths to each leaf node
    SELECT r.target AS leaf_node, r.path, r.path_length,
           ROW_NUMBER() OVER (PARTITION BY r.target ORDER BY r.path_length) AS rn
    FROM recursive_paths r
    WHERE r.target IN (SELECT target FROM leaf_nodes)
)
SELECT leaf_node, path, path_length
FROM shortest_paths
WHERE rn = 1
ORDER BY leaf_node;


>>>>>>>>>>>>>>>>>>>>>>>>>>>>>change the above query switching source and target columns to do reverse traversal
WITH recursive_paths AS (
    -- Anchor: Start from the given target node (reverse traversal)
    SELECT 
	, source, target || ' -> ' || source AS path, 1 AS path_length
    FROM graph_data
    WHERE target = :input_target -- Replace with your target value, e.g., 'E'
    UNION ALL
    -- Recursive: Extend paths by joining with graph_data, reverse direction
    SELECT g.target, g.source, p.path || ' -> ' || g.source, p.path_length + 1
    FROM recursive_paths p
    JOIN graph_data g ON p.source = g.target
    WHERE p.path NOT LIKE '%' || g.source || '%' -- Avoid cycles
    AND p.path_length < 39 -- Limit recursion depth
),
leaf_nodes AS (
    -- Identify reverse leaf nodes (sources with no outgoing edges in reversed graph)
    SELECT source
    FROM graph_data
    WHERE source NOT IN (SELECT target FROM graph_data)
),
shortest_paths AS (
    -- Find shortest paths to each reverse leaf node
    SELECT r.source AS leaf_node, r.path, r.path_length,
           ROW_NUMBER() OVER (PARTITION BY r.source ORDER BY r.path_length) AS rn
    FROM recursive_paths r
    WHERE r.source IN (SELECT source FROM leaf_nodes)
)
SELECT leaf_node, path, path_length
FROM shortest_paths
WHERE rn = 1
ORDER BY leaf_node;