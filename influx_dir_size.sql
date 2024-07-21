SELECT SUM(size) 
FROM your_measurement 
WHERE time >= now() - 1h 
  AND directory_path =~ /^(\/)?([^\/]*\/)*$/ 
GROUP BY directory_path

SELECT SUM(size) 
FROM your_measurement
WHERE directory_path =~ /^(\/)?([^\/]*\/)*$/
GROUP BY directory_path



SELECT SUM(size): Calculates the sum of the size field.
FROM your_measurement: Specifies the measurement to query. Replace your_measurement with the actual measurement name.
WHERE directory_path =~ /^(\/)?([^\/]*\/)*$/: Filters data to include only directory paths. The regular expression matches paths starting with an optional / followed by zero or more non-slash characters and a slash.
GROUP BY directory_path: Groups the results by the directory_path tag or field.


Additional Considerations:
Performance: For large datasets, consider using continuous queries or downsampling to optimize performance.
Data Retention Policy: Ensure your data retention policy covers the desired time range.
Data Modeling: If you're storing a large number of files, consider optimizing your data model by using tags for common directory prefixes to reduce cardinality.
Regular Expression Refinement: Adjust the regular expression in the WHERE clause based on your specific directory structure needs (e.g., to exclude certain directories).