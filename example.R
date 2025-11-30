library(sparklyr)
library(tidyverse)

# -------------------------------------------------------------------
# Quickstart: Connect to Spark (IOMETE), run dplyr + SQL examples
# -------------------------------------------------------------------

# Spark connection URL from IOMETE
connection_string <- "sc://..."

# Connect to the Spark cluster
sc <- spark_connect(
  master  = connection_string,
  method  = "spark_connect",
  version = "3.5.5"
)

# -------------------------------------------------------------------
# Example 1: Use a local R data frame (mtcars) in Spark
# -------------------------------------------------------------------

# Upload data frame to Spark as a temporary Spark table
tbl_mtcars <- copy_to(sc, mtcars)

# Run dplyr operations directly on Spark
tbl_mtcars |>
  group_by(am) |>
  summarise(mpg = mean(mpg, na.rm = TRUE))

# -------------------------------------------------------------------
# Example 2: Use an existing Spark table
# -------------------------------------------------------------------

employee <- tbl(sc, "employee")

employee |> 
  filter(salary > 1000) |> 
  count()

# -------------------------------------------------------------------
# Example 3: Use DBI for raw SQL queries
# -------------------------------------------------------------------

# List available tables (with schema info)
DBI::dbGetQuery(sc, "SHOW TABLES")

# Run a SQL query
DBI::dbGetQuery(sc, "
  SELECT COUNT(*) 
  FROM employee 
  WHERE salary > 1000
")