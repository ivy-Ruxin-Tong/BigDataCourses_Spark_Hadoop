-- Databricks notebook source
-- MAGIC %md
-- MAGIC lists of window functions
-- MAGIC     # lag/lead -good
-- MAGIC     # first_value/last_value [frame unit] -this notebook
-- MAGIC     # dense_rank, rank, row_number - good
-- MAGIC     # ntile, nth_value[frame_unit], percent_rank -this notebook
-- MAGIC     # cume_dist (approx formula : ROW_NUMBER() / total_rows) - this notebook
-- MAGIC     
-- MAGIC Default frame unit it range between unbounded preceding and current row
-- MAGIC 
-- MAGIC Note: rows between XXX
-- MAGIC 
-- MAGIC Disclaimer: All codes are from here https://www.mysqltutorial.org/mysql-window-functions/. This notebook is created for the purpose of self-learning. This is easy for me to review when i want to refresh my understanding of window functions.  This website is amazing for learning mysql. Check it out!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![test](https://www.mysqltutorial.org/wp-content/uploads/2018/09/mysql-window-functions-frame-clause-bound.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### First/Last_Values

-- COMMAND ----------

CREATE TABLE overtime (
    employee_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    hours INT NOT NULL
);
INSERT INTO overtime(employee_name, department, hours)
VALUES('Diane Murphy','Accounting',37),
('Mary Patterson','Accounting',74),
('Jeff Firrelli','Accounting',40),
('William Patterson','Finance',58),
('Gerard Bondur','Finance',47),
('Anthony Bow','Finance',66),
('Leslie Jennings','IT',90),
('Leslie Thompson','IT',88),
('Julie Firrelli','Sales',81),
('Steve Patterson','Sales',29),
('Foon Yue Tseng','Sales',65),
('George Vanauf','Marketing',89),
('Loui Bondur','Marketing',49),
('Gerard Hernandez','Marketing',66),
('Pamela Castillo','SCM',96),
('Larry Bott','SCM',100),
('Barry Jones','SCM',65);

-- COMMAND ----------

select * from overtime

-- COMMAND ----------

SELECT
    employee_name,
    department,
    hours,
    LAST_VALUE(employee_name) OVER (
		PARTITION BY department
        ORDER BY hours
	) most_overtime_employee
FROM
    overtime;
    
--  default RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, 
-- It means that the frame starts at the first row and ends at the current row of the result set.

-- COMMAND ----------

SELECT
    employee_name,
    hours,
    LAST_VALUE(employee_name) OVER (
        ORDER BY hours
        RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING
    ) highest_overtime_employee
FROM
    overtime;
    
-- This indicates that the frame starts at the first row and ends at the last row of the result set.

-- COMMAND ----------

select employee_name,
hours, 
Last_value(employee_name) over(
partition by department
order by hours
range between unbounded preceding
and unbounded following) as highest_overtime_employee_dept
from 
overtime;

-- COMMAND ----------

--  first value does not need to specifiy/change range

select employee_name, 
hours, 
first_value(employee_name) over(
order by hours) as loswest_overtime_employee
from overtime;

-- COMMAND ----------

select employee_name, 
hours, 
first_value(employee_name) over(
partition by department
order by hours) as loswest_overtime_employee
from overtime;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cum_Dist

-- COMMAND ----------

CREATE TABLE scores (
    name VARCHAR(20),
    score INT NOT NULL
);

INSERT INTO
	scores(name, score)
VALUES
	('Smith',81),
	('Jones',55),
	('Williams',55),
	('Taylor',62),
	('Brown',62),
	('Davies',84),
	('Evans',87),
	('Wilson',72),
	('Thomas',72),
	('Johnson',100);

-- COMMAND ----------

select * from scores

-- COMMAND ----------

select *, 
row_number() over(order by score) as row_nb,
cume_dist() over(order by score) as cum_dist
from scores

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Percent_Rank((rank - 1) / (total_rows - 1))
-- MAGIC     # always return 0 for the first value
-- MAGIC     # repeated values will always receive the same percent_rank

-- COMMAND ----------

--  we can use this window funciton to rank different options or lines. 0.5 means that option is better than 50% of the groups, 1 is the best

select *, 
round(percent_rank() over(order by score),2) as percent_rank
from scores

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### NTILE(n) 
-- MAGIC     # OVER (PARTITION BY <expression>[{,<expression>...}] ORDER BY <expression> [ASC|DESC], [{,<expression>...}])
-- MAGIC     # if dividiable by n , then n parts, if not, "result in groups of two sizes with the difference by one. The larger groups always come before the smaller group in the order specified by the ORDER BY clause."
-- MAGIC     # NTILE() function to distribute rows into a specified number of groups

-- COMMAND ----------

CREATE TABLE t (
    val INT NOT NULL
);

INSERT INTO t(val) 
VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9);


SELECT * FROM t;

-- COMMAND ----------

select *, ntile(4) over(order by val) as tile
from t

--  9/4 is not divisible, = 2.5, so 3, 2, 2, 2 (larger comes first)

-- COMMAND ----------

select *, ntile(3) over(order by val) as tile
from t

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### The NTH_VALUE() 
-- MAGIC     # is a window function that allows you to get a value from the Nth row in an ordered set of rows.
-- MAGIC     # NTH_VALUE(expression, N)
-- MAGIC       FROM FIRST
-- MAGIC       OVER (
-- MAGIC         partition_clause
-- MAGIC         order_clause
-- MAGIC         frame_clause
-- MAGIC     )
-- MAGIC     # if does not exist, return null

-- COMMAND ----------

CREATE TABLE basic_pays(
    employee_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary INT NOT NULL
);

INSERT INTO 
	basic_pays(employee_name, 
			   department, 
			   salary)
VALUES
	('Diane Murphy','Accounting',8435),
	('Mary Patterson','Accounting',9998),
	('Jeff Firrelli','Accounting',8992),
	('William Patterson','Accounting',8870),
	('Gerard Bondur','Accounting',11472),
	('Anthony Bow','Accounting',6627),
	('Leslie Jennings','IT',8113),
	('Leslie Thompson','IT',5186),
	('Julie Firrelli','Sales',9181),
	('Steve Patterson','Sales',9441),
	('Foon Yue Tseng','Sales',6660),
	('George Vanauf','Sales',10563),
	('Loui Bondur','SCM',10449),
	('Gerard Hernandez','SCM',6949),
	('Pamela Castillo','SCM',11303),
	('Larry Bott','SCM',11798),
	('Barry Jones','SCM',10586);

-- COMMAND ----------

SELECT *,
    NTH_VALUE(employee_name, 2) OVER  (
        ORDER BY salary DESC
    ) second_highest_salary
FROM
    basic_pays;

-- COMMAND ----------

SELECT
   *,
    NTH_VALUE(employee_name, 2) OVER  (
        partition by department
        ORDER BY salary DESC
    ) second_highest_salary
FROM
    basic_pays;
    
--      first value returns null

-- COMMAND ----------

--  using this, specify frame unit, not null
SELECT *,
	NTH_VALUE(employee_name, 2) OVER  (
		PARTITION BY department
		ORDER BY salary DESC
		RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	) second_highest_salary
FROM
	basic_pays;

-- COMMAND ----------


