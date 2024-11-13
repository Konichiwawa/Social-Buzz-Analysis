-- Data Cleaning Steps:
-- 1). Remove duplicates
-- 2). Drop unwanted columns
-- 3). Fix column names and data types
-- 4). Remove rows with missing data
-- 5). Standardize 
-- 6). Join Tables
-- 7). Analyze

-- 1). Remove duplicates

-- CONTENT 
SELECT * FROM content; -- 1000 rows total

-- Get a list of column names
SELECT GROUP_CONCAT(column_name SEPARATOR ', ')
FROM INFORMATION_SCHEMa.COLUMNS
WHERE table_name = 'content' AND table_schema = 'social_buzz'; -- Copy and paste the list in the next CTE statement

-- Find duplicate rows
WITH duplicate_content AS (
	SELECT `Content ID`, `User ID`, `Type`, Category, URL, ROW_NUMBER() OVER(PARTITION BY `Content ID`, `User ID`, `Type`, Category, URL) AS row_num
	FROM content
)
SELECT * FROM duplicate_content WHERE row_num > 1; -- There is no duplicate rows in Content table. 


-- REACTIONS
SELECT * FROM reactions; -- 25553 rows total

SELECT GROUP_CONCAT(column_name SEPARATOR ', ')
FROM INFORMATION_SCHEMa.COLUMNS
WHERE table_name = 'reactions' AND table_schema = 'social_buzz';

-- Find duplicate rows
WITH duplicate_reactions AS (
	SELECT `Content ID`, `User ID`, `Datetime`, ROW_NUMBER() OVER(PARTITION BY `Content ID`, `User ID`, `Datetime`) AS row_num
	FROM reactions
)
SELECT * FROM duplicate_reactions WHERE row_num > 1; -- There is no duplicate rows in Reactions table. 


-- REACTION_TYPES
SELECT * FROM reaction_types; -- 16 rows total

SELECT GROUP_CONCAT(column_name SEPARATOR ', ')
FROM INFORMATION_SCHEMa.COLUMNS
WHERE table_name = 'reaction_types' AND table_schema = 'social_buzz';

-- Find duplicate rows
WITH duplicate_reaction_types AS (
	SELECT `Type`, Sentiment, Score, ROW_NUMBER() OVER(PARTITION BY `Type`, Sentiment, Score) AS row_num
	FROM reaction_types
)
SELECT * FROM duplicate_reaction_types WHERE row_num > 1; -- There is no duplicate rows in Reaction_Types table. 


-- 2). Drop unwanted columns
--  Client Request: An analysis of their content categories that highlights the top 5 categories with the largest aggregate popularity  

-- CONTENT 
-- Keep Content ID, Type, and Category
SELECT * FROM content;

ALTER TABLE content 
DROP COLUMN `User ID`,
DROP COLUMN URL;


-- REACTIONS
-- Keep Content ID, Type, and Datetime
SELECT * FROM reactions;

ALTER TABLE reactions 
DROP COLUMN `User ID`;


-- REACITON_TYPES
-- Everything is ketp so no columns are dropped


-- 3). Fix column names and data types

-- CONTENT
DESCRIBE content; 

-- Renaming columns; No data type conversion 
ALTER TABLE content 
RENAME COLUMN `Content ID` TO content_id,
RENAME COLUMN `Type` TO content_type,
RENAME COLUMN Category TO content_category;

SELECT * FROM content;


-- REACTIONS
DESCRIBE reactions;

-- Renaming columns; datatime conversion
ALTER TABLE reactions 
RENAME COLUMN `Content ID` TO content_id,
RENAME COLUMN `Type` TO content_type,
RENAME COLUMN `Datetime` TO dates;

ALTER TABLE reactions
MODIFY COLUMN dates DATETIME;

SELECT * FROM reactions;

-- REACTION_TYPES
DESCRIBE reaction_types;

ALTER TABLE reaction_types
RENAME COLUMN `Type` TO content_type,
RENAME COLUMN Sentiment TO sentiment,
RENAME COLUMN Score TO score;

SELECT * FROM reaction_types;


-- 4). Remove rows with missing data
-- CONTENT
SELECT * FROM content
WHERE (content_id IS NULL OR content_id = '') OR (content_type IS NULL OR content_type = '') OR (content_category IS NULL OR content_category = ''); -- No nulls or blanks in Content table


-- REACTIONS
SELECT * FROM reactions
WHERE (content_id IS NULL OR content_id = '') OR (content_type IS NULL OR content_type = '') OR (dates IS NULL); -- Missing content_types in 980 rows

DELETE FROM reactions
WHERE (content_id IS NULL OR content_id = '') OR (content_type IS NULL OR content_type = '') OR (dates IS NULL);


-- REACTION_TYPES
SELECT * FROM reaction_types
WHERE (content_type IS NULL OR content_type = '') OR (sentiment IS NULL OR sentiment = '') OR (score IS NULL); -- No nulls or blanks in Reaction_Type table


-- 5). Standardize 

-- CONTENT
SELECT DISTINCT content_type FROM content ORDER BY content_type; -- everything looks good
SELECT DISTINCT content_category FROM content ORDER BY content_category; -- needs cleaning

-- Remove the quotes from the texts in content_category column
SELECT content_category FROM content WHERE content_category LIKE '%"%' ORDER BY content_category;

UPDATE content 
SET content_category = REPLACE(content_category, '"', ''); -- 17 rows affected

-- REACTIONS
SELECT DISTINCT content_type FROM reactions ORDER BY content_type; -- everything looks good


-- REACTION_TYPES
SELECT DISTINCT content_type FROM reaction_types ORDER BY content_type; -- everything looks good
SELECT DISTINCT sentiment FROM reaction_types ORDER BY sentiment; -- everything looks good

-- 6). Join Tables
CREATE TABLE final_set AS
SELECT content.content_type, content_category, DATE(dates), sentiment, score
FROM content
	JOIN reactions ON content.content_id = reactions.content_id
	JOIN reaction_types ON reactions.content_type = reaction_types.content_type
; -- 24573 rows returned 


-- 7). Client Request: An analysis of their content categories that highlights the top 5 categories with the largest aggregate popularity  
SELECT RANK() OVER(ORDER BY SUM(score) DESC) AS ranking, content_category, SUM(score) AS aggregate_score
FROM final_set
GROUP BY content_category
ORDER BY ranking;

-- RESULT:
-- category | score
-- 1	Animals	74965
-- 2	science	71168
-- 3	healthy eating	69339
-- 4	technology	68738
-- 5	food	66676

SELECT * FROM final_set;