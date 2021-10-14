# Amazon latest books
ETL pipeline example processing Amazon latest books releases data in France.

## Architecture overview
![amazon-latest-books-schema drawio](https://user-images.githubusercontent.com/38778970/137316506-f30d840f-c47e-4a6d-84c4-16a77b7bbf5e.png)

#### Extraction
Data is firstly scraped from Amazon web page and loaded into a first S3 bucket as raw data.

#### Transformation
Data is pulled from the S3 bucket and transformed by cleaning operations (types changed for price and ratings, rounded numbers..), and then put into another S3 bucket as cleaned data.

#### Loading into database
After being pulled from the cleaned S3 bucket, data is loaded into a database isolated in a Docker container

#### Data visualization
A dashboard built with [Streamlit](https://streamlit.io/) displays insights about Amazon latest books releases by querying the database.
